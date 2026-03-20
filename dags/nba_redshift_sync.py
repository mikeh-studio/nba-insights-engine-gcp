"""Cross-cloud sync module: BigQuery -> GCS -> S3 -> Redshift.

This module is only used when ENABLE_REDSHIFT=true. It has no impact
on the primary BigQuery pipeline.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager

logger = logging.getLogger("nba_redshift_sync")


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def _get_env(key: str, default: str | None = None) -> str:
    """Read an environment variable with optional default."""
    val = os.getenv(key, default)
    if val is None:
        raise ValueError(f"Required env var {key} is not set")
    return val


@contextmanager
def get_redshift_connection():
    """Yield a psycopg2 connection to Redshift Serverless."""
    import psycopg2

    conn = psycopg2.connect(
        host=_get_env("REDSHIFT_HOST"),
        port=int(_get_env("REDSHIFT_PORT", "5439")),
        dbname=_get_env("REDSHIFT_DB", "nba_analytics"),
        user=_get_env("REDSHIFT_USER"),
        password=_get_env("REDSHIFT_PASSWORD"),
    )
    try:
        yield conn
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# BQ -> GCS export
# ---------------------------------------------------------------------------

def export_bq_to_gcs_parquet(
    project_id: str,
    dataset: str,
    table: str,
    gcs_bucket: str,
    gcs_prefix: str,
) -> str:
    """Export a BigQuery table to GCS as Parquet. Returns the GCS URI prefix."""
    from google.cloud import bigquery as bq

    client = bq.Client(project=project_id)
    destination_uri = f"gs://{gcs_bucket}/{gcs_prefix}/{table}/*.parquet"
    source_ref = f"{project_id}.{dataset}.{table}"

    job_config = bq.ExtractJobConfig(
        destination_format=bq.DestinationFormat.PARQUET,
    )
    extract_job = client.extract_table(source_ref, destination_uri, job_config=job_config)
    extract_job.result()  # wait for completion
    logger.info("Exported %s to %s", source_ref, destination_uri)
    return f"gs://{gcs_bucket}/{gcs_prefix}/{table}/"


# ---------------------------------------------------------------------------
# GCS -> S3 copy
# ---------------------------------------------------------------------------

def copy_gcs_to_s3(
    gcs_bucket: str,
    gcs_prefix: str,
    s3_bucket: str,
    s3_prefix: str,
) -> int:
    """Stream objects from a GCS prefix to S3. Returns the count of files copied."""
    import boto3
    from google.cloud import storage as gcs

    gcs_client = gcs.Client()
    s3_client = boto3.client("s3")

    blobs = list(gcs_client.list_blobs(gcs_bucket, prefix=gcs_prefix))
    copied = 0
    for blob in blobs:
        if blob.name.endswith("/"):
            continue
        relative = blob.name[len(gcs_prefix):].lstrip("/")
        s3_key = f"{s3_prefix}/{relative}"
        content = blob.download_as_bytes()
        s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=content)
        copied += 1
        logger.info("Copied gs://%s/%s -> s3://%s/%s", gcs_bucket, blob.name, s3_bucket, s3_key)

    logger.info("Copied %d files from GCS to S3", copied)
    return copied


# ---------------------------------------------------------------------------
# Redshift DDL
# ---------------------------------------------------------------------------

_SCHEMAS = ["nba_bronze", "nba_silver", "nba_gold"]

_RAW_GAME_LOGS_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.raw_game_logs (
    game_date       DATE,
    matchup         VARCHAR(50),
    wl              VARCHAR(2),
    min             DOUBLE PRECISION,
    pts             BIGINT,
    reb             BIGINT,
    ast             BIGINT,
    stl             BIGINT,
    blk             BIGINT,
    tov             BIGINT,
    season          VARCHAR(10),
    ingested_at_utc TIMESTAMP,
    player_id       BIGINT,
    player_name     VARCHAR(200)
)
DISTSTYLE KEY
DISTKEY (player_id)
SORTKEY (game_date, player_id);
"""

_RAW_SCHEDULE_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.raw_schedule (
    schedule_date   DATE,
    team_abbr       VARCHAR(10),
    opponent_abbr   VARCHAR(10),
    home_away       VARCHAR(10),
    season          VARCHAR(10),
    ingested_at_utc TIMESTAMP
)
DISTSTYLE EVEN
SORTKEY (schedule_date, team_abbr);
"""


def create_redshift_schemas_and_tables():
    """Create bronze/silver/gold schemas and base tables in Redshift."""
    bronze_schema = _get_env("REDSHIFT_SCHEMA_BRONZE", "nba_bronze")

    with get_redshift_connection() as conn:
        with conn.cursor() as cur:
            for schema in _SCHEMAS:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")

            cur.execute(_RAW_GAME_LOGS_DDL.format(schema=bronze_schema))
            cur.execute(_RAW_SCHEDULE_DDL.format(schema=bronze_schema))

        conn.commit()
    logger.info("Redshift schemas and tables ensured")


# ---------------------------------------------------------------------------
# S3 -> Redshift COPY
# ---------------------------------------------------------------------------

def load_s3_to_redshift(
    s3_bucket: str,
    s3_prefix: str,
    redshift_schema: str,
    redshift_table: str,
    iam_role_arn: str,
) -> int:
    """Load Parquet from S3 into a Redshift staging table via COPY."""
    staging_table = f"{redshift_schema}.stg_{redshift_table}"

    with get_redshift_connection() as conn:
        with conn.cursor() as cur:
            # Create staging table like the target
            cur.execute(f"DROP TABLE IF EXISTS {staging_table};")
            cur.execute(
                f"CREATE TABLE {staging_table} (LIKE {redshift_schema}.{redshift_table});"
            )

            copy_sql = f"""
                COPY {staging_table}
                FROM 's3://{s3_bucket}/{s3_prefix}'
                IAM_ROLE '{iam_role_arn}'
                FORMAT AS PARQUET;
            """
            cur.execute(copy_sql)

            cur.execute(f"SELECT COUNT(*) FROM {staging_table};")
            row_count = cur.fetchone()[0]

        conn.commit()

    logger.info(
        "Loaded %d rows into %s from s3://%s/%s",
        row_count, staging_table, s3_bucket, s3_prefix,
    )
    return row_count


# ---------------------------------------------------------------------------
# Redshift merge (DELETE + INSERT pattern)
# ---------------------------------------------------------------------------

def merge_redshift_staging(
    redshift_schema: str,
    table_name: str,
    business_keys: list[str],
) -> dict:
    """Merge staging into target using DELETE + INSERT in a transaction.

    Redshift lacks native MERGE, so we delete matching rows then insert.
    """
    staging = f"{redshift_schema}.stg_{table_name}"
    target = f"{redshift_schema}.{table_name}"
    join_clause = " AND ".join(
        f"{target}.{k} = {staging}.{k}" for k in business_keys
    )

    with get_redshift_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {staging};")
            staging_count = cur.fetchone()[0]

            cur.execute("BEGIN TRANSACTION;")

            delete_sql = f"""
                DELETE FROM {target}
                USING {staging}
                WHERE {join_clause};
            """
            cur.execute(delete_sql)

            insert_sql = f"""
                INSERT INTO {target}
                SELECT * FROM {staging};
            """
            cur.execute(insert_sql)

            cur.execute("END TRANSACTION;")

            cur.execute(f"DROP TABLE IF EXISTS {staging};")

        conn.commit()

    logger.info(
        "Merged %d staging rows into %s",
        staging_count, target,
    )
    return {"table": target, "rows_merged": staging_count}


# ---------------------------------------------------------------------------
# Redshift DQ checks (mirrors BQ pipeline checks)
# ---------------------------------------------------------------------------

def run_redshift_dq_checks(
    redshift_schema: str,
    table_name: str,
    business_keys: list[str],
) -> dict:
    """Run data quality checks matching the BigQuery pipeline gates."""
    full_table = f"{redshift_schema}.{table_name}"
    results = {}

    with get_redshift_connection() as conn:
        with conn.cursor() as cur:
            # Row count check
            cur.execute(f"SELECT COUNT(*) FROM {full_table};")
            row_count = cur.fetchone()[0]
            results["row_count"] = row_count
            if row_count == 0:
                raise ValueError(f"DQ FAIL: {full_table} has zero rows")

            # Null business key checks
            for key in business_keys:
                cur.execute(
                    f"SELECT COUNT(*) FROM {full_table} WHERE {key} IS NULL;"
                )
                null_count = cur.fetchone()[0]
                results[f"null_{key}"] = null_count
                if null_count > 0:
                    raise ValueError(
                        f"DQ FAIL: {full_table}.{key} has {null_count} null values"
                    )

            # Duplicate business key check
            key_cols = ", ".join(business_keys)
            cur.execute(f"""
                SELECT {key_cols}, COUNT(*) AS cnt
                FROM {full_table}
                GROUP BY {key_cols}
                HAVING COUNT(*) > 1
                LIMIT 5;
            """)
            dupes = cur.fetchall()
            results["duplicate_keys"] = len(dupes)
            if dupes:
                raise ValueError(
                    f"DQ FAIL: {full_table} has {len(dupes)} duplicate key groups"
                )

    logger.info("DQ checks passed for %s: %s", full_table, results)
    return results


# ---------------------------------------------------------------------------
# Orchestration helpers (called from the DAG)
# ---------------------------------------------------------------------------

def sync_bronze_to_redshift(
    project_id: str,
    gcs_bucket: str,
    s3_bucket: str,
    iam_role_arn: str,
    bronze_dataset: str = "nba_bronze",
    redshift_schema_bronze: str = "nba_bronze",
) -> dict:
    """Full sync of bronze tables: BQ -> GCS -> S3 -> Redshift."""
    import pandas as pd

    run_stamp = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
    gcs_prefix = f"redshift_sync/{run_stamp}"
    s3_prefix = f"redshift_sync/{run_stamp}"

    tables = [
        {
            "name": "raw_game_logs",
            "business_keys": ["player_id", "game_date", "matchup"],
        },
        {
            "name": "raw_schedule",
            "business_keys": ["schedule_date", "team_abbr", "opponent_abbr"],
        },
    ]

    create_redshift_schemas_and_tables()

    results = {}
    for tbl in tables:
        table_name = tbl["name"]

        # Skip tables that don't exist yet in BigQuery
        from google.cloud import bigquery as bq

        try:
            bq.Client(project=project_id).get_table(
                f"{project_id}.{bronze_dataset}.{table_name}"
            )
        except Exception:
            logger.warning("BQ table %s.%s not found, skipping", bronze_dataset, table_name)
            results[table_name] = {"skipped": True, "reason": "BQ table not found"}
            continue

        export_bq_to_gcs_parquet(
            project_id, bronze_dataset, table_name, gcs_bucket, gcs_prefix,
        )

        copy_gcs_to_s3(
            gcs_bucket, f"{gcs_prefix}/{table_name}/",
            s3_bucket, f"{s3_prefix}/{table_name}",
        )

        load_s3_to_redshift(
            s3_bucket, f"{s3_prefix}/{table_name}/",
            redshift_schema_bronze, table_name, iam_role_arn,
        )

        merge_result = merge_redshift_staging(
            redshift_schema_bronze, table_name, tbl["business_keys"],
        )

        dq_result = run_redshift_dq_checks(
            redshift_schema_bronze, table_name, tbl["business_keys"],
        )

        results[table_name] = {**merge_result, "dq": dq_result}

    return results
