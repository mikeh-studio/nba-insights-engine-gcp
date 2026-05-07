{{ config(
    materialized='table',
    schema=env_var('BQ_DATASET_SILVER', env_var('BQ_DATASET', 'nba_silver'))
) }}

{% set injury_relation = adapter.get_relation(
    database=env_var('BQ_PROJECT', env_var('GCP_PROJECT_ID', 'local-project')),
    schema=env_var('BQ_DATASET_BRONZE', env_var('BQ_DATASET', 'nba_bronze')),
    identifier='raw_player_injury_reports'
) %}

{% if injury_relation is not none %}
with source as (
    select
        cast(report_date as date) as report_date,
        cast(report_time_et as {{ varchar_type() }}) as report_time_et,
        cast(report_timestamp_utc as timestamp) as report_timestamp_utc,
        cast(game_date as date) as game_date,
        cast(game_time_et as {{ varchar_type() }}) as game_time_et,
        upper(cast(matchup as {{ varchar_type() }})) as matchup,
        cast(season as {{ varchar_type() }}) as season,
        upper(cast(team_abbr as {{ varchar_type() }})) as team_abbr,
        cast(team_name as {{ varchar_type() }}) as team_name,
        cast(player_id as {{ int64_type() }}) as player_id,
        cast(player_name as {{ varchar_type() }}) as player_name,
        cast(player_name_source as {{ varchar_type() }}) as player_name_source,
        cast(
            case
                when lower(trim(cast(injury_status as {{ varchar_type() }}))) = 'available' then 'Available'
                when lower(trim(cast(injury_status as {{ varchar_type() }}))) = 'probable' then 'Probable'
                when lower(trim(cast(injury_status as {{ varchar_type() }}))) = 'questionable' then 'Questionable'
                when lower(trim(cast(injury_status as {{ varchar_type() }}))) = 'doubtful' then 'Doubtful'
                when lower(trim(cast(injury_status as {{ varchar_type() }}))) = 'out' then 'Out'
                else trim(cast(injury_status as {{ varchar_type() }}))
            end as {{ varchar_type() }}
        ) as injury_status,
        cast(reason as {{ varchar_type() }}) as reason,
        cast(source_url as {{ varchar_type() }}) as source_url,
        cast(source_system as {{ varchar_type() }}) as source_system,
        cast(ingested_at_utc as timestamp) as ingested_at_utc
    from {{ source('bronze', 'raw_player_injury_reports') }}
    where cast(report_date as date) between date('2025-07-01') and date('2026-06-30')
      and cast(game_date as date) between date('2025-07-01') and date('2026-06-30')
),
deduped as (
    select
        *,
        row_number() over (
            partition by report_timestamp_utc, game_date, matchup, team_abbr, player_name_source
            order by ingested_at_utc desc, source_url desc
        ) as row_num
    from source
)
select
    report_date,
    report_time_et,
    report_timestamp_utc,
    game_date,
    game_time_et,
    matchup,
    season,
    team_abbr,
    team_name,
    player_id,
    player_name,
    player_name_source,
    injury_status,
    reason,
    source_url,
    source_system,
    ingested_at_utc
from deduped
where row_num = 1
{% else %}
select
    cast(null as date) as report_date,
    cast(null as {{ varchar_type() }}) as report_time_et,
    cast(null as timestamp) as report_timestamp_utc,
    cast(null as date) as game_date,
    cast(null as {{ varchar_type() }}) as game_time_et,
    cast(null as {{ varchar_type() }}) as matchup,
    cast(null as {{ varchar_type() }}) as season,
    cast(null as {{ varchar_type() }}) as team_abbr,
    cast(null as {{ varchar_type() }}) as team_name,
    cast(null as {{ int64_type() }}) as player_id,
    cast(null as {{ varchar_type() }}) as player_name,
    cast(null as {{ varchar_type() }}) as player_name_source,
    cast(null as {{ varchar_type() }}) as injury_status,
    cast(null as {{ varchar_type() }}) as reason,
    cast(null as {{ varchar_type() }}) as source_url,
    cast(null as {{ varchar_type() }}) as source_system,
    cast(null as timestamp) as ingested_at_utc
from (select 1 as _empty_source)
where 1 = 0
{% endif %}
