{{ config(
    materialized='view',
    schema=env_var('BQ_DATASET_SILVER', env_var('BQ_DATASET', 'nba_silver'))
) }}

{% set schedule_relation = adapter.get_relation(
    database=env_var('BQ_PROJECT', env_var('GCP_PROJECT_ID', 'local-project')),
    schema=env_var('BQ_DATASET_BRONZE', env_var('BQ_DATASET', 'nba_bronze')),
    identifier='raw_schedule'
) %}

{% if schedule_relation is not none %}
select
    cast(schedule_date as date) as schedule_date,
    upper(cast(team_abbr as string)) as team_abbr,
    upper(cast(opponent_abbr as string)) as opponent_abbr,
    cast(home_away as string) as home_away,
    cast(coalesce(is_back_to_back, false) as bool) as is_back_to_back,
    cast(coalesce(game_status, 'scheduled') as string) as game_status,
    cast(source_updated_at_utc as timestamp) as source_updated_at_utc
from {{ source('bronze', 'raw_schedule') }}
where cast(schedule_date as date) between date('2025-07-01') and date('2026-06-30')
{% else %}
select
    cast(null as date) as schedule_date,
    cast(null as string) as team_abbr,
    cast(null as string) as opponent_abbr,
    cast(null as string) as home_away,
    cast(null as bool) as is_back_to_back,
    cast(null as string) as game_status,
    cast(null as timestamp) as source_updated_at_utc
where 1 = 0
{% endif %}
