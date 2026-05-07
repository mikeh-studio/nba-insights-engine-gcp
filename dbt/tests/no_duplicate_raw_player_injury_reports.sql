{% set injury_relation = adapter.get_relation(
    database=env_var('BQ_PROJECT', env_var('GCP_PROJECT_ID', 'local-project')),
    schema=env_var('BQ_DATASET_BRONZE', env_var('BQ_DATASET', 'nba_bronze')),
    identifier='raw_player_injury_reports'
) %}

{% if injury_relation is not none %}
select
    report_timestamp_utc,
    game_date,
    matchup,
    team_abbr,
    player_name_source,
    count(*) as row_count
from {{ source('bronze', 'raw_player_injury_reports') }}
group by 1, 2, 3, 4, 5
having count(*) > 1
{% else %}
select
    cast(null as timestamp) as report_timestamp_utc,
    cast(null as date) as game_date,
    cast(null as {{ varchar_type() }}) as matchup,
    cast(null as {{ varchar_type() }}) as team_abbr,
    cast(null as {{ varchar_type() }}) as player_name_source,
    cast(null as {{ int64_type() }}) as row_count
from (select 1 as _empty_source)
where 1 = 0
{% endif %}
