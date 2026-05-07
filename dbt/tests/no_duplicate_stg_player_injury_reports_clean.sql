select
    report_timestamp_utc,
    game_date,
    matchup,
    team_abbr,
    player_name_source,
    count(*) as row_count
from {{ ref('stg_player_injury_reports_clean') }}
group by 1, 2, 3, 4, 5
having count(*) > 1
