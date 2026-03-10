select
    player_id,
    game_date,
    matchup,
    season
from {{ ref('stg_game_logs_clean') }}
where season != '2025-26'
   or game_date < date('2025-07-01')
   or game_date > date('2026-06-30')
