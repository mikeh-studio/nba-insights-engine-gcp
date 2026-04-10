select
    player_id,
    game_date,
    matchup,
    count(*) as duplicate_count
from {{ ref('stg_game_logs_clean') }}
group by 1, 2, 3
having count(*) > 1
