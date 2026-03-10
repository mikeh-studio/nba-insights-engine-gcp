select
    player_id,
    game_date,
    matchup,
    count(*) as duplicate_count
from {{ source('bronze', 'raw_game_logs') }}
group by 1, 2, 3
having count(*) > 1
