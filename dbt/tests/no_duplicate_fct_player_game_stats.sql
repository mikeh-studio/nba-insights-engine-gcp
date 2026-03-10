select
    player_id,
    game_date,
    matchup,
    count(*) as duplicate_count
from {{ ref('fct_player_game_stats') }}
group by 1, 2, 3
having count(*) > 1
