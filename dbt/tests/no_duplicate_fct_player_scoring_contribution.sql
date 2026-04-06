select
    game_id,
    player_id,
    count(*) as row_count
from {{ ref('fct_player_scoring_contribution') }}
group by 1, 2
having count(*) > 1
