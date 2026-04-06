select
    game_id,
    team_id,
    count(*) as row_count
from {{ ref('fct_team_game_scores') }}
group by 1, 2
having count(*) > 1
