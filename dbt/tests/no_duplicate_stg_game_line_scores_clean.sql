select
    game_id,
    team_id,
    count(*) as row_count
from {{ ref('stg_game_line_scores_clean') }}
group by 1, 2
having count(*) > 1
