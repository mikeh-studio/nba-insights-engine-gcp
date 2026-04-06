select
    game_id,
    team_id,
    count(*) as row_count
from {{ source('bronze', 'raw_game_line_scores') }}
group by 1, 2
having count(*) > 1
