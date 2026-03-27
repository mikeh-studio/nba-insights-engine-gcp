select
    season,
    as_of_date,
    player_id,
    count(*) as duplicate_count
from {{ ref('workbench_player_detail') }}
group by 1, 2, 3
having count(*) > 1
