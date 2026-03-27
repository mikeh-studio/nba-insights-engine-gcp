select
    season,
    as_of_date,
    player_id,
    window_key,
    count(*) as duplicate_count
from {{ ref('workbench_compare') }}
group by 1, 2, 3, 4
having count(*) > 1
