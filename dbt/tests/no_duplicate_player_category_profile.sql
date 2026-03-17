select
    season,
    player_id,
    count(*) as duplicate_count
from {{ ref('player_category_profile') }}
group by 1, 2
having count(*) > 1
