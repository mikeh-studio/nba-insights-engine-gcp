select
    player_id,
    count(*) as duplicate_count
from {{ ref('player_fantasy_rankings') }}
group by 1
having count(*) > 1
