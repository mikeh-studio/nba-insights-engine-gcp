select
    insight_id,
    count(*) as duplicate_count
from {{ ref('fantasy_insights') }}
group by 1
having count(*) > 1
