select
    snapshot_id,
    count(*) as duplicate_count
from {{ source('gold_runtime', 'analysis_snapshots') }}
group by 1
having count(*) > 1
