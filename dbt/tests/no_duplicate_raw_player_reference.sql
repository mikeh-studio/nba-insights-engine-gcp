select
    player_id,
    count(*) as row_count
from {{ source('bronze', 'raw_player_reference') }}
group by 1
having count(*) > 1
