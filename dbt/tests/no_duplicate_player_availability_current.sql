select
    season,
    as_of_date,
    player_id,
    count(*) as row_count
from {{ ref('player_availability_current') }}
group by 1, 2, 3
having count(*) > 1
