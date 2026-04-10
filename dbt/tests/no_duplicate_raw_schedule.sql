select
    schedule_date,
    team_abbr,
    opponent_abbr,
    count(*) as row_count
from {{ source('bronze', 'raw_schedule') }}
group by 1, 2, 3
having count(*) > 1
