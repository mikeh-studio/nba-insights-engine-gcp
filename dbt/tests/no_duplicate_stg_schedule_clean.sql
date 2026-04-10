select
    schedule_date,
    team_abbr,
    opponent_abbr,
    count(*) as row_count
from {{ ref('stg_schedule_clean') }}
group by 1, 2, 3
having count(*) > 1
