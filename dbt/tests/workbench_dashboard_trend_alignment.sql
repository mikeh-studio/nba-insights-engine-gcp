select
    season,
    as_of_date,
    player_id,
    trend_status,
    trend_delta
from {{ ref('workbench_dashboard') }}
where
    (trend_status = 'rising' and coalesce(trend_delta, 0) <= 0)
    or (trend_status = 'falling' and coalesce(trend_delta, 0) >= 0)
    or (trend_status = 'flat' and coalesce(trend_delta, 0) != 0)
