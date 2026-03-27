select
    season,
    as_of_date,
    player_id
from {{ ref('workbench_player_detail') }}
where overall_rank is not null
  and reason_primary_code is null
  and reason_secondary_code is null
  and reason_context_code is null
