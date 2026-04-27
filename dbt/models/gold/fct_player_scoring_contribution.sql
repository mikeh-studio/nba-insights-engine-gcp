{{ config(
    materialized='table',
    schema=env_var('BQ_DATASET_GOLD', env_var('BQ_DATASET', 'nba_gold'))
) }}

select
    p.season,
    p.game_id,
    p.game_date,
    p.player_id,
    p.player_name,
    p.team_abbr,
    p.opponent_abbr,
    p.home_away,
    p.matchup,
    p.pts as player_pts,
    t.team_pts,
    t.opponent_team_pts,
    t.team_pts_qtr1,
    t.team_pts_qtr2,
    t.team_pts_qtr3,
    t.team_pts_qtr4,
    t.team_pts_ot_total,
    t.scoring_margin,
    coalesce(
        round({{ safe_divide('p.pts', 'nullif(t.team_pts, 0)') }}, 4),
        0
    ) as player_points_share_of_team,
    coalesce(
        round(
            {{ safe_divide('p.pts', 'nullif(t.team_pts + coalesce(t.opponent_team_pts, 0), 0)') }},
            4
        ),
        0
    ) as player_points_share_of_game,
    p.ingested_at_utc
from {{ ref('fct_player_game_stats') }} p
left join {{ ref('fct_team_game_scores') }} t
    on p.game_id = t.game_id
   and p.team_abbr = t.team_abbr
