{{ config(
    materialized='table',
    schema=env_var('BQ_DATASET_GOLD', env_var('BQ_DATASET', 'nba_gold'))
) }}

with team_scores as (
    select
        game_id,
        game_date,
        season,
        team_id,
        team_abbr,
        team_city_name,
        team_nickname,
        team_wins_losses,
        pts_qtr1,
        pts_qtr2,
        pts_qtr3,
        pts_qtr4,
        pts_ot1,
        pts_ot2,
        pts_ot3,
        pts_ot4,
        pts_ot5,
        pts_ot6,
        pts_ot7,
        pts_ot8,
        pts_ot9,
        pts_ot10,
        pts as team_pts,
        (
            coalesce(pts_ot1, 0)
            + coalesce(pts_ot2, 0)
            + coalesce(pts_ot3, 0)
            + coalesce(pts_ot4, 0)
            + coalesce(pts_ot5, 0)
            + coalesce(pts_ot6, 0)
            + coalesce(pts_ot7, 0)
            + coalesce(pts_ot8, 0)
            + coalesce(pts_ot9, 0)
            + coalesce(pts_ot10, 0)
        ) as team_pts_ot_total,
        ingested_at_utc
    from {{ ref('stg_game_line_scores_clean') }}
),
with_opponent as (
    select
        t.game_id,
        t.game_date,
        t.season,
        t.team_id,
        t.team_abbr,
        t.team_city_name,
        t.team_nickname,
        t.team_wins_losses,
        o.team_id as opponent_team_id,
        o.team_abbr as opponent_team_abbr,
        o.team_pts as opponent_team_pts,
        t.pts_qtr1 as team_pts_qtr1,
        t.pts_qtr2 as team_pts_qtr2,
        t.pts_qtr3 as team_pts_qtr3,
        t.pts_qtr4 as team_pts_qtr4,
        t.team_pts_ot_total,
        t.team_pts,
        t.team_pts - o.team_pts as scoring_margin,
        case
            when t.team_pts > o.team_pts then 'W'
            when t.team_pts < o.team_pts then 'L'
            else 'T'
        end as game_result,
        t.ingested_at_utc
    from team_scores t
    left join team_scores o
        on t.game_id = o.game_id
       and t.team_id != o.team_id
)
select *
from with_opponent
