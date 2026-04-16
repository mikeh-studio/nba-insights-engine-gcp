{{ config(
    materialized='table',
    schema=env_var('BQ_DATASET_GOLD', env_var('BQ_DATASET', 'nba_gold'))
) }}

{% set today = 'current_date()' if target.type == 'bigquery' else 'current_date' %}

with recent_form as (
    select
        season,
        player_id,
        player_name,
        team_abbr,
        season_games,
        avg_min_last_5,
        avg_min_last_10,
        fantasy_points_last_5,
        fantasy_points_last_10,
        pts_last_5,
        pts_last_10,
        reb_last_5,
        reb_last_10,
        ast_last_5,
        ast_last_10,
        stl_last_5,
        stl_last_10,
        blk_last_5,
        blk_last_10,
        fg3m_last_5,
        fg3m_last_10,
        tov_last_5,
        tov_last_10,
        fantasy_points_delta_vs_season,
        minutes_delta_vs_season
    from {{ ref('player_recent_form') }}
),
category_profile as (
    select
        season,
        player_id,
        player_name,
        latest_team_abbr,
        games_sampled,
        avg_pts,
        avg_reb,
        avg_ast,
        avg_stl,
        avg_blk,
        avg_fg3m,
        avg_tov,
        avg_min,
        avg_fantasy_points_simple,
        z_pts,
        z_reb,
        z_ast,
        z_stl,
        z_blk,
        z_fg3m,
        z_tov,
        z_min,
        z_fantasy_points_simple
    from {{ ref('player_category_profile') }}
),
player_dimension as (
    select
        player_id,
        latest_season,
        latest_team_abbr,
        position
    from {{ ref('dim_player') }}
),
scoring_contribution_base as (
    select
        season,
        player_id,
        row_number() over (
            partition by season, player_id
            order by game_date desc, game_id desc
        ) as game_num,
        player_points_share_of_team,
        player_points_share_of_game
    from {{ ref('fct_player_scoring_contribution') }}
),
scoring_contribution as (
    select
        season,
        player_id,
        round(avg(player_points_share_of_team), 4) as season_points_share_of_team,
        round(avg(case when game_num <= 10 then player_points_share_of_team end), 4) as recent_points_share_of_team,
        round(avg(player_points_share_of_game), 4) as season_points_share_of_game,
        round(avg(case when game_num <= 10 then player_points_share_of_game end), 4) as recent_points_share_of_game
    from scoring_contribution_base
    group by 1, 2
)
select
    f.season,
    {{ today }} as as_of_date,
    f.player_id,
    f.player_name,
    coalesce(d.latest_team_abbr, c.latest_team_abbr, f.team_abbr) as team_abbr,
    d.position,
    coalesce(c.games_sampled, f.season_games, 0) as games_sampled,
    case
        when coalesce(c.games_sampled, f.season_games, 0) >= 10 then 'ready'
        when coalesce(c.games_sampled, f.season_games, 0) >= 5 then 'limited_sample'
        else 'insufficient_sample'
    end as sample_status,
    coalesce(c.avg_pts, 0) as season_avg_pts,
    coalesce(c.avg_reb, 0) as season_avg_reb,
    coalesce(c.avg_ast, 0) as season_avg_ast,
    coalesce(c.avg_stl, 0) as season_avg_stl,
    coalesce(c.avg_blk, 0) as season_avg_blk,
    coalesce(c.avg_fg3m, 0) as season_avg_fg3m,
    coalesce(c.avg_tov, 0) as season_avg_tov,
    coalesce(c.avg_min, 0) as season_avg_min,
    coalesce(c.avg_fantasy_points_simple, 0) as season_avg_fantasy_points,
    coalesce(f.pts_last_10, f.pts_last_5, c.avg_pts, 0) as recent_pts,
    coalesce(f.reb_last_10, f.reb_last_5, c.avg_reb, 0) as recent_reb,
    coalesce(f.ast_last_10, f.ast_last_5, c.avg_ast, 0) as recent_ast,
    coalesce(f.stl_last_10, f.stl_last_5, c.avg_stl, 0) as recent_stl,
    coalesce(f.blk_last_10, f.blk_last_5, c.avg_blk, 0) as recent_blk,
    coalesce(f.fg3m_last_10, f.fg3m_last_5, c.avg_fg3m, 0) as recent_fg3m,
    coalesce(f.tov_last_10, f.tov_last_5, c.avg_tov, 0) as recent_tov,
    coalesce(f.avg_min_last_10, f.avg_min_last_5, c.avg_min, 0) as recent_min,
    coalesce(
        f.fantasy_points_last_10,
        f.fantasy_points_last_5,
        c.avg_fantasy_points_simple,
        0
    ) as recent_fantasy_points,
    coalesce(f.fantasy_points_delta_vs_season, 0) as fantasy_points_delta_vs_season,
    coalesce(f.minutes_delta_vs_season, 0) as minutes_delta_vs_season,
    coalesce(s.season_points_share_of_team, 0) as season_points_share_of_team,
    coalesce(
        s.recent_points_share_of_team,
        s.season_points_share_of_team,
        0
    ) as recent_points_share_of_team,
    coalesce(s.season_points_share_of_game, 0) as season_points_share_of_game,
    coalesce(
        s.recent_points_share_of_game,
        s.season_points_share_of_game,
        0
    ) as recent_points_share_of_game,
    coalesce(c.z_pts, 0) as z_pts,
    coalesce(c.z_reb, 0) as z_reb,
    coalesce(c.z_ast, 0) as z_ast,
    coalesce(c.z_stl, 0) as z_stl,
    coalesce(c.z_blk, 0) as z_blk,
    coalesce(c.z_fg3m, 0) as z_fg3m,
    coalesce(c.z_tov, 0) as z_tov,
    coalesce(c.z_min, 0) as z_min,
    coalesce(c.z_fantasy_points_simple, 0) as z_fantasy_points
from recent_form f
left join category_profile c
    on f.season = c.season
   and f.player_id = c.player_id
left join player_dimension d
    on f.player_id = d.player_id
   and f.season = d.latest_season
left join scoring_contribution s
    on f.season = s.season
   and f.player_id = s.player_id
where coalesce(c.games_sampled, f.season_games, 0) >= 3
