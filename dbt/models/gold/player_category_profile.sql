{{ config(
    materialized='table',
    schema=env_var('BQ_DATASET_GOLD', env_var('BQ_DATASET', 'nba_gold'))
) }}

with player_means as (
    select
        season,
        player_id,
        player_name,
        max(team_abbr) as team_abbr,
        round(avg(pts), 2) as avg_pts,
        round(avg(reb), 2) as avg_reb,
        round(avg(ast), 2) as avg_ast,
        round(avg(stl), 2) as avg_stl,
        round(avg(blk), 2) as avg_blk,
        round(avg(fg3m), 2) as avg_fg3m,
        round(avg(tov), 2) as avg_tov,
        round(avg(min), 2) as avg_min,
        round(avg(fantasy_points_simple), 2) as avg_fantasy_points_simple,
        count(*) as games_sampled
    from {{ ref('fct_player_game_stats') }}
    group by 1, 2, 3
),
league_baseline as (
    select
        avg(avg_pts) as league_avg_pts,
        stddev_pop(avg_pts) as league_sd_pts,
        avg(avg_reb) as league_avg_reb,
        stddev_pop(avg_reb) as league_sd_reb,
        avg(avg_ast) as league_avg_ast,
        stddev_pop(avg_ast) as league_sd_ast,
        avg(avg_stl) as league_avg_stl,
        stddev_pop(avg_stl) as league_sd_stl,
        avg(avg_blk) as league_avg_blk,
        stddev_pop(avg_blk) as league_sd_blk,
        avg(avg_fg3m) as league_avg_fg3m,
        stddev_pop(avg_fg3m) as league_sd_fg3m,
        avg(avg_tov) as league_avg_tov,
        stddev_pop(avg_tov) as league_sd_tov,
        avg(avg_min) as league_avg_min,
        stddev_pop(avg_min) as league_sd_min,
        avg(avg_fantasy_points_simple) as league_avg_fantasy_points_simple,
        stddev_pop(avg_fantasy_points_simple) as league_sd_fantasy_points_simple
    from player_means
)
select
    p.season,
    p.player_id,
    p.player_name,
    p.team_abbr,
    p.games_sampled,
    p.avg_pts,
    p.avg_reb,
    p.avg_ast,
    p.avg_stl,
    p.avg_blk,
    p.avg_fg3m,
    p.avg_tov,
    p.avg_min,
    p.avg_fantasy_points_simple,
    round({{ safe_divide('p.avg_pts - b.league_avg_pts', 'nullif(b.league_sd_pts, 0)') }}, 2) as z_pts,
    round({{ safe_divide('p.avg_reb - b.league_avg_reb', 'nullif(b.league_sd_reb, 0)') }}, 2) as z_reb,
    round({{ safe_divide('p.avg_ast - b.league_avg_ast', 'nullif(b.league_sd_ast, 0)') }}, 2) as z_ast,
    round({{ safe_divide('p.avg_stl - b.league_avg_stl', 'nullif(b.league_sd_stl, 0)') }}, 2) as z_stl,
    round({{ safe_divide('p.avg_blk - b.league_avg_blk', 'nullif(b.league_sd_blk, 0)') }}, 2) as z_blk,
    round({{ safe_divide('p.avg_fg3m - b.league_avg_fg3m', 'nullif(b.league_sd_fg3m, 0)') }}, 2) as z_fg3m,
    round(-{{ safe_divide('p.avg_tov - b.league_avg_tov', 'nullif(b.league_sd_tov, 0)') }}, 2) as z_tov,
    round({{ safe_divide('p.avg_min - b.league_avg_min', 'nullif(b.league_sd_min, 0)') }}, 2) as z_min,
    round({{ safe_divide('p.avg_fantasy_points_simple - b.league_avg_fantasy_points_simple', 'nullif(b.league_sd_fantasy_points_simple, 0)') }}, 2) as z_fantasy_points_simple,
    round(
        coalesce({{ safe_divide('p.avg_pts - b.league_avg_pts', 'nullif(b.league_sd_pts, 0)') }}, 0)
        + coalesce({{ safe_divide('p.avg_reb - b.league_avg_reb', 'nullif(b.league_sd_reb, 0)') }}, 0)
        + coalesce({{ safe_divide('p.avg_ast - b.league_avg_ast', 'nullif(b.league_sd_ast, 0)') }}, 0)
        + coalesce({{ safe_divide('p.avg_stl - b.league_avg_stl', 'nullif(b.league_sd_stl, 0)') }}, 0)
        + coalesce({{ safe_divide('p.avg_blk - b.league_avg_blk', 'nullif(b.league_sd_blk, 0)') }}, 0)
        - coalesce({{ safe_divide('p.avg_tov - b.league_avg_tov', 'nullif(b.league_sd_tov, 0)') }}, 0),
        2
    ) as category_score_6cat,
    round(
        coalesce({{ safe_divide('p.avg_pts - b.league_avg_pts', 'nullif(b.league_sd_pts, 0)') }}, 0)
        + coalesce({{ safe_divide('p.avg_reb - b.league_avg_reb', 'nullif(b.league_sd_reb, 0)') }}, 0)
        + coalesce({{ safe_divide('p.avg_ast - b.league_avg_ast', 'nullif(b.league_sd_ast, 0)') }}, 0)
        + coalesce({{ safe_divide('p.avg_stl - b.league_avg_stl', 'nullif(b.league_sd_stl, 0)') }}, 0)
        + coalesce({{ safe_divide('p.avg_blk - b.league_avg_blk', 'nullif(b.league_sd_blk, 0)') }}, 0)
        + coalesce({{ safe_divide('p.avg_fg3m - b.league_avg_fg3m', 'nullif(b.league_sd_fg3m, 0)') }}, 0)
        - coalesce({{ safe_divide('p.avg_tov - b.league_avg_tov', 'nullif(b.league_sd_tov, 0)') }}, 0),
        2
    ) as category_score_7cat,
    7 as available_category_count,
    9 as target_category_count,
    'partial_until_fg_ft_available' as category_coverage_status
from player_means p
cross join league_baseline b
