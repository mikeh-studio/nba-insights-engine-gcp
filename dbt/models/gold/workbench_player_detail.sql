{{ config(
    materialized='table',
    schema=env_var('BQ_DATASET_GOLD', env_var('BQ_DATASET', 'nba_gold'))
) }}

with dashboard as (
    select * from {{ ref('workbench_dashboard') }}
),
last_5 as (
    select * from {{ ref('workbench_compare') }} where window_key = 'last_5'
),
prior_5 as (
    select * from {{ ref('workbench_compare') }} where window_key = 'prior_5'
),
last_10 as (
    select * from {{ ref('workbench_compare') }} where window_key = 'last_10'
),
category_profile as (
    select
        season,
        player_id,
        avg_pts,
        avg_reb,
        avg_ast,
        avg_stl,
        avg_blk,
        avg_fg3m,
        avg_tov,
        z_pts,
        z_reb,
        z_ast,
        z_stl,
        z_blk,
        z_fg3m,
        z_tov,
        category_score_7cat,
        category_coverage_status
    from {{ ref('player_category_profile') }}
)
select
    d.season,
    d.as_of_date,
    d.player_id,
    d.player_name,
    d.latest_team_abbr,
    d.latest_game_date,
    d.overall_rank,
    d.recommendation_score,
    d.recommendation_tier,
    d.category_strengths,
    d.category_risks,
    d.trend_delta,
    d.trend_pct_change,
    d.trend_status,
    d.next_game_date,
    d.next_opponent_abbr,
    d.games_next_7d,
    d.back_to_backs_next_7d,
    d.opportunity_score,
    d.reason_primary_code,
    d.reason_primary_value,
    d.reason_secondary_code,
    d.reason_secondary_value,
    d.reason_context_code,
    d.reason_context_value,
    c.avg_pts as season_avg_pts,
    c.avg_reb as season_avg_reb,
    c.avg_ast as season_avg_ast,
    c.avg_stl as season_avg_stl,
    c.avg_blk as season_avg_blk,
    c.avg_fg3m as season_avg_fg3m,
    c.avg_tov as season_avg_tov,
    c.z_pts,
    c.z_reb,
    c.z_ast,
    c.z_stl,
    c.z_blk,
    c.z_fg3m,
    c.z_tov,
    c.category_score_7cat,
    c.category_coverage_status,
    l5.games_in_window as last_5_games,
    l5.avg_min as last_5_avg_min,
    l5.avg_pts as last_5_avg_pts,
    l5.avg_reb as last_5_avg_reb,
    l5.avg_ast as last_5_avg_ast,
    l5.avg_stl as last_5_avg_stl,
    l5.avg_blk as last_5_avg_blk,
    l5.avg_fg3m as last_5_avg_fg3m,
    l5.avg_tov as last_5_avg_tov,
    l5.fantasy_proxy_score as last_5_fantasy_proxy,
    p5.games_in_window as prior_5_games,
    p5.avg_min as prior_5_avg_min,
    p5.avg_pts as prior_5_avg_pts,
    p5.avg_reb as prior_5_avg_reb,
    p5.avg_ast as prior_5_avg_ast,
    p5.avg_stl as prior_5_avg_stl,
    p5.avg_blk as prior_5_avg_blk,
    p5.avg_fg3m as prior_5_avg_fg3m,
    p5.avg_tov as prior_5_avg_tov,
    p5.fantasy_proxy_score as prior_5_fantasy_proxy,
    l10.games_in_window as last_10_games,
    l10.avg_min as last_10_avg_min,
    l10.avg_pts as last_10_avg_pts,
    l10.avg_reb as last_10_avg_reb,
    l10.avg_ast as last_10_avg_ast,
    l10.avg_stl as last_10_avg_stl,
    l10.avg_blk as last_10_avg_blk,
    l10.avg_fg3m as last_10_avg_fg3m,
    l10.avg_tov as last_10_avg_tov,
    l10.fantasy_proxy_score as last_10_fantasy_proxy
from dashboard d
left join last_5 l5
    on d.season = l5.season
   and d.player_id = l5.player_id
left join prior_5 p5
    on d.season = p5.season
   and d.player_id = p5.player_id
left join last_10 l10
    on d.season = l10.season
   and d.player_id = l10.player_id
left join category_profile c
    on d.season = c.season
   and d.player_id = c.player_id
