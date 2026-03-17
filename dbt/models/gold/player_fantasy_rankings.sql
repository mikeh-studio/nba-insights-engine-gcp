{{ config(
    materialized='table',
    schema=env_var('BQ_DATASET_GOLD', env_var('BQ_DATASET', 'nba_gold'))
) }}

select
    f.season,
    f.player_id,
    f.player_name,
    f.team_abbr,
    f.latest_game_date,
    f.season_games,
    f.avg_min_last_5,
    f.fantasy_points_last_5,
    f.fantasy_points_last_10,
    f.fantasy_points_delta_vs_season,
    f.minutes_delta_vs_season,
    f.season_avg_min,
    f.season_avg_fantasy_points_simple,
    c.category_score_6cat,
    c.category_coverage_status,
    o.first_game_date as next_game_date,
    o.first_game_date,
    o.next_opponent,
    o.next_7d_games,
    o.next_7d_back_to_backs,
    o.opportunity_score,
    array_to_string(
        array(
            select stat_name
            from unnest([
                struct('PTS' as stat_name, c.z_pts as stat_value),
                struct('REB' as stat_name, c.z_reb as stat_value),
                struct('AST' as stat_name, c.z_ast as stat_value),
                struct('STL' as stat_name, c.z_stl as stat_value),
                struct('BLK' as stat_name, c.z_blk as stat_value),
                struct('TOV' as stat_name, c.z_tov as stat_value)
            ])
            where stat_value >= 0.5
            order by stat_value desc, stat_name
            limit 3
        ),
        ', '
    ) as category_strengths,
    array_to_string(
        array(
            select stat_name
            from unnest([
                struct('PTS' as stat_name, c.z_pts as stat_value),
                struct('REB' as stat_name, c.z_reb as stat_value),
                struct('AST' as stat_name, c.z_ast as stat_value),
                struct('STL' as stat_name, c.z_stl as stat_value),
                struct('BLK' as stat_name, c.z_blk as stat_value),
                struct('TOV' as stat_name, c.z_tov as stat_value)
            ])
            where stat_value <= -0.25
            order by stat_value asc, stat_name
            limit 3
        ),
        ', '
    ) as category_risks,
    round(
        coalesce(c.category_score_6cat, 0) * 0.5
        + coalesce(f.fantasy_points_delta_vs_season, 0) * 0.8
        + coalesce(f.minutes_delta_vs_season, 0) * 0.3
        + coalesce(o.opportunity_score, 0) * 0.6,
        2
    ) as recommendation_score,
    dense_rank() over (
        order by
            round(
                coalesce(c.category_score_6cat, 0) * 0.5
                + coalesce(f.fantasy_points_delta_vs_season, 0) * 0.8
                + coalesce(f.minutes_delta_vs_season, 0) * 0.3
                + coalesce(o.opportunity_score, 0) * 0.6,
                2
            ) desc,
            f.fantasy_points_last_5 desc,
            f.player_name
    ) as fantasy_rank_9cat_proxy,
    dense_rank() over (
        order by
            round(
                coalesce(c.category_score_6cat, 0) * 0.5
                + coalesce(f.fantasy_points_delta_vs_season, 0) * 0.8
                + coalesce(f.minutes_delta_vs_season, 0) * 0.3
                + coalesce(o.opportunity_score, 0) * 0.6,
                2
            ) desc,
            f.fantasy_points_last_5 desc,
            f.player_name
    ) as overall_rank,
    case
        when
            round(
                coalesce(c.category_score_6cat, 0) * 0.5
                + coalesce(f.fantasy_points_delta_vs_season, 0) * 0.8
                + coalesce(f.minutes_delta_vs_season, 0) * 0.3
                + coalesce(o.opportunity_score, 0) * 0.6,
                2
            ) >= 8
            then 'strong_add'
        when
            round(
                coalesce(c.category_score_6cat, 0) * 0.5
                + coalesce(f.fantasy_points_delta_vs_season, 0) * 0.8
                + coalesce(f.minutes_delta_vs_season, 0) * 0.3
                + coalesce(o.opportunity_score, 0) * 0.6,
                2
            ) >= 4
            then 'hold_or_stream'
        else 'watchlist'
    end as recommendation_tier,
    o.next_7d_games as games_next_7d,
    o.next_7d_back_to_backs as back_to_backs_next_7d
from {{ ref('player_recent_form') }} f
left join {{ ref('player_category_profile') }} c
    on f.player_id = c.player_id
left join {{ ref('player_opportunity_outlook') }} o
    on f.player_id = o.player_id
