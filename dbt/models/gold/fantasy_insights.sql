{{ config(
    materialized='table',
    schema=env_var('BQ_DATASET_GOLD', env_var('BQ_DATASET', 'nba_gold'))
) }}

with ranking_base as (
    select
        r.*,
        t.stat as trend_stat,
        t.delta as trend_delta,
        row_number() over (
            partition by r.player_id
            order by abs(t.delta) desc, t.stat
        ) as trend_row_num
    from {{ ref('player_fantasy_rankings') }} r
    left join {{ ref('player_trends') }} t
        on r.player_id = t.player_id
),
top_trend as (
    select *
    from ranking_base
    where trend_row_num = 1
),
hot_streak as (
    select
        concat(cast(player_id as {{ int64_type() }}), '_hot_streak') as insight_id,
        latest_game_date as as_of_date,
        season,
        player_id,
        player_name,
        team_abbr,
        'hot_streak' as insight_type,
        recommendation_score as priority_score,
        least(10.0, greatest(0.0, 5.0 + coalesce(fantasy_points_delta_vs_season, 0) / 2.0)) as confidence_score,
        coalesce(trend_stat, 'FANTASY_POINTS_SIMPLE') as category_focus,
        concat(player_name, ' is trending up for category leagues') as title,
        concat(
            'Last 5 fantasy proxy: ',
            cast(fantasy_points_last_5 as {{ varchar_type() }}),
            ' vs season ',
            cast(season_avg_fantasy_points_simple as {{ varchar_type() }}),
            '; opportunity score ',
            cast(opportunity_score as {{ varchar_type() }}),
            '.'
        ) as summary,
        {% if target.type == 'bigquery' %}
        to_json_string(struct(
            fantasy_points_last_5,
            season_avg_fantasy_points_simple,
            minutes_delta_vs_season,
            opportunity_score,
            trend_stat,
            trend_delta
        )) as evidence_json,
        {% else %}
        '{"fantasy_points_last_5":' || coalesce(cast(fantasy_points_last_5 as varchar), 'null')
            || ',"season_avg":' || coalesce(cast(season_avg_fantasy_points_simple as varchar), 'null')
            || ',"minutes_delta":' || coalesce(cast(minutes_delta_vs_season as varchar), 'null')
            || ',"opportunity_score":' || coalesce(cast(opportunity_score as varchar), 'null')
            || ',"trend_stat":"' || coalesce(cast(trend_stat as varchar), '') || '"'
            || ',"trend_delta":' || coalesce(cast(trend_delta as varchar), 'null')
            || '}' as evidence_json,
        {% endif %}
        'consider_add' as recommendation,
        concat('rank_tier=', recommendation_tier) as source_label
    from top_trend
    where fantasy_points_delta_vs_season >= 3
),
minutes_spike as (
    select
        concat(cast(player_id as {{ int64_type() }}), '_minutes_spike') as insight_id,
        latest_game_date as as_of_date,
        season,
        player_id,
        player_name,
        team_abbr,
        'minutes_spike' as insight_type,
        recommendation_score as priority_score,
        least(10.0, greatest(0.0, 5.0 + minutes_delta_vs_season / 2.0)) as confidence_score,
        'MIN' as category_focus,
        concat(player_name, ' has gained recent workload') as title,
        concat(
            'Minutes delta vs season: ',
            cast(minutes_delta_vs_season as {{ varchar_type() }}),
            '; next 7-day games: ',
            cast(next_7d_games as {{ varchar_type() }}),
            '.'
        ) as summary,
        {% if target.type == 'bigquery' %}
        to_json_string(struct(
            avg_min_last_5,
            season_avg_min,
            minutes_delta_vs_season,
            next_7d_games,
            next_7d_back_to_backs
        )) as evidence_json,
        {% else %}
        '{"avg_min_last_5":' || coalesce(cast(avg_min_last_5 as varchar), 'null')
            || ',"season_avg_min":' || coalesce(cast(season_avg_min as varchar), 'null')
            || ',"minutes_delta":' || coalesce(cast(minutes_delta_vs_season as varchar), 'null')
            || ',"next_7d_games":' || coalesce(cast(next_7d_games as varchar), 'null')
            || ',"next_7d_b2b":' || coalesce(cast(next_7d_back_to_backs as varchar), 'null')
            || '}' as evidence_json,
        {% endif %}
        'watch_role_change' as recommendation,
        concat('rank_tier=', recommendation_tier) as source_label
    from top_trend
    where minutes_delta_vs_season >= 4
),
sell_high as (
    select
        concat(cast(player_id as {{ int64_type() }}), '_sell_high') as insight_id,
        latest_game_date as as_of_date,
        season,
        player_id,
        player_name,
        team_abbr,
        'sell_high' as insight_type,
        recommendation_score as priority_score,
        least(10.0, greatest(0.0, 5.0 + fantasy_points_delta_vs_season / 3.0)) as confidence_score,
        coalesce(trend_stat, 'FANTASY_POINTS_SIMPLE') as category_focus,
        concat(player_name, ' may be at a short-term high point') as title,
        concat(
            'Recent production is elevated, but schedule opportunity is only ',
            cast(opportunity_score as {{ varchar_type() }}),
            '.'
        ) as summary,
        {% if target.type == 'bigquery' %}
        to_json_string(struct(
            fantasy_points_last_3,
            fantasy_points_last_10,
            fantasy_points_delta_vs_season,
            opportunity_score
        )) as evidence_json,
        {% else %}
        '{"fantasy_points_last_3":' || coalesce(cast(fantasy_points_last_3 as varchar), 'null')
            || ',"fantasy_points_last_10":' || coalesce(cast(fantasy_points_last_10 as varchar), 'null')
            || ',"delta_vs_season":' || coalesce(cast(fantasy_points_delta_vs_season as varchar), 'null')
            || ',"opportunity_score":' || coalesce(cast(opportunity_score as varchar), 'null')
            || '}' as evidence_json,
        {% endif %}
        'consider_trade' as recommendation,
        concat('rank_tier=', recommendation_tier) as source_label
    from top_trend
    where fantasy_points_delta_vs_season >= 5 and opportunity_score <= 2
)
select * from hot_streak
union all
select * from minutes_spike
union all
select * from sell_high
