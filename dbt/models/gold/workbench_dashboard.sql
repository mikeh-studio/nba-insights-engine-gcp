{{ config(
    materialized='table',
    schema=env_var('BQ_DATASET_GOLD', env_var('BQ_DATASET', 'nba_gold'))
) }}

{% set empty_string = "''" if target.type == 'bigquery' else "''" %}
{% set today = 'current_date()' if target.type == 'bigquery' else 'current_date' %}

with rankings as (
    select
        season,
        player_id,
        player_name,
        team_abbr,
        latest_game_date,
        overall_rank,
        recommendation_score,
        recommendation_tier,
        category_strengths,
        category_risks
    from {{ ref('player_fantasy_rankings') }}
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
opportunity as (
    select
        season,
        player_id,
        first_game_date as next_game_date,
        next_opponent as next_opponent_abbr,
        next_7d_games as games_next_7d,
        next_7d_back_to_backs as back_to_backs_next_7d,
        opportunity_score
    from {{ ref('player_opportunity_outlook') }}
),
joined as (
    select
        r.season,
        {{ today }} as as_of_date,
        r.player_id,
        r.player_name,
        coalesce(l5.latest_team_abbr, r.team_abbr) as latest_team_abbr,
        coalesce(l5.latest_game_date, r.latest_game_date) as latest_game_date,
        r.overall_rank,
        r.recommendation_score,
        r.recommendation_tier,
        r.category_strengths,
        r.category_risks,
        l5.games_in_window as last_5_games,
        p5.games_in_window as prior_5_games,
        l10.games_in_window as last_10_games,
        l5.fantasy_proxy_score as fantasy_proxy_last_5,
        p5.fantasy_proxy_score as fantasy_proxy_prior_5,
        l10.fantasy_proxy_score as fantasy_proxy_last_10,
        l5.season_avg_fantasy_proxy,
        l5.avg_min as avg_min_last_5,
        l5.season_avg_min,
        o.next_game_date,
        o.next_opponent_abbr,
        o.games_next_7d,
        o.back_to_backs_next_7d,
        o.opportunity_score
    from rankings r
    left join last_5 l5
        on r.season = l5.season
       and r.player_id = l5.player_id
    left join prior_5 p5
        on r.season = p5.season
       and r.player_id = p5.player_id
    left join last_10 l10
        on r.season = l10.season
       and r.player_id = l10.player_id
    left join opportunity o
        on r.season = o.season
       and r.player_id = o.player_id
),
enriched as (
    select
        *,
        round(fantasy_proxy_last_5 - fantasy_proxy_prior_5, 1) as trend_delta,
        round(
            {{ safe_divide(
                'fantasy_proxy_last_5 - fantasy_proxy_prior_5',
                'nullif(fantasy_proxy_prior_5, 0)'
            ) }} * 100,
            1
        ) as trend_pct_change,
        case
            when coalesce(last_5_games, 0) < 3 or coalesce(prior_5_games, 0) < 3 then 'insufficient_sample'
            when round(fantasy_proxy_last_5 - fantasy_proxy_prior_5, 1) > 0 then 'rising'
            when round(fantasy_proxy_last_5 - fantasy_proxy_prior_5, 1) < 0 then 'falling'
            else 'flat'
        end as trend_status,
        round(fantasy_proxy_last_5 - season_avg_fantasy_proxy, 1) as recent_fp_delta_vs_season,
        round(avg_min_last_5 - season_avg_min, 1) as minutes_delta_vs_season
    from joined
)
select
    season,
    as_of_date,
    player_id,
    player_name,
    latest_team_abbr,
    latest_game_date,
    overall_rank,
    recommendation_score,
    recommendation_tier,
    category_strengths,
    category_risks,
    last_5_games,
    prior_5_games,
    last_10_games,
    fantasy_proxy_last_5,
    fantasy_proxy_prior_5,
    fantasy_proxy_last_10,
    trend_delta,
    trend_pct_change,
    trend_status,
    next_game_date,
    next_opponent_abbr,
    games_next_7d,
    back_to_backs_next_7d,
    opportunity_score,
    'recent_fp_delta' as reason_primary_code,
    cast(recent_fp_delta_vs_season as {{ varchar_type() }}) as reason_primary_value,
    case
        when minutes_delta_vs_season is not null and abs(minutes_delta_vs_season) >= 0.5 then 'minutes_delta'
        when trend_status in ('rising', 'falling') then 'trend_stat_delta'
        else null
    end as reason_secondary_code,
    case
        when minutes_delta_vs_season is not null and abs(minutes_delta_vs_season) >= 0.5
            then cast(minutes_delta_vs_season as {{ varchar_type() }})
        when trend_status in ('rising', 'falling')
            then cast(trend_delta as {{ varchar_type() }})
        else null
    end as reason_secondary_value,
    case
        when games_next_7d is not null then 'games_next_7d'
        when coalesce(category_strengths, {{ empty_string }}) != {{ empty_string }} then 'category_edge'
        else null
    end as reason_context_code,
    case
        when games_next_7d is not null then cast(games_next_7d as {{ varchar_type() }})
        when coalesce(category_strengths, {{ empty_string }}) != {{ empty_string }} then category_strengths
        else null
    end as reason_context_value
from enriched
