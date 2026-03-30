{{ config(
    materialized='table',
    schema=env_var('BQ_DATASET_GOLD', env_var('BQ_DATASET', 'nba_gold'))
) }}

{% if target.type == 'bigquery' %}
    {% set next_7_day_expr = "date_add(ds.as_of_date, interval 7 day)" %}
{% else %}
    {% set next_7_day_expr = "ds.as_of_date + interval '7 day'" %}
{% endif %}

with date_spine as (
    {% for offset in range(6, -1, -1) %}
    select
        {% if target.type == 'bigquery' %}
        date_sub(current_date(), interval {{ offset }} day)
        {% else %}
        dateadd(day, -{{ offset }}, current_date)::date
        {% endif %} as as_of_date
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),
scored_games as (
    select
        ds.as_of_date,
        g.season,
        g.player_id,
        g.player_name,
        g.team_abbr,
        g.game_date,
        g.min,
        g.pts,
        g.reb,
        g.ast,
        g.stl,
        g.blk,
        g.fg3m,
        g.tov,
        g.ingested_at_utc,
        round(
            g.pts
            + (g.reb * 1.2)
            + (g.ast * 1.5)
            + (g.stl * 3.0)
            + (g.blk * 3.0)
            + g.fg3m
            - g.tov,
            2
        ) as fantasy_proxy_score,
        row_number() over (
            partition by ds.as_of_date, g.season, g.player_id
            order by g.game_date desc, g.ingested_at_utc desc
        ) as game_num
    from date_spine ds
    inner join {{ ref('fct_player_game_stats') }} g
        on g.game_date <= ds.as_of_date
),
season_stats as (
    select
        as_of_date,
        season,
        player_id,
        max(game_date) as latest_game_date,
        count(*) as season_games,
        round(avg(min), 1) as season_avg_min,
        round(avg(fantasy_proxy_score), 1) as season_avg_fantasy_proxy
    from scored_games
    group by 1, 2, 3
),
latest_player as (
    select
        as_of_date,
        season,
        player_id,
        player_name,
        team_abbr as latest_team_abbr
    from scored_games
    where game_num = 1
),
windowed as (
    select
        as_of_date,
        season,
        player_id,
        'last_5' as window_key,
        5 as window_games_expected,
        min,
        pts,
        reb,
        ast,
        stl,
        blk,
        fg3m,
        tov,
        fantasy_proxy_score
    from scored_games
    where game_num <= 5

    union all

    select
        as_of_date,
        season,
        player_id,
        'prior_5' as window_key,
        5 as window_games_expected,
        min,
        pts,
        reb,
        ast,
        stl,
        blk,
        fg3m,
        tov,
        fantasy_proxy_score
    from scored_games
    where game_num between 6 and 10

    union all

    select
        as_of_date,
        season,
        player_id,
        'last_10' as window_key,
        10 as window_games_expected,
        min,
        pts,
        reb,
        ast,
        stl,
        blk,
        fg3m,
        tov,
        fantasy_proxy_score
    from scored_games
    where game_num <= 10
),
aggregated_windows as (
    select
        as_of_date,
        season,
        player_id,
        window_key,
        window_games_expected,
        count(*) as games_in_window,
        round(avg(min), 1) as avg_min,
        round(avg(pts), 1) as avg_pts,
        round(avg(reb), 1) as avg_reb,
        round(avg(ast), 1) as avg_ast,
        round(avg(stl), 1) as avg_stl,
        round(avg(blk), 1) as avg_blk,
        round(avg(fg3m), 1) as avg_fg3m,
        round(avg(tov), 1) as avg_tov,
        round(avg(fantasy_proxy_score), 1) as fantasy_proxy_score
    from windowed
    group by 1, 2, 3, 4, 5
),
last_5 as (
    select * from aggregated_windows where window_key = 'last_5'
),
prior_5 as (
    select * from aggregated_windows where window_key = 'prior_5'
),
last_10 as (
    select * from aggregated_windows where window_key = 'last_10'
),
player_means as (
    select
        as_of_date,
        season,
        player_id,
        any_value(player_name) as player_name,
        round(avg(pts), 2) as avg_pts,
        round(avg(reb), 2) as avg_reb,
        round(avg(ast), 2) as avg_ast,
        round(avg(stl), 2) as avg_stl,
        round(avg(blk), 2) as avg_blk,
        round(avg(fg3m), 2) as avg_fg3m,
        round(avg(tov), 2) as avg_tov
    from scored_games
    group by 1, 2, 3
),
league_baseline as (
    select
        as_of_date,
        season,
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
        stddev_pop(avg_tov) as league_sd_tov
    from player_means
    group by 1, 2
),
category_profile as (
    select
        p.as_of_date,
        p.season,
        p.player_id,
        round({{ safe_divide('p.avg_pts - b.league_avg_pts', 'nullif(b.league_sd_pts, 0)') }}, 2) as z_pts,
        round({{ safe_divide('p.avg_reb - b.league_avg_reb', 'nullif(b.league_sd_reb, 0)') }}, 2) as z_reb,
        round({{ safe_divide('p.avg_ast - b.league_avg_ast', 'nullif(b.league_sd_ast, 0)') }}, 2) as z_ast,
        round({{ safe_divide('p.avg_stl - b.league_avg_stl', 'nullif(b.league_sd_stl, 0)') }}, 2) as z_stl,
        round({{ safe_divide('p.avg_blk - b.league_avg_blk', 'nullif(b.league_sd_blk, 0)') }}, 2) as z_blk,
        round({{ safe_divide('p.avg_fg3m - b.league_avg_fg3m', 'nullif(b.league_sd_fg3m, 0)') }}, 2) as z_fg3m,
        round(-{{ safe_divide('p.avg_tov - b.league_avg_tov', 'nullif(b.league_sd_tov, 0)') }}, 2) as z_tov,
        round(
            coalesce({{ safe_divide('p.avg_pts - b.league_avg_pts', 'nullif(b.league_sd_pts, 0)') }}, 0)
            + coalesce({{ safe_divide('p.avg_reb - b.league_avg_reb', 'nullif(b.league_sd_reb, 0)') }}, 0)
            + coalesce({{ safe_divide('p.avg_ast - b.league_avg_ast', 'nullif(b.league_sd_ast, 0)') }}, 0)
            + coalesce({{ safe_divide('p.avg_stl - b.league_avg_stl', 'nullif(b.league_sd_stl, 0)') }}, 0)
            + coalesce({{ safe_divide('p.avg_blk - b.league_avg_blk', 'nullif(b.league_sd_blk, 0)') }}, 0)
            + coalesce({{ safe_divide('p.avg_fg3m - b.league_avg_fg3m', 'nullif(b.league_sd_fg3m, 0)') }}, 0),
            2
        ) as category_score_7cat
    from player_means p
    inner join league_baseline b
        on p.as_of_date = b.as_of_date
       and p.season = b.season
),
schedule_future as (
    select
        ds.as_of_date,
        lp.season,
        lp.player_id,
        lp.player_name,
        lp.latest_team_abbr as team_abbr,
        s.schedule_date,
        s.opponent_abbr,
        s.is_back_to_back,
        row_number() over (
            partition by ds.as_of_date, lp.season, lp.player_id
            order by s.schedule_date, s.opponent_abbr
        ) as schedule_rn
    from date_spine ds
    inner join latest_player lp
        on ds.as_of_date = lp.as_of_date
    left join {{ ref('stg_schedule_clean') }} s
        on lp.latest_team_abbr = s.team_abbr
       and lower(s.game_status) = 'scheduled'
       and s.schedule_date >= ds.as_of_date
       and s.schedule_date <= {{ next_7_day_expr }}
),
opportunity as (
    select
        as_of_date,
        season,
        player_id,
        min(case when schedule_rn = 1 then schedule_date end) as next_game_date,
        min(case when schedule_rn = 1 then opponent_abbr end) as next_opponent_abbr,
        {{ countif('schedule_date is not null') }} as games_next_7d,
        {{ countif('schedule_date is not null and is_back_to_back') }} as back_to_backs_next_7d,
        round(
            {{ countif('schedule_date is not null') }} * 1.8
            - {{ countif('schedule_date is not null and is_back_to_back') }} * 0.5,
            2
        ) as opportunity_score
    from schedule_future
    group by 1, 2, 3
),
rankings as (
    select
        ss.as_of_date,
        ss.season,
        ss.player_id,
        lp.player_name,
        lp.latest_team_abbr,
        ss.latest_game_date,
        round(
            coalesce(cp.category_score_7cat, 0) * 0.5
            + coalesce(l5.fantasy_proxy_score - ss.season_avg_fantasy_proxy, 0) * 0.8
            + coalesce(l5.avg_min - ss.season_avg_min, 0) * 0.3
            + coalesce(o.opportunity_score, 0) * 0.6,
            2
        ) as recommendation_score,
        case
            when round(
                coalesce(cp.category_score_7cat, 0) * 0.5
                + coalesce(l5.fantasy_proxy_score - ss.season_avg_fantasy_proxy, 0) * 0.8
                + coalesce(l5.avg_min - ss.season_avg_min, 0) * 0.3
                + coalesce(o.opportunity_score, 0) * 0.6,
                2
            ) >= 8 then 'strong_add'
            when round(
                coalesce(cp.category_score_7cat, 0) * 0.5
                + coalesce(l5.fantasy_proxy_score - ss.season_avg_fantasy_proxy, 0) * 0.8
                + coalesce(l5.avg_min - ss.season_avg_min, 0) * 0.3
                + coalesce(o.opportunity_score, 0) * 0.6,
                2
            ) >= 4 then 'hold_or_stream'
            else 'watchlist'
        end as recommendation_tier,
        array_to_string(
            array(
                select stat_name
                from unnest([
                    struct('PTS' as stat_name, cp.z_pts as stat_value),
                    struct('REB' as stat_name, cp.z_reb as stat_value),
                    struct('AST' as stat_name, cp.z_ast as stat_value),
                    struct('STL' as stat_name, cp.z_stl as stat_value),
                    struct('BLK' as stat_name, cp.z_blk as stat_value),
                    struct('FG3M' as stat_name, cp.z_fg3m as stat_value)
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
                    struct('PTS' as stat_name, cp.z_pts as stat_value),
                    struct('REB' as stat_name, cp.z_reb as stat_value),
                    struct('AST' as stat_name, cp.z_ast as stat_value),
                    struct('STL' as stat_name, cp.z_stl as stat_value),
                    struct('BLK' as stat_name, cp.z_blk as stat_value),
                    struct('FG3M' as stat_name, cp.z_fg3m as stat_value),
                    struct('TOV' as stat_name, cp.z_tov as stat_value)
                ])
                where stat_value <= -0.25
                order by stat_value asc, stat_name
                limit 3
            ),
            ', '
        ) as category_risks,
        dense_rank() over (
            partition by ss.as_of_date
            order by round(
                coalesce(cp.category_score_7cat, 0) * 0.5
                + coalesce(l5.fantasy_proxy_score - ss.season_avg_fantasy_proxy, 0) * 0.8
                + coalesce(l5.avg_min - ss.season_avg_min, 0) * 0.3
                + coalesce(o.opportunity_score, 0) * 0.6,
                2
            ) desc,
            l5.fantasy_proxy_score desc,
            lp.player_name
        ) as overall_rank
    from season_stats ss
    inner join latest_player lp
        on ss.as_of_date = lp.as_of_date
       and ss.season = lp.season
       and ss.player_id = lp.player_id
    left join last_5 l5
        on ss.as_of_date = l5.as_of_date
       and ss.season = l5.season
       and ss.player_id = l5.player_id
    left join category_profile cp
        on ss.as_of_date = cp.as_of_date
       and ss.season = cp.season
       and ss.player_id = cp.player_id
    left join opportunity o
        on ss.as_of_date = o.as_of_date
       and ss.season = o.season
       and ss.player_id = o.player_id
),
joined as (
    select
        r.as_of_date,
        r.season,
        r.player_id,
        r.player_name,
        r.latest_team_abbr,
        r.latest_game_date,
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
        round(l5.avg_pts - p5.avg_pts, 1) as pts_delta,
        round(l5.avg_reb - p5.avg_reb, 1) as reb_delta,
        round(l5.avg_ast - p5.avg_ast, 1) as ast_delta,
        round(l5.avg_stl - p5.avg_stl, 1) as stl_delta,
        round(l5.avg_blk - p5.avg_blk, 1) as blk_delta,
        round(l5.avg_fg3m - p5.avg_fg3m, 1) as fg3m_delta,
        round(l5.avg_min - p5.avg_min, 1) as min_delta,
        ss.season_avg_fantasy_proxy,
        l5.avg_min as avg_min_last_5,
        ss.season_avg_min,
        o.next_game_date,
        o.next_opponent_abbr,
        o.games_next_7d,
        o.back_to_backs_next_7d,
        o.opportunity_score
    from rankings r
    left join season_stats ss
        on r.as_of_date = ss.as_of_date
       and r.season = ss.season
       and r.player_id = ss.player_id
    left join last_5 l5
        on r.as_of_date = l5.as_of_date
       and r.season = l5.season
       and r.player_id = l5.player_id
    left join prior_5 p5
        on r.as_of_date = p5.as_of_date
       and r.season = p5.season
       and r.player_id = p5.player_id
    left join last_10 l10
        on r.as_of_date = l10.as_of_date
       and r.season = l10.season
       and r.player_id = l10.player_id
    left join opportunity o
        on r.as_of_date = o.as_of_date
       and r.season = o.season
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
    pts_delta,
    reb_delta,
    ast_delta,
    stl_delta,
    blk_delta,
    fg3m_delta,
    min_delta,
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
        when coalesce(category_strengths, '') != '' then 'category_edge'
        else null
    end as reason_context_code,
    case
        when games_next_7d is not null then cast(games_next_7d as {{ varchar_type() }})
        when coalesce(category_strengths, '') != '' then category_strengths
        else null
    end as reason_context_value
from enriched
