{{ config(
    materialized='table',
    schema=env_var('BQ_DATASET_GOLD', env_var('BQ_DATASET', 'nba_gold'))
) }}

with latest_team as (
    select
        player_id,
        player_name,
        season,
        team_abbr,
        row_number() over (
            partition by player_id
            order by game_date desc, ingested_at_utc desc
        ) as row_num
    from {{ ref('fct_player_game_stats') }}
),
player_base as (
    select
        player_id,
        player_name,
        season,
        team_abbr
    from latest_team
    where row_num = 1
),
schedule_outlook as (
    select
        team_abbr,
        countif(schedule_date between current_date() and date_add(current_date(), interval 7 day)) as next_7d_games,
        countif(
            schedule_date between current_date() and date_add(current_date(), interval 7 day)
            and is_back_to_back
        ) as next_7d_back_to_backs,
        min(case when schedule_date >= current_date() then schedule_date end) as first_game_date,
        array_agg(
            case when schedule_date >= current_date() then opponent_abbr end
            ignore nulls
            order by schedule_date, opponent_abbr
            limit 1
        )[safe_offset(0)] as next_opponent
    from {{ ref('stg_schedule_clean') }}
    where lower(game_status) = 'scheduled'
    group by 1
)
select
    current_date() as as_of_date,
    p.season,
    p.player_id,
    p.player_name,
    p.team_abbr,
    s.first_game_date,
    s.next_opponent,
    coalesce(s.next_7d_games, 0) as next_7d_games,
    coalesce(s.next_7d_back_to_backs, 0) as next_7d_back_to_backs,
    round(
        coalesce(s.next_7d_games, 0) * 1.8
        - coalesce(s.next_7d_back_to_backs, 0) * 0.5,
        2
    ) as opportunity_score
from player_base p
left join schedule_outlook s
    on p.team_abbr = s.team_abbr
