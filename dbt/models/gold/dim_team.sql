{{ config(
    materialized='table',
    schema=env_var('BQ_DATASET_GOLD', env_var('BQ_DATASET', 'nba_gold'))
) }}

with team_lookup as (
    select 'ATL' as team_abbr, 1610612737 as team_id, 'Atlanta' as team_city_name, 'Hawks' as team_nickname
    union all select 'BOS', 1610612738, 'Boston', 'Celtics'
    union all select 'BKN', 1610612751, 'Brooklyn', 'Nets'
    union all select 'CHA', 1610612766, 'Charlotte', 'Hornets'
    union all select 'CHI', 1610612741, 'Chicago', 'Bulls'
    union all select 'CLE', 1610612739, 'Cleveland', 'Cavaliers'
    union all select 'DAL', 1610612742, 'Dallas', 'Mavericks'
    union all select 'DEN', 1610612743, 'Denver', 'Nuggets'
    union all select 'DET', 1610612765, 'Detroit', 'Pistons'
    union all select 'GSW', 1610612744, 'Golden State', 'Warriors'
    union all select 'HOU', 1610612745, 'Houston', 'Rockets'
    union all select 'IND', 1610612754, 'Indiana', 'Pacers'
    union all select 'LAC', 1610612746, 'LA', 'Clippers'
    union all select 'LAL', 1610612747, 'Los Angeles', 'Lakers'
    union all select 'MEM', 1610612763, 'Memphis', 'Grizzlies'
    union all select 'MIA', 1610612748, 'Miami', 'Heat'
    union all select 'MIL', 1610612749, 'Milwaukee', 'Bucks'
    union all select 'MIN', 1610612750, 'Minnesota', 'Timberwolves'
    union all select 'NOP', 1610612740, 'New Orleans', 'Pelicans'
    union all select 'NYK', 1610612752, 'New York', 'Knicks'
    union all select 'OKC', 1610612760, 'Oklahoma City', 'Thunder'
    union all select 'ORL', 1610612753, 'Orlando', 'Magic'
    union all select 'PHI', 1610612755, 'Philadelphia', '76ers'
    union all select 'PHX', 1610612756, 'Phoenix', 'Suns'
    union all select 'POR', 1610612757, 'Portland', 'Trail Blazers'
    union all select 'SAC', 1610612758, 'Sacramento', 'Kings'
    union all select 'SAS', 1610612759, 'San Antonio', 'Spurs'
    union all select 'TOR', 1610612761, 'Toronto', 'Raptors'
    union all select 'UTA', 1610612762, 'Utah', 'Jazz'
    union all select 'WAS', 1610612764, 'Washington', 'Wizards'
),
observed_teams as (
    select
        team_abbr,
        max(season) as season,
        max(ingested_at_utc) as last_seen_at_utc
    from {{ ref('int_player_game_enriched') }}
    group by 1
)
select
    o.team_abbr,
    l.team_id,
    l.team_city_name,
    l.team_nickname,
    trim(concat(l.team_city_name, ' ', l.team_nickname)) as team_full_name,
    o.season,
    o.last_seen_at_utc
from observed_teams o
inner join team_lookup l
    on o.team_abbr = l.team_abbr
