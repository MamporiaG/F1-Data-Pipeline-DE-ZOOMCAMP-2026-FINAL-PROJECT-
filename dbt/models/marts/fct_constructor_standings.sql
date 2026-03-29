/*
  models/marts/fct_constructor_standings.sql
  ────────────────────────────────────────────
  Constructor championship standings after each race.
  Powers the constructor championship chart in Looker Studio.
*/

with standings as (
    select * from {{ ref('stg_constructor_standings') }}
),

races as (
    select * from {{ ref('stg_races') }}
),

constructors as (
    select * from {{ ref('stg_constructors') }}
),

final as (

    select
        s.standing_id,
        s.race_id,
        s.constructor_id,

        ra.season_year,
        ra.race_round,
        ra.race_name,
        ra.race_date,

        c.constructor_name,
        c.constructor_nationality,

        s.cumulative_points,
        s.championship_position,
        s.cumulative_wins,

        first_value(s.cumulative_points) over (
            partition by s.race_id
            order by s.championship_position asc
        ) - s.cumulative_points             as points_gap_to_leader

    from standings      s
    left join races     ra on s.race_id        = ra.race_id
    left join constructors c on s.constructor_id = c.constructor_id

)

select * from final
order by season_year, race_round, championship_position
