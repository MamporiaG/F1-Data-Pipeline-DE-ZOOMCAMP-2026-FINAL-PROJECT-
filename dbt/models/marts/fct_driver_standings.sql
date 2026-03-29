/*
  models/marts/fct_driver_standings.sql
  ───────────────────────────────────────
  Driver championship standings snapshot after every race.
  One row = one driver's standing at one point in the season.

  Use this to build "championship race" line charts in Looker Studio
  showing how points accumulated over the course of a season.
  ~33,000 rows total.
*/

with standings as (
    select * from {{ ref('stg_driver_standings') }}
),

races as (
    select * from {{ ref('stg_races') }}
),

drivers as (
    select * from {{ ref('stg_drivers') }}
),

final as (

    select
        s.standing_id,
        s.race_id,
        s.driver_id,

        ra.season_year,
        ra.race_round,
        ra.race_name,
        ra.race_date,

        d.full_name                 as driver_name,
        d.driver_code,
        d.nationality               as driver_nationality,

        s.cumulative_points,
        s.championship_position,
        s.cumulative_wins,

        -- Points gap to leader (useful for "gap to P1" chart)
        first_value(s.cumulative_points) over (
            partition by s.race_id
            order by s.championship_position asc
        ) - s.cumulative_points         as points_gap_to_leader

    from standings   s
    left join races  ra on s.race_id  = ra.race_id
    left join drivers d on s.driver_id = d.driver_id

)

select * from final
order by season_year, race_round, championship_position
