/*
  models/marts/dim_drivers.sql
  ──────────────────────────────
  Driver dimension with aggregated career statistics.
  One row per driver. 858 rows total.

  Use this for:
  - All-time leaderboards (most wins, most podiums, etc.)
  - Driver profile cards
  - Nationality breakdown charts
  - Career span analysis
*/

with race_results as (
    select * from {{ ref('fct_race_results') }}
),

career as (

    select
        driver_id,
        driver_name,
        driver_code,
        driver_nationality,
        date_of_birth,

        -- Career span
        min(season_year)                                        as debut_year,
        max(season_year)                                        as last_year,
        count(distinct season_year)                             as active_seasons,

        -- Race counts
        count(distinct race_id)                                 as total_races,
        countif(is_winner)                                      as total_wins,
        countif(is_podium)                                      as total_podiums,
        countif(is_pole_position)                               as total_poles,
        countif(is_points_finish)                               as total_points_finishes,
        countif(is_dnf)                                         as total_dnfs,

        -- Points
        round(sum(points_scored), 1)                            as total_points,
        round(avg(points_scored), 2)                            as avg_points_per_race,

        -- Win rate as a percentage
        round(
            safe_divide(countif(is_winner), count(distinct race_id)) * 100,
            1
        )                                                       as win_rate_pct,

        -- Podium rate
        round(
            safe_divide(countif(is_podium), count(distinct race_id)) * 100,
            1
        )                                                       as podium_rate_pct,

        -- Best finish position ever
        min(finish_position)                                    as best_finish_position,

        -- Teams (constructors) driven for
        count(distinct constructor_id)                          as num_constructors,
        string_agg(
            distinct constructor_name, ', '
            order by constructor_name limit 10
        )                                                       as constructors_driven_for

    from race_results
    where finish_position is not null
    group by
        driver_id, driver_name, driver_code,
        driver_nationality, date_of_birth

)

select * from career
order by total_wins desc, total_podiums desc
