/*
  models/marts/dim_constructors.sql
  ───────────────────────────────────
  Constructor dimension with career statistics.
  One row per constructor/team.

  Use for: all-time team leaderboards, nationality maps,
  era dominance analysis.
*/

with race_results as (
    select * from {{ ref('fct_race_results') }}
),

career as (

    select
        constructor_id,
        constructor_name,
        constructor_nationality,

        min(season_year)                                        as debut_year,
        max(season_year)                                        as last_year,
        count(distinct season_year)                             as active_seasons,
        count(distinct race_id)                                 as total_race_entries,

        countif(is_winner)                                      as total_wins,
        countif(is_podium)                                      as total_podiums,
        countif(is_pole_position)                               as total_poles,
        round(sum(points_scored), 1)                            as total_points,

        round(
            safe_divide(countif(is_winner), count(distinct race_id)) * 100,
            1
        )                                                       as win_rate_pct,

        count(distinct driver_id)                               as total_drivers,
        string_agg(
            distinct driver_name, ', '
            order by driver_name limit 15
        )                                                       as notable_drivers

    from race_results
    group by constructor_id, constructor_name, constructor_nationality

)

select * from career
order by total_wins desc
