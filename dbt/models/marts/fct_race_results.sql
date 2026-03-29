/*
  models/marts/fct_race_results.sql
  ───────────────────────────────────
  PRIMARY FACT TABLE — used for most Looker Studio charts.

  One row = one driver's result in one race.
  Joins race context, driver info, constructor info, circuit info,
  and finish status into a single wide table.

  This is the table you'll query most in Looker Studio.
  It has ~26,000 rows covering every race result from 1950–2024.
*/

with results as (
    select * from {{ ref('stg_results') }}
),

races as (
    select * from {{ ref('stg_races') }}
),

drivers as (
    select * from {{ ref('stg_drivers') }}
),

constructors as (
    select * from {{ ref('stg_constructors') }}
),

circuits as (
    select * from {{ ref('stg_circuits') }}
),

status as (
    select * from {{ ref('stg_status') }}
),

final as (

    select
        -- ── Surrogate key ────────────────────────────────────────────────
        r.result_id,

        -- ── Foreign keys ─────────────────────────────────────────────────
        r.race_id,
        r.driver_id,
        r.constructor_id,

        -- ── Race context ─────────────────────────────────────────────────
        ra.season_year,
        ra.race_round,
        ra.race_name,
        ra.race_date,

        -- ── Circuit context ───────────────────────────────────────────────
        c.circuit_id,
        c.circuit_name,
        c.city                      as circuit_city,
        c.country                   as circuit_country,
        c.latitude,
        c.longitude,

        -- ── Driver context ────────────────────────────────────────────────
        d.full_name                 as driver_name,
        d.driver_code,
        d.nationality               as driver_nationality,
        d.date_of_birth,

        -- ── Constructor context ───────────────────────────────────────────
        con.constructor_name,
        con.constructor_nationality,

        -- ── Race result ───────────────────────────────────────────────────
        r.grid_position,
        r.finish_position,
        r.position_text,
        r.points_scored,
        r.laps_completed,
        r.finish_time,

        -- ── Finish status ─────────────────────────────────────────────────
        s.status_description        as finish_status,

        -- ── Fastest lap ───────────────────────────────────────────────────
        r.fastest_lap_time,
        r.fastest_lap_speed_kph,
        r.fastest_lap_rank,

        -- ── Boolean flags ─────────────────────────────────────────────────
        r.is_winner,
        r.is_podium,
        r.is_pole_position,
        r.is_points_finish,
        r.is_dnf,

        -- ── Era bucketing ─────────────────────────────────────────────────
        -- Useful for era-based analysis and filtering in Looker Studio
        case
            when ra.season_year between 1950 and 1966 then '1950s–60s Pre-Wing Era'
            when ra.season_year between 1967 and 1979 then '1967–79 Cosworth Era'
            when ra.season_year between 1980 and 1988 then '1980s Turbo Era'
            when ra.season_year between 1989 and 1994 then '1989–94 Active Car Era'
            when ra.season_year between 1995 and 2004 then '1995–04 Schumacher Era'
            when ra.season_year between 2005 and 2013 then '2005–13 Aero War Era'
            when ra.season_year between 2014 and 2021 then '2014–21 Hybrid Era'
            when ra.season_year >= 2022              then '2022+ Ground Effect Era'
        end                         as season_era

    from results         r
    left join races      ra  on r.race_id        = ra.race_id
    left join drivers    d   on r.driver_id      = d.driver_id
    left join constructors con on r.constructor_id = con.constructor_id
    left join circuits   c   on ra.circuit_id    = c.circuit_id
    left join status     s   on r.status_id      = s.status_id

)

select * from final
order by season_year, race_round, finish_position
