/*
  models/staging/stg_results.sql
  ───────────────────────────────
  The most important staging model. Cleans race results and
  adds boolean helper flags that make downstream marts simpler.
*/

with source as (

    select * from {{ source('f1_raw', 'results') }}

),

cleaned as (

    select
        -- Keys
        resultid                                                    as result_id,
        raceid                                                      as race_id,
        driverid                                                    as driver_id,
        constructorid                                               as constructor_id,
        statusid                                                    as status_id,

        -- Grid & finish
        grid                                                        as grid_position,
        cast(nullif(position, '\\N') as int64)                     as finish_position,
        positiontext                                                as position_text,
        positionorder                                               as position_order,

        -- Points & laps
        points                                                      as points_scored,
        laps                                                        as laps_completed,

        -- Timing
        nullif(time, '\\N')                                        as finish_time,
        cast(nullif(milliseconds, '\\N') as int64)                 as finish_milliseconds,

        -- Fastest lap
        cast(nullif(fastestlap, '\\N') as int64)                   as fastest_lap_number,
        cast(nullif(rank, '\\N') as int64)                         as fastest_lap_rank,
        nullif(fastestlaptime, '\\N')                              as fastest_lap_time,
        cast(nullif(fastestlapspeed, '\\N') as float64)            as fastest_lap_speed_kph,

        -- ── Boolean flags (makes mart SQL much cleaner) ───────────────────
        -- A driver won if they finished 1st in position order
        (positionorder = 1)                                         as is_winner,
        -- Podium = top 3 finishers
        (positionorder <= 3)                                        as is_podium,
        -- Pole position = started from grid position 1
        (grid = 1)                                                  as is_pole_position,
        -- Points finish = scored at least 1 point
        (points > 0)                                                as is_points_finish,
        -- DNF = did not finish (position is NULL, status is not 'Finished')
        (position is null or position = '\\N')                     as is_dnf

    from source

)

select * from cleaned
