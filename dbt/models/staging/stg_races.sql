/*
  models/staging/stg_races.sql
  ─────────────────────────────
  Cleans the raw races table.
  Parses the race date to a proper DATE column.
*/

with source as (

    select * from {{ source('f1_raw', 'races') }}

),

cleaned as (

    select
        raceid                                              as race_id,
        year                                                as season_year,
        round                                               as race_round,
        circuitid                                           as circuit_id,
        name                                                as race_name,
        safe.parse_date('%Y-%m-%d', date)                  as race_date,
        nullif(time, '\\N')                                 as race_time_utc,
        url                                                 as wikipedia_url

    from source

)

select * from cleaned
