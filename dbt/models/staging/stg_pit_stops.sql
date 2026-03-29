/*
  models/staging/stg_pit_stops.sql
*/
with source as (
    select * from {{ source('f1_raw', 'pit_stops') }}
)
select
    raceid                              as race_id,
    driverid                            as driver_id,
    stop                                as stop_number,
    lap                                 as lap_number,
    time                                as stop_time_of_day,
    nullif(duration, '\\N')            as stop_duration,
    nullif(milliseconds, 0)            as stop_milliseconds
from source
