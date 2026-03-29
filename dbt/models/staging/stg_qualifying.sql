/*
  models/staging/stg_qualifying.sql
*/
with source as (
    select * from {{ source('f1_raw', 'qualifying') }}
)
select
    qualifyid                           as qualify_id,
    raceid                              as race_id,
    driverid                            as driver_id,
    constructorid                       as constructor_id,
    number                              as car_number,
    position                            as qualifying_position,
    nullif(q1, '\\N')                  as q1_time,
    nullif(q2, '\\N')                  as q2_time,
    nullif(q3, '\\N')                  as q3_time
from source
