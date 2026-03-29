/*
  models/staging/stg_driver_standings.sql
*/
with source as (
    select * from {{ source('f1_raw', 'driver_standings') }}
)
select
    driverstandingsid   as standing_id,
    raceid              as race_id,
    driverid            as driver_id,
    points              as cumulative_points,
    position            as championship_position,
    positiontext        as position_text,
    wins                as cumulative_wins
from source
