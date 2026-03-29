/*
  models/staging/stg_constructor_standings.sql
*/
with source as (
    select * from {{ source('f1_raw', 'constructor_standings') }}
)
select
    constructorstandingsid  as standing_id,
    raceid                  as race_id,
    constructorid           as constructor_id,
    points                  as cumulative_points,
    position                as championship_position,
    positiontext            as position_text,
    wins                    as cumulative_wins
from source
