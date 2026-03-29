/*
  models/staging/stg_circuits.sql
*/
with source as (
    select * from {{ source('f1_raw', 'circuits') }}
)
select
    circuitid                                               as circuit_id,
    circuitref                                              as circuit_ref,
    name                                                    as circuit_name,
    location                                                as city,
    country,
    lat                                                     as latitude,
    lng                                                     as longitude,
    nullif(cast(alt as string), '\\N')                     as altitude_metres,
    url                                                     as wikipedia_url
from source
