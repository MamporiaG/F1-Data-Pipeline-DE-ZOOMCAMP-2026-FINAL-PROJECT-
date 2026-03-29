/*
  models/staging/stg_status.sql
  Lookup table for race finish status codes.
  e.g. status_id=1 → "Finished", status_id=4 → "Accident"
*/
with source as (
    select * from {{ source('f1_raw', 'status') }}
)
select
    statusid    as status_id,
    status      as status_description
from source
