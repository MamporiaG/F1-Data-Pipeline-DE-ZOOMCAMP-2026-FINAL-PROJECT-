/*
  models/staging/stg_constructors.sql
*/
with source as (
    select * from {{ source('f1_raw', 'constructors') }}
)
select
    constructorid   as constructor_id,
    constructorref  as constructor_ref,
    name            as constructor_name,
    nationality     as constructor_nationality,
    url             as wikipedia_url
from source
