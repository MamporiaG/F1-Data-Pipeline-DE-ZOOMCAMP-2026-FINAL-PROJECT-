/*
  models/staging/stg_drivers.sql
  ────────────────────────────────
  Cleans the raw drivers table:
  - Renames columns to snake_case
  - Builds full_name computed column
  - Replaces Kaggle null marker (\N) with actual SQL NULL
  - Parses date of birth as a DATE type
*/

with source as (

    select * from {{ source('f1_raw', 'drivers') }}

),

cleaned as (

    select
        -- Keys
        driverid                                                as driver_id,
        driverref                                               as driver_ref,

        -- Identity
        nullif(code,   '\\N')                                  as driver_code,
        nullif(number, '\\N')                                  as permanent_number,
        forename                                                as first_name,
        surname                                                 as last_name,
        concat(forename, ' ', surname)                          as full_name,

        -- Demographics
        safe.parse_date('%Y-%m-%d', nullif(dob, '\\N'))        as date_of_birth,
        nationality,

        -- Metadata
        url                                                     as wikipedia_url

    from source

)

select * from cleaned
