/*
  models/marts/dim_circuits.sql
  ───────────────────────────────
  Circuit dimension enriched with racing statistics.
  One row per circuit.

  Use for: world map visualisation, most-raced circuits,
  fastest circuits by average lap speed.
*/

with circuits as (
    select * from {{ ref('stg_circuits') }}
),

race_results as (
    select * from {{ ref('fct_race_results') }}
),

-- 1. Pre-calculate the most successful driver per circuit
driver_wins as (
    select
        circuit_id,
        driver_name,
        count(*) as total_wins
    from race_results
    where is_winner = true
    group by circuit_id, driver_name
),

top_driver_per_circuit as (
    select
        circuit_id,
        driver_name as most_successful_driver
    from driver_wins
    -- QUALIFY acts like a WHERE clause for window functions. 
    -- It easily grabs the #1 driver for each circuit.
    qualify row_number() over (partition by circuit_id order by total_wins desc) = 1
),

-- 2. Pre-calculate the most successful constructor per circuit
constructor_wins as (
    select
        circuit_id,
        constructor_name,
        count(*) as total_wins
    from race_results
    where is_winner = true
    group by circuit_id, constructor_name
),

top_constructor_per_circuit as (
    select
        circuit_id,
        constructor_name as most_successful_constructor
    from constructor_wins
    qualify row_number() over (partition by circuit_id order by total_wins desc) = 1
),

-- 3. Calculate main circuit stats
circuit_stats as (
    select
        c.circuit_id,
        c.circuit_name,
        c.city,
        c.country,
        c.latitude,
        c.longitude,
        c.altitude_metres,
        c.wikipedia_url,

        count(distinct r.race_id)                   as times_hosted,
        min(r.season_year)                          as first_race_year,
        max(r.season_year)                          as last_race_year

    from circuits c
    left join race_results r on c.circuit_id = r.circuit_id
    group by
        c.circuit_id, c.circuit_name, c.city, c.country,
        c.latitude, c.longitude, c.altitude_metres, c.wikipedia_url
)

-- 4. Bring it all together with standard JOINs
select
    cs.*,
    td.most_successful_driver,
    tc.most_successful_constructor
from circuit_stats cs
left join top_driver_per_circuit td on cs.circuit_id = td.circuit_id
left join top_constructor_per_circuit tc on cs.circuit_id = tc.circuit_id