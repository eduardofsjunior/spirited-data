{{
  config(
    materialized='view',
    schema='staging'
  )
}}

/*
Staging model for Ghibli API locations data.

Source: raw.locations (Ghibli API /locations endpoint)
Purpose: Clean and standardize location data with type-safe numeric fields
*/

SELECT
    id,
    name,
    COALESCE(climate, '') AS climate,
    COALESCE(terrain, '') AS terrain,
    CASE
        WHEN surface_water IS NOT NULL AND surface_water != ''
        THEN CAST(REPLACE(surface_water, '%', '') AS DOUBLE)
        ELSE NULL
    END AS surface_water_pct,
    residents,
    films,
    loaded_at,
    source
FROM {{ source('raw', 'locations') }}
