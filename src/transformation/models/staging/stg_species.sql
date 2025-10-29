{{
  config(
    materialized='view',
    schema='staging'
  )
}}

/*
Staging model for Ghibli API species data.

Source: raw.species (Ghibli API /species endpoint)
Purpose: Standardize species/creature classification data
*/

SELECT
    id,
    name,
    classification,
    eye_colors,
    hair_colors,
    people,
    films,
    loaded_at,
    source
FROM {{ source('raw', 'species') }}
