{{
  config(
    materialized='view',
    schema='staging'
  )
}}

/*
Staging model for Ghibli API people data.

Source: raw.people (Ghibli API /people endpoint)
Purpose: Clean and standardize character data with proper gender normalization
Note: API calls them "people" but they represent characters in films
*/

SELECT
    id,
    name,
    CASE
        WHEN gender IN ('Male', 'Female') THEN gender
        WHEN gender = 'NA' THEN NULL
        ELSE NULL
    END AS gender,
    age,
    eye_color,
    hair_color,
    CASE
        WHEN species IS NOT NULL AND species != '' THEN SPLIT_PART(species, '/', -1)
        ELSE NULL
    END AS species_id,
    films,
    loaded_at,
    source
FROM {{ source('raw', 'people') }}
