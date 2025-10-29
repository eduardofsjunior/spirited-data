{{
  config(
    materialized='view',
    schema='staging'
  )
}}

/*
Staging model for Ghibli API films data.

Source: raw.films (Ghibli API /films endpoint)
Purpose: Clean, type-cast, and standardize film data for downstream models
*/

SELECT
    id,
    title,
    CAST(release_date AS INTEGER) AS release_year,
    CAST(running_time AS INTEGER) AS running_time,
    CAST(rt_score AS INTEGER) AS rt_score,
    COALESCE(director, '') AS director,
    COALESCE(producer, '') AS producer,
    COALESCE(description, '') AS description,
    image,
    movie_banner,
    people,
    species,
    locations,
    vehicles,
    loaded_at,
    source
FROM {{ source('raw', 'films') }}
