{{
  config(
    materialized='view',
    schema='staging'
  )
}}

/*
Staging model for Kaggle films dataset merged with Ghibli API film IDs.

Source: raw.kaggle_films (Kaggle CSV)
Purpose: Enrich Kaggle data with Ghibli API film IDs using fuzzy title matching
Note: Fuzzy matching uses LOWER(TRIM(title)) for case-insensitive comparison
Expected: 22 matched films, possibly 1 unmatched (Grave of the Fireflies)
*/

WITH kaggle_cleaned AS (
    SELECT
        -- Remove newlines, extra spaces, and years in parentheses from title
        TRIM(REGEXP_REPLACE(REGEXP_REPLACE(name, '\n.*', ''), '\s*\([0-9]{4}\)\s*$', '')) AS title_cleaned,
        name AS title_original,
        year AS release_year,
        director,
        screenplay,
        category,
        genre_1,
        genre_2,
        genre_3,
        -- Parse duration from '1h 43m' format to integer minutes
        -- Extract hours and minutes, convert to total minutes
        CASE
            WHEN duration ~ '^\d+h\s+\d+m$' THEN
                -- Format: '1h 43m'
                CAST(REGEXP_EXTRACT(duration, '^(\d+)h', 1) AS INTEGER) * 60 +
                CAST(REGEXP_EXTRACT(duration, '(\d+)m$', 1) AS INTEGER)
            WHEN duration ~ '^\d+h$' THEN
                -- Format: '1h' (hours only)
                CAST(REGEXP_EXTRACT(duration, '^(\d+)h', 1) AS INTEGER) * 60
            WHEN duration ~ '^\d+m$' THEN
                -- Format: '43m' (minutes only)
                CAST(REGEXP_EXTRACT(duration, '^(\d+)m', 1) AS INTEGER)
            WHEN duration ~ '^\d+$' THEN
                -- Already an integer (minutes)
                CAST(duration AS INTEGER)
            ELSE
                NULL  -- Invalid format
        END AS duration,
        budget,
        revenue,
        loaded_at,
        source
    FROM {{ source('raw', 'kaggle_films') }}
)

SELECT
    f.id AS film_id,
    k.title_cleaned AS title,
    k.release_year,
    k.director,
    k.screenplay,
    k.category,
    k.genre_1,
    k.genre_2,
    k.genre_3,
    k.duration,
    k.budget,
    k.revenue,
    k.loaded_at,
    k.source
FROM kaggle_cleaned k
LEFT JOIN {{ source('raw', 'films') }} f
    ON LOWER(TRIM(k.title_cleaned)) = LOWER(TRIM(f.title))
