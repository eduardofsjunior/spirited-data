{{
  config(
    materialized='ephemeral'
  )
}}

/*
  Intermediate model: Film-Location Edges
  
  Purpose: Extract film-location relationships from nested array structure.
  Source: raw.locations.films[] (TEXT[] of film URLs) joined with raw.films
  
  This model uses reverse direction extraction: it unnests the films array from locations table
  and joins with films table to create edges for the knowledge graph. Each edge represents a
  film being filmed at a location.
  
  Why reverse direction?
  - films.locations[] contains base URLs only: 'https://ghibliapi.vercel.app/locations/' (no UUIDs)
  - locations.films[] contains full URLs: 'https://ghibliapi.vercel.app/films/{uuid}' (with UUIDs)
  - Using locations.films[] → films.url ensures proper URL matching and produces ~26 edges
  - Edge direction (Location → Film) remains correct: source_node_id = location, target_node_id = film
*/

-- CTE 1: Unnest films array from locations (reverse direction)
WITH location_films_unnest AS (
    SELECT
        id AS location_id,
        name AS location_name,
        UNNEST(films) AS film_url
    FROM {{ source('raw', 'locations') }}
    WHERE films IS NOT NULL
        AND ARRAY_LENGTH(films) > 0
),

-- CTE 2: Lookup table with film IDs and URLs
films_with_ids AS (
    SELECT
        id AS film_id,
        title AS film_title,
        url AS film_url
    FROM {{ source('raw', 'films') }}
)

-- Final SELECT: Join and generate edge properties
SELECT
    CONCAT('edge_loc_film_', ROW_NUMBER() OVER (ORDER BY l.location_id, f.film_id)) AS edge_id,
    CONCAT('location_', l.location_id) AS source_node_id,
    CONCAT('film_', f.film_id) AS target_node_id,
    'filmed_at' AS edge_type,
    'location' AS source_node_type,
    'film' AS target_node_type,
    l.location_name AS source_node_name,
    f.film_title AS target_node_name,
    CURRENT_TIMESTAMP AS created_at
FROM location_films_unnest l
INNER JOIN films_with_ids f ON l.film_url = f.film_url
