{{
  config(
    materialized='ephemeral'
  )
}}

/*
  Intermediate model: Film-Location Edges
  
  Purpose: Extract film-location relationships from nested array structure.
  Source: raw.films.locations[] (TEXT[] of location URLs) joined with raw.locations
  
  This model unnests the locations array from films table and joins with locations table
  to create edges for the knowledge graph. Each edge represents a film being filmed at a location.
*/

-- CTE 1: Unnest locations array from films
WITH film_locations_unnest AS (
    SELECT
        id AS film_id,
        title AS film_title,
        UNNEST(locations) AS location_url
    FROM {{ source('raw', 'films') }}
    WHERE locations IS NOT NULL
        AND ARRAY_LENGTH(locations) > 0
),

-- CTE 2: Lookup table with location IDs and URLs
locations_with_ids AS (
    SELECT
        id AS location_id,
        name AS location_name,
        url AS location_url
    FROM {{ source('raw', 'locations') }}
)

-- Final SELECT: Join and generate edge properties
SELECT
    CONCAT('edge_loc_film_', ROW_NUMBER() OVER (ORDER BY f.film_id, l.location_id)) AS edge_id,
    CONCAT('location_', l.location_id) AS source_node_id,
    CONCAT('film_', f.film_id) AS target_node_id,
    'filmed_at' AS edge_type,
    'location' AS source_node_type,
    'film' AS target_node_type,
    l.location_name AS source_node_name,
    f.film_title AS target_node_name,
    CURRENT_TIMESTAMP AS created_at
FROM film_locations_unnest f
INNER JOIN locations_with_ids l ON f.location_url = l.location_url
