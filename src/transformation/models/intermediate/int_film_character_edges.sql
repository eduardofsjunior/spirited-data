{{
  config(
    materialized='ephemeral'
  )
}}

/*
  Intermediate model: Character-Film Edges (Reverse Direction)
  
  Purpose: Extract character-film relationships from nested array structure.
  Source: raw.people.films[] (TEXT[] of film URLs) joined with raw.films
  
  This model uses reverse direction extraction: it unnests the films array from people table
  and joins with films table to create edges for the knowledge graph. Each edge represents a
  character appearing in a film.
  
  Why reverse direction?
  - films.people[] contains base URLs only for 17 films: 'https://ghibliapi.vercel.app/people/' (no UUIDs)
  - people.films[] contains full URLs: 'https://ghibliapi.vercel.app/films/{uuid}' (with UUIDs)
  - Using people.films[] → films.url ensures proper URL matching and produces ~59 edges (vs 34 with forward direction)
  - Edge direction (Character → Film) remains correct: source_node_id = character, target_node_id = film
  
  This mirrors the approach used in int_film_location_edges.sql for the same data quality reason.
*/

-- CTE 1: Unnest films array from people (reverse direction)
WITH people_films_unnest AS (
    SELECT
        id AS person_id,
        name AS person_name,
        UNNEST(films) AS film_url
    FROM {{ source('raw', 'people') }}
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
    CONCAT('edge_char_film_', ROW_NUMBER() OVER (ORDER BY p.person_id, f.film_id)) AS edge_id,
    CONCAT('character_', p.person_id) AS source_node_id,
    CONCAT('film_', f.film_id) AS target_node_id,
    'appears_in' AS edge_type,
    'character' AS source_node_type,
    'film' AS target_node_type,
    p.person_name AS source_node_name,
    f.film_title AS target_node_name,
    CURRENT_TIMESTAMP AS created_at
FROM people_films_unnest p
INNER JOIN films_with_ids f ON p.film_url = f.film_url
