{{
  config(
    materialized='ephemeral'
  )
}}

/*
  Intermediate model: Film-Character Edges
  
  Purpose: Extract character-film relationships from nested array structure.
  Source: raw.films.people[] (TEXT[] of person URLs) joined with raw.people
  
  This model unnests the people array from films table and joins with people table
  to create edges for the knowledge graph. Each edge represents a character appearing in a film.
*/

-- CTE 1: Unnest people array from films
WITH film_people_unnest AS (
    SELECT
        id AS film_id,
        title AS film_title,
        UNNEST(people) AS person_url
    FROM {{ source('raw', 'films') }}
    WHERE people IS NOT NULL
        AND ARRAY_LENGTH(people) > 0
),

-- CTE 2: Lookup table with person IDs and URLs
people_with_ids AS (
    SELECT
        id AS person_id,
        name AS person_name,
        url AS person_url
    FROM {{ source('raw', 'people') }}
)

-- Final SELECT: Join and generate edge properties
SELECT
    CONCAT('edge_char_film_', ROW_NUMBER() OVER (ORDER BY f.film_id, p.person_id)) AS edge_id,
    CONCAT('character_', p.person_id) AS source_node_id,
    CONCAT('film_', f.film_id) AS target_node_id,
    'appears_in' AS edge_type,
    'character' AS source_node_type,
    'film' AS target_node_type,
    p.person_name AS source_node_name,
    f.film_title AS target_node_name,
    CURRENT_TIMESTAMP AS created_at
FROM film_people_unnest f
INNER JOIN people_with_ids p ON f.person_url = p.person_url
