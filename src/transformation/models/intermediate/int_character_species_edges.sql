{{
  config(
    materialized='ephemeral'
  )
}}

/*
  Intermediate model: Character-Species Edges
  
  Purpose: Extract character-species relationships from species URL field.
  Source: raw.people.species (VARCHAR, single URL) joined with raw.species
  
  This model matches characters to their species by joining on species URLs.
  Note: Unlike films.people[] which is an array, people.species is a single VARCHAR field.
  Each edge represents a character belonging to a species.
*/

-- CTE 1: Filter people with species information
WITH people_species AS (
    SELECT
        id AS person_id,
        name AS person_name,
        species AS species_url
    FROM {{ source('raw', 'people') }}
    WHERE species IS NOT NULL
        AND species != ''
),

-- CTE 2: Lookup table with species IDs and URLs
species_with_ids AS (
    SELECT
        id AS species_id,
        name AS species_name,
        url AS species_url
    FROM {{ source('raw', 'species') }}
)

-- Final SELECT: Join and generate edge properties
SELECT
    CONCAT('edge_char_species_', ROW_NUMBER() OVER (ORDER BY p.person_id, s.species_id)) AS edge_id,
    CONCAT('character_', p.person_id) AS source_node_id,
    CONCAT('species_', s.species_id) AS target_node_id,
    'is_species' AS edge_type,
    'character' AS source_node_type,
    'species' AS target_node_type,
    p.person_name AS source_node_name,
    s.species_name AS target_node_name,
    CURRENT_TIMESTAMP AS created_at
FROM people_species p
INNER JOIN species_with_ids s ON p.species_url = s.species_url
