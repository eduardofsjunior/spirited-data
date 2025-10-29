{{
  config(
    materialized='ephemeral'
  )
}}

/*
  Intermediate model: Director-Film Edges
  
  Purpose: Extract director-film relationships from film data.
  Source: stg_films (cleaned staging model with director field)
  
  This model creates edges connecting directors to films they directed.
  Note: Directors don't have natural UUIDs in the source data (only name strings),
  so we use MD5 hash of director name to create stable, consistent IDs.
  This ensures the same director always gets the same ID across runs.
*/

SELECT
    CONCAT('edge_dir_film_', ROW_NUMBER() OVER (ORDER BY MD5(director), id)) AS edge_id,
    CONCAT('director_', MD5(director)) AS source_node_id,
    CONCAT('film_', id) AS target_node_id,
    'directed' AS edge_type,
    'director' AS source_node_type,
    'film' AS target_node_type,
    director AS source_node_name,
    title AS target_node_name,
    CURRENT_TIMESTAMP AS created_at
FROM {{ ref('stg_films') }}
WHERE director IS NOT NULL
    AND director != ''
