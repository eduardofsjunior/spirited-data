{{
  config(
    materialized='table',
    schema='marts'
  )
}}

/*
Mart model: Graph Nodes

Purpose: Unified catalog of all graph nodes (films, characters, locations, species, directors)
Source: staging models (stg_films, stg_people, stg_locations, stg_species, stg_kaggle_films)
Consumed by: Story 2.6 (NetworkX graph construction), Epic 4 (RAG system queries)

Structure: UNION ALL of 5 CTEs, each representing a different node type:
  - film_nodes: Film entities with metadata (release_year, director, running_time, rt_score, description, box_office)
  - character_nodes: Character entities with attributes (gender, age, eye_color, hair_color, species)
  - location_nodes: Location entities with terrain/climate info (climate, terrain, surface_water_pct)
  - species_nodes: Species/creature entities (classification, eye_colors, hair_colors)
  - director_nodes: Director entities with aggregated stats (film_count, total_box_office)

Node ID Format: {node_type}_{source_id}
  - Films: film_{ghibli_uuid}
  - Characters: character_{ghibli_uuid}
  - Locations: location_{ghibli_uuid}
  - Species: species_{ghibli_uuid}
  - Directors: director_{MD5_hash_of_director_name}
*/

WITH film_nodes AS (
  -- CTE 1: Film nodes
  -- Generates nodes for all Studio Ghibli films with enriched metadata
  SELECT
    CONCAT('film_', f.id) AS node_id,
    'film' AS node_type,
    f.title AS name,
    JSON_OBJECT(
      'release_year', f.release_year,
      'director', f.director,
      'running_time', f.running_time,
      'rt_score', f.rt_score,
      'description', f.description,
      'box_office', k.revenue
    ) AS properties
  FROM {{ ref('stg_films') }} f
  LEFT JOIN {{ ref('stg_kaggle_films') }} k
    ON f.id = k.film_id
),

character_nodes AS (
  -- CTE 2: Character nodes
  -- Generates nodes for all characters from the Ghibli API people endpoint
  SELECT
    CONCAT('character_', p.id) AS node_id,
    'character' AS node_type,
    p.name AS name,
    JSON_OBJECT(
      'gender', p.gender,
      'age', p.age,
      'eye_color', p.eye_color,
      'hair_color', p.hair_color,
      'species', p.species_id
    ) AS properties
  FROM {{ ref('stg_people') }} p
),

location_nodes AS (
  -- CTE 3: Location nodes
  -- Generates nodes for all locations from the Ghibli API locations endpoint
  SELECT
    CONCAT('location_', l.id) AS node_id,
    'location' AS node_type,
    l.name AS name,
    JSON_OBJECT(
      'climate', l.climate,
      'terrain', l.terrain,
      'surface_water', l.surface_water_pct
    ) AS properties
  FROM {{ ref('stg_locations') }} l
),

species_nodes AS (
  -- CTE 4: Species nodes
  -- Generates nodes for all species/creatures from the Ghibli API species endpoint
  SELECT
    CONCAT('species_', s.id) AS node_id,
    'species' AS node_type,
    s.name AS name,
    JSON_OBJECT(
      'classification', s.classification,
      'eye_colors', s.eye_colors,
      'hair_colors', s.hair_colors
    ) AS properties
  FROM {{ ref('stg_species') }} s
),

director_nodes AS (
  -- CTE 5: Director nodes
  -- Generates nodes for directors with aggregated film statistics
  SELECT DISTINCT
    CONCAT('director_', MD5(f.director)) AS node_id,
    'director' AS node_type,
    f.director AS name,
    JSON_OBJECT(
      'film_count', COUNT(*) OVER (PARTITION BY f.director),
      'total_box_office', SUM(COALESCE(k.revenue, 0)) OVER (PARTITION BY f.director)
    ) AS properties
  FROM {{ ref('stg_films') }} f
  LEFT JOIN {{ ref('stg_kaggle_films') }} k
    ON f.id = k.film_id
  WHERE f.director IS NOT NULL
)

SELECT * FROM film_nodes
UNION ALL
SELECT * FROM character_nodes
UNION ALL
SELECT * FROM location_nodes
UNION ALL
SELECT * FROM species_nodes
UNION ALL
SELECT * FROM director_nodes

