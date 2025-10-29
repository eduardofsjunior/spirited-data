{{
  config(
    materialized='table',
    schema='marts'
  )
}}

/*
Mart model: Graph Edges

Purpose: Unified catalog of all graph edges (character-film, film-location, character-species, director-film relationships)
Source: intermediate models (int_film_character_edges, int_film_location_edges, int_character_species_edges, int_director_film_edges)
Consumed by: Story 2.6 (NetworkX graph construction), Epic 4 (RAG system queries)

Structure: UNION ALL of 4 edge types:
  - appears_in: Character → Film (character appears in film)
  - filmed_at: Film → Location (film was filmed at location)
  - is_species: Character → Species (character belongs to species)
  - directed: Director → Film (director directed film)

Edge ID Format: edge_{type}_{row_number} (already generated in intermediate models)
Edge Properties: JSON object containing relationship_strength (default 1.0) and created_at timestamp
*/

-- Film-Character edges (appears_in)
SELECT
  edge_id,
  source_node_id,
  target_node_id,
  edge_type,
  JSON_OBJECT(
    'relationship_strength', 1.0,
    'created_at', created_at
  ) AS properties
FROM {{ ref('int_film_character_edges') }}

UNION ALL

-- Film-Location edges (filmed_at)
SELECT
  edge_id,
  source_node_id,
  target_node_id,
  edge_type,
  JSON_OBJECT(
    'relationship_strength', 1.0,
    'created_at', created_at
  ) AS properties
FROM {{ ref('int_film_location_edges') }}

UNION ALL

-- Character-Species edges (is_species)
SELECT
  edge_id,
  source_node_id,
  target_node_id,
  edge_type,
  JSON_OBJECT(
    'relationship_strength', 1.0,
    'created_at', created_at
  ) AS properties
FROM {{ ref('int_character_species_edges') }}

UNION ALL

-- Director-Film edges (directed)
SELECT
  edge_id,
  source_node_id,
  target_node_id,
  edge_type,
  JSON_OBJECT(
    'relationship_strength', 1.0,
    'created_at', created_at
  ) AS properties
FROM {{ ref('int_director_film_edges') }}

