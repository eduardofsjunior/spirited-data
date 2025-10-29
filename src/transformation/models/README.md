# dbt Models

This directory contains all dbt transformation models organized into three layers:

## Naming Conventions

### Staging Models (`staging/`)
- **Pattern**: `stg_{source}_{entity}.sql`
- **Examples**:
  - `stg_films.sql` - Staged films from Ghibli API
  - `stg_people.sql` - Staged characters from Ghibli API
  - `stg_kaggle_films.sql` - Staged Kaggle film data
- **Materialization**: `view` (lightweight, always fresh)
- **Purpose**: Clean and standardize raw data with type casting and NULL handling

## Staging Layer Documentation

### Overview
The staging layer transforms raw data from multiple sources into clean, type-safe data structures that downstream models can consume reliably. All staging models are materialized as **views** to ensure they always reflect the latest raw data without storage overhead.

### Current Staging Models

1. **stg_films** - Ghibli API films data
   - Source: `raw.films`
   - Transformations:
     - Type cast `release_date` → `release_year` (INTEGER)
     - Type cast `running_time` (INTEGER)
     - Type cast `rt_score` (INTEGER)
     - COALESCE empty strings for optional fields (director, producer, description)
   - Row count: 22 films

2. **stg_people** - Ghibli API characters data
   - Source: `raw.people`
   - Transformations:
     - Standardize gender: 'NA' → NULL, keep 'Male'/'Female'
     - Extract species_id from species URL using SPLIT_PART
     - Keep age as string (contains mixed values: numbers, "Adult", "unknown")
   - Row count: 57 characters

3. **stg_locations** - Ghibli API locations data
   - Source: `raw.locations`
   - Transformations:
     - Remove '%' symbol and convert surface_water to DOUBLE as `surface_water_pct`
     - COALESCE empty strings for climate and terrain
   - Row count: 25 locations

4. **stg_species** - Ghibli API species data
   - Source: `raw.species`
   - Transformations:
     - Preserve classification as-is (Mammal, Bird, Supernatural, etc.)
     - Keep eye_colors and hair_colors as comma-separated strings
   - Row count: 7 species

5. **stg_kaggle_films** - Kaggle CSV film data with Ghibli IDs
   - Source: `raw.kaggle_films` + `raw.films` (fuzzy join)
   - Transformations:
     - Clean title: remove newlines and year suffixes using REGEXP_REPLACE
     - Fuzzy match to Ghibli API films using LOWER(TRIM(title))
     - Type cast duration to INTEGER
     - Bring in budget and revenue as DOUBLE
   - Row count: 23 films (19 matched to Ghibli API, 4 unmatched)
   - Known unmatched: Ocean Waves, Nausicaä of the Valley of the Wind, The Secret World of Arrietty, The Boy and the Heron (not in Ghibli API)

### Querying Staging Models

All staging views are created in the `main_staging` schema. Example queries:

```sql
-- Query films with their Rotten Tomatoes scores
SELECT title, release_year, rt_score
FROM main_staging.stg_films
ORDER BY rt_score DESC;

-- Query characters by gender
SELECT gender, COUNT(*) as character_count
FROM main_staging.stg_people
GROUP BY gender;

-- Query films with Kaggle financial data
SELECT f.title, k.budget, k.revenue
FROM main_staging.stg_films f
INNER JOIN main_staging.stg_kaggle_films k ON f.id = k.film_id
WHERE k.revenue IS NOT NULL;
```

### Known Data Quality Issues

1. **Kaggle Title Matching**: 4 films in Kaggle dataset don't match Ghibli API (films not available in API)
2. **Missing Species URLs**: Some characters have NULL species_id (not all characters have species data in API)
3. **Age Field Format**: Age field contains mixed formats ("10", "Adult", "unknown") - kept as string for flexibility
4. **Gender Values**: 4 characters have NULL gender (standardized from 'NA' or missing values)

### Testing Strategy

All staging models have dbt data tests defined in `staging/schema.yml`:
- **not_null** tests on id and name/title fields
- **unique** tests on all id fields
- **relationships** test for stg_kaggle_films.film_id → stg_films.id

Run tests with:
```bash
dbt test --select staging.*
```

## Intermediate Layer Documentation

### Overview
The intermediate layer transforms staging data into graph relationship structures (edges). All intermediate models are materialized as **ephemeral** models, meaning they exist only as CTEs (Common Table Expressions) within downstream queries and are not persisted as database objects. This approach reduces storage overhead while maintaining clean, modular SQL code.

### Purpose
Intermediate models extract entity relationships from nested data structures (arrays, URLs) and prepare them for the knowledge graph. They:
- Unnest TEXT[] arrays from raw tables (e.g., `films.people[]`)
- Match URL references to entity IDs
- Generate standardized edge properties (edge_id, source_node_id, target_node_id)
- Apply consistent node ID prefixing (e.g., "character_<uuid>", "film_<uuid>")

### Current Intermediate Models

1. **int_film_character_edges** - Character-Film Relationships
   - Source: `raw.films.people[]` (TEXT[] array) joined with `raw.people`
   - Purpose: Extract "appears_in" relationships between characters and films
   - Process:
     - Unnest people array from films table
     - Join on person_url to match URLs to person records
     - Generate edge_id: `edge_char_film_<row_number>`
     - Generate source_node_id: `character_<person_uuid>`
     - Generate target_node_id: `film_<film_uuid>`
   - Edge type: `appears_in`
   - Expected edge count: ~150 edges (57 characters across 22 films)

2. **int_film_location_edges** - Film-Location Relationships
   - Source: `raw.locations.films[]` (TEXT[] array) joined with `raw.films` (reverse direction extraction)
   - Purpose: Extract "filmed_at" relationships between films and locations
   - Process:
     - Unnest films array from locations table (reverse direction due to data quality issue)
     - Join on film_url to match URLs to film records
     - Generate edge_id: `edge_loc_film_<row_number>`
     - Generate source_node_id: `location_<location_uuid>`
     - Generate target_node_id: `film_<film_uuid>`
   - Edge type: `filmed_at`
   - Expected edge count: ~26 edges
   - **Note**: Uses reverse direction (`locations.films[]` → `films.url`) because `films.locations[]` contains base URLs only (no UUIDs), while `locations.films[]` contains full URLs with UUIDs that match properly.

3. **int_character_species_edges** - Character-Species Relationships
   - Source: `raw.people.species` (VARCHAR, single URL) joined with `raw.species`
   - Purpose: Extract "is_species" relationships between characters and species
   - Process:
     - Filter people with non-NULL species field
     - Join on species_url to match URLs to species records
     - Generate edge_id: `edge_char_species_<row_number>`
     - Generate source_node_id: `character_<person_uuid>`
     - Generate target_node_id: `species_<species_uuid>`
   - Edge type: `is_species`
   - Expected edge count: ~40 edges (subset of 57 characters have species data)

4. **int_director_film_edges** - Director-Film Relationships
   - Source: `stg_films` (uses staging model, not raw)
   - Purpose: Extract "directed" relationships between directors and films
   - Process:
     - Filter films with non-NULL director field
     - Generate edge_id: `edge_dir_film_<row_number>`
     - Generate source_node_id: `director_<md5_hash>` (MD5 hash of director name)
     - Generate target_node_id: `film_<film_uuid>`
   - Edge type: `directed`
   - Expected edge count: 22 edges (one director per film)
   - **Note**: Directors don't have natural UUIDs in source data, so MD5 hash ensures stable, consistent IDs

### Naming Convention
- **Pattern**: `int_{source}_{target}_edges.sql`
- **Examples**: 
  - `int_film_character_edges.sql` (film → character relationship)
  - `int_character_species_edges.sql` (character → species relationship)

### Materialization Strategy
- **All intermediate models**: `ephemeral` (not materialized as views/tables)
- **Rationale**: 
  - Reduces database object clutter
  - Improves query performance (inlined as CTEs)
  - Only used by downstream mart models (not queried independently)

### Edge Schema Structure
All intermediate edge models follow this standardized schema:

```sql
SELECT
    edge_id VARCHAR,              -- Unique edge identifier: "edge_{type}_{number}"
    source_node_id VARCHAR,       -- Source node with type prefix: "character_<uuid>"
    target_node_id VARCHAR,       -- Target node with type prefix: "film_<uuid>"
    edge_type VARCHAR,            -- Relationship type: "appears_in", "filmed_at", etc.
    source_node_type VARCHAR,     -- Source node type: "character", "location", "director"
    target_node_type VARCHAR,     -- Target node type: "film", "species"
    source_node_name VARCHAR,     -- Human-readable source name (for debugging)
    target_node_name VARCHAR,     -- Human-readable target name (for debugging)
    created_at TIMESTAMP          -- Edge creation timestamp
FROM ...
```

### Node ID Prefixing Convention

**For entities with natural UUIDs** (films, people, locations, species):
```sql
CONCAT('film_', id)           -- "film_2baf70d1-42bb-4437-b551-e5fed5a87abe"
CONCAT('character_', id)      -- "character_ba924631-068e-4436-b6de-f3283fa848eb"
CONCAT('location_', id)       -- "location_42f787d8-1fcb-4d3d-95dc-7cd8b50dd4eb"
CONCAT('species_', id)        -- "species_af3910a6-429f-4c74-9ad5-dfe1c4aa04f2"
```

**For entities without natural IDs** (directors):
```sql
CONCAT('director_', MD5(director))  -- "director_a3f4b2c1..." (MD5 hash of name)
```

### Edge Type Vocabulary
Consistent edge_type values used across all models:
- `appears_in` - character → film
- `filmed_at` - location → film
- `is_species` - character → species
- `directed` - director → film

### How Intermediate Models Are Used

Intermediate models are referenced by mart models using dbt's `ref()` function:

```sql
-- Example: marts/mart_graph_edges.sql
{{
  config(
    materialized='table'
  )
}}

-- Union all edge types into single edges table
SELECT * FROM {{ ref('int_film_character_edges') }}
UNION ALL
SELECT * FROM {{ ref('int_film_location_edges') }}
UNION ALL
SELECT * FROM {{ ref('int_character_species_edges') }}
UNION ALL
SELECT * FROM {{ ref('int_director_film_edges') }}
```

### Array Unnesting Pattern (DuckDB)

DuckDB syntax for flattening TEXT[] arrays:

```sql
-- Unnest people array from films
WITH film_people_unnest AS (
    SELECT
        id AS film_id,
        title AS film_title,
        UNNEST(people) AS person_url  -- Flattens array into rows
    FROM {{ source('raw', 'films') }}
    WHERE people IS NOT NULL
        AND ARRAY_LENGTH(people) > 0
)
-- Result: one row per (film_id, person_url) pair
```

### Testing Strategy

**No Direct dbt Tests**: Ephemeral models can't be tested directly with dbt tests (not materialized). They're validated through:
1. **Compilation**: `dbt compile --select intermediate.*` ensures SQL syntax is valid
2. **Downstream Tests**: Mart models that reference intermediate models will fail if intermediate logic is broken
3. **Manual Validation**: Query intermediate models via downstream references to verify edge counts and data quality

Run compilation check:
```bash
cd src/transformation
dbt compile --select intermediate.*
```

### Development Workflow

1. Create intermediate model SQL file in `models/intermediate/` directory
2. Add configuration block: `{{ config(materialized='ephemeral') }}`
3. Define CTEs for data extraction and transformation
4. Generate standardized edge properties in final SELECT
5. Test compilation: `dbt compile --select model_name`
6. Reference model in downstream mart models using `{{ ref('model_name') }}`

### Mart Models (`marts/`)
- **Pattern**: `mart_{entity}.sql`
- **Examples**:
  - `mart_graph_nodes.sql` - Graph nodes for knowledge graph
  - `mart_graph_edges.sql` - Graph edges for knowledge graph
- **Materialization**: `table` (fast queries, production-ready)
- **Purpose**: Final analytical models ready for consumption

## Directory Structure

```
models/
├── sources.yml          # Raw data source definitions
├── schema.yml           # Model documentation
├── README.md            # This file
├── staging/             # Staging models (views)
├── intermediate/        # Intermediate models (ephemeral)
└── marts/               # Mart models (tables)
```

## Development Workflow

1. Create model SQL file in appropriate directory
2. Add configuration block: `{{ config(materialized='view') }}`
3. Document model in `schema.yml`
4. Test model: `dbt run --select model_name`
5. Add data tests in `schema.yml`
