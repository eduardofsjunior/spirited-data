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
- **Purpose**: Clean and standardize raw data

### Intermediate Models (`intermediate/`)
- **Pattern**: `int_{description}.sql`
- **Examples**:
  - `int_film_character_edges.sql` - Film-character relationships
  - `int_film_location_edges.sql` - Film-location relationships
  - `int_character_species_edges.sql` - Character-species relationships
- **Materialization**: `ephemeral` (not materialized, CTEs only)
- **Purpose**: Business logic transformations, graph edge construction

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
