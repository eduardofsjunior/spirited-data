# SpiritedData: Studio Ghibli Knowledge Graph & RAG System

A comprehensive data engineering and AI portfolio project featuring:
- Multi-source data pipeline (Ghibli API, Kaggle, TMDB)
- DuckDB analytics database with dbt transformations
- Knowledge graph construction with NetworkX
- RAG-powered conversational AI using LangChain and OpenAI
- Interactive Streamlit visualization dashboard

## Quick Start

See [docs/guides/quickstart.md](docs/guides/quickstart.md) for setup instructions.

## Project Structure

```
ghibli_pipeline/
├── data/           # Data storage (raw, processed, vectors)
├── src/            # Source code (ingestion, transformation, AI)
├── tests/          # Test suite
├── docs/           # Documentation
└── notebooks/      # Jupyter notebooks (optional)
```

## Technology Stack

- **Languages**: Python 3.9+
- **Database**: DuckDB
- **Transformation**: dbt Core
- **AI/RAG**: LangChain, OpenAI, ChromaDB
- **Frontend**: Streamlit

## dbt Transformation Layer

### Installation

Install dbt with DuckDB adapter:

```bash
pip install "dbt-core>=1.6.0" "dbt-duckdb>=1.6.0"
```

### Setup

1. Create `~/.dbt/` directory:
   ```bash
   mkdir -p ~/.dbt
   ```

2. Copy profiles example:
   ```bash
   cp src/transformation/profiles.yml.example ~/.dbt/profiles.yml
   ```

3. Ensure `DUCKDB_PATH` is set in `.env`:
   ```bash
   DUCKDB_PATH=data/ghibli.duckdb
   ```

4. Verify connection:
   ```bash
   cd src/transformation
   dbt debug
   ```

### Common dbt Commands

- `dbt run` - Run all transformation models
- `dbt test` - Run all data quality tests
- `dbt docs generate` - Generate documentation
- `dbt docs serve` - Launch documentation site (http://localhost:8080)

### Viewing dbt Documentation

The dbt documentation site provides interactive browsing of all models, sources, tests, and data lineage.

**Generate Documentation:**

```bash
cd src/transformation
export DUCKDB_PATH=$(pwd)/../../data/ghibli.duckdb  # Use absolute path or set in .env
dbt docs generate
```

**Launch Documentation Site:**

```bash
cd src/transformation
dbt docs serve
```

The documentation site will be available at `http://localhost:8080` (default port).

**What You'll See:**

- **Model Documentation**: Browse all staging, intermediate, and mart models with descriptions and column documentation
- **DAG Visualization**: Interactive lineage graph showing data flow: sources → staging → intermediate → marts
- **Data Lineage**: Click any model to see upstream dependencies (sources and parent models) and downstream consumers
- **Column Details**: Detailed descriptions for each column including data types, business logic, and transformations
- **Test Results**: View all data quality tests and their results

**DAG Flow:**

The lineage graph shows clear data flow:
- **Sources** (bottom): `raw.films`, `raw.people`, `raw.locations`, `raw.species`, `raw.kaggle_films`
- **Staging** (middle): `stg_films`, `stg_people`, `stg_locations`, `stg_species`, `stg_kaggle_films`
- **Intermediate** (above staging): `int_film_character_edges`, `int_film_location_edges`, `int_character_species_edges`, `int_director_film_edges`
- **Marts** (top): `mart_graph_nodes`, `mart_graph_edges`

**Troubleshooting:**

| Issue | Solution |
|-------|----------|
| Port 8080 already in use | Use custom port: `dbt docs serve --port 8081` |
| "Cannot open file" error | Ensure `DUCKDB_PATH` points to existing database file (use absolute path) |
| Catalog.json not generated | Database connection may have failed - check `dbt debug` output |
| Models not appearing | Run `dbt compile` or `dbt run` first to ensure models are compiled |
| Lineage shows "No resources" | Known dbt docs UI limitation: Individual model pages may show "No resources" in Depends On/Referenced By sections, but DAG visualization (View Lineage Graph) works correctly and shows all dependencies. Dependencies exist in manifest.json. Use DAG for lineage exploration. |
| Description shows "not documented" | Ensure schema.yml files have model descriptions and regenerate docs: `dbt docs generate` |

### Troubleshooting

| Error | Solution |
|-------|----------|
| "Connection failed" | Check `DUCKDB_PATH` in `.env` file |
| "Adapter not found" | Reinstall: `pip install dbt-duckdb` |
| "Database locked" | Close other DuckDB connections |

For more details, see [architecture documentation](docs/architecture/unified-project-structure.md).

## Graph Construction

After running dbt transformations, build the NetworkX knowledge graph:

```bash
python src/graph/build_graph.py
```

This script:
- Loads nodes and edges from `marts.mart_graph_nodes` and `marts.mart_graph_edges`
- Constructs a NetworkX `MultiDiGraph` object
- Calculates graph metrics (node count, edge count, average degree, connected components)
- Generates a summary report with top nodes by degree centrality
- Saves the graph as `data/processed/ghibli_graph.pkl`
- Exports a text report to `data/processed/graph_summary_report.txt`

**Expected Output:**
- Graph file: `data/processed/ghibli_graph.pkl` (~5 MB)
- Summary report: `data/processed/graph_summary_report.txt`
- Expected graph size: ~150+ nodes, ~130+ edges

**Prerequisites:**
- dbt models must be run first (`dbt run` in `src/transformation/`)
- DuckDB database must exist at `DUCKDB_PATH` (default: `data/ghibli.duckdb`)

## Subtitle Data Acquisition

Subtitle files are **not included** in this repository due to copyright considerations.

To reproduce this project, you need to acquire English and Japanese subtitle files:

### Option 1: Automated (OpenSubtitles API)

1. Get a free API key from [OpenSubtitles.com](https://www.opensubtitles.com/en/users/newuser)
2. Add to `.env`:
   ```bash
   OPEN_SUBTITLES_API_KEY=your_key_here
   ```
3. Run the fetcher script:
   ```bash
   python fetch_subtitles_test.py
   ```

### Option 2: Manual Download

1. Download English and Japanese .srt files for at least 2 films:
   - Spirited Away (2001)
   - Princess Mononoke (1997)
2. Place files in `data/raw/subtitles/` with naming convention:
   - `spirited_away_en.srt`
   - `spirited_away_ja.srt`
   - `princess_mononoke_en.srt`
   - `princess_mononoke_ja.srt`
3. Run validation:
   ```bash
   python src/ingestion/validate_subtitles.py
   ```

**Suggested sources:**
- OpenSubtitles.org (requires free registration)
- Subscene.com

## Subtitle File Parsing

After acquiring subtitle files, parse them into structured JSON format for text analysis:

```bash
python -m src.nlp.parse_subtitles
```

This script processes all English subtitle files (`*_en.srt`) in `data/raw/subtitles/` and saves parsed JSON files to `data/processed/subtitles/`.

### Usage

**Process all English subtitle files:**
```bash
python -m src.nlp.parse_subtitles
```

**Process specific films:**
```bash
python -m src.nlp.parse_subtitles --films spirited_away_en princess_mononoke_en
```

**Enable validation (recommended):**
```bash
python -m src.nlp.parse_subtitles --validate
```

**Custom directory:**
```bash
python -m src.nlp.parse_subtitles --directory data/raw/subtitles
```

### Output Format

Parsed subtitle files are saved as JSON with the following structure:

```json
{
  "metadata": {
    "film_name": "Spirited Away",
    "film_slug": "spirited_away_en",
    "total_subtitles": 1158,
    "total_duration": 7480.5,
    "parse_timestamp": "2025-01-27T10:30:00"
  },
  "subtitles": [
    {
      "subtitle_index": 1,
      "start_time": 12.5,
      "end_time": 15.2,
      "duration": 2.7,
      "dialogue_text": "Chihiro, stay close!"
    },
    ...
  ]
}
```

### Features

- **Encoding Detection**: Automatically detects UTF-8 or Latin-1 encoding
- **Error Handling**: Gracefully handles malformed timestamps and skips problematic entries
- **Text Cleaning**: Removes HTML tags (`<i>`, `<b>`, etc.) and normalizes whitespace
- **Validation**: Compare parsed JSON with original .srt files to verify accuracy

### Output Files

Parsed JSON files are saved to:
- Location: `data/processed/subtitles/`
- Pattern: `{film_slug}_parsed.json`
- Examples: `spirited_away_en_parsed.json`, `princess_mononoke_en_parsed.json`

**Note**: Only English subtitle files (`*_en.srt`) are processed. Japanese files (`*_ja.srt`) are excluded.

## Documentation

Full documentation available in [docs/](docs/README.md).
