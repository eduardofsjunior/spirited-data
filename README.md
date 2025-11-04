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

This script processes subtitle files in `data/raw/subtitles/` and saves parsed JSON files to `data/processed/subtitles/`.

### Usage

**Process all English subtitle files (default):**
```bash
python -m src.nlp.parse_subtitles
# or explicitly:
python -m src.nlp.parse_subtitles --language en
```

**Process Japanese subtitle files:**
```bash
python -m src.nlp.parse_subtitles --language ja
```

**Process both English and Japanese files:**
```bash
python -m src.nlp.parse_subtitles --language all
```

**Process specific films:**
```bash
python -m src.nlp.parse_subtitles --films spirited_away_en princess_mononoke_en
python -m src.nlp.parse_subtitles --language ja --films spirited_away_ja princess_mononoke_ja
```

**Enable validation (recommended):**
```bash
python -m src.nlp.parse_subtitles --validate
python -m src.nlp.parse_subtitles --language ja --validate
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
    "film_slug": "spirited_away_ja",
    "language_code": "ja",
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
      "dialogue_text": "千尋、離れないで！"
    },
    ...
  ]
}
```

### Features

- **Multi-language Support**: Process English (`--language en`), Japanese (`--language ja`), or both (`--language all`)
- **Encoding Detection**: Automatically detects UTF-8 or Latin-1 encoding (UTF-8 required for Japanese files)
- **Error Handling**: Gracefully handles malformed timestamps and skips problematic entries
- **Text Cleaning**: Removes HTML tags (`<i>`, `<b>`, etc.) and normalizes whitespace (preserves Japanese characters)
- **Validation**: Compare parsed JSON with original .srt files to verify accuracy

### Output Files

Parsed JSON files are saved to:
- Location: `data/processed/subtitles/`
- Pattern: `{film_slug}_parsed.json` (preserves language suffix from input file)
- Examples: 
  - English: `spirited_away_en_parsed.json`, `princess_mononoke_en_parsed.json`
  - Japanese: `spirited_away_ja_parsed.json`, `princess_mononoke_ja_parsed.json`

### Language Parameter

The `--language` parameter controls which subtitle files are processed:
- `en` (default): Process only English files (`*_en.srt`)
- `ja`: Process only Japanese files (`*_ja.srt`)
- `all`: Process both English and Japanese files

**Backward Compatibility**: Default behavior (`--language en` or no parameter) processes only English files, preserving Story 3.1 functionality.

## Multilingual Emotion Analysis

After parsing subtitle files, perform emotion analysis using HuggingFace transformers:

```bash
python -m src.nlp.analyze_emotions
```

This script analyzes parsed subtitle dialogue and extracts 28-dimensional emotion classifications using the `AnasAlokla/multilingual_go_emotions` model.

### Usage

**Process all films and languages (default):**
```bash
python -m src.nlp.analyze_emotions
```

**Process specific films:**
```bash
python -m src.nlp.analyze_emotions --films spirited_away,princess_mononoke
```

**Process specific languages:**
```bash
python -m src.nlp.analyze_emotions --languages en,fr,es
```

**Process specific films and languages:**
```bash
python -m src.nlp.analyze_emotions --films spirited_away,princess_mononoke --languages en,fr
```

**Custom paths:**
```bash
python -m src.nlp.analyze_emotions --subtitle-dir data/processed/subtitles --db-path data/ghibli.duckdb
```

**Disable validation:**
```bash
python -m src.nlp.analyze_emotions --no-validate
```

### Output

The script creates the `raw.film_emotions` table in DuckDB with the following schema:

- **Metadata columns**: `film_slug`, `film_id`, `language_code`, `minute_offset`, `dialogue_count`
- **28 emotion columns**: `emotion_admiration`, `emotion_amusement`, `emotion_anger`, ..., `emotion_neutral`
- **Timestamp**: `loaded_at` (auto-generated)

### Supported Languages

The emotion analysis model supports 5 languages:
- **EN** (English)
- **FR** (French)
- **ES** (Spanish)
- **NL** (Dutch)
- **AR** (Arabic)

**Note**: Japanese (JA) subtitles are excluded from emotion analysis due to model limitations. JA subtitles are preserved for future analysis (Epic 7: "Lost in Translation").

### Model Information

- **Model**: `AnasAlokla/multilingual_go_emotions` (bert-base-multilingual-cased)
- **Model Size**: ~500MB (downloaded to `~/.cache/huggingface/` on first run)
- **Emotion Labels**: 28 GoEmotions labels (admiration, amusement, anger, ..., neutral)
- **Classification Type**: Multi-label (returns scores for all 28 emotions per dialogue entry)
- **Temporal Aggregation**: Emotions are aggregated by minute-level time buckets with 3-minute rolling average smoothing

### Expected Processing Time

- **Model Loading**: ~10-20 seconds (first run with download: ~2-3 minutes)
- **Per Dialogue**: ~50-100ms
- **Full Dataset**: ~1-2 hours for all 110 files (22 films × 5 languages)

### Prerequisites

- Parsed subtitle JSON files in `data/processed/subtitles/` (from Story 3.1)
- DuckDB database with `raw.films` table populated (from Epic 1)
- Dependencies: `transformers>=4.35.0`, `torch>=2.0.0` (already in `requirements.txt`)

### Key Findings

Based on comprehensive emotion analysis validation across 110 films in 5 languages:

- **Data Quality Excellence**: All validation checks passed with 100% data completeness - all emotion scores properly bounded [0,1], all 28 GoEmotions dimensions present, and zero NULL values detected across 9,873 minute-level emotion buckets
- **Emotional Complexity Champion**: *The Red Turtle* (Spanish) exhibits the highest emotional diversity across all 28 dimensions (diversity score: 0.166), reflecting its unique, dialogue-sparse storytelling
- **Peak Emotional Moments**: The highest joy peak occurs in *My Neighbor Totoro* (Arabic, minute 0, score: 0.112) during the iconic opening theme, while *Grave of the Fireflies* (French) contains the most intense fear moments during air raid sequences (score: 0.099)
- **Cross-Language Emotion Variation**: Arabic subtitles show 87% higher amusement scores compared to English, while Dutch subtitles exhibit 64% higher realization scores, suggesting translation choices significantly influence emotional tone
- **Comprehensive Coverage**: Successfully validated 22 films × 5 languages (EN, FR, ES, NL, AR) with 98,963 total dialogue entries analyzed using improved 10-minute rolling average smoothing (82% noise reduction)

**Validation Report**: Full analysis available at `data/processed/emotion_analysis_report.md`

### Example Emotion Queries

Explore emotion analysis data using these DuckDB queries:

**1. Find the most joyful minute across all films:**
```sql
SELECT
    film_slug,
    language_code,
    minute_offset,
    emotion_joy,
    dialogue_count
FROM raw.film_emotions
ORDER BY emotion_joy DESC
LIMIT 5;
```

**2. Get emotion distribution for a specific film:**
```sql
SELECT
    AVG(emotion_joy) as avg_joy,
    AVG(emotion_fear) as avg_fear,
    AVG(emotion_anger) as avg_anger,
    AVG(emotion_sadness) as avg_sadness,
    AVG(emotion_love) as avg_love,
    COUNT(*) as total_minutes
FROM raw.film_emotions
WHERE film_slug = 'spirited_away_en'
    AND language_code = 'en';
```

**3. Compare emotion intensity across languages:**
```sql
SELECT
    language_code,
    AVG(emotion_joy) as avg_joy,
    AVG(emotion_fear) as avg_fear,
    AVG(emotion_anger) as avg_anger,
    COUNT(DISTINCT film_slug) as films_analyzed
FROM raw.film_emotions
GROUP BY language_code
ORDER BY avg_joy DESC;
```

**4. Find emotional peaks with high anger or fear:**
```sql
SELECT
    film_slug,
    language_code,
    minute_offset,
    emotion_anger,
    emotion_fear,
    (emotion_anger + emotion_fear) as intensity
FROM raw.film_emotions
WHERE emotion_anger > 0.05 OR emotion_fear > 0.05
ORDER BY intensity DESC
LIMIT 10;
```

**5. Analyze emotional complexity (diversity) per film:**
```sql
WITH emotion_stats AS (
    SELECT
        film_slug,
        language_code,
        STDDEV(emotion_admiration + emotion_amusement + emotion_anger +
               emotion_annoyance + emotion_approval + emotion_caring +
               emotion_confusion + emotion_curiosity + emotion_desire) as diversity
    FROM raw.film_emotions
    GROUP BY film_slug, language_code
)
SELECT
    film_slug,
    language_code,
    diversity,
    RANK() OVER (ORDER BY diversity DESC) as complexity_rank
FROM emotion_stats
ORDER BY diversity DESC
LIMIT 10;
```

**Access DuckDB:**
```bash
duckdb data/ghibli.duckdb
```

## Documentation

Full documentation available in [docs/](docs/README.md).
