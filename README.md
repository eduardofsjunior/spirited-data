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

### Troubleshooting

| Error | Solution |
|-------|----------|
| "Connection failed" | Check `DUCKDB_PATH` in `.env` file |
| "Adapter not found" | Reinstall: `pip install dbt-duckdb` |
| "Database locked" | Close other DuckDB connections |

For more details, see [architecture documentation](docs/architecture/unified-project-structure.md).

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

## Documentation

Full documentation available in [docs/](docs/README.md).
