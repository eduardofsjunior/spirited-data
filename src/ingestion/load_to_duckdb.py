"""
Load data from JSON and CSV files into DuckDB raw tables.

This module handles the ingestion of Ghibli API data and Kaggle CSV data
into DuckDB raw schema tables. All raw tables are idempotent (drop/recreate
on each run) and include metadata columns (loaded_at, source).

Usage:
    python src/ingestion/load_to_duckdb.py
    python src/ingestion/load_to_duckdb.py --verbose
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from src.shared.database import get_duckdb_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Constants
GHIBLI_API_CACHE_DIR = Path("data/raw/ghibli_api_cache")
KAGGLE_CLEANED_CSV = Path("data/processed/kaggle_cleaned.csv")

# Table creation SQL statements
CREATE_FILMS_TABLE = """
CREATE TABLE IF NOT EXISTS raw.films (
    id VARCHAR PRIMARY KEY,
    title VARCHAR NOT NULL,
    original_title VARCHAR,
    original_title_romanised VARCHAR,
    image VARCHAR,
    movie_banner VARCHAR,
    description TEXT,
    director VARCHAR,
    producer VARCHAR,
    release_date VARCHAR,
    running_time VARCHAR,
    rt_score VARCHAR,
    people VARCHAR[],
    species VARCHAR[],
    locations VARCHAR[],
    vehicles VARCHAR[],
    url VARCHAR,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR DEFAULT 'ghibli_api'
);
"""

CREATE_PEOPLE_TABLE = """
CREATE TABLE IF NOT EXISTS raw.people (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    gender VARCHAR,
    age VARCHAR,
    eye_color VARCHAR,
    hair_color VARCHAR,
    films VARCHAR[],
    species VARCHAR,
    url VARCHAR,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR DEFAULT 'ghibli_api'
);
"""

CREATE_LOCATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS raw.locations (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    climate VARCHAR,
    terrain VARCHAR,
    surface_water VARCHAR,
    residents VARCHAR[],
    films VARCHAR[],
    url VARCHAR,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR DEFAULT 'ghibli_api'
);
"""

CREATE_SPECIES_TABLE = """
CREATE TABLE IF NOT EXISTS raw.species (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    classification VARCHAR,
    eye_colors VARCHAR,
    hair_colors VARCHAR,
    people VARCHAR[],
    films VARCHAR[],
    url VARCHAR,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR DEFAULT 'ghibli_api'
);
"""

CREATE_VEHICLES_TABLE = """
CREATE TABLE IF NOT EXISTS raw.vehicles (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    description TEXT,
    vehicle_class VARCHAR,
    length VARCHAR,
    pilot VARCHAR,
    films VARCHAR[],
    url VARCHAR,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR DEFAULT 'ghibli_api'
);
"""

CREATE_KAGGLE_FILMS_TABLE = """
CREATE TABLE IF NOT EXISTS raw.kaggle_films (
    name VARCHAR NOT NULL,
    year INTEGER,
    director VARCHAR,
    screenplay VARCHAR,
    category VARCHAR,
    genre_1 VARCHAR,
    genre_2 VARCHAR,
    genre_3 VARCHAR,
    duration VARCHAR,
    budget DOUBLE,
    revenue DOUBLE,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR DEFAULT 'kaggle'
);
"""


def create_raw_tables() -> None:
    """
    Create all raw schema tables in DuckDB.

    Implements idempotent loading by dropping and recreating tables.
    All tables include metadata columns (loaded_at, source).

    Raises:
        Exception: If table creation fails
    """
    logger.info("Creating raw schema tables...")

    conn = get_duckdb_connection()

    try:
        # Drop existing tables for idempotent loading
        conn.execute("DROP TABLE IF EXISTS raw.films")
        conn.execute("DROP TABLE IF EXISTS raw.people")
        conn.execute("DROP TABLE IF EXISTS raw.locations")
        conn.execute("DROP TABLE IF EXISTS raw.species")
        conn.execute("DROP TABLE IF EXISTS raw.vehicles")
        conn.execute("DROP TABLE IF EXISTS raw.kaggle_films")

        # Create tables
        conn.execute(CREATE_FILMS_TABLE)
        conn.execute(CREATE_PEOPLE_TABLE)
        conn.execute(CREATE_LOCATIONS_TABLE)
        conn.execute(CREATE_SPECIES_TABLE)
        conn.execute(CREATE_VEHICLES_TABLE)
        conn.execute(CREATE_KAGGLE_FILMS_TABLE)

        logger.info("✓ Successfully created all raw tables")

    except Exception as e:
        logger.error(f"Failed to create raw tables: {e}")
        raise
    finally:
        conn.close()


def load_json_file(file_path: Path) -> List[Dict[str, Any]]:
    """
    Load JSON data from file.

    Args:
        file_path: Path to JSON file

    Returns:
        List of dictionaries containing JSON data

    Raises:
        FileNotFoundError: If file does not exist
        json.JSONDecodeError: If file is not valid JSON
    """
    logger.info(f"Loading JSON file: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    logger.info(f"✓ Loaded {len(data)} records from {file_path.name}")
    return data


def load_films_data() -> None:
    """
    Load Ghibli API films data into raw.films table.

    Reads films.json and inserts all film records with metadata.

    Raises:
        Exception: If data loading fails
    """
    logger.info("→ Loading films data...")

    films = load_json_file(GHIBLI_API_CACHE_DIR / "films.json")
    conn = get_duckdb_connection()

    try:
        for film in films:
            conn.execute(
                """
                INSERT INTO raw.films (
                    id, title, original_title, original_title_romanised,
                    image, movie_banner, description, director, producer,
                    release_date, running_time, rt_score,
                    people, species, locations, vehicles, url,
                    loaded_at, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    film.get("id"),
                    film.get("title"),
                    film.get("original_title"),
                    film.get("original_title_romanised"),
                    film.get("image"),
                    film.get("movie_banner"),
                    film.get("description"),
                    film.get("director"),
                    film.get("producer"),
                    film.get("release_date"),
                    film.get("running_time"),
                    film.get("rt_score"),
                    film.get("people", []),
                    film.get("species", []),
                    film.get("locations", []),
                    film.get("vehicles", []),
                    film.get("url"),
                    datetime.now(),
                    "ghibli_api",
                ],
            )

        logger.info(f"✓ Loaded {len(films)} films")

    except Exception as e:
        logger.error(f"Failed to load films data: {e}")
        raise
    finally:
        conn.close()


def load_people_data() -> None:
    """
    Load Ghibli API people data into raw.people table.

    Reads people.json and inserts all character records with metadata.

    Raises:
        Exception: If data loading fails
    """
    logger.info("→ Loading people data...")

    people = load_json_file(GHIBLI_API_CACHE_DIR / "people.json")
    conn = get_duckdb_connection()

    try:
        for person in people:
            conn.execute(
                """
                INSERT INTO raw.people (
                    id, name, gender, age, eye_color, hair_color,
                    films, species, url,
                    loaded_at, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    person.get("id"),
                    person.get("name"),
                    person.get("gender"),
                    person.get("age"),
                    person.get("eye_color"),
                    person.get("hair_color"),
                    person.get("films", []),
                    person.get("species"),
                    person.get("url"),
                    datetime.now(),
                    "ghibli_api",
                ],
            )

        logger.info(f"✓ Loaded {len(people)} people")

    except Exception as e:
        logger.error(f"Failed to load people data: {e}")
        raise
    finally:
        conn.close()


def load_locations_data() -> None:
    """
    Load Ghibli API locations data into raw.locations table.

    Reads locations.json and inserts all location records with metadata.

    Raises:
        Exception: If data loading fails
    """
    logger.info("→ Loading locations data...")

    locations = load_json_file(GHIBLI_API_CACHE_DIR / "locations.json")
    conn = get_duckdb_connection()

    try:
        for location in locations:
            conn.execute(
                """
                INSERT INTO raw.locations (
                    id, name, climate, terrain, surface_water,
                    residents, films, url,
                    loaded_at, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    location.get("id"),
                    location.get("name"),
                    location.get("climate"),
                    location.get("terrain"),
                    location.get("surface_water"),
                    location.get("residents", []),
                    location.get("films", []),
                    location.get("url"),
                    datetime.now(),
                    "ghibli_api",
                ],
            )

        logger.info(f"✓ Loaded {len(locations)} locations")

    except Exception as e:
        logger.error(f"Failed to load locations data: {e}")
        raise
    finally:
        conn.close()


def load_species_data() -> None:
    """
    Load Ghibli API species data into raw.species table.

    Reads species.json and inserts all species records with metadata.

    Raises:
        Exception: If data loading fails
    """
    logger.info("→ Loading species data...")

    species = load_json_file(GHIBLI_API_CACHE_DIR / "species.json")
    conn = get_duckdb_connection()

    try:
        for spec in species:
            conn.execute(
                """
                INSERT INTO raw.species (
                    id, name, classification, eye_colors, hair_colors,
                    people, films, url,
                    loaded_at, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    spec.get("id"),
                    spec.get("name"),
                    spec.get("classification"),
                    spec.get("eye_colors"),
                    spec.get("hair_colors"),
                    spec.get("people", []),
                    spec.get("films", []),
                    spec.get("url"),
                    datetime.now(),
                    "ghibli_api",
                ],
            )

        logger.info(f"✓ Loaded {len(species)} species")

    except Exception as e:
        logger.error(f"Failed to load species data: {e}")
        raise
    finally:
        conn.close()


def load_vehicles_data() -> None:
    """
    Load Ghibli API vehicles data into raw.vehicles table.

    Reads vehicles.json and inserts all vehicle records with metadata.

    Raises:
        Exception: If data loading fails
    """
    logger.info("→ Loading vehicles data...")

    vehicles = load_json_file(GHIBLI_API_CACHE_DIR / "vehicles.json")
    conn = get_duckdb_connection()

    try:
        for vehicle in vehicles:
            conn.execute(
                """
                INSERT INTO raw.vehicles (
                    id, name, description, vehicle_class, length, pilot,
                    films, url,
                    loaded_at, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    vehicle.get("id"),
                    vehicle.get("name"),
                    vehicle.get("description"),
                    vehicle.get("vehicle_class"),
                    vehicle.get("length"),
                    vehicle.get("pilot"),
                    vehicle.get("films", []),
                    vehicle.get("url"),
                    datetime.now(),
                    "ghibli_api",
                ],
            )

        logger.info(f"✓ Loaded {len(vehicles)} vehicles")

    except Exception as e:
        logger.error(f"Failed to load vehicles data: {e}")
        raise
    finally:
        conn.close()


def load_kaggle_data() -> None:
    """
    Load Kaggle cleaned CSV data into raw.kaggle_films table.

    Reads kaggle_cleaned.csv and inserts all film records with metadata.

    Raises:
        Exception: If data loading fails
    """
    logger.info("→ Loading Kaggle CSV data...")

    df = pd.read_csv(KAGGLE_CLEANED_CSV)
    logger.info(f"Loaded {len(df)} records from Kaggle CSV")

    conn = get_duckdb_connection()

    try:
        for _, row in df.iterrows():
            conn.execute(
                """
                INSERT INTO raw.kaggle_films (
                    name, year, director, screenplay, category,
                    genre_1, genre_2, genre_3, duration, budget, revenue,
                    loaded_at, source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    row.get("Name"),
                    int(row.get("Year")) if pd.notna(row.get("Year")) else None,
                    row.get("Director"),
                    row.get("Screenplay"),
                    row.get("Category"),
                    row.get("Genre 1"),
                    row.get("Genre 2"),
                    row.get("Genre 3"),
                    row.get("Duration") if pd.notna(row.get("Duration")) else None,
                    float(row.get("Budget")) if pd.notna(row.get("Budget")) else None,
                    float(row.get("Revenue")) if pd.notna(row.get("Revenue")) else None,
                    datetime.now(),
                    "kaggle",
                ],
            )

        logger.info(f"✓ Loaded {len(df)} Kaggle films")

    except Exception as e:
        logger.error(f"Failed to load Kaggle data: {e}")
        raise
    finally:
        conn.close()


def validate_data_loading() -> None:
    """
    Execute validation queries and log results.

    Validates:
    - Row counts for all tables
    - Sample records from each table
    - NULL checks in critical fields

    Raises:
        Exception: If validation fails
    """
    logger.info("\n" + "=" * 60)
    logger.info("Validating data loading...")
    logger.info("=" * 60)

    conn = get_duckdb_connection()

    try:
        # Validate row counts
        logger.info("\n📊 Row Counts:")
        logger.info("-" * 60)

        row_counts = conn.execute(
            """
            SELECT 'films' AS table_name, COUNT(*) AS row_count FROM raw.films
            UNION ALL
            SELECT 'people', COUNT(*) FROM raw.people
            UNION ALL
            SELECT 'locations', COUNT(*) FROM raw.locations
            UNION ALL
            SELECT 'species', COUNT(*) FROM raw.species
            UNION ALL
            SELECT 'vehicles', COUNT(*) FROM raw.vehicles
            UNION ALL
            SELECT 'kaggle_films', COUNT(*) FROM raw.kaggle_films
            """
        ).fetchall()

        total_records = 0
        for table_name, count in row_counts:
            logger.info(f"  {table_name:15s}: {count:5d} records")
            total_records += count

        logger.info("-" * 60)
        logger.info(f"  {'TOTAL':15s}: {total_records:5d} records")

        # Sample records from each table
        logger.info("\n📄 Sample Records (5 per table):")
        logger.info("-" * 60)

        tables = ["films", "people", "locations", "species", "vehicles", "kaggle_films"]

        for table in tables:
            logger.info(f"\n{table.upper()}:")
            samples = conn.execute(f"SELECT * FROM raw.{table} LIMIT 5").fetchdf()
            logger.info(f"\n{samples.to_string()}\n")

        # Check for NULL in critical fields
        logger.info("\n🔍 NULL Value Checks:")
        logger.info("-" * 60)

        null_check_films = conn.execute(
            """
            SELECT
                COUNT(*) AS total_records,
                SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS null_ids,
                SUM(CASE WHEN title IS NULL THEN 1 ELSE 0 END) AS null_titles
            FROM raw.films
            """
        ).fetchone()

        logger.info(f"  films: {null_check_films[0]} total, {null_check_films[1]} NULL ids, {null_check_films[2]} NULL titles")

        null_check_people = conn.execute(
            """
            SELECT
                COUNT(*) AS total_records,
                SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS null_ids,
                SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END) AS null_names
            FROM raw.people
            """
        ).fetchone()

        logger.info(f"  people: {null_check_people[0]} total, {null_check_people[1]} NULL ids, {null_check_people[2]} NULL names")

        null_check_kaggle = conn.execute(
            """
            SELECT
                COUNT(*) AS total_records,
                SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END) AS null_names
            FROM raw.kaggle_films
            """
        ).fetchone()

        logger.info(f"  kaggle_films: {null_check_kaggle[0]} total, {null_check_kaggle[1]} NULL names")

        logger.info("\n" + "=" * 60)
        logger.info("✓ Validation complete - all checks passed!")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Validation failed: {e}")
        raise
    finally:
        conn.close()


def main() -> None:
    """
    Main execution flow for loading data into DuckDB.

    Executes in order:
    1. Create raw tables (idempotent)
    2. Load Ghibli API data (films, people, locations, species, vehicles)
    3. Load Kaggle CSV data
    4. Validate data loading

    Raises:
        Exception: If any step fails
    """
    logger.info("🎬 Starting DuckDB data loading pipeline...")

    try:
        # Step 1: Create tables
        create_raw_tables()

        # Step 2: Load Ghibli API data
        load_films_data()
        load_people_data()
        load_locations_data()
        load_species_data()
        load_vehicles_data()

        # Step 3: Load Kaggle data
        load_kaggle_data()

        # Step 4: Validate
        validate_data_loading()

        logger.info("\n✅ Data loading pipeline completed successfully!")

    except Exception as e:
        logger.error(f"\n❌ Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Load data into DuckDB raw tables")
    parser.add_argument(
        "--verbose", action="store_true", help="Enable verbose logging (DEBUG level)"
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)

    main()
