"""
Unit tests for data quality check module.

Tests all validation functions with controlled test data to ensure
correct behavior for passing and failing scenarios.
"""

import tempfile
from pathlib import Path
from typing import Generator

import duckdb
import pytest

from src.ingestion.data_quality_check import (
    generate_report,
    validate_completeness,
    validate_kaggle_api_crossref,
    validate_no_duplicates,
    validate_record_counts,
    validate_referential_integrity,
)


@pytest.fixture
def clean_test_db() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Create temporary test database with clean data (all checks pass)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_clean.duckdb"
        conn = duckdb.connect(str(db_path))

        # Create schema
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

        # Create films table
        conn.execute(
            """
            CREATE TABLE raw.films (
                id VARCHAR PRIMARY KEY,
                title VARCHAR NOT NULL,
                people VARCHAR[],
                species VARCHAR[],
                locations VARCHAR[],
                vehicles VARCHAR[]
            )
            """
        )
        conn.execute(
            """
            INSERT INTO raw.films VALUES
            ('film1', 'Spirited Away', [], [], [], []),
            ('film2', 'My Neighbor Totoro', [], [], [], [])
            """
        )

        # Create people table
        conn.execute(
            """
            CREATE TABLE raw.people (
                id VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL,
                films VARCHAR[]
            )
            """
        )
        conn.execute(
            """
            INSERT INTO raw.people VALUES
            ('person1', 'Chihiro', ['https://ghibliapi.vercel.app/films/film1']),
            ('person2', 'Satsuki', ['https://ghibliapi.vercel.app/films/film2'])
            """
        )

        # Create locations table
        conn.execute(
            """
            CREATE TABLE raw.locations (
                id VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL,
                films VARCHAR[]
            )
            """
        )
        conn.execute(
            """
            INSERT INTO raw.locations VALUES
            ('loc1', 'Bathhouse', ['https://ghibliapi.vercel.app/films/film1'])
            """
        )

        # Create species table
        conn.execute(
            """
            CREATE TABLE raw.species (
                id VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL,
                films VARCHAR[]
            )
            """
        )
        conn.execute(
            """
            INSERT INTO raw.species VALUES
            ('species1', 'Spirit', ['https://ghibliapi.vercel.app/films/film1'])
            """
        )

        # Create vehicles table
        conn.execute(
            """
            CREATE TABLE raw.vehicles (
                id VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL,
                films VARCHAR[]
            )
            """
        )
        conn.execute(
            """
            INSERT INTO raw.vehicles VALUES
            ('vehicle1', 'Catbus', ['https://ghibliapi.vercel.app/films/film2'])
            """
        )

        # Create kaggle_films table (using "name" column like real database)
        conn.execute(
            """
            CREATE TABLE raw.kaggle_films (
                name VARCHAR NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO raw.kaggle_films VALUES
            ('Spirited Away'),
            ('My Neighbor Totoro')
            """
        )

        yield conn

        conn.close()


@pytest.fixture
def duplicate_test_db() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Create test database with duplicate IDs and titles."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_duplicate.duckdb"
        conn = duckdb.connect(str(db_path))

        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

        # Films with duplicate IDs
        conn.execute(
            """
            CREATE TABLE raw.films (
                id VARCHAR,
                title VARCHAR NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO raw.films VALUES
            ('film1', 'Spirited Away'),
            ('film1', 'Duplicate Film')
            """
        )

        # Kaggle films with duplicate titles (using "name" column like real database)
        conn.execute(
            """
            CREATE TABLE raw.kaggle_films (
                name VARCHAR NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO raw.kaggle_films VALUES
            ('Spirited Away'),
            ('spirited away')
            """
        )

        yield conn

        conn.close()


@pytest.fixture
def incomplete_test_db() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Create test database with NULL critical fields."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_incomplete.duckdb"
        conn = duckdb.connect(str(db_path))

        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

        # Films with NULL title
        conn.execute(
            """
            CREATE TABLE raw.films (
                id VARCHAR,
                title VARCHAR
            )
            """
        )
        conn.execute(
            """
            INSERT INTO raw.films VALUES
            ('film1', 'Spirited Away'),
            ('film2', NULL)
            """
        )

        yield conn

        conn.close()


@pytest.fixture
def orphaned_refs_test_db() -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Create test database with orphaned film references."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_orphaned.duckdb"
        conn = duckdb.connect(str(db_path))

        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

        # Films table
        conn.execute(
            """
            CREATE TABLE raw.films (
                id VARCHAR PRIMARY KEY,
                title VARCHAR NOT NULL
            )
            """
        )
        conn.execute(
            """
            INSERT INTO raw.films VALUES
            ('film1', 'Spirited Away')
            """
        )

        # People table with orphaned reference
        conn.execute(
            """
            CREATE TABLE raw.people (
                id VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL,
                films VARCHAR[]
            )
            """
        )
        conn.execute(
            """
            INSERT INTO raw.people VALUES
            ('person1', 'Chihiro', ['https://ghibliapi.vercel.app/films/film1']),
            ('person2', 'Orphaned', ['https://ghibliapi.vercel.app/films/nonexistent'])
            """
        )

        # Empty tables for other checks
        conn.execute(
            """
            CREATE TABLE raw.locations (
                id VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL,
                films VARCHAR[]
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE raw.species (
                id VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL,
                films VARCHAR[]
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE raw.vehicles (
                id VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL,
                films VARCHAR[]
            )
            """
        )

        yield conn

        conn.close()


def test_validate_record_counts_pass(clean_test_db: duckdb.DuckDBPyConnection) -> None:
    """Test record count validation with expected counts."""
    result = validate_record_counts(clean_test_db)

    assert "films" in result
    assert result["films"]["actual"] == 2
    assert result["films"]["status"] in ["PASS", "WARN"]


def test_validate_completeness_all_complete(
    clean_test_db: duckdb.DuckDBPyConnection,
) -> None:
    """Test completeness validation with 100% complete fields."""
    result = validate_completeness(clean_test_db)

    assert "films" in result
    assert result["films"]["id"] == 100.0
    assert result["films"]["title"] == 100.0
    assert result["films"]["status"] == "PASS"


def test_validate_completeness_incomplete(
    incomplete_test_db: duckdb.DuckDBPyConnection,
) -> None:
    """Test completeness validation with NULL fields."""
    result = validate_completeness(incomplete_test_db)

    assert "films" in result
    assert result["films"]["title"] < 100.0
    assert result["films"]["status"] == "WARN"


def test_validate_no_duplicates_pass(clean_test_db: duckdb.DuckDBPyConnection) -> None:
    """Test duplicate detection with clean data."""
    result = validate_no_duplicates(clean_test_db)

    assert "films" in result
    assert result["films"]["status"] == "PASS"
    assert len(result["films"].get("duplicate_ids", [])) == 0


def test_validate_no_duplicates_fail(
    duplicate_test_db: duckdb.DuckDBPyConnection,
) -> None:
    """Test duplicate detection with duplicate IDs."""
    result = validate_no_duplicates(duplicate_test_db)

    assert "films" in result
    assert result["films"]["status"] == "FAIL"
    assert len(result["films"]["duplicate_ids"]) > 0


def test_validate_referential_integrity_pass(
    clean_test_db: duckdb.DuckDBPyConnection,
) -> None:
    """Test referential integrity with valid references."""
    result = validate_referential_integrity(clean_test_db)

    assert result["status"] == "PASS"
    assert len(result["orphaned_references"]) == 0


def test_validate_referential_integrity_fail(
    orphaned_refs_test_db: duckdb.DuckDBPyConnection,
) -> None:
    """Test referential integrity with orphaned references."""
    result = validate_referential_integrity(orphaned_refs_test_db)

    assert result["status"] == "FAIL"
    assert len(result["orphaned_references"]) > 0
    assert result["orphaned_references"][0]["table"] == "people"


def test_validate_kaggle_api_crossref(
    clean_test_db: duckdb.DuckDBPyConnection,
) -> None:
    """Test Kaggle-API cross-reference with matching titles."""
    result = validate_kaggle_api_crossref(clean_test_db)

    assert result["match_count"] == 2
    assert len(result["in_kaggle_not_api"]) == 0
    assert len(result["in_api_not_kaggle"]) == 0


def test_generate_report() -> None:
    """Test report generation with sample validation results."""
    validation_results = {
        "record_counts": {
            "films": {"actual": 22, "expected": "22", "status": "PASS"},
        },
        "completeness": {
            "films": {"id": 100.0, "title": 100.0, "status": "PASS"},
        },
        "duplicates": {
            "films": {"duplicate_ids": [], "status": "PASS"},
        },
        "referential_integrity": {
            "orphaned_references": [],
            "status": "PASS",
        },
        "kaggle_api_crossref": {
            "in_kaggle_not_api": [],
            "in_api_not_kaggle": [],
            "match_count": 22,
        },
    }

    report = generate_report(validation_results)

    assert "Data Quality Validation Report" in report
    assert "RECORD COUNTS" in report
    assert "COMPLETENESS CHECK" in report
    assert "DUPLICATE DETECTION" in report
    assert "REFERENTIAL INTEGRITY" in report
    assert "KAGGLE-API CROSS-REFERENCE" in report
    assert "SUMMARY" in report
