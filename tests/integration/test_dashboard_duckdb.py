"""Integration tests for validation dashboard with real DuckDB."""

import pytest
from pathlib import Path
import duckdb
import pandas as pd
from unittest.mock import patch

from src.validation.dashboard import load_films


@pytest.fixture
def test_duckdb():
    """
    Create temporary test DuckDB database with sample data.

    Yields:
        duckdb.DuckDBPyConnection: Connection to test database

    Cleanup:
        Removes test database file after test completion
    """
    test_db_path = Path("tests/fixtures/test_dashboard.duckdb")

    # Remove existing test database if present
    if test_db_path.exists():
        test_db_path.unlink()

    # Create test database
    conn = duckdb.connect(str(test_db_path))

    # Create schemas (using dbt naming convention: main_staging, main_marts)
    conn.execute("CREATE SCHEMA IF NOT EXISTS main_staging")
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute("CREATE SCHEMA IF NOT EXISTS main_marts")

    # Create test films table
    conn.execute("""
        CREATE TABLE main_staging.stg_films (
            id VARCHAR PRIMARY KEY,
            title VARCHAR NOT NULL,
            release_year INTEGER,
            director VARCHAR,
            rt_score INTEGER,
            running_time INTEGER,
            description TEXT
        )
    """)

    # Insert sample film data
    sample_films = [
        ("film-1", "Castle in the Sky", 1986, "Hayao Miyazaki", 95, 125, "A young orphan girl..."),
        ("film-2", "My Neighbor Totoro", 1988, "Hayao Miyazaki", 93, 86, "Two sisters discover..."),
        ("film-3", "Spirited Away", 2001, "Hayao Miyazaki", 97, 125, "A young girl enters..."),
        ("film-4", "Princess Mononoke", 1997, "Hayao Miyazaki", 93, 134, "A prince fights..."),
        ("film-5", "Grave of the Fireflies", 1988, "Isao Takahata", 97, 89, "Two siblings struggle..."),
    ]

    for film in sample_films:
        conn.execute(
            "INSERT INTO main_staging.stg_films VALUES (?, ?, ?, ?, ?, ?, ?)",
            film
        )

    yield conn

    # Cleanup
    conn.close()
    if test_db_path.exists():
        test_db_path.unlink()


class TestDashboardDuckDBIntegration:
    """Integration tests with real DuckDB database."""

    def test_load_films_from_real_database(self, test_duckdb):
        """Test loading films from real DuckDB staging table."""
        # Execute
        films_df = load_films(test_duckdb)

        # Verify
        assert isinstance(films_df, pd.DataFrame)
        assert len(films_df) == 5
        assert "id" in films_df.columns
        assert "title" in films_df.columns
        assert "release_year" in films_df.columns
        assert "director" in films_df.columns

        # Verify data content
        assert "Spirited Away" in films_df["title"].tolist()
        assert "Hayao Miyazaki" in films_df["director"].tolist()

    def test_films_sorted_by_title(self, test_duckdb):
        """Test that films are sorted alphabetically by title."""
        # Execute
        films_df = load_films(test_duckdb)

        # Verify sorting
        titles = films_df["title"].tolist()
        assert titles == sorted(titles)
        assert titles[0] == "Castle in the Sky"

    def test_load_films_with_multiple_directors(self, test_duckdb):
        """Test loading films from multiple directors."""
        # Execute
        films_df = load_films(test_duckdb)

        # Verify
        directors = films_df["director"].unique().tolist()
        assert len(directors) == 2
        assert "Hayao Miyazaki" in directors
        assert "Isao Takahata" in directors

    def test_film_data_types(self, test_duckdb):
        """Test that film data has correct types after loading."""
        # Execute
        films_df = load_films(test_duckdb)

        # Verify types
        assert films_df["id"].dtype == object
        assert films_df["title"].dtype == object
        # Accept int32 or int64 (DuckDB may return either)
        assert films_df["release_year"].dtype in [int, "int32", "int64", "Int64"]
        assert films_df["director"].dtype == object

    def test_film_id_uniqueness(self, test_duckdb):
        """Test that film IDs are unique."""
        # Execute
        films_df = load_films(test_duckdb)

        # Verify
        assert films_df["id"].is_unique
        assert len(films_df["id"]) == len(films_df["id"].unique())

    def test_no_null_required_fields(self, test_duckdb):
        """Test that required fields (id, title) have no null values."""
        # Execute
        films_df = load_films(test_duckdb)

        # Verify
        assert films_df["id"].notna().all()
        assert films_df["title"].notna().all()

    @patch("src.validation.dashboard.st")
    def test_empty_films_table(self, mock_st, test_duckdb):
        """Test handling of empty films table."""
        # Clear films table
        test_duckdb.execute("DELETE FROM main_staging.stg_films")

        # Execute
        films_df = load_films(test_duckdb)

        # Verify
        assert isinstance(films_df, pd.DataFrame)
        assert len(films_df) == 0
        mock_st.warning.assert_called_once()

    def test_film_year_range(self, test_duckdb):
        """Test that film years are in valid range."""
        # Execute
        films_df = load_films(test_duckdb)

        # Verify
        years = films_df["release_year"].dropna()
        assert (years >= 1900).all()
        assert (years <= 2025).all()


class TestDashboardPerformance:
    """Performance tests for dashboard operations."""

    def test_load_films_performance(self, test_duckdb):
        """Test that film loading completes within acceptable time."""
        import time

        # Execute with timing
        start_time = time.time()
        films_df = load_films(test_duckdb)
        elapsed_time = time.time() - start_time

        # Verify
        assert elapsed_time < 1.0  # Should complete in under 1 second
        assert len(films_df) > 0

    def test_query_efficiency(self, test_duckdb):
        """Test that query selects only required columns."""
        # Execute
        films_df = load_films(test_duckdb)

        # Verify - should only have 4 columns (id, title, release_year, director)
        assert len(films_df.columns) == 4
        assert "rt_score" not in films_df.columns
        assert "running_time" not in films_df.columns
        assert "description" not in films_df.columns

