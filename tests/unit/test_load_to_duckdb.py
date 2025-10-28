"""Unit tests for DuckDB data loading module."""

import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from src.ingestion.load_to_duckdb import (
    create_raw_tables,
    load_films_data,
    load_json_file,
    load_kaggle_data,
    load_locations_data,
    load_people_data,
    load_species_data,
    load_vehicles_data,
)


class TestLoadJSONFile:
    """Tests for load_json_file function."""

    def test_load_json_file_success(self, tmp_path):
        """Test successful JSON file loading."""
        # Create test JSON file
        test_data = [
            {"id": "test-1", "title": "Test Film 1"},
            {"id": "test-2", "title": "Test Film 2"},
        ]

        json_file = tmp_path / "test.json"
        with open(json_file, "w") as f:
            json.dump(test_data, f)

        # Load and verify
        result = load_json_file(json_file)

        assert len(result) == 2
        assert result[0]["id"] == "test-1"
        assert result[1]["title"] == "Test Film 2"

    def test_load_json_file_not_found(self):
        """Test loading non-existent JSON file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            load_json_file(Path("nonexistent.json"))


class TestCreateRawTables:
    """Tests for create_raw_tables function."""

    @patch("src.ingestion.load_to_duckdb.get_duckdb_connection")
    def test_create_raw_tables_success(self, mock_conn):
        """Test successful table creation."""
        # Setup mock
        mock_connection = Mock()
        mock_conn.return_value = mock_connection

        # Execute
        create_raw_tables()

        # Verify DROP statements executed
        assert any(
            "DROP TABLE IF EXISTS raw.films" in str(call)
            for call in mock_connection.execute.call_args_list
        )

        # Verify CREATE statements executed
        assert any(
            "CREATE TABLE IF NOT EXISTS raw.films" in str(call)
            for call in mock_connection.execute.call_args_list
        )

        # Verify connection closed
        mock_connection.close.assert_called_once()


class TestLoadFilmsData:
    """Tests for load_films_data function."""

    @patch("src.ingestion.load_to_duckdb.get_duckdb_connection")
    @patch("src.ingestion.load_to_duckdb.load_json_file")
    def test_load_films_data_success(self, mock_load_json, mock_conn):
        """Test successful films data loading."""
        # Setup mocks
        mock_connection = Mock()
        mock_conn.return_value = mock_connection

        test_films = [
            {
                "id": "test-id-1",
                "title": "Test Film 1",
                "director": "Test Director",
                "release_date": "2001",
                "rt_score": "95",
                "people": ["url1", "url2"],
                "species": ["url3"],
                "locations": [],
                "vehicles": [],
            }
        ]
        mock_load_json.return_value = test_films

        # Execute
        load_films_data()

        # Verify
        assert mock_connection.execute.called
        assert mock_connection.close.called

        # Check that data was inserted
        insert_call_args = mock_connection.execute.call_args_list[0]
        assert "INSERT INTO raw.films" in insert_call_args[0][0]


class TestLoadPeopleData:
    """Tests for load_people_data function."""

    @patch("src.ingestion.load_to_duckdb.get_duckdb_connection")
    @patch("src.ingestion.load_to_duckdb.load_json_file")
    def test_load_people_data_success(self, mock_load_json, mock_conn):
        """Test successful people data loading."""
        # Setup mocks
        mock_connection = Mock()
        mock_conn.return_value = mock_connection

        test_people = [
            {
                "id": "person-1",
                "name": "Test Character",
                "gender": "Female",
                "age": "10",
                "films": ["url1"],
            }
        ]
        mock_load_json.return_value = test_people

        # Execute
        load_people_data()

        # Verify
        assert mock_connection.execute.called
        assert mock_connection.close.called


class TestLoadKaggleData:
    """Tests for load_kaggle_data function."""

    @patch("src.ingestion.load_to_duckdb.get_duckdb_connection")
    @patch("src.ingestion.load_to_duckdb.pd.read_csv")
    def test_load_kaggle_data_success(self, mock_read_csv, mock_conn):
        """Test successful Kaggle CSV data loading."""
        # Setup mocks
        mock_connection = Mock()
        mock_conn.return_value = mock_connection

        # Create test dataframe
        test_df = pd.DataFrame(
            {
                "Name": ["Test Film"],
                "Year": [2001],
                "Director": ["Test Director"],
                "Screenplay": ["Test Writer"],
                "Category": ["Feature Film"],
                "Genre 1": ["Animation"],
                "Genre 2": ["Adventure"],
                "Genre 3": ["Fantasy"],
                "Duration": ["2h 15m"],
                "Budget": [10000000.0],
                "Revenue": [50000000.0],
            }
        )
        mock_read_csv.return_value = test_df

        # Execute
        load_kaggle_data()

        # Verify
        assert mock_connection.execute.called
        assert mock_connection.close.called

        # Check that data was inserted
        insert_call_args = mock_connection.execute.call_args_list[0]
        assert "INSERT INTO raw.kaggle_films" in insert_call_args[0][0]


class TestIntegrationWithTestDB:
    """Integration tests using temporary test database."""

    @pytest.fixture
    def test_db(self):
        """Create temporary test database."""
        import duckdb

        # Create temp database path (let DuckDB create the file)
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "test.duckdb"

        conn = duckdb.connect(str(db_path))
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

        yield conn

        # Cleanup
        conn.close()
        if db_path.exists():
            db_path.unlink()
        Path(temp_dir).rmdir()

    def test_table_creation_in_test_db(self, test_db):
        """Test creating tables in a test database."""
        # Create films table
        test_db.execute(
            """
            CREATE TABLE raw.films (
                id VARCHAR PRIMARY KEY,
                title VARCHAR NOT NULL,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                source VARCHAR DEFAULT 'ghibli_api'
            )
            """
        )

        # Verify table exists
        result = test_db.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'raw' AND table_name = 'films'
            """
        ).fetchone()

        assert result is not None
        assert result[0] == "films"

    def test_insert_and_query_data(self, test_db):
        """Test inserting and querying data."""
        # Create table
        test_db.execute(
            """
            CREATE TABLE raw.test_table (
                id VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL
            )
            """
        )

        # Insert data
        test_db.execute(
            "INSERT INTO raw.test_table (id, name) VALUES (?, ?)", ["test-1", "Test Name"]
        )

        # Query data
        result = test_db.execute("SELECT * FROM raw.test_table").fetchone()

        assert result is not None
        assert result[0] == "test-1"
        assert result[1] == "Test Name"

    def test_idempotent_loading(self, test_db):
        """Test that tables can be dropped and recreated (idempotent)."""
        # Create table
        test_db.execute(
            """
            CREATE TABLE raw.test_table (
                id VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL
            )
            """
        )

        # Insert data
        test_db.execute(
            "INSERT INTO raw.test_table (id, name) VALUES (?, ?)", ["test-1", "Test Name"]
        )

        # Drop and recreate
        test_db.execute("DROP TABLE IF EXISTS raw.test_table")
        test_db.execute(
            """
            CREATE TABLE raw.test_table (
                id VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL
            )
            """
        )

        # Verify table is empty
        result = test_db.execute("SELECT COUNT(*) FROM raw.test_table").fetchone()
        assert result[0] == 0
