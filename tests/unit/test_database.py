"""Unit tests for database connection functionality."""
import pytest
from pathlib import Path
from src.shared.database import get_duckdb_connection


def test_duckdb_connection_creates_database():
    """Test that DuckDB connection creates database file."""
    conn = get_duckdb_connection()

    # Verify connection is valid
    assert conn is not None

    # Verify database file exists
    db_path = Path("data/ghibli.duckdb")
    assert db_path.exists()

    # Verify schemas exist
    schemas = conn.execute("SELECT schema_name FROM information_schema.schemata").fetchall()
    schema_names = [s[0] for s in schemas]

    assert "raw" in schema_names
    assert "staging" in schema_names
    assert "marts" in schema_names

    conn.close()


def test_duckdb_connection_is_reusable():
    """Test that multiple connections can be created."""
    conn1 = get_duckdb_connection()
    conn2 = get_duckdb_connection()

    assert conn1 is not None
    assert conn2 is not None

    conn1.close()
    conn2.close()
