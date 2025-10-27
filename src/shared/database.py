"""Database connection utilities for DuckDB."""
import duckdb
from src.shared.config import DUCKDB_PATH


def get_duckdb_connection():
    """
    Get DuckDB connection with schema creation.

    Returns:
        duckdb.DuckDBPyConnection: Active DuckDB connection with schemas initialized
    """
    conn = duckdb.connect(DUCKDB_PATH)

    # Create schemas if they don't exist
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
    conn.execute("CREATE SCHEMA IF NOT EXISTS marts")

    return conn
