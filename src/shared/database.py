"""Database connection utilities for DuckDB."""
import duckdb
from src.shared.config import DUCKDB_PATH


def get_duckdb_connection(read_only: bool = False):
    """
    Get DuckDB connection with schema creation.

    Args:
        read_only: If True, open database in read-only mode (default: False)

    Returns:
        duckdb.DuckDBPyConnection: Active DuckDB connection with schemas initialized
    """
    conn = duckdb.connect(DUCKDB_PATH, read_only=read_only)

    if not read_only:
        # Create schemas if they don't exist (only in write mode)
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
        conn.execute("CREATE SCHEMA IF NOT EXISTS marts")

    return conn
