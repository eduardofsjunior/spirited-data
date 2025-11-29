"""
Unit tests for Streamlit app data loader utilities.

Tests DuckDB connection and mart loading functions.

[Source: architecture/13-testing-strategy.md]
"""

import pytest
import pandas as pd
from pathlib import Path
import duckdb


def test_duckdb_path_exists() -> None:
    """Test that DuckDB database file exists."""
    from src.app.utils.config import DUCKDB_PATH

    assert DUCKDB_PATH.exists(), f"DuckDB database not found at {DUCKDB_PATH}"


def test_duckdb_connection() -> None:
    """Test DuckDB connection can be established."""
    from src.app.utils.config import DUCKDB_PATH

    conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    assert conn is not None
    conn.close()


def test_film_list_query() -> None:
    """Test film list query returns expected films."""
    from src.app.utils.config import DUCKDB_PATH

    conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)

    df = conn.execute(
        """
        SELECT
            id as film_id,
            title,
            director,
            release_year,
            rt_score
        FROM main_staging.stg_films
        ORDER BY release_year DESC
    """
    ).fetch_df()

    # Verify we have films
    assert len(df) > 0, "Should return at least one film"

    # Verify required columns exist
    assert "film_id" in df.columns
    assert "title" in df.columns
    assert "director" in df.columns
    assert "release_year" in df.columns
    assert "rt_score" in df.columns

    conn.close()


def test_director_profile_mart_exists() -> None:
    """Test director profile mart table exists and has data."""
    from src.app.utils.config import DUCKDB_PATH, EMOTION_MARTS

    conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)

    table_name = EMOTION_MARTS["director_profile"]
    df = conn.execute(f"SELECT * FROM {table_name}").fetch_df()

    # Verify mart has data
    assert len(df) > 0, "Director profile mart should have at least one row"

    conn.close()


def test_emotion_marts_config() -> None:
    """Test EMOTION_MARTS configuration has all expected marts."""
    from src.app.utils.config import EMOTION_MARTS

    expected_marts = [
        "director_profile",
        "emotion_peaks_smoothed",
        "emotion_peaks_raw",
        "film_similarity",
        "cross_language",
        "kaggle_correlation",
        "methodology_metrics",
    ]

    for mart in expected_marts:
        assert mart in EMOTION_MARTS, f"Missing mart: {mart}"


def test_emotion_labels_count() -> None:
    """Test EMOTION_LABELS has 28 GoEmotions dimensions."""
    from src.app.utils.config import EMOTION_LABELS

    assert len(EMOTION_LABELS) == 28, "Should have 28 emotion labels"


def test_data_stats_config() -> None:
    """Test DATA_STATS configuration has expected fields."""
    from src.app.utils.config import DATA_STATS

    assert "film_count" in DATA_STATS
    assert "emotion_data_points" in DATA_STATS
    assert "languages" in DATA_STATS
    assert "language_codes" in DATA_STATS
    assert "dialogue_entries" in DATA_STATS
    assert "emotion_dimensions" in DATA_STATS
    assert "subtitle_validation_pass_rate" in DATA_STATS

    # Verify metrics
    assert DATA_STATS["emotion_dimensions"] == 28
    assert DATA_STATS["subtitle_validation_pass_rate"] == 0.724


def test_config_imports() -> None:
    """Test all config imports work."""
    from src.app.utils.config import (
        APP_TITLE,
        APP_SUBTITLE,
        APP_ICON,
        THEME,
        DATA_STATS,
        EMOTION_MARTS,
        EMOTION_LABELS,
        DUCKDB_PATH,
    )

    assert APP_TITLE == "SpiritedData: Emotional Landscape of Studio Ghibli"
    assert APP_ICON == "🎬"
    assert len(EMOTION_LABELS) == 28
    assert len(EMOTION_MARTS) == 7
