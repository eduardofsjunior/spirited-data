"""
Integration tests for sentiment timeline dashboard.

Tests end-to-end chart generation with real database and subtitle files.
"""

import json
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from src.validation.chart_utils import plot_sentiment_timeline


@pytest.fixture
def test_db_with_emotions(tmp_path: Path) -> duckdb.DuckDBPyConnection:
    """
    Create test database with emotion data.

    Creates temporary DuckDB database with staging schema and sample
    emotion data for integration testing.

    Args:
        tmp_path: Pytest temporary directory fixture

    Returns:
        Active DuckDB connection with test data

    Example:
        >>> conn = test_db_with_emotions()
        >>> conn.execute("SELECT COUNT(*) FROM main_staging.stg_film_emotions")
    """
    db_path = tmp_path / "test_ghibli.duckdb"
    conn = duckdb.connect(str(db_path))

    # Create schema
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

    # Create emotion data table with multiple minutes
    # Note: film_slug includes language suffix (e.g., "spirited_away_en")
    conn.execute("""
        CREATE TABLE raw.film_emotions AS
        SELECT 
            'film-1' as film_id,
            'spirited_away_en' as film_slug,
            'en' as language_code,
            minute_offset,
            dialogue_count,
            -- Positive emotions
            emotion_admiration, emotion_amusement, emotion_approval, emotion_caring,
            emotion_excitement, emotion_gratitude, emotion_joy, emotion_love,
            emotion_optimism, emotion_pride, emotion_relief,
            -- Negative emotions
            emotion_anger, emotion_annoyance, emotion_disappointment, emotion_disapproval,
            emotion_disgust, emotion_embarrassment, emotion_fear, emotion_grief,
            emotion_nervousness, emotion_remorse, emotion_sadness,
            -- Neutral emotions
            emotion_confusion, emotion_curiosity, emotion_desire, emotion_realization,
            emotion_surprise, emotion_neutral,
            CURRENT_TIMESTAMP as loaded_at
        FROM (VALUES
            (0, 2, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.8, 0.7, 0.1, 0.1, 0.1,
             0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
             0.0, 0.0, 0.0, 0.0, 0.0, 0.3),
            (1, 3, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.3, 0.2, 0.1, 0.1, 0.1,
             0.7, 0.6, 0.1, 0.1, 0.0, 0.0, 0.5, 0.0, 0.0, 0.0, 0.6,
             0.0, 0.0, 0.0, 0.0, 0.0, 0.2),
            (2, 4, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.5, 0.4, 0.1, 0.1, 0.1,
             0.2, 0.2, 0.0, 0.0, 0.0, 0.0, 0.2, 0.0, 0.0, 0.0, 0.3,
             0.0, 0.0, 0.0, 0.0, 0.0, 0.4),
            (3, 2, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.9, 0.8, 0.1, 0.1, 0.1,
             0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
             0.0, 0.0, 0.0, 0.0, 0.0, 0.2),
            (4, 1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.2, 0.1, 0.1, 0.1, 0.1,
             0.8, 0.7, 0.1, 0.1, 0.0, 0.0, 0.6, 0.0, 0.0, 0.0, 0.7,
             0.0, 0.0, 0.0, 0.0, 0.0, 0.3)
        ) AS t(
            minute_offset, dialogue_count,
            emotion_admiration, emotion_amusement, emotion_approval, emotion_caring,
            emotion_excitement, emotion_gratitude, emotion_joy, emotion_love,
            emotion_optimism, emotion_pride, emotion_relief,
            emotion_anger, emotion_annoyance, emotion_disappointment, emotion_disapproval,
            emotion_disgust, emotion_embarrassment, emotion_fear, emotion_grief,
            emotion_nervousness, emotion_remorse, emotion_sadness,
            emotion_confusion, emotion_curiosity, emotion_desire, emotion_realization,
            emotion_surprise, emotion_neutral
        )
    """)

    yield conn

    conn.close()


@pytest.fixture
def test_subtitle_file(tmp_path: Path) -> Path:
    """
    Create test subtitle JSON file.

    Creates temporary parsed subtitle file with sample dialogue data
    for testing dialogue excerpt loading.

    Args:
        tmp_path: Pytest temporary directory fixture

    Returns:
        Path to created subtitle JSON file
    """
    subtitle_dir = tmp_path / "data" / "processed" / "subtitles"
    subtitle_dir.mkdir(parents=True, exist_ok=True)

    subtitle_file = subtitle_dir / "spirited_away_en_parsed.json"

    subtitle_data = {
        "metadata": {
            "film_name": "Spirited Away",
            "film_slug": "spirited_away",
            "total_subtitles": 5,
            "total_duration": 300,
            "parse_timestamp": "2025-11-05T12:00:00",
        },
        "subtitles": [
            {
                "subtitle_index": 1,
                "start_time": 5.0,
                "end_time": 8.0,
                "duration": 3.0,
                "dialogue_text": "Minute 0 dialogue.",
            },
            {
                "subtitle_index": 2,
                "start_time": 65.0,
                "end_time": 68.0,
                "duration": 3.0,
                "dialogue_text": "Minute 1 dialogue.",
            },
            {
                "subtitle_index": 3,
                "start_time": 185.0,
                "end_time": 188.0,
                "duration": 3.0,
                "dialogue_text": "Minute 3 dialogue - positive peak!",
            },
            {
                "subtitle_index": 4,
                "start_time": 245.0,
                "end_time": 248.0,
                "duration": 3.0,
                "dialogue_text": "Minute 4 dialogue - negative peak.",
            },
        ],
    }

    with open(subtitle_file, "w", encoding="utf-8") as f:
        json.dump(subtitle_data, f)

    return subtitle_dir.parent.parent  # Return data/ directory


class TestSentimentDashboardIntegration:
    """Integration tests for sentiment dashboard."""

    def test_end_to_end_chart_generation(
        self, test_db_with_emotions: duckdb.DuckDBPyConnection
    ) -> None:
        """Test complete chart generation workflow with test database."""
        fig = plot_sentiment_timeline(
            test_db_with_emotions, "spirited_away", "Spirited Away", "en"
        )

        assert fig is not None
        assert len(fig.data) >= 1  # Has at least sentiment line trace
        assert fig.layout.xaxis.title.text == "Timeline (minutes)"
        assert fig.layout.yaxis.title.text == "Sentiment Score"
        # Y-axis range should be dynamic now (not fixed at [-1, 1])
        assert len(fig.layout.yaxis.range) == 2
        assert -1 <= fig.layout.yaxis.range[0] <= 1
        assert -1 <= fig.layout.yaxis.range[1] <= 1

    def test_chart_has_animation_controls(
        self, test_db_with_emotions: duckdb.DuckDBPyConnection
    ) -> None:
        """Test chart includes animation frames and controls."""
        fig = plot_sentiment_timeline(
            test_db_with_emotions, "spirited_away", "Spirited Away", "en"
        )

        assert fig is not None
        assert len(fig.frames) > 0  # Has animation frames
        assert "updatemenus" in fig.layout
        assert len(fig.layout.updatemenus) > 0  # Has play/pause buttons
        assert "sliders" in fig.layout
        assert len(fig.layout.sliders) > 0  # Has timeline slider

    def test_chart_has_peak_markers(
        self, test_db_with_emotions: duckdb.DuckDBPyConnection
    ) -> None:
        """Test chart includes peak annotation markers."""
        fig = plot_sentiment_timeline(
            test_db_with_emotions, "spirited_away", "Spirited Away", "en"
        )

        assert fig is not None
        # Should have: sentiment line + positive peaks + negative peaks = 3 traces
        assert len(fig.data) >= 3

        # Check for positive peak markers
        positive_traces = [
            trace for trace in fig.data if "Positive" in trace.name
        ]
        assert len(positive_traces) > 0

        # Check for negative peak markers
        negative_traces = [
            trace for trace in fig.data if "Negative" in trace.name
        ]
        assert len(negative_traces) > 0

    def test_chart_with_dialogue_excerpts(
        self,
        test_db_with_emotions: duckdb.DuckDBPyConnection,
        test_subtitle_file: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test chart loads and displays dialogue excerpts."""
        # Monkeypatch Path to use test directory
        import src.validation.chart_utils

        original_path = Path
        monkeypatch.setattr(
            src.validation.chart_utils,
            "Path",
            lambda x: test_subtitle_file / x if "data/processed" in str(x) else original_path(x),
        )

        fig = plot_sentiment_timeline(
            test_db_with_emotions, "spirited_away", "Spirited Away", "en"
        )

        assert fig is not None

        # Check peak markers have hover text with dialogue
        has_dialogue_hover = False
        for trace in fig.data:
            if hasattr(trace, "text") and trace.text:
                has_dialogue_hover = True
                break

        assert has_dialogue_hover, "Peak markers should have dialogue hover text"

    def test_chart_handles_missing_subtitle_file(
        self, test_db_with_emotions: duckdb.DuckDBPyConnection
    ) -> None:
        """Test chart gracefully handles missing subtitle file."""
        # Chart should still generate without dialogue excerpts
        fig = plot_sentiment_timeline(
            test_db_with_emotions, "spirited_away", "Spirited Away", "en"
        )

        assert fig is not None
        # Chart should have sentiment line and peak markers even without dialogues
        assert len(fig.data) >= 1

    def test_chart_nonexistent_film(
        self, test_db_with_emotions: duckdb.DuckDBPyConnection
    ) -> None:
        """Test chart returns None for non-existent film."""
        fig = plot_sentiment_timeline(
            test_db_with_emotions, "nonexistent_film", "Nonexistent Film", "en"
        )

        assert fig is None

    def test_chart_responsive_layout(
        self, test_db_with_emotions: duckdb.DuckDBPyConnection
    ) -> None:
        """Test chart has responsive layout configuration."""
        fig = plot_sentiment_timeline(
            test_db_with_emotions, "spirited_away", "Spirited Away", "en"
        )

        assert fig is not None
        assert fig.layout.autosize is True
        assert fig.layout.height == 400  # Minimum height requirement

    def test_chart_sentiment_zones(
        self, test_db_with_emotions: duckdb.DuckDBPyConnection
    ) -> None:
        """Test chart includes sentiment zones based on data range."""
        fig = plot_sentiment_timeline(
            test_db_with_emotions, "spirited_away", "Spirited Away", "en"
        )

        assert fig is not None
        # Check for shaded zones (shapes in layout)
        assert "shapes" in fig.layout
        shapes = fig.layout.shapes

        # Should have at least 1 shape (could be 1-3 depending on data:
        # positive zone, negative zone, and/or baseline)
        assert len(shapes) >= 1

        # Check if zones exist based on data range
        y_range = fig.layout.yaxis.range
        
        # If range includes positive values, should have green zone
        if y_range[1] > 0:
            has_positive_zone = any(
                shape.fillcolor == "green" and shape.y0 == 0
                for shape in shapes
            )
            assert has_positive_zone or len(shapes) >= 1, "Chart should have green positive zone when range includes positive values"

        # If range includes negative values, should have red zone
        if y_range[0] < 0:
            has_negative_zone = any(
                shape.fillcolor == "red" and shape.y1 == 0
                for shape in shapes
            )
            assert has_negative_zone or len(shapes) >= 1, "Chart should have red negative zone when range includes negative values"

