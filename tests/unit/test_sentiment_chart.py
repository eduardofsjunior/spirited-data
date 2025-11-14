"""
Unit tests for sentiment chart utilities.

Tests sentiment timeline generation, peak identification, dialogue excerpt
loading, and error handling for validation dashboard.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pandas as pd
import pytest

from src.validation.chart_utils import (
    calculate_compound_score,
    identify_peaks,
    load_dialogue_excerpts,
    plot_sentiment_timeline,
    get_film_duration,
)


class TestCalculateCompoundScore:
    """Test compound sentiment score calculation."""

    def test_all_positive_emotions(self) -> None:
        """Test compound score with only positive emotions."""
        row = pd.Series({
            "emotion_joy": 1.0,
            "emotion_love": 1.0,
            "emotion_gratitude": 1.0,
            "emotion_admiration": 1.0,
            "emotion_amusement": 1.0,
            "emotion_approval": 1.0,
            "emotion_caring": 1.0,
            "emotion_excitement": 1.0,
            "emotion_optimism": 1.0,
            "emotion_pride": 1.0,
            "emotion_relief": 1.0,
            "emotion_anger": 0.0,
            "emotion_fear": 0.0,
            "emotion_sadness": 0.0,
            "emotion_annoyance": 0.0,
            "emotion_disappointment": 0.0,
            "emotion_disapproval": 0.0,
            "emotion_disgust": 0.0,
            "emotion_embarrassment": 0.0,
            "emotion_grief": 0.0,
            "emotion_nervousness": 0.0,
            "emotion_remorse": 0.0,
        })

        score = calculate_compound_score(row)

        assert score == pytest.approx(1.0), "All positive should give score of 1.0"

    def test_all_negative_emotions(self) -> None:
        """Test compound score with only negative emotions."""
        row = pd.Series({
            "emotion_joy": 0.0,
            "emotion_love": 0.0,
            "emotion_gratitude": 0.0,
            "emotion_admiration": 0.0,
            "emotion_amusement": 0.0,
            "emotion_approval": 0.0,
            "emotion_caring": 0.0,
            "emotion_excitement": 0.0,
            "emotion_optimism": 0.0,
            "emotion_pride": 0.0,
            "emotion_relief": 0.0,
            "emotion_anger": 1.0,
            "emotion_fear": 1.0,
            "emotion_sadness": 1.0,
            "emotion_annoyance": 1.0,
            "emotion_disappointment": 1.0,
            "emotion_disapproval": 1.0,
            "emotion_disgust": 1.0,
            "emotion_embarrassment": 1.0,
            "emotion_grief": 1.0,
            "emotion_nervousness": 1.0,
            "emotion_remorse": 1.0,
        })

        score = calculate_compound_score(row)

        assert score == pytest.approx(-1.0), "All negative should give score of -1.0"

    def test_balanced_emotions(self) -> None:
        """Test compound score with balanced positive and negative emotions."""
        row = pd.Series({
            "emotion_joy": 0.5,
            "emotion_love": 0.5,
            "emotion_gratitude": 0.5,
            "emotion_admiration": 0.5,
            "emotion_amusement": 0.5,
            "emotion_approval": 0.5,
            "emotion_caring": 0.5,
            "emotion_excitement": 0.5,
            "emotion_optimism": 0.5,
            "emotion_pride": 0.5,
            "emotion_relief": 0.5,
            "emotion_anger": 0.5,
            "emotion_fear": 0.5,
            "emotion_sadness": 0.5,
            "emotion_annoyance": 0.5,
            "emotion_disappointment": 0.5,
            "emotion_disapproval": 0.5,
            "emotion_disgust": 0.5,
            "emotion_embarrassment": 0.5,
            "emotion_grief": 0.5,
            "emotion_nervousness": 0.5,
            "emotion_remorse": 0.5,
        })

        score = calculate_compound_score(row)

        assert score == pytest.approx(0.0, abs=0.001), "Balanced should give score near 0.0"

    def test_missing_emotion_columns(self) -> None:
        """Test compound score handles missing emotion columns gracefully."""
        row = pd.Series({
            "emotion_joy": 0.8,
            "emotion_anger": 0.2,
        })

        score = calculate_compound_score(row)

        assert -1.0 <= score <= 1.0, "Score should be in valid range even with missing columns"


class TestIdentifyPeaks:
    """Test sentiment peak identification."""

    def test_identify_positive_peaks(self) -> None:
        """Test identification of positive sentiment peaks."""
        df = pd.DataFrame({
            "minute_offset": [0, 1, 2, 3, 4, 5, 6],
            "emotion_joy": [0.1, 0.9, 0.2, 0.8, 0.3, 0.7, 0.4],
            "emotion_anger": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_love": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_gratitude": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_admiration": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_amusement": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_approval": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_caring": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_excitement": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_optimism": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_pride": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_relief": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_sadness": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_fear": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_annoyance": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_disappointment": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_disapproval": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_disgust": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_embarrassment": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_grief": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_nervousness": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_remorse": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        })

        peaks = identify_peaks(df)

        assert "positive" in peaks
        assert len(peaks["positive"]) <= 5
        # Highest joy should be at minute 1 (0.9)
        assert peaks["positive"][0]["minute_offset"] == 1

    def test_identify_negative_peaks(self) -> None:
        """Test identification of negative sentiment peaks."""
        df = pd.DataFrame({
            "minute_offset": [0, 1, 2, 3, 4],
            "emotion_anger": [0.1, 0.9, 0.2, 0.8, 0.3],
            "emotion_joy": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_love": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_gratitude": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_admiration": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_amusement": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_approval": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_caring": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_excitement": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_optimism": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_pride": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_relief": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_sadness": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_fear": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_annoyance": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_disappointment": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_disapproval": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_disgust": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_embarrassment": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_grief": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_nervousness": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_remorse": [0.0, 0.0, 0.0, 0.0, 0.0],
        })

        peaks = identify_peaks(df)

        assert "negative" in peaks
        assert len(peaks["negative"]) <= 5
        # Highest anger should be at minute 1 (0.9)
        assert peaks["negative"][0]["minute_offset"] == 1

    def test_fewer_than_five_peaks(self) -> None:
        """Test peak identification with fewer than 5 data points."""
        df = pd.DataFrame({
            "minute_offset": [0, 1],
            "emotion_joy": [0.8, 0.2],
            "emotion_anger": [0.0, 0.0],
            "emotion_love": [0.0, 0.0],
            "emotion_gratitude": [0.0, 0.0],
            "emotion_admiration": [0.0, 0.0],
            "emotion_amusement": [0.0, 0.0],
            "emotion_approval": [0.0, 0.0],
            "emotion_caring": [0.0, 0.0],
            "emotion_excitement": [0.0, 0.0],
            "emotion_optimism": [0.0, 0.0],
            "emotion_pride": [0.0, 0.0],
            "emotion_relief": [0.0, 0.0],
            "emotion_sadness": [0.0, 0.0],
            "emotion_fear": [0.0, 0.0],
            "emotion_annoyance": [0.0, 0.0],
            "emotion_disappointment": [0.0, 0.0],
            "emotion_disapproval": [0.0, 0.0],
            "emotion_disgust": [0.0, 0.0],
            "emotion_embarrassment": [0.0, 0.0],
            "emotion_grief": [0.0, 0.0],
            "emotion_nervousness": [0.0, 0.0],
            "emotion_remorse": [0.0, 0.0],
        })

        peaks = identify_peaks(df)

        assert len(peaks["positive"]) == 2
        assert len(peaks["negative"]) == 2


class TestLoadDialogueExcerpts:
    """Test dialogue excerpt loading from subtitle JSON."""

    def test_load_dialogue_excerpts_success(self) -> None:
        """Test successful dialogue excerpt loading."""
        mock_subtitle_data = {
            "metadata": {
                "film_name": "Spirited Away",
                "film_slug": "spirited_away",
                "total_subtitles": 2,
            },
            "subtitles": [
                {
                    "subtitle_index": 1,
                    "start_time": 65.0,  # 1 minute 5 seconds
                    "end_time": 68.0,
                    "duration": 3.0,
                    "dialogue_text": "Don't be scared.",
                },
                {
                    "subtitle_index": 2,
                    "start_time": 70.0,  # 1 minute 10 seconds
                    "end_time": 73.0,
                    "duration": 3.0,
                    "dialogue_text": "It's just a statue.",
                },
            ],
        }

        with patch("builtins.open", mock_open(read_data=json.dumps(mock_subtitle_data))):
            with patch("pathlib.Path.exists", return_value=True):
                excerpts = load_dialogue_excerpts("spirited_away", "en", [1])

        assert 1 in excerpts
        assert isinstance(excerpts[1], list)
        assert len(excerpts[1]) <= 3  # Top 3 dialogues
        assert any("It's just a statue." in d for d in excerpts[1])

    def test_load_dialogue_excerpts_file_not_found(self) -> None:
        """Test dialogue loading when file doesn't exist."""
        with patch("pathlib.Path.exists", return_value=False):
            excerpts = load_dialogue_excerpts("nonexistent_film", "en", [1, 2])

        assert excerpts == {}

    def test_load_dialogue_excerpts_malformed_json(self) -> None:
        """Test dialogue loading handles malformed JSON."""
        with patch("builtins.open", mock_open(read_data="not valid json")):
            with patch("pathlib.Path.exists", return_value=True):
                excerpts = load_dialogue_excerpts("spirited_away", "en", [1])

        assert excerpts == {}

    def test_load_dialogue_excerpts_truncation(self) -> None:
        """Test dialogue excerpt truncation at 80 chars per dialogue."""
        long_text = "A" * 100  # 100 character dialogue
        mock_subtitle_data = {
            "metadata": {"film_slug": "test"},
            "subtitles": [
                {
                    "subtitle_index": 1,
                    "start_time": 0.0,
                    "end_time": 5.0,
                    "duration": 5.0,
                    "dialogue_text": long_text,
                }
            ],
        }

        with patch("builtins.open", mock_open(read_data=json.dumps(mock_subtitle_data))):
            with patch("pathlib.Path.exists", return_value=True):
                excerpts = load_dialogue_excerpts("test", "en", [0])

        assert 0 in excerpts
        assert isinstance(excerpts[0], list)
        assert len(excerpts[0][0]) <= 80
        assert excerpts[0][0].endswith("...")

    def test_load_dialogue_excerpts_no_dialogue_in_minute(self) -> None:
        """Test dialogue loading when no dialogue in requested minute."""
        mock_subtitle_data = {
            "metadata": {"film_slug": "test"},
            "subtitles": [
                {
                    "subtitle_index": 1,
                    "start_time": 0.0,
                    "end_time": 5.0,
                    "duration": 5.0,
                    "dialogue_text": "Early dialogue.",
                }
            ],
        }

        with patch("builtins.open", mock_open(read_data=json.dumps(mock_subtitle_data))):
            with patch("pathlib.Path.exists", return_value=True):
                excerpts = load_dialogue_excerpts("test", "en", [5])  # Minute 5

        assert 5 in excerpts
        assert isinstance(excerpts[5], list)
        assert excerpts[5] == ["[No dialogue]"]


class TestPlotSentimentTimeline:
    """Test sentiment timeline chart generation."""

    def test_plot_sentiment_timeline_empty_data(self) -> None:
        """Test chart handles empty dataset gracefully."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetch_df.return_value = pd.DataFrame()

        fig = plot_sentiment_timeline(mock_conn, "nonexistent_film", "Test Film", "en")

        assert fig is None

    def test_plot_sentiment_timeline_valid_data(self) -> None:
        """Test chart generation with valid emotion data."""
        mock_df = pd.DataFrame({
            "minute_offset": [0, 1, 2],
            "emotion_joy": [0.8, 0.5, 0.3],
            "emotion_love": [0.6, 0.4, 0.2],
            "emotion_anger": [0.1, 0.3, 0.7],
            "emotion_sadness": [0.2, 0.4, 0.6],
            "emotion_gratitude": [0.0, 0.0, 0.0],
            "emotion_admiration": [0.0, 0.0, 0.0],
            "emotion_amusement": [0.0, 0.0, 0.0],
            "emotion_approval": [0.0, 0.0, 0.0],
            "emotion_caring": [0.0, 0.0, 0.0],
            "emotion_excitement": [0.0, 0.0, 0.0],
            "emotion_optimism": [0.0, 0.0, 0.0],
            "emotion_pride": [0.0, 0.0, 0.0],
            "emotion_relief": [0.0, 0.0, 0.0],
            "emotion_fear": [0.0, 0.0, 0.0],
            "emotion_annoyance": [0.0, 0.0, 0.0],
            "emotion_disappointment": [0.0, 0.0, 0.0],
            "emotion_disapproval": [0.0, 0.0, 0.0],
            "emotion_disgust": [0.0, 0.0, 0.0],
            "emotion_embarrassment": [0.0, 0.0, 0.0],
            "emotion_grief": [0.0, 0.0, 0.0],
            "emotion_nervousness": [0.0, 0.0, 0.0],
            "emotion_remorse": [0.0, 0.0, 0.0],
        })

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetch_df.return_value = mock_df

        with patch("src.validation.chart_utils.load_dialogue_excerpts", return_value={}):
            fig = plot_sentiment_timeline(mock_conn, "spirited_away", "Spirited Away", "en")

        assert fig is not None
        assert len(fig.data) >= 1  # At least sentiment line
        assert fig.layout.xaxis.title.text == "Timeline (minutes)"
        assert fig.layout.yaxis.title.text == "Sentiment Score"
        # Y-axis range should be dynamic now (not fixed at [-1, 1])
        assert len(fig.layout.yaxis.range) == 2
        assert -1 <= fig.layout.yaxis.range[0] <= 1
        assert -1 <= fig.layout.yaxis.range[1] <= 1
        assert fig.layout.height == 400

    def test_plot_sentiment_timeline_database_error(self) -> None:
        """Test chart handles database errors gracefully."""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Database connection failed")

        fig = plot_sentiment_timeline(mock_conn, "test_film", "Test Film", "en")

        assert fig is None


class TestGetFilmDuration:
    """Test film duration retrieval function."""

    def test_get_film_duration_with_data(self) -> None:
        """Test retrieving film duration when data exists."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = (0, 120)

        min_minute, max_minute = get_film_duration(mock_conn, "spirited_away", "en")

        assert min_minute == 0
        assert max_minute == 120
        mock_conn.execute.assert_called_once()

    def test_get_film_duration_no_data(self) -> None:
        """Test retrieving duration when no data exists."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = (None, None)

        min_minute, max_minute = get_film_duration(mock_conn, "nonexistent_film", "en")

        assert min_minute == 0
        assert max_minute == 0

    def test_get_film_duration_empty_result(self) -> None:
        """Test retrieving duration when query returns empty result."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = None

        min_minute, max_minute = get_film_duration(mock_conn, "test_film", "en")

        assert min_minute == 0
        assert max_minute == 0

    def test_get_film_duration_database_error(self) -> None:
        """Test handling database errors."""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Database error")

        min_minute, max_minute = get_film_duration(mock_conn, "test_film", "en")

        assert min_minute == 0
        assert max_minute == 0


class TestTimeRangeFiltering:
    """Test sentiment timeline with time range filters."""

    def test_plot_sentiment_timeline_with_time_range(self) -> None:
        """Test chart generation with time range filter."""
        mock_df = pd.DataFrame({
            "minute_offset": [10, 11, 12, 13, 14, 15],  # Only minutes 10-15
            "emotion_joy": [0.8, 0.7, 0.6, 0.5, 0.4, 0.3],
            "emotion_love": [0.6, 0.5, 0.4, 0.3, 0.2, 0.1],
            "emotion_anger": [0.1, 0.1, 0.1, 0.1, 0.1, 0.1],
            "emotion_sadness": [0.1, 0.1, 0.1, 0.1, 0.1, 0.1],
            "emotion_gratitude": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_admiration": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_amusement": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_approval": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_caring": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_excitement": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_optimism": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_pride": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_relief": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_fear": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_annoyance": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_disappointment": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_disapproval": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_disgust": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_embarrassment": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_grief": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_nervousness": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_remorse": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        })

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetch_df.return_value = mock_df

        with patch("src.validation.chart_utils.load_dialogue_excerpts", return_value={}):
            fig = plot_sentiment_timeline(
                mock_conn, "spirited_away", "Spirited Away", "en",
                time_range_min=10, time_range_max=15
            )

        assert fig is not None
        # Verify minute_offset range is filtered
        x_data = fig.data[0].x
        assert min(x_data) >= 10
        assert max(x_data) <= 15

    def test_plot_sentiment_timeline_time_range_min_only(self) -> None:
        """Test chart with only minimum time range (no max)."""
        mock_df = pd.DataFrame({
            "minute_offset": [20, 21, 22],
            "emotion_joy": [0.5, 0.5, 0.5],
            "emotion_love": [0.3, 0.3, 0.3],
            "emotion_anger": [0.1, 0.1, 0.1],
            "emotion_sadness": [0.1, 0.1, 0.1],
            "emotion_gratitude": [0.0, 0.0, 0.0],
            "emotion_admiration": [0.0, 0.0, 0.0],
            "emotion_amusement": [0.0, 0.0, 0.0],
            "emotion_approval": [0.0, 0.0, 0.0],
            "emotion_caring": [0.0, 0.0, 0.0],
            "emotion_excitement": [0.0, 0.0, 0.0],
            "emotion_optimism": [0.0, 0.0, 0.0],
            "emotion_pride": [0.0, 0.0, 0.0],
            "emotion_relief": [0.0, 0.0, 0.0],
            "emotion_fear": [0.0, 0.0, 0.0],
            "emotion_annoyance": [0.0, 0.0, 0.0],
            "emotion_disappointment": [0.0, 0.0, 0.0],
            "emotion_disapproval": [0.0, 0.0, 0.0],
            "emotion_disgust": [0.0, 0.0, 0.0],
            "emotion_embarrassment": [0.0, 0.0, 0.0],
            "emotion_grief": [0.0, 0.0, 0.0],
            "emotion_nervousness": [0.0, 0.0, 0.0],
            "emotion_remorse": [0.0, 0.0, 0.0],
        })

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetch_df.return_value = mock_df

        with patch("src.validation.chart_utils.load_dialogue_excerpts", return_value={}):
            fig = plot_sentiment_timeline(
                mock_conn, "spirited_away", "Spirited Away", "en",
                time_range_min=20, time_range_max=None
            )

        assert fig is not None
        x_data = fig.data[0].x
        assert min(x_data) >= 20


class TestIntensityThresholdFiltering:
    """Test intensity threshold filtering for peak identification."""

    def test_identify_peaks_with_zero_threshold(self) -> None:
        """Test that zero threshold includes all peaks (default behavior)."""
        df = pd.DataFrame({
            "minute_offset": [0, 1, 2, 3, 4],
            "emotion_joy": [0.8, 0.2, 0.6, 0.1, 0.9],  # High positive at 0, 2, 4
            "emotion_anger": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_love": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_gratitude": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_admiration": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_amusement": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_approval": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_caring": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_excitement": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_optimism": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_pride": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_relief": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_sadness": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_fear": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_annoyance": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_disappointment": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_disapproval": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_disgust": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_embarrassment": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_grief": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_nervousness": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_remorse": [0.0, 0.0, 0.0, 0.0, 0.0],
        })

        peaks = identify_peaks(df, threshold=0.0)

        assert "positive" in peaks
        assert len(peaks["positive"]) == 5  # All 5 data points qualify
        # Highest joy should be at minute 4 (0.9)
        assert peaks["positive"][0]["minute_offset"] == 4

    def test_identify_peaks_with_high_threshold(self) -> None:
        """Test that high threshold filters out low-intensity peaks."""
        # Note: Compound score = (sum of 11 positive emotions)/11 - (sum of 11 negative emotions)/11
        # To get compound > 0.1, need most positive emotions high
        df = pd.DataFrame({
            "minute_offset": [0, 1, 2, 3, 4, 5],
            # Set all 11 positive emotions high for minutes 0, 2, 5
            "emotion_joy": [0.9, 0.1, 0.8, 0.2, 0.05, 0.85],
            "emotion_love": [0.8, 0.05, 0.75, 0.15, 0.0, 0.8],
            "emotion_gratitude": [0.9, 0.1, 0.8, 0.2, 0.05, 0.85],
            "emotion_admiration": [0.85, 0.1, 0.8, 0.15, 0.05, 0.8],
            "emotion_amusement": [0.8, 0.05, 0.7, 0.1, 0.0, 0.75],
            "emotion_approval": [0.9, 0.1, 0.8, 0.2, 0.05, 0.85],
            "emotion_caring": [0.8, 0.1, 0.75, 0.15, 0.05, 0.8],
            "emotion_excitement": [0.9, 0.1, 0.8, 0.2, 0.05, 0.85],
            "emotion_optimism": [0.85, 0.05, 0.8, 0.15, 0.0, 0.8],
            "emotion_pride": [0.9, 0.1, 0.8, 0.2, 0.05, 0.85],
            "emotion_relief": [0.8, 0.05, 0.75, 0.15, 0.0, 0.8],
            "emotion_anger": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_sadness": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_fear": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_annoyance": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_disappointment": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_disapproval": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_disgust": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_embarrassment": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_grief": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_nervousness": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_remorse": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        })

        # Apply threshold of 0.1 - should filter out very low scores
        peaks = identify_peaks(df, threshold=0.1)

        assert "positive" in peaks
        # Should have at least 3 peaks above threshold (minutes 0, 2, 5 definitely pass)
        assert len(peaks["positive"]) >= 3
        # Verify the high-intensity peaks are included
        peak_minutes = [p["minute_offset"] for p in peaks["positive"]]
        assert 0 in peak_minutes
        assert 2 in peak_minutes
        assert 5 in peak_minutes
        
        # Apply higher threshold that should filter out more
        peaks_filtered = identify_peaks(df, threshold=0.7)
        # Should have fewer peaks with higher threshold
        assert len(peaks_filtered["positive"]) < len(peaks["positive"])

    def test_identify_peaks_with_threshold_filters_negative(self) -> None:
        """Test that threshold also filters negative peaks."""
        # Set all 11 negative emotions high for minutes 0, 2, 4
        df = pd.DataFrame({
            "minute_offset": [0, 1, 2, 3, 4],
            "emotion_anger": [0.9, 0.1, 0.8, 0.15, 0.85],
            "emotion_sadness": [0.85, 0.1, 0.8, 0.15, 0.8],
            "emotion_fear": [0.9, 0.1, 0.75, 0.2, 0.85],
            "emotion_annoyance": [0.8, 0.05, 0.8, 0.1, 0.8],
            "emotion_disappointment": [0.9, 0.1, 0.8, 0.15, 0.85],
            "emotion_disapproval": [0.85, 0.1, 0.75, 0.15, 0.8],
            "emotion_disgust": [0.9, 0.1, 0.8, 0.2, 0.85],
            "emotion_embarrassment": [0.8, 0.05, 0.75, 0.1, 0.8],
            "emotion_grief": [0.9, 0.1, 0.8, 0.15, 0.85],
            "emotion_nervousness": [0.85, 0.1, 0.8, 0.15, 0.8],
            "emotion_remorse": [0.9, 0.05, 0.75, 0.2, 0.85],
            "emotion_joy": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_love": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_gratitude": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_admiration": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_amusement": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_approval": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_caring": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_excitement": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_optimism": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_pride": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_relief": [0.0, 0.0, 0.0, 0.0, 0.0],
        })

        # Apply threshold of 0.1 - should filter out low scores
        peaks = identify_peaks(df, threshold=0.1)

        assert "negative" in peaks
        # Should have at least 3 negative peaks above threshold (minutes 0, 2, 4 definitely pass)
        assert len(peaks["negative"]) >= 3
        peak_minutes = [p["minute_offset"] for p in peaks["negative"]]
        assert 0 in peak_minutes
        assert 2 in peak_minutes
        assert 4 in peak_minutes
        
        # Apply higher threshold that should filter out more
        peaks_filtered = identify_peaks(df, threshold=0.7)
        # Should have fewer peaks with higher threshold
        assert len(peaks_filtered["negative"]) < len(peaks["negative"])

    def test_identify_peaks_with_very_high_threshold_returns_empty(self) -> None:
        """Test that very high threshold returns empty lists when no peaks qualify."""
        df = pd.DataFrame({
            "minute_offset": [0, 1, 2],
            "emotion_joy": [0.3, 0.4, 0.2],  # All below 0.9 threshold
            "emotion_anger": [0.2, 0.3, 0.1],
            "emotion_love": [0.0, 0.0, 0.0],
            "emotion_gratitude": [0.0, 0.0, 0.0],
            "emotion_admiration": [0.0, 0.0, 0.0],
            "emotion_amusement": [0.0, 0.0, 0.0],
            "emotion_approval": [0.0, 0.0, 0.0],
            "emotion_caring": [0.0, 0.0, 0.0],
            "emotion_excitement": [0.0, 0.0, 0.0],
            "emotion_optimism": [0.0, 0.0, 0.0],
            "emotion_pride": [0.0, 0.0, 0.0],
            "emotion_relief": [0.0, 0.0, 0.0],
            "emotion_sadness": [0.0, 0.0, 0.0],
            "emotion_fear": [0.0, 0.0, 0.0],
            "emotion_annoyance": [0.0, 0.0, 0.0],
            "emotion_disappointment": [0.0, 0.0, 0.0],
            "emotion_disapproval": [0.0, 0.0, 0.0],
            "emotion_disgust": [0.0, 0.0, 0.0],
            "emotion_embarrassment": [0.0, 0.0, 0.0],
            "emotion_grief": [0.0, 0.0, 0.0],
            "emotion_nervousness": [0.0, 0.0, 0.0],
            "emotion_remorse": [0.0, 0.0, 0.0],
        })

        # Apply very high threshold - no peaks should pass
        peaks = identify_peaks(df, threshold=0.9)

        assert "positive" in peaks
        assert "negative" in peaks
        # Both should be empty lists
        assert len(peaks["positive"]) == 0
        assert len(peaks["negative"]) == 0

    def test_plot_sentiment_timeline_with_intensity_threshold(self) -> None:
        """Test that plot_sentiment_timeline accepts and uses intensity_threshold parameter."""
        mock_df = pd.DataFrame({
            "minute_offset": [0, 1, 2, 3, 4],
            "emotion_joy": [0.9, 0.1, 0.8, 0.2, 0.85],
            "emotion_anger": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_love": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_gratitude": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_admiration": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_amusement": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_approval": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_caring": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_excitement": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_optimism": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_pride": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_relief": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_sadness": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_fear": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_annoyance": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_disappointment": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_disapproval": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_disgust": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_embarrassment": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_grief": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_nervousness": [0.0, 0.0, 0.0, 0.0, 0.0],
            "emotion_remorse": [0.0, 0.0, 0.0, 0.0, 0.0],
        })

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetch_df.return_value = mock_df

        with patch("src.validation.chart_utils.load_dialogue_excerpts", return_value={}):
            # Call with intensity threshold
            fig = plot_sentiment_timeline(
                mock_conn, "spirited_away", "Spirited Away", "en",
                intensity_threshold=0.5
            )

        # Verify figure was created
        assert fig is not None
        # Chart should have data even with threshold applied
        assert len(fig.data) > 0

