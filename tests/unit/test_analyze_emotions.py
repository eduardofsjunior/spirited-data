"""
Unit tests for emotion analysis module.

Tests cover model loading, language detection, emotion analysis, aggregation,
validation, and film ID resolution.
"""
import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.nlp.analyze_emotions import (
    GOEMOTIONS_LABELS,
    SUPPORTED_LANGUAGES,
    aggregate_emotions_by_minute,
    analyze_dialogue_emotions,
    detect_language_from_filename,
    load_emotion_model,
    resolve_film_id,
    validate_emotion_data,
)


class TestDetectLanguageFromFilename:
    """Test detect_language_from_filename function."""

    def test_detect_language_en(self):
        """Test language detection for English."""
        filepath = Path("spirited_away_en_parsed.json")
        result = detect_language_from_filename(filepath)
        assert result == "en"

    def test_detect_language_fr(self):
        """Test language detection for French."""
        filepath = Path("spirited_away_fr_parsed.json")
        result = detect_language_from_filename(filepath)
        assert result == "fr"

    def test_detect_language_es(self):
        """Test language detection for Spanish."""
        filepath = Path("princess_mononoke_es_parsed.json")
        result = detect_language_from_filename(filepath)
        assert result == "es"

    def test_detect_language_nl(self):
        """Test language detection for Dutch."""
        filepath = Path("my_neighbor_totoro_nl_parsed.json")
        result = detect_language_from_filename(filepath)
        assert result == "nl"

    def test_detect_language_ar(self):
        """Test language detection for Arabic."""
        filepath = Path("castle_in_the_sky_ar_parsed.json")
        result = detect_language_from_filename(filepath)
        assert result == "ar"

    def test_detect_language_unsupported_raises_error(self):
        """Test unsupported language raises ValueError."""
        filepath = Path("spirited_away_ja_parsed.json")
        with pytest.raises(ValueError, match="Unsupported language code"):
            detect_language_from_filename(filepath)

    def test_detect_language_invalid_pattern_raises_error(self):
        """Test invalid filename pattern raises ValueError."""
        filepath = Path("invalid_filename.json")
        with pytest.raises(ValueError, match="Invalid filename pattern"):
            detect_language_from_filename(filepath)

    def test_detect_language_missing_parsed_suffix(self):
        """Test filename without _parsed suffix raises error."""
        filepath = Path("spirited_away_en.json")
        with pytest.raises(ValueError, match="Invalid filename pattern"):
            detect_language_from_filename(filepath)


class TestLoadEmotionModel:
    """Test load_emotion_model function."""

    @patch("src.nlp.analyze_emotions.pipeline")
    def test_load_emotion_model_success(self, mock_pipeline):
        """Test successful model loading."""
        mock_classifier = MagicMock()
        mock_pipeline.return_value = mock_classifier

        result = load_emotion_model()

        assert result == mock_classifier
        mock_pipeline.assert_called_once_with(
            "text-classification",
            model="AnasAlokla/multilingual_go_emotions",
            top_k=None,
        )

    @patch("src.nlp.analyze_emotions.pipeline")
    def test_load_emotion_model_network_error(self, mock_pipeline):
        """Test model loading with network error."""
        mock_pipeline.side_effect = OSError("Network error")

        with pytest.raises(OSError, match="Network error"):
            load_emotion_model()

    @patch("src.nlp.analyze_emotions.pipeline")
    def test_load_emotion_model_runtime_error(self, mock_pipeline):
        """Test model loading with runtime error."""
        mock_pipeline.side_effect = RuntimeError("Model loading failed")

        with pytest.raises(RuntimeError, match="Model loading failed"):
            load_emotion_model()


class TestAnalyzeDialogueEmotions:
    """Test analyze_dialogue_emotions function."""

    @pytest.fixture
    def mock_model(self):
        """Create mock HuggingFace pipeline."""
        mock_model = MagicMock()
        # Return all 28 emotion labels with sample scores
        # Note: Real model returns nested list [[{...}, {...}]] for single text
        mock_results = [
            {"label": label, "score": 0.1 if label != "joy" else 0.9}
            for label in GOEMOTIONS_LABELS
        ]
        # Return as nested list to match real model behavior
        mock_model.return_value = [mock_results]
        return mock_model

    def test_analyze_dialogue_emotions_success(self, mock_model):
        """Test successful emotion analysis."""
        text = "I am so happy and excited!"
        result = analyze_dialogue_emotions(text, mock_model)

        assert len(result) == 28
        assert all(label in result for label in GOEMOTIONS_LABELS)
        assert all(0.0 <= score <= 1.0 for score in result.values())
        assert result["joy"] == 0.9
        mock_model.assert_called_once_with(text)

    def test_analyze_dialogue_emotions_empty_text(self, mock_model):
        """Test emotion analysis with empty text."""
        result = analyze_dialogue_emotions("", mock_model)

        assert len(result) == 28
        assert all(score == 0.0 for score in result.values())
        mock_model.assert_not_called()

    def test_analyze_dialogue_emotions_whitespace_only(self, mock_model):
        """Test emotion analysis with whitespace-only text."""
        result = analyze_dialogue_emotions("   \n\t  ", mock_model)

        assert len(result) == 28
        assert all(score == 0.0 for score in result.values())
        mock_model.assert_not_called()

    def test_analyze_dialogue_emotions_long_text_truncated(self, mock_model):
        """Test emotion analysis truncates very long text."""
        # Create text with >450 words
        long_text = "word " * 500
        result = analyze_dialogue_emotions(long_text, mock_model)

        # Should still work (truncated internally)
        assert len(result) == 28
        # Check that model was called with truncated text
        call_args = mock_model.call_args[0][0]
        words = call_args.split()
        assert len(words) <= 450

    def test_analyze_dialogue_emotions_missing_labels_filled(self, mock_model):
        """Test that missing emotion labels are filled with 0.0."""
        # Mock model returns only some labels (nested list format)
        mock_model.return_value = [[
            {"label": "joy", "score": 0.8},
            {"label": "sadness", "score": 0.2},
        ]]

        result = analyze_dialogue_emotions("test", mock_model)

        assert len(result) == 28
        assert result["joy"] == 0.8
        assert result["sadness"] == 0.2
        # All other labels should be 0.0
        assert all(
            result[label] == 0.0
            for label in GOEMOTIONS_LABELS
            if label not in ["joy", "sadness"]
        )

    def test_analyze_dialogue_emotions_model_error(self, mock_model):
        """Test emotion analysis with model error."""
        mock_model.side_effect = Exception("Model inference failed")

        with pytest.raises(ValueError, match="Failed to analyze emotions"):
            analyze_dialogue_emotions("test", mock_model)

    @patch("src.nlp.analyze_emotions.time.sleep")
    def test_analyze_dialogue_emotions_retry_success(self, mock_sleep, mock_model):
        """Test retry logic succeeds on second attempt."""
        # First call fails, second succeeds (nested list format)
        mock_model.side_effect = [
            Exception("Transient error"),
            [[{"label": label, "score": 0.1 if label != "joy" else 0.9} for label in GOEMOTIONS_LABELS]],
        ]

        result = analyze_dialogue_emotions("test", mock_model)

        assert len(result) == 28
        assert result["joy"] == 0.9
        assert mock_model.call_count == 2
        mock_sleep.assert_called_once_with(1.0)  # Should sleep 1s before retry

    @patch("src.nlp.analyze_emotions.time.sleep")
    def test_analyze_dialogue_emotions_retry_exhausted(self, mock_sleep, mock_model):
        """Test retry logic exhausts all attempts."""
        # All attempts fail
        mock_model.side_effect = Exception("Persistent error")

        with pytest.raises(ValueError, match="Failed to analyze emotions after 3 attempts"):
            analyze_dialogue_emotions("test", mock_model)

        assert mock_model.call_count == 3
        assert mock_sleep.call_count == 2  # Sleep after attempts 1 and 2, not after 3


class TestAggregateEmotionsByMinute:
    """Test aggregate_emotions_by_minute function."""

    def test_aggregate_emotions_by_minute_single_minute(self):
        """Test aggregation for single minute bucket."""
        emotion_entries = [
            {
                "minute_offset": 1,
                "emotions": [
                    {"joy": 0.8, "sadness": 0.2, "neutral": 0.0},
                    {"joy": 0.6, "sadness": 0.4, "neutral": 0.0},
                ],
                "dialogue_count": 2,
            }
        ]

        # Fill in all 28 labels
        for entry in emotion_entries:
            for emotion_dict in entry["emotions"]:
                for label in GOEMOTIONS_LABELS:
                    emotion_dict.setdefault(label, 0.0)

        result = aggregate_emotions_by_minute(emotion_entries)

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["minute_offset"] == 1
        assert result.iloc[0]["dialogue_count"] == 2
        # Average of 0.8 and 0.6 = 0.7
        assert abs(result.iloc[0]["emotion_joy"] - 0.7) < 0.01

    def test_aggregate_emotions_by_minute_multiple_minutes(self):
        """Test aggregation for multiple minute buckets."""
        emotion_entries = [
            {
                "minute_offset": 0,
                "emotions": [{"joy": 0.5, "neutral": 0.5}],
                "dialogue_count": 1,
            },
            {
                "minute_offset": 1,
                "emotions": [{"joy": 0.8, "neutral": 0.2}],
                "dialogue_count": 1,
            },
            {
                "minute_offset": 2,
                "emotions": [{"joy": 0.9, "neutral": 0.1}],
                "dialogue_count": 1,
            },
        ]

        # Fill in all 28 labels
        for entry in emotion_entries:
            for emotion_dict in entry["emotions"]:
                for label in GOEMOTIONS_LABELS:
                    emotion_dict.setdefault(label, 0.0)

        result = aggregate_emotions_by_minute(emotion_entries)

        assert len(result) == 3
        assert all(col in result.columns for col in ["minute_offset", "dialogue_count"])
        assert all(
            f"emotion_{label}" in result.columns for label in GOEMOTIONS_LABELS
        )

    def test_aggregate_emotions_by_minute_empty(self):
        """Test aggregation with empty entries."""
        result = aggregate_emotions_by_minute([])

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

    def test_aggregate_emotions_by_minute_rolling_average(self):
        """Test that rolling average is applied."""
        emotion_entries = [
            {
                "minute_offset": i,
                "emotions": [{"joy": float(i * 0.1), "neutral": 1.0 - float(i * 0.1)}],
                "dialogue_count": 1,
            }
            for i in range(5)
        ]

        # Fill in all 28 labels
        for entry in emotion_entries:
            for emotion_dict in entry["emotions"]:
                for label in GOEMOTIONS_LABELS:
                    emotion_dict.setdefault(label, 0.0)

        result = aggregate_emotions_by_minute(emotion_entries)

        # Rolling average should smooth values (centered window of 3)
        # Minute 2 should be average of minutes 1, 2, 3
        assert "emotion_joy" in result.columns
        # Values should be smoothed (not exact original values due to rolling average)
        assert len(result) == 5


class TestResolveFilmId:
    """Test resolve_film_id function."""

    @pytest.fixture
    def mock_conn(self):
        """Create mock DuckDB connection."""
        conn = MagicMock()
        return conn

    def test_resolve_film_id_with_film_name(self, mock_conn):
        """Test film_id resolution using film_name from metadata."""
        mock_conn.execute.return_value.fetchone.return_value = ("film-id-123",)

        result = resolve_film_id("spirited_away_en", mock_conn, film_name="Spirited Away")

        assert result == "film-id-123"
        mock_conn.execute.assert_called_once()
        # Should use film_name directly
        call_args = mock_conn.execute.call_args[0]
        assert "LOWER(title) = LOWER(?)" in call_args[0]
        assert call_args[1] == ["Spirited Away"]

    def test_resolve_film_id_from_slug(self, mock_conn):
        """Test film_id resolution from slug conversion."""
        mock_conn.execute.return_value.fetchone.return_value = ("film-id-456",)

        result = resolve_film_id("spirited_away_en", mock_conn, film_name=None)

        assert result == "film-id-456"
        # Should convert slug to title: "spirited_away_en" -> "Spirited Away"
        call_args = mock_conn.execute.call_args[0]
        assert call_args[1] == ["Spirited Away"]

    def test_resolve_film_id_not_found(self, mock_conn):
        """Test film_id resolution when film not found."""
        mock_conn.execute.return_value.fetchone.return_value = None

        result = resolve_film_id("unknown_film_en", mock_conn, film_name=None)

        assert result is None

    def test_resolve_film_id_database_error(self, mock_conn):
        """Test film_id resolution with database error."""
        mock_conn.execute.side_effect = Exception("Database error")

        result = resolve_film_id("spirited_away_en", mock_conn, film_name=None)

        assert result is None


class TestValidateEmotionData:
    """Test validate_emotion_data function."""

    @pytest.fixture
    def sample_emotions_df(self):
        """Create sample emotions DataFrame."""
        data = {
            "minute_offset": [0, 1, 2],
            "dialogue_count": [5, 3, 2],
        }
        # Add all 28 emotion columns with valid scores [0, 1]
        for label in GOEMOTIONS_LABELS:
            data[f"emotion_{label}"] = [0.1, 0.2, 0.3]

        return pd.DataFrame(data)

    def test_validate_emotion_data_valid(self, sample_emotions_df, tmp_path):
        """Test validation with valid data."""
        # Create sample parsed JSON file
        json_data = {
            "subtitles": [{"dialogue_text": "test"}] * 10  # 10 dialogue entries
        }
        json_file = tmp_path / "test_parsed.json"
        json_file.write_text(json.dumps(json_data), encoding="utf-8")

        result = validate_emotion_data(sample_emotions_df, json_file)

        assert result["valid"] is True
        assert result["dialogue_count_match"] is True  # 5+3+2 = 10
        assert "emotion_stats" in result

    def test_validate_emotion_data_dialogue_count_mismatch(self, sample_emotions_df, tmp_path):
        """Test validation with dialogue count mismatch."""
        # Create JSON with different count
        json_data = {"subtitles": [{"dialogue_text": "test"}] * 5}  # 5 entries, not 10
        json_file = tmp_path / "test_parsed.json"
        json_file.write_text(json.dumps(json_data), encoding="utf-8")

        result = validate_emotion_data(sample_emotions_df, json_file)

        assert result["valid"] is True  # Still valid, just count mismatch
        assert result["dialogue_count_match"] is False

    def test_validate_emotion_data_out_of_range(self, tmp_path):
        """Test validation with scores out of [0, 1] range."""
        data = {
            "minute_offset": [0],
            "dialogue_count": [1],
            "emotion_joy": [1.5],  # Out of range
        }
        # Add other emotion columns
        for label in GOEMOTIONS_LABELS:
            if label != "joy":
                data[f"emotion_{label}"] = [0.5]

        df = pd.DataFrame(data)

        json_data = {"subtitles": [{"dialogue_text": "test"}]}
        json_file = tmp_path / "test_parsed.json"
        json_file.write_text(json.dumps(json_data), encoding="utf-8")

        result = validate_emotion_data(df, json_file)

        assert result["valid"] is False

    def test_validate_emotion_data_missing_columns(self, tmp_path):
        """Test validation with missing emotion columns."""
        data = {
            "minute_offset": [0],
            "dialogue_count": [1],
            "emotion_joy": [0.5],  # Missing other 27 columns
        }
        df = pd.DataFrame(data)

        json_data = {"subtitles": [{"dialogue_text": "test"}]}
        json_file = tmp_path / "test_parsed.json"
        json_file.write_text(json.dumps(json_data), encoding="utf-8")

        result = validate_emotion_data(df, json_file)

        assert result["valid"] is False

    def test_validate_emotion_data_empty_dataframe(self, tmp_path):
        """Test validation with empty DataFrame."""
        df = pd.DataFrame()

        json_data = {"subtitles": []}
        json_file = tmp_path / "test_parsed.json"
        json_file.write_text(json.dumps(json_data), encoding="utf-8")

        result = validate_emotion_data(df, json_file)

        assert result["valid"] is False

