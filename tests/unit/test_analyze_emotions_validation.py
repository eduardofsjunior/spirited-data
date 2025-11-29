"""
Unit tests for emotion analysis validation logic.

Tests the validation features added in Story 3.6.3:
- Duration validation
- V2 subtitle prioritization
- Missing duration handling
- Empty subtitle handling
- Negative overrun handling
- Subtitle version metadata tracking
"""

import json
import logging
import tempfile
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, Mock, patch

import pytest

from src.nlp.analyze_emotions import (
    DataValidationError,
    VALIDATION_BUFFER_MINUTES,
    build_subtitle_priority_map,
    process_film_subtitles,
)


# Test fixtures
@pytest.fixture
def mock_model():
    """Mock HuggingFace emotion classification model."""
    model = Mock()
    # Return mock emotion scores
    model.return_value = [
        [
            {"label": "neutral", "score": 0.7},
            {"label": "joy", "score": 0.2},
            {"label": "admiration", "score": 0.1},
        ]
    ]
    return model


@pytest.fixture
def sample_subtitle_data() -> Dict[str, Any]:
    """Sample subtitle data with valid metadata."""
    return {
        "metadata": {
            "film_name": "Test Film",
            "film_slug": "test_film_en",
            "language_code": "en",
            "total_subtitles": 3,
            "total_duration": 6000.0,  # 100 minutes in seconds
            "parse_timestamp": "2025-11-27T00:00:00",
        },
        "subtitles": [
            {
                "subtitle_index": 1,
                "start_time": 10.0,
                "end_time": 15.0,
                "duration": 5.0,
                "dialogue_text": "Hello world",
            },
            {
                "subtitle_index": 2,
                "start_time": 3000.0,  # 50 minutes
                "end_time": 3005.0,
                "duration": 5.0,
                "dialogue_text": "Middle dialogue",
            },
            {
                "subtitle_index": 3,
                "start_time": 5940.0,  # 99 minutes
                "end_time": 5945.0,
                "duration": 5.0,
                "dialogue_text": "Final dialogue",
            },
        ],
    }


# Test 1: Duration validation passes when emotion data within bounds
def test_duration_validation_pass(sample_subtitle_data, mock_model, tmp_path):
    """Test validation passes when emotion data within subtitle duration bounds."""
    # Create temporary JSON file
    json_file = tmp_path / "test_film_en_parsed.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(sample_subtitle_data, f)

    # Process subtitles - should NOT raise exception
    result = process_film_subtitles(json_file, mock_model, "v1")

    # Assert: Should return emotion entries without raising error
    assert isinstance(result, list)
    assert len(result) > 0
    # Check that max minute is within bounds (99 minutes < 100 + 10 buffer)
    max_minute = max(entry["minute_offset"] for entry in result)
    expected_duration_minutes = sample_subtitle_data["metadata"]["total_duration"] / 60.0
    assert max_minute <= expected_duration_minutes + VALIDATION_BUFFER_MINUTES


# Test 2: Duration validation fails when emotion exceeds duration
def test_duration_validation_fail(sample_subtitle_data, mock_model, tmp_path):
    """Test validation raises error when emotion data exceeds subtitle duration."""
    # Modify data to have subtitle beyond film duration
    sample_subtitle_data["metadata"]["total_duration"] = 3000.0  # 50 minutes
    # But subtitle at 99 minutes (way beyond)
    sample_subtitle_data["subtitles"][2]["start_time"] = 5940.0  # 99 minutes

    # Create temporary JSON file
    json_file = tmp_path / "test_overrun_en_parsed.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(sample_subtitle_data, f)

    # Process subtitles - should raise DataValidationError
    with pytest.raises(DataValidationError) as exc_info:
        process_film_subtitles(json_file, mock_model, "v1")

    # Assert: Error message contains expected details
    error_msg = str(exc_info.value)
    assert "Emotion data extends" in error_msg
    assert "beyond subtitle duration" in error_msg
    # Film slug from metadata is test_film_en
    assert "test_film_en" in error_msg


# Test 3: Missing duration raises error
def test_missing_duration_raises_error(sample_subtitle_data, mock_model, tmp_path):
    """Test missing total_duration raises DataValidationError."""
    # Remove total_duration from metadata
    del sample_subtitle_data["metadata"]["total_duration"]

    # Create temporary JSON file
    json_file = tmp_path / "test_missing_duration_en_parsed.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(sample_subtitle_data, f)

    # Process subtitles - should raise DataValidationError
    with pytest.raises(DataValidationError) as exc_info:
        process_film_subtitles(json_file, mock_model, "v1")

    # Assert: Error message mentions missing duration
    error_msg = str(exc_info.value)
    assert "Missing total_duration" in error_msg
    assert "Re-parse subtitle file" in error_msg


# Test 4: Empty subtitles returns empty list
def test_empty_subtitles_returns_empty_list(sample_subtitle_data, mock_model, tmp_path, caplog):
    """Test empty subtitles returns empty list without error."""
    # Empty subtitles array
    sample_subtitle_data["subtitles"] = []

    # Create temporary JSON file
    json_file = tmp_path / "test_empty_en_parsed.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(sample_subtitle_data, f)

    # Process subtitles - should return empty list
    with caplog.at_level(logging.WARNING):
        result = process_film_subtitles(json_file, mock_model, "v1")

    # Assert: Returns empty list
    assert result == []
    # Assert: Warning logged
    assert "No subtitles found" in caplog.text
    assert "Skipping" in caplog.text


# Test 5: Negative overrun logs INFO (valid case)
def test_negative_overrun_logs_info(sample_subtitle_data, mock_model, tmp_path, caplog):
    """Test negative overrun (emotion ends early) logs INFO message."""
    # Set duration to 120 minutes, but last subtitle at 99 minutes
    sample_subtitle_data["metadata"]["total_duration"] = 7200.0  # 120 minutes

    # Create temporary JSON file
    json_file = tmp_path / "test_negative_overrun_en_parsed.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(sample_subtitle_data, f)

    # Process subtitles - should NOT raise error
    with caplog.at_level(logging.INFO):
        result = process_film_subtitles(json_file, mock_model, "v1")

    # Assert: No error raised
    assert isinstance(result, list)
    # Assert: INFO log message about early termination
    assert "before subtitle duration (valid)" in caplog.text


# Test 6: Build subtitle priority map - v2 overrides v1
def test_build_subtitle_priority_map_v2_override(tmp_path):
    """Test v2 files override v1 in priority map."""
    # Create v1 directory with files
    v1_dir = tmp_path / "v1"
    v1_dir.mkdir()
    (v1_dir / "spirited_away_en_parsed.json").touch()
    (v1_dir / "ponyo_en_parsed.json").touch()

    # Create v2 directory with improved version of spirited_away only
    v2_dir = tmp_path / "v2"
    v2_dir.mkdir()
    (v2_dir / "spirited_away_en_v2_parsed.json").touch()

    # Build priority map
    priority_map = build_subtitle_priority_map(v1_dir, v2_dir)

    # Assert: spirited_away uses v2
    assert "spirited_away_en" in priority_map
    filepath, version = priority_map["spirited_away_en"]
    assert version == "v2"
    assert "v2_parsed.json" in str(filepath)

    # Assert: ponyo uses v1 (no v2 available)
    assert "ponyo_en" in priority_map
    filepath, version = priority_map["ponyo_en"]
    assert version == "v1"
    assert "v2_parsed.json" not in str(filepath)


# Test 7: Subtitle version metadata included in emotion entries
def test_subtitle_version_metadata_included(sample_subtitle_data, mock_model, tmp_path):
    """Test subtitle_version included in emotion entries."""
    # Create temporary JSON file
    json_file = tmp_path / "test_version_en_parsed.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(sample_subtitle_data, f)

    # Process with v2 version tag
    result = process_film_subtitles(json_file, mock_model, subtitle_version="v2")

    # Assert: All emotion entries have subtitle_version = "v2"
    assert len(result) > 0
    for entry in result:
        assert "subtitle_version" in entry
        assert entry["subtitle_version"] == "v2"


# Test 8: Build priority map with no v2 directory
def test_build_subtitle_priority_map_no_v2_dir(tmp_path):
    """Test priority map works when v2 directory doesn't exist."""
    # Create v1 directory with files
    v1_dir = tmp_path / "v1"
    v1_dir.mkdir()
    (v1_dir / "totoro_en_parsed.json").touch()

    # v2 directory doesn't exist
    v2_dir = tmp_path / "v2"  # Not created

    # Build priority map - should not crash
    priority_map = build_subtitle_priority_map(v1_dir, v2_dir)

    # Assert: Only v1 file present
    assert "totoro_en" in priority_map
    filepath, version = priority_map["totoro_en"]
    assert version == "v1"


# Test 9: Film and language metadata included in emotion entries
def test_emotion_entries_include_metadata(sample_subtitle_data, mock_model, tmp_path):
    """Test emotion entries include film_slug and language_code."""
    # Create temporary JSON file
    json_file = tmp_path / "test_metadata_en_parsed.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(sample_subtitle_data, f)

    # Process subtitles
    result = process_film_subtitles(json_file, mock_model, "v1")

    # Assert: All entries have required metadata fields
    assert len(result) > 0
    for entry in result:
        assert "film_slug" in entry
        assert entry["film_slug"] == "test_film_en"
        assert "language_code" in entry
        assert entry["language_code"] == "en"
        assert "minute_offset" in entry
        assert "emotions" in entry
        assert "dialogue_count" in entry
        assert "subtitle_version" in entry


# Test 10: Duration validation with 10-minute buffer (Story 3.6.4)
def test_duration_validation_10min_buffer_pass(sample_subtitle_data, mock_model, tmp_path):
    """Test validation allows emotion within 10-minute buffer (Story 3.6.4)."""
    # Set last subtitle within 10-minute buffer
    duration_seconds = 6000.0  # 100 minutes
    sample_subtitle_data["metadata"]["total_duration"] = duration_seconds
    # Last subtitle at 109 minutes (within 10-minute buffer)
    sample_subtitle_data["subtitles"][2]["start_time"] = 6540.0  # 109 minutes

    # Create temporary JSON file
    json_file = tmp_path / "test_10min_buffer_pass_en_parsed.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(sample_subtitle_data, f)

    # Process subtitles - should NOT raise exception (within 10-min buffer)
    result = process_film_subtitles(json_file, mock_model, "v1")

    # Assert: Should succeed
    assert isinstance(result, list)
    assert len(result) > 0
    # Verify we're within the buffer
    max_minute = max(entry["minute_offset"] for entry in result)
    expected_duration_minutes = duration_seconds / 60.0
    assert max_minute <= expected_duration_minutes + VALIDATION_BUFFER_MINUTES


# Test 11: Duration validation fails beyond 10-minute buffer (Story 3.6.4)
def test_duration_validation_10min_buffer_fail(sample_subtitle_data, mock_model, tmp_path):
    """Test validation fails when emotion exceeds 10-minute buffer (Story 3.6.4)."""
    # Set last subtitle beyond 10-minute buffer
    duration_seconds = 6000.0  # 100 minutes
    sample_subtitle_data["metadata"]["total_duration"] = duration_seconds
    # Last subtitle at 115 minutes (exceeds 10-minute buffer by 5 minutes)
    sample_subtitle_data["subtitles"][2]["start_time"] = 6900.0  # 115 minutes

    # Create temporary JSON file
    json_file = tmp_path / "test_10min_buffer_fail_en_parsed.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(sample_subtitle_data, f)

    # Process subtitles - should raise DataValidationError
    with pytest.raises(DataValidationError) as exc_info:
        process_film_subtitles(json_file, mock_model, "v1")

    # Assert: Error message contains expected details
    error_msg = str(exc_info.value)
    assert "Emotion data extends" in error_msg
    assert "beyond subtitle duration" in error_msg
