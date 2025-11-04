"""
Unit tests for emotion insights report generation module.

Tests individual functions with mocked dependencies to ensure correctness
of coverage summary, pattern identification, validation, and report generation.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest

from src.nlp.emotion_insights_report import (
    compare_languages,
    extract_emotional_peaks,
    generate_coverage_summary,
    generate_markdown_report,
    identify_emotional_patterns,
    print_console_summary,
    validate_data_quality,
)


@pytest.fixture
def mock_duckdb_conn():
    """Create mock DuckDB connection."""
    return MagicMock()


def test_generate_coverage_summary(mock_duckdb_conn):
    """Test data coverage summary generation."""
    # Mock query results
    mock_duckdb_conn.execute.return_value.fetchone.return_value = (22, 5, 50000, 5000)
    mock_duckdb_conn.execute.return_value.fetchall.return_value = [
        ("en", 22),
        ("fr", 22),
        ("es", 22),
        ("nl", 22),
        ("ar", 22),
    ]

    summary = generate_coverage_summary(mock_duckdb_conn)

    assert summary["total_films"] == 22
    assert summary["total_languages"] == 5
    assert summary["total_dialogue_entries"] == 50000
    assert summary["total_minute_buckets"] == 5000
    assert "en" in summary["language_breakdown"]
    assert summary["language_breakdown"]["en"] == 22


def test_identify_emotional_patterns(mock_duckdb_conn):
    """Test emotional pattern identification."""
    # Mock query results for different emotions
    mock_duckdb_conn.execute.return_value.fetchone.side_effect = [
        ("my_neighbor_totoro", 0.65),  # Most joyful
        ("princess_mononoke", 0.45),  # Most fearful
        ("spirited_away", 0.23),  # Most complex
    ]

    patterns = identify_emotional_patterns(mock_duckdb_conn)

    assert patterns["most_joyful"]["film_slug"] == "my_neighbor_totoro"
    assert patterns["most_joyful"]["avg_joy"] == 0.65
    assert patterns["most_fearful"]["film_slug"] == "princess_mononoke"
    assert patterns["most_fearful"]["avg_fear"] == 0.45
    assert patterns["most_complex"]["film_slug"] == "spirited_away"


def test_validate_data_quality_all_pass(mock_duckdb_conn):
    """Test data quality validation with all checks passing."""
    # Mock query results: no invalid ranges, no nulls, 28 dimensions, 100% completeness
    mock_duckdb_conn.execute.return_value.fetchone.side_effect = [
        (0,),  # No invalid ranges
        (0,),  # No nulls
        (5000, 5000),  # All valid records
    ]

    mock_duckdb_conn.execute.return_value.fetchall.return_value = [
        (f"emotion_label_{i}",) for i in range(28)
    ]

    validation = validate_data_quality(mock_duckdb_conn)

    assert validation["range_check"]["passed"] is True
    assert validation["null_check"]["passed"] is True
    assert validation["dimensions_check"]["passed"] is True
    assert validation["completeness"]["percentage"] == 100.0


def test_validate_data_quality_with_failures(mock_duckdb_conn):
    """Test data quality validation with some checks failing."""
    # Mock query results: 10 invalid ranges, 5 nulls, 27 dimensions
    mock_duckdb_conn.execute.return_value.fetchone.side_effect = [
        (10,),  # 10 invalid ranges
        (5,),  # 5 nulls
        (5000, 4500),  # total_records, valid_records = 90% completeness
    ]

    mock_duckdb_conn.execute.return_value.fetchall.return_value = [
        (f"emotion_label_{i}",) for i in range(27)  # Missing 1 dimension
    ]

    validation = validate_data_quality(mock_duckdb_conn)

    assert validation["range_check"]["passed"] is False
    assert validation["range_check"]["invalid_count"] == 10
    assert validation["null_check"]["passed"] is False
    assert validation["dimensions_check"]["passed"] is False
    assert validation["completeness"]["percentage"] == 90.0


def test_compare_languages(mock_duckdb_conn):
    """Test cross-language emotion comparison."""
    # Mock query results with emotion averages for 3 languages
    mock_duckdb_conn.execute.return_value.fetchall.return_value = [
        ("en", 0.3, 0.2, 0.1, 0.05, 0.15) + tuple([0.01] * 23),  # EN emotions
        ("fr", 0.25, 0.25, 0.15, 0.05, 0.1) + tuple([0.01] * 23),  # FR emotions
        ("es", 0.35, 0.15, 0.08, 0.06, 0.18) + tuple([0.01] * 23),  # ES emotions
    ]

    comparison = compare_languages(mock_duckdb_conn)

    assert "en" in comparison["language_averages"]
    assert "fr" in comparison["language_averages"]
    assert len(comparison["top_emotions_by_language"]) == 3
    assert len(comparison["top_emotions_by_language"]["en"]) == 3


def test_extract_emotional_peaks(mock_duckdb_conn, tmp_path):
    """Test emotional peak extraction with dialogue excerpts."""
    # Mock query results for joy peaks
    mock_duckdb_conn.execute.return_value.fetchall.return_value = [
        ("spirited_away", "en", 10, 0.85),
        ("totoro", "en", 5, 0.80),
    ]

    # Create mock subtitle files
    subtitle_data = {
        "metadata": {"film_name": "Spirited Away"},
        "subtitles": [
            {
                "subtitle_index": 1,
                "start_time": 600,  # 10 minutes
                "end_time": 605,
                "dialogue_text": "This is a joyful moment!",
            }
        ],
    }

    subtitle_path = tmp_path / "data" / "processed" / "subtitles"
    subtitle_path.mkdir(parents=True, exist_ok=True)

    (subtitle_path / "spirited_away_en_parsed.json").write_text(json.dumps(subtitle_data))

    # Patch Path to use tmp_path
    with patch("src.nlp.emotion_insights_report.Path") as mock_path:
        mock_path.return_value = subtitle_path / "spirited_away_en_parsed.json"

        # Since extract_emotional_peaks iterates over KEY_EMOTIONS, we need to mock all 5
        mock_duckdb_conn.execute.return_value.fetchall.side_effect = [
            [("spirited_away", "en", 10, 0.85)] for _ in range(5)
        ]

        peaks = extract_emotional_peaks(mock_duckdb_conn)

        assert "joy" in peaks
        assert len(peaks) == 5  # All KEY_EMOTIONS


def test_generate_markdown_report():
    """Test markdown report generation."""
    summary = {
        "total_films": 22,
        "total_languages": 5,
        "total_dialogue_entries": 50000,
        "total_minute_buckets": 5000,
        "language_breakdown": {"en": 22, "fr": 22},
    }

    patterns = {
        "most_joyful": {"film_slug": "totoro", "avg_joy": 0.65},
        "most_fearful": {"film_slug": "mononoke", "avg_fear": 0.45},
        "most_complex": {"film_slug": "spirited_away", "emotion_diversity_score": 0.23},
    }

    peaks = {
        "joy": [
            {
                "film_slug": "totoro",
                "language_code": "en",
                "timestamp": "10:00",
                "emotion_score": 0.85,
                "dialogue_excerpt": "Happy dialogue",
            }
        ],
        "fear": [],
        "anger": [],
        "love": [],
        "sadness": [],
    }

    validation = {
        "range_check": {"passed": True, "invalid_count": 0},
        "null_check": {"passed": True, "null_count": 0},
        "dimensions_check": {"passed": True, "present_count": 28, "expected_count": 28},
        "completeness": {"total_records": 5000, "valid_records": 5000, "percentage": 100.0},
    }

    language_comparison = {
        "language_averages": {"en": {}, "fr": {}},
        "top_emotions_by_language": {"en": ["joy", "fear", "anger"]},
        "significant_differences": [],
    }

    report = generate_markdown_report(summary, patterns, peaks, validation, language_comparison)

    # Verify report structure
    assert "# Emotion Analysis Validation & Insights Report" in report
    assert "## Executive Summary" in report
    assert "## Data Coverage" in report
    assert "## Emotional Patterns" in report
    assert "## Data Quality Validation" in report
    assert "totoro" in report
    assert "22" in report


def test_print_console_summary(capsys):
    """Test console summary output."""
    summary = {
        "total_films": 22,
        "total_languages": 5,
        "total_dialogue_entries": 50000,
        "total_minute_buckets": 5000,
    }

    patterns = {
        "most_joyful": {"film_slug": "totoro", "avg_joy": 0.65},
        "most_fearful": {"film_slug": "mononoke", "avg_fear": 0.45},
        "most_complex": {"film_slug": "spirited_away", "emotion_diversity_score": 0.23},
    }

    validation = {
        "range_check": {"passed": True, "invalid_count": 0},
        "null_check": {"passed": True, "null_count": 0},
        "dimensions_check": {"passed": True, "present_count": 28, "expected_count": 28},
        "completeness": {"total_records": 5000, "valid_records": 5000, "percentage": 100.0},
    }

    print_console_summary(summary, patterns, validation)

    captured = capsys.readouterr()

    assert "EMOTION ANALYSIS REPORT SUMMARY" in captured.out
    assert "Films Analyzed:      22" in captured.out
    assert "totoro" in captured.out
    assert "PASS ✅" in captured.out
