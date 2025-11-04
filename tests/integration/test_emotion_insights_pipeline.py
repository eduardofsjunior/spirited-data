"""
Integration tests for emotion insights report generation pipeline.

Tests end-to-end report generation with real DuckDB database to verify
the complete workflow from database queries to report file creation.
"""

import json
from pathlib import Path

import duckdb
import pytest

from src.nlp.emotion_insights_report import (
    compare_languages,
    extract_emotional_peaks,
    generate_coverage_summary,
    generate_markdown_report,
    identify_emotional_patterns,
    validate_data_quality,
)


@pytest.fixture
def duckdb_connection():
    """
    Create connection to real DuckDB database for integration testing.

    Uses the actual project database if it exists, otherwise skips tests.
    """
    db_path = Path("data/ghibli.duckdb")

    if not db_path.exists():
        pytest.skip(f"DuckDB database not found at {db_path}")

    conn = duckdb.connect(str(db_path), read_only=True)
    yield conn
    conn.close()


def test_end_to_end_report_generation(duckdb_connection, tmp_path):
    """Test complete report generation workflow with real database."""
    # Generate all analysis components
    summary = generate_coverage_summary(duckdb_connection)
    patterns = identify_emotional_patterns(duckdb_connection)
    peaks = extract_emotional_peaks(duckdb_connection)
    validation = validate_data_quality(duckdb_connection)
    language_comparison = compare_languages(duckdb_connection)

    # Verify summary has expected structure
    assert "total_films" in summary
    assert "total_languages" in summary
    assert summary["total_films"] > 0

    # Verify patterns identified
    assert "most_joyful" in patterns
    assert "most_fearful" in patterns
    assert "most_complex" in patterns

    # Verify peaks extracted for all key emotions
    assert "joy" in peaks
    assert "fear" in peaks
    assert "anger" in peaks
    assert "love" in peaks
    assert "sadness" in peaks

    # Verify validation checks
    assert "range_check" in validation
    assert "null_check" in validation
    assert "dimensions_check" in validation
    assert "completeness" in validation

    # Verify language comparison
    assert "language_averages" in language_comparison
    assert "top_emotions_by_language" in language_comparison

    # Generate markdown report
    report_content = generate_markdown_report(
        summary, patterns, peaks, validation, language_comparison
    )

    # Verify report structure
    assert "# Emotion Analysis Validation & Insights Report" in report_content
    assert "## Executive Summary" in report_content
    assert "## Data Coverage" in report_content
    assert "## Emotional Patterns" in report_content
    assert "## Data Quality Validation" in report_content

    # Save report to temp directory
    report_path = tmp_path / "emotion_analysis_report.md"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_content)

    # Verify report file created and not empty
    assert report_path.exists()
    assert report_path.stat().st_size > 0


def test_coverage_summary_real_data(duckdb_connection):
    """Test coverage summary with real database data."""
    summary = generate_coverage_summary(duckdb_connection)

    # Verify all required fields present
    assert "total_films" in summary
    assert "total_languages" in summary
    assert "total_dialogue_entries" in summary
    assert "total_minute_buckets" in summary
    assert "language_breakdown" in summary

    # Verify data types
    assert isinstance(summary["total_films"], int)
    assert isinstance(summary["total_languages"], int)
    assert isinstance(summary["total_dialogue_entries"], int)
    assert isinstance(summary["language_breakdown"], dict)

    # Verify logical constraints
    assert summary["total_films"] > 0
    assert summary["total_languages"] > 0
    assert summary["total_dialogue_entries"] > 0


def test_validation_with_real_data(duckdb_connection):
    """Test data quality validation with real database."""
    validation = validate_data_quality(duckdb_connection)

    # Verify all checks present
    assert "range_check" in validation
    assert "null_check" in validation
    assert "dimensions_check" in validation
    assert "completeness" in validation

    # For a properly populated database, these should pass
    assert validation["range_check"]["passed"] is True
    assert validation["null_check"]["passed"] is True
    assert validation["dimensions_check"]["passed"] is True
    assert validation["dimensions_check"]["present_count"] == 28

    # Completeness should be high
    assert validation["completeness"]["percentage"] > 50.0


def test_emotional_patterns_real_data(duckdb_connection):
    """Test emotional pattern identification with real data."""
    patterns = identify_emotional_patterns(duckdb_connection)

    # Verify all patterns identified
    assert "most_joyful" in patterns
    assert "most_fearful" in patterns
    assert "most_complex" in patterns

    # Verify structure
    assert "film_slug" in patterns["most_joyful"]
    assert "avg_joy" in patterns["most_joyful"]
    assert "film_slug" in patterns["most_fearful"]
    assert "avg_fear" in patterns["most_fearful"]
    assert "film_slug" in patterns["most_complex"]
    assert "emotion_diversity_score" in patterns["most_complex"]

    # Verify scores are valid floats in expected range
    assert 0.0 <= patterns["most_joyful"]["avg_joy"] <= 1.0
    assert 0.0 <= patterns["most_fearful"]["avg_fear"] <= 1.0
    assert patterns["most_complex"]["emotion_diversity_score"] >= 0.0


def test_emotional_peaks_extraction(duckdb_connection):
    """Test emotional peak extraction with real data."""
    peaks = extract_emotional_peaks(duckdb_connection)

    # Verify all key emotions have peaks
    assert "joy" in peaks
    assert "fear" in peaks
    assert "anger" in peaks
    assert "love" in peaks
    assert "sadness" in peaks

    # Verify each emotion has up to 5 peaks
    for emotion, peak_list in peaks.items():
        assert isinstance(peak_list, list)
        assert len(peak_list) <= 5

        # Verify peak structure if any peaks exist
        if peak_list:
            peak = peak_list[0]
            assert "film_slug" in peak
            assert "language_code" in peak
            assert "timestamp" in peak
            assert "emotion_score" in peak
            assert "dialogue_excerpt" in peak


def test_cross_language_comparison(duckdb_connection):
    """Test cross-language emotion comparison with real data."""
    comparison = compare_languages(duckdb_connection)

    # Verify structure
    assert "language_averages" in comparison
    assert "top_emotions_by_language" in comparison
    assert "significant_differences" in comparison

    # Verify language averages
    assert isinstance(comparison["language_averages"], dict)
    assert len(comparison["language_averages"]) > 0

    # Verify top emotions by language
    for lang, emotions in comparison["top_emotions_by_language"].items():
        assert isinstance(emotions, list)
        assert len(emotions) == 3  # Top 3 emotions
