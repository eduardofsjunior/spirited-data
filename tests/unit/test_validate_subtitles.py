"""
Unit tests for validate_subtitles.py module.

Tests cover:
- Valid .srt file format validation
- Invalid timestamp format detection
- Missing text content detection
- Empty file handling
- Statistics extraction (counts, time span)
- Encoding fallback (UTF-8 → Latin-1)
"""

import pytest
from pathlib import Path
from src.ingestion.validate_subtitles import (
    validate_srt_format,
    extract_subtitle_stats,
)


# Fixture paths
FIXTURES_DIR = Path("tests/fixtures")
VALID_FIXTURE = FIXTURES_DIR / "subtitle_sample_valid.srt"
INVALID_TIMESTAMPS_FIXTURE = FIXTURES_DIR / "subtitle_sample_invalid_timestamps.srt"
MISSING_TEXT_FIXTURE = FIXTURES_DIR / "subtitle_sample_missing_text.srt"
EMPTY_FIXTURE = FIXTURES_DIR / "subtitle_sample_empty.srt"


class TestValidateSrtFormat:
    """Test validate_srt_format function."""

    def test_valid_srt_file_returns_success(self):
        """Valid .srt file should pass all validation checks."""
        result = validate_srt_format(str(VALID_FIXTURE))

        assert result["valid"] is True
        assert len(result["errors"]) == 0
        assert result["entry_count"] == 5
        assert result["file_name"] == "subtitle_sample_valid.srt"
        assert result["encoding"] in ["utf-8", "latin-1"]

    def test_invalid_timestamp_format_detected(self):
        """Invalid timestamp format should be detected and reported."""
        result = validate_srt_format(str(INVALID_TIMESTAMPS_FIXTURE))

        assert result["valid"] is False
        assert len(result["errors"]) > 0

        # Check that timestamp error is reported
        error_messages = " ".join(result["errors"])
        assert "timestamp" in error_messages.lower()

    def test_missing_text_content_detected(self):
        """Subtitles with no text content should be flagged."""
        result = validate_srt_format(str(MISSING_TEXT_FIXTURE))

        assert result["valid"] is False
        assert len(result["errors"]) > 0

        # Check for missing text error
        error_messages = " ".join(result["errors"])
        assert "text content" in error_messages.lower()

    def test_empty_file_handled_gracefully(self):
        """Empty .srt file should be handled without crashing."""
        result = validate_srt_format(str(EMPTY_FIXTURE))

        assert result["valid"] is True  # No entries = no errors
        assert result["entry_count"] == 0
        assert len(result["errors"]) == 0

    def test_file_not_found_raises_error(self):
        """Non-existent file should raise FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            validate_srt_format("nonexistent_file.srt")


class TestExtractSubtitleStats:
    """Test extract_subtitle_stats function."""

    def test_correct_subtitle_count(self):
        """Should count correct number of subtitle entries."""
        stats = extract_subtitle_stats(str(VALID_FIXTURE))

        assert stats["entry_count"] == 5

    def test_correct_word_count(self):
        """Should count total words in dialogue text."""
        stats = extract_subtitle_stats(str(VALID_FIXTURE))

        # Valid fixture has:
        # 1: "This is the first subtitle." = 5 words
        # 2: "This is the second subtitle. It can span multiple lines." = 10 words
        # 3: "Subtitles can also include formatting tags." = 6 words (HTML removed)
        # 4: "Fourth subtitle entry." = 3 words
        # 5: "Fifth and final subtitle." = 4 words
        # Total = 28 words
        assert stats["word_count"] == 28

    def test_time_span_calculation(self):
        """Should calculate duration from first to last subtitle."""
        stats = extract_subtitle_stats(str(VALID_FIXTURE))

        # First: 00:00:20,000 = 20 seconds
        # Last:  00:00:40,000 = 40 seconds
        # Span = 20 seconds = 0.333... minutes
        assert stats["time_span_minutes"] == pytest.approx(0.3, abs=0.1)

    def test_film_slug_extraction(self):
        """Should extract film slug from file name."""
        stats = extract_subtitle_stats(str(VALID_FIXTURE))

        assert stats["film_slug"] == "subtitle_sample_valid"
        assert stats["file_name"] == "subtitle_sample_valid.srt"

    def test_empty_file_statistics(self):
        """Empty file should return zero statistics."""
        stats = extract_subtitle_stats(str(EMPTY_FIXTURE))

        assert stats["entry_count"] == 0
        assert stats["word_count"] == 0
        assert stats["time_span_minutes"] == 0.0

    def test_file_not_found_raises_error(self):
        """Non-existent file should raise FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            extract_subtitle_stats("nonexistent_file.srt")


class TestRealSubtitleFiles:
    """Integration tests with real subtitle files (if available)."""

    @pytest.mark.skipif(
        not Path("data/raw/subtitles/spirited_away_en.srt").exists(),
        reason="Real subtitle files not available"
    )
    def test_spirited_away_english_validation(self):
        """Test validation on real Spirited Away English subtitle."""
        result = validate_srt_format("data/raw/subtitles/spirited_away_en.srt")

        assert result["valid"] is True
        assert result["entry_count"] > 1000  # Should have many subtitles

    @pytest.mark.skipif(
        not Path("data/raw/subtitles/princess_mononoke_en.srt").exists(),
        reason="Real subtitle files not available"
    )
    def test_princess_mononoke_stats(self):
        """Test statistics extraction on real Princess Mononoke file."""
        stats = extract_subtitle_stats("data/raw/subtitles/princess_mononoke_en.srt")

        assert stats["entry_count"] > 1000
        assert stats["word_count"] > 5000
        assert stats["time_span_minutes"] > 100  # Film is ~133 minutes
        assert "princess_mononoke" in stats["film_slug"]
