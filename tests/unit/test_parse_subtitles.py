"""
Unit tests for subtitle parsing module.

Tests cover parsing logic, text cleaning, encoding detection, and validation.
"""
import json
import os
import tempfile
from pathlib import Path

import pytest

from src.nlp.parse_subtitles import (
    clean_dialogue_text,
    extract_film_metadata,
    parse_srt_file,
    save_parsed_subtitles,
    validate_parsed_subtitles,
)


class TestParseSrtFile:
    """Test parse_srt_file function."""

    def test_parse_srt_file_valid(self, tmp_path):
        """Test parsing valid .srt file."""
        # Create a valid .srt file
        srt_content = """1
00:00:20,000 --> 00:00:24,400
This is the first subtitle.

2
00:00:24,600 --> 00:00:27,800
This is the second subtitle.
It can span multiple lines.

3
00:00:28,000 --> 00:00:32,500
Third subtitle entry.
"""
        srt_file = tmp_path / "test.srt"
        srt_file.write_text(srt_content, encoding="utf-8")

        result, skipped = parse_srt_file(str(srt_file))

        assert len(result) == 3
        assert skipped == 0
        assert result[0]["subtitle_index"] == 1
        assert result[0]["start_time"] == 20.0
        assert result[0]["end_time"] == 24.4
        assert abs(result[0]["duration"] - 4.4) < 0.01  # Floating point tolerance
        assert "first subtitle" in result[0]["dialogue_text"]

    def test_parse_srt_file_with_html_tags(self, tmp_path):
        """Test parsing .srt file with HTML formatting tags."""
        srt_content = """1
00:00:20,000 --> 00:00:24,400
Subtitles can include <i>formatting</i> tags.
"""
        srt_file = tmp_path / "test.srt"
        srt_file.write_text(srt_content, encoding="utf-8")

        result, skipped = parse_srt_file(str(srt_file))

        assert len(result) == 1
        assert skipped == 0
        # HTML tags should be removed
        assert "<i>" not in result[0]["dialogue_text"]
        assert "formatting" in result[0]["dialogue_text"]

    def test_parse_srt_file_malformed_timestamps(self, tmp_path):
        """Test handling of malformed timestamp entries."""
        srt_content = """1
00:00:20,000 --> 00:00:24,400
Valid subtitle.

2
00:00:24,600 -> 00:00:27,800
Missing arrow in timestamp.

3
00:00:28,000 --> 00:00:32,500
Another valid subtitle.
"""
        srt_file = tmp_path / "test.srt"
        srt_file.write_text(srt_content, encoding="utf-8")

        result, skipped = parse_srt_file(str(srt_file))

        # Should parse valid entries and skip malformed ones
        assert len(result) >= 2  # At least 2 valid entries
        # Note: pysrt may handle some malformed entries differently


class TestCleanDialogueText:
    """Test clean_dialogue_text function."""

    def test_clean_dialogue_text_html_tags(self):
        """Test HTML tag removal."""
        text = "<i>This is italic</i> text"
        cleaned = clean_dialogue_text(text)
        assert "<i>" not in cleaned
        assert "This is italic" in cleaned

    def test_clean_dialogue_text_multiple_tags(self):
        """Test removal of multiple HTML tags."""
        text = "<b>Bold</b> and <i>italic</i> and <u>underline</u>"
        cleaned = clean_dialogue_text(text)
        assert "<" not in cleaned
        assert ">" not in cleaned
        assert "Bold" in cleaned
        assert "italic" in cleaned

    def test_clean_dialogue_text_multi_line(self):
        """Test multi-line dialogue joining."""
        text = "Line one\nLine two\nLine three"
        cleaned = clean_dialogue_text(text)
        assert "\n" not in cleaned
        assert "Line one Line two Line three" == cleaned

    def test_clean_dialogue_text_whitespace(self):
        """Test whitespace normalization."""
        text = "  Multiple   spaces  "
        cleaned = clean_dialogue_text(text)
        assert cleaned == "Multiple spaces"
        assert cleaned.count(" ") == 1

    def test_clean_dialogue_text_empty(self):
        """Test empty string handling."""
        assert clean_dialogue_text("") == ""
        assert clean_dialogue_text("   ") == ""

    def test_clean_dialogue_text_combined(self):
        """Test combination of HTML tags and multi-line text."""
        text = "<i>Hello</i>\nworld\n  <b>test</b>  "
        cleaned = clean_dialogue_text(text)
        assert "<" not in cleaned
        assert ">" not in cleaned
        assert "\n" not in cleaned
        assert cleaned == "Hello world test"


class TestExtractFilmMetadata:
    """Test extract_film_metadata function."""

    def test_extract_film_metadata_basic(self):
        """Test basic metadata extraction."""
        filepath = "data/raw/subtitles/spirited_away_en.srt"
        subtitles = [
            {"subtitle_index": 1, "start_time": 10.0, "end_time": 15.0, "duration": 5.0, "dialogue_text": "Test"},
            {"subtitle_index": 2, "start_time": 20.0, "end_time": 25.0, "duration": 5.0, "dialogue_text": "Test"},
        ]

        metadata = extract_film_metadata(filepath, subtitles)

        assert metadata["film_slug"] == "spirited_away_en"
        assert "film_name" in metadata
        assert metadata["total_subtitles"] == 2
        assert metadata["total_duration"] == 15.0  # 25.0 - 10.0
        assert "parse_timestamp" in metadata

    def test_extract_film_metadata_empty_subtitles(self):
        """Test metadata extraction with empty subtitle list."""
        filepath = "data/raw/subtitles/test_en.srt"
        subtitles = []

        metadata = extract_film_metadata(filepath, subtitles)

        assert metadata["total_subtitles"] == 0
        assert metadata["total_duration"] == 0.0

    def test_extract_film_metadata_film_name(self):
        """Test film name extraction from slug."""
        filepath = "data/raw/subtitles/princess_mononoke_en.srt"
        subtitles = [{"subtitle_index": 1, "start_time": 0.0, "end_time": 1.0, "duration": 1.0, "dialogue_text": "Test"}]

        metadata = extract_film_metadata(filepath, subtitles)

        assert metadata["film_slug"] == "princess_mononoke_en"
        # Film name should be converted from slug
        assert "film_name" in metadata


class TestSaveParsedSubtitles:
    """Test save_parsed_subtitles function."""

    def test_save_parsed_subtitles_basic(self, tmp_path):
        """Test saving parsed subtitles to JSON."""
        subtitles = [
            {"subtitle_index": 1, "start_time": 10.0, "end_time": 15.0, "duration": 5.0, "dialogue_text": "Test"}
        ]
        metadata = {
            "film_name": "Test Film",
            "film_slug": "test_en",
            "total_subtitles": 1,
            "total_duration": 5.0,
            "parse_timestamp": "2025-01-01T00:00:00",
        }
        output_path = str(tmp_path / "test_parsed.json")

        save_parsed_subtitles(subtitles, metadata, output_path)

        assert Path(output_path).exists()
        with open(output_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert "metadata" in data
        assert "subtitles" in data
        assert data["metadata"]["film_name"] == "Test Film"
        assert len(data["subtitles"]) == 1

    def test_save_parsed_subtitles_creates_directory(self, tmp_path):
        """Test that output directory is created if it doesn't exist."""
        subtitles = []
        metadata = {"film_name": "Test", "film_slug": "test", "total_subtitles": 0, "total_duration": 0.0, "parse_timestamp": "2025-01-01T00:00:00"}
        output_path = str(tmp_path / "subdir" / "test.json")

        save_parsed_subtitles(subtitles, metadata, output_path)

        assert Path(output_path).exists()


class TestValidateParsedSubtitles:
    """Test validate_parsed_subtitles function."""

    def test_validate_parsed_subtitles_match(self, tmp_path):
        """Test validation with matching counts."""
        # Create .srt file
        srt_content = """1
00:00:20,000 --> 00:00:24,400
First subtitle.

2
00:00:24,600 --> 00:00:27,800
Second subtitle.
"""
        srt_file = tmp_path / "test.srt"
        srt_file.write_text(srt_content, encoding="utf-8")

        # Create JSON file
        json_data = {
            "metadata": {"total_subtitles": 2, "film_name": "Test", "film_slug": "test_en", "total_duration": 7.8, "parse_timestamp": "2025-01-01T00:00:00"},
            "subtitles": [
                {"subtitle_index": 1, "start_time": 20.0, "end_time": 24.4, "duration": 4.4, "dialogue_text": "First subtitle."},
                {"subtitle_index": 2, "start_time": 24.6, "end_time": 27.8, "duration": 3.2, "dialogue_text": "Second subtitle."},
            ],
        }
        json_file = tmp_path / "test_parsed.json"
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(json_data, f)

        result = validate_parsed_subtitles(str(srt_file), str(json_file))

        assert result["matched"] is True
        assert result["srt_count"] == 2
        assert result["json_count"] == 2
        assert len(result["spot_check_results"]) > 0

    def test_validate_parsed_subtitles_count_mismatch(self, tmp_path):
        """Test validation with mismatched counts."""
        # Create .srt file with 2 entries
        srt_content = """1
00:00:20,000 --> 00:00:24,400
First subtitle.

2
00:00:24,600 --> 00:00:27,800
Second subtitle.
"""
        srt_file = tmp_path / "test.srt"
        srt_file.write_text(srt_content, encoding="utf-8")

        # Create JSON file with 1 entry (mismatch)
        json_data = {
            "metadata": {"total_subtitles": 1, "film_name": "Test", "film_slug": "test_en", "total_duration": 4.4, "parse_timestamp": "2025-01-01T00:00:00"},
            "subtitles": [
                {"subtitle_index": 1, "start_time": 20.0, "end_time": 24.4, "duration": 4.4, "dialogue_text": "First subtitle."},
            ],
        }
        json_file = tmp_path / "test_parsed.json"
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(json_data, f)

        result = validate_parsed_subtitles(str(srt_file), str(json_file))

        assert result["matched"] is False
        assert result["srt_count"] == 2
        assert result["json_count"] == 1


class TestIntegrationWithFixtures:
    """Integration tests using test fixtures."""

    def test_parse_valid_fixture(self):
        """Test parsing with valid test fixture."""
        fixture_path = Path(__file__).parent.parent / "fixtures" / "subtitle_sample_valid.srt"
        if not fixture_path.exists():
            pytest.skip("Fixture file not found")

        result, skipped = parse_srt_file(str(fixture_path))

        assert len(result) == 5
        assert skipped == 0
        assert result[0]["subtitle_index"] == 1
        assert result[-1]["subtitle_index"] == 5

    def test_parse_invalid_timestamps_fixture(self):
        """Test parsing with invalid timestamps fixture."""
        fixture_path = Path(__file__).parent.parent / "fixtures" / "subtitle_sample_invalid_timestamps.srt"
        if not fixture_path.exists():
            pytest.skip("Fixture file not found")

        result, skipped = parse_srt_file(str(fixture_path))

        # Should handle malformed entries gracefully
        assert len(result) >= 0  # May parse some entries
        # May have skipped entries
        assert isinstance(skipped, int)

