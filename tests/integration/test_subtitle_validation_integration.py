"""
Integration tests for subtitle timing validation.

Tests the validation script with real subtitle files and film metadata.
"""

import json
from pathlib import Path

import pytest

from src.validation.validate_subtitle_timing import (
    generate_validation_report,
    load_film_versions,
    validate_cross_language_consistency,
    validate_subtitle_timing,
)


class TestSubtitleValidationIntegration:
    """Integration tests for subtitle validation system."""

    def test_load_film_versions(self):
        """Test loading film versions metadata."""
        film_versions = load_film_versions("data/metadata/film_versions.json")

        assert isinstance(film_versions, dict)
        assert len(film_versions) > 0

        # Check expected films are present
        expected_films = ["spirited_away", "princess_mononoke", "my_neighbor_totoro"]
        for film in expected_films:
            assert film in film_versions, f"{film} should be in film_versions"

        # Validate structure of film version data
        for film_slug, data in film_versions.items():
            assert "title" in data
            assert "runtime_seconds" in data
            assert isinstance(data["runtime_seconds"], (int, float))
            assert data["runtime_seconds"] > 0

    def test_validate_subtitle_timing_with_real_file(self):
        """Test validation with a real subtitle file."""
        film_versions = load_film_versions("data/metadata/film_versions.json")

        # Find a subtitle file to test
        subtitle_dir = Path("data/processed/subtitles")
        subtitle_files = list(subtitle_dir.glob("*_parsed.json"))

        if subtitle_files:
            # Test with first available subtitle file
            subtitle_file = subtitle_files[0]

            result = validate_subtitle_timing(subtitle_file, film_versions)

            # Validate result structure
            assert "status" in result
            assert result["status"] in ["PASS", "WARN", "FAIL"]
            assert "timing_drift_percent" in result
            assert "last_subtitle_time" in result
            assert "documented_runtime" in result
            assert "subtitle_duration" in result
            assert "issues" in result
            assert "warnings" in result

            # Validate types
            assert isinstance(result["issues"], list)
            assert isinstance(result["warnings"], list)

    def test_validate_cross_language_consistency_with_real_data(self):
        """Test cross-language consistency validation with real subtitle files."""
        # Test with a film that has multiple language versions
        result = validate_cross_language_consistency(
            "my_neighbor_totoro", subtitle_dir="data/processed/subtitles"
        )

        # Validate result structure
        assert "status" in result
        assert result["status"] in ["PASS", "FAIL"]
        assert "max_drift_percent" in result
        assert "durations" in result
        assert "issues" in result
        assert "warnings" in result

        # Validate durations
        assert isinstance(result["durations"], dict)
        if result["durations"]:
            for lang, duration in result["durations"].items():
                assert isinstance(duration, (int, float))
                assert duration > 0

    def test_generate_validation_report_creates_files(self):
        """Test that validation report is generated successfully."""
        output_path = "data/processed/test_subtitle_validation_report.md"

        try:
            results = generate_validation_report(
                subtitle_dir="data/processed/subtitles",
                metadata_path="data/metadata/film_versions.json",
                output_path=output_path,
            )

            # Check results structure
            assert isinstance(results, dict)
            assert len(results) > 0

            # Verify each film has validation data
            for film_slug, data in results.items():
                assert "per_language" in data
                assert "cross_language" in data
                assert isinstance(data["per_language"], dict)
                assert isinstance(data["cross_language"], dict)

            # Check report file was created
            report_file = Path(output_path)
            assert report_file.exists(), "Validation report should be created"

            # Check report content
            report_content = report_file.read_text()
            assert "# Subtitle Version Validation Report" in report_content
            assert "Executive Summary" in report_content
            assert "PASS" in report_content or "WARN" in report_content or "FAIL" in report_content

        finally:
            # Clean up test report file
            if Path(output_path).exists():
                Path(output_path).unlink()

    def test_validation_detects_timing_drift(self):
        """Test that validation correctly detects timing drift."""
        film_versions = load_film_versions("data/metadata/film_versions.json")

        subtitle_dir = Path("data/processed/subtitles")
        subtitle_files = list(subtitle_dir.glob("*_parsed.json"))

        if subtitle_files:
            pass_count = 0
            warn_count = 0
            fail_count = 0

            # Validate first 10 files
            for subtitle_file in subtitle_files[:10]:
                result = validate_subtitle_timing(subtitle_file, film_versions)

                if result["status"] == "PASS":
                    # PASS should have low drift
                    if result["timing_drift_percent"] is not None:
                        assert result["timing_drift_percent"] < 2.0
                    pass_count += 1
                elif result["status"] == "WARN":
                    # WARN should have moderate drift
                    if result["timing_drift_percent"] is not None:
                        assert 2.0 <= result["timing_drift_percent"] <= 5.0
                    warn_count += 1
                elif result["status"] == "FAIL":
                    # FAIL should have high drift or missing data
                    if result["timing_drift_percent"] is not None:
                        assert result["timing_drift_percent"] > 5.0
                    fail_count += 1

            # Should have at least some results
            total_validated = pass_count + warn_count + fail_count
            assert total_validated > 0, "Should have validated at least one file"

    def test_validation_handles_missing_film_version(self):
        """Test that validation handles missing film version gracefully."""
        film_versions = {}  # Empty versions

        subtitle_dir = Path("data/processed/subtitles")
        subtitle_files = list(subtitle_dir.glob("*_parsed.json"))

        if subtitle_files:
            subtitle_file = subtitle_files[0]
            result = validate_subtitle_timing(subtitle_file, film_versions)

            # Should return FAIL status with appropriate error
            assert result["status"] == "FAIL"
            assert len(result["issues"]) > 0
            assert any("No documented film version" in issue for issue in result["issues"])

    def test_cross_language_consistency_detects_drift(self):
        """Test that cross-language validation detects drift correctly."""
        # Test with a film known to have some cross-language drift
        result = validate_cross_language_consistency(
            "spirited_away", subtitle_dir="data/processed/subtitles"
        )

        # Should have multiple languages
        assert len(result["durations"]) > 1

        # Check drift calculation
        if len(result["durations"]) > 1:
            durations = list(result["durations"].values())
            avg = sum(durations) / len(durations)
            max_drift = max(abs(d - avg) / avg * 100 for d in durations)

            # Reported drift should match calculation
            assert abs(result["max_drift_percent"] - max_drift) < 0.1

            # Status should match drift threshold
            if max_drift > 3.0:
                assert result["status"] == "FAIL"
            else:
                assert result["status"] == "PASS"


class TestValidationReportContent:
    """Tests for validation report content and format."""

    @pytest.fixture
    def validation_results(self):
        """Load validation results for testing."""
        results_path = Path("data/processed/subtitle_validation_results.json")

        if not results_path.exists():
            pytest.skip("Validation results not available")

        with open(results_path) as f:
            return json.load(f)

    def test_validation_results_structure(self, validation_results):
        """Test that validation results have correct structure."""
        assert isinstance(validation_results, dict)

        for film_slug, data in validation_results.items():
            assert "per_language" in data
            assert "cross_language" in data

            # Check per-language results
            for lang, validation in data["per_language"].items():
                assert "status" in validation
                assert validation["status"] in ["PASS", "WARN", "FAIL"]
                assert "timing_drift_percent" in validation
                assert "issues" in validation
                assert "warnings" in validation

            # Check cross-language results
            cl = data["cross_language"]
            assert "status" in cl
            assert cl["status"] in ["PASS", "FAIL"]
            assert "durations" in cl

    def test_validation_report_exists(self):
        """Test that validation report file exists."""
        report_path = Path("data/processed/subtitle_validation_report.md")

        assert report_path.exists(), "Validation report should exist"

        # Check file is not empty
        content = report_path.read_text()
        assert len(content) > 0
        assert "# Subtitle Version Validation Report" in content


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
