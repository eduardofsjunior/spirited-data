"""
Unit tests for scripts/identify_priority_films.py

Tests priority calculation, film categorization, and report generation logic.
"""
import json
import tempfile
from pathlib import Path

import pytest

# Import functions from the script
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scripts.identify_priority_films import (
    CROSS_LANG_INCONSISTENCY_THRESHOLD,
    FEATURED_FILMS,
    LOW_PASS_RATE_THRESHOLD,
    MEDIUM_INCONSISTENCY_THRESHOLD,
    MEDIUM_PASS_RATE_THRESHOLD,
    FilmValidationSummary,
    analyze_validation_results,
    calculate_pass_rate,
    calculate_priority_score,
    categorize_priority,
    format_film_title,
    generate_priority_report,
    load_validation_results,
)


class TestLoadValidationResults:
    """Test validation results loading."""

    def test_load_valid_json(self, tmp_path: Path) -> None:
        """Test loading valid JSON file."""
        # Create test JSON file
        test_data = {
            "test_film": {
                "per_language": {"en": {"status": "PASS"}},
                "cross_language": {"status": "PASS"},
            }
        }
        test_file = tmp_path / "test_results.json"
        with open(test_file, "w") as f:
            json.dump(test_data, f)

        # Load results
        results = load_validation_results(test_file)

        assert results == test_data
        assert "test_film" in results

    def test_load_missing_file(self) -> None:
        """Test loading non-existent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            load_validation_results(Path("/nonexistent/file.json"))

    def test_load_invalid_json(self, tmp_path: Path) -> None:
        """Test loading malformed JSON raises ValueError."""
        test_file = tmp_path / "invalid.json"
        with open(test_file, "w") as f:
            f.write("{ invalid json }")

        with pytest.raises(ValueError, match="Failed to parse JSON"):
            load_validation_results(test_file)


class TestCalculatePassRate:
    """Test pass rate calculation."""

    def test_all_pass(self) -> None:
        """Test when all languages pass."""
        per_language = {
            "en": {"status": "PASS"},
            "fr": {"status": "PASS"},
            "es": {"status": "PASS"},
        }

        passed, warned, failed, pass_rate = calculate_pass_rate(per_language)

        assert passed == 3
        assert warned == 0
        assert failed == 0
        assert pass_rate == 1.0

    def test_all_fail(self) -> None:
        """Test when all languages fail."""
        per_language = {
            "en": {"status": "FAIL"},
            "fr": {"status": "FAIL"},
        }

        passed, warned, failed, pass_rate = calculate_pass_rate(per_language)

        assert passed == 0
        assert warned == 0
        assert failed == 2
        assert pass_rate == 0.0

    def test_mixed_statuses(self) -> None:
        """Test mixed pass/warn/fail statuses."""
        per_language = {
            "en": {"status": "PASS"},
            "fr": {"status": "WARN"},
            "es": {"status": "FAIL"},
            "ar": {"status": "PASS"},
        }

        passed, warned, failed, pass_rate = calculate_pass_rate(per_language)

        assert passed == 2
        assert warned == 1
        assert failed == 1
        assert pass_rate == 0.5

    def test_empty_languages(self) -> None:
        """Test with no languages."""
        per_language = {}

        passed, warned, failed, pass_rate = calculate_pass_rate(per_language)

        assert passed == 0
        assert warned == 0
        assert failed == 0
        assert pass_rate == 0.0


class TestCalculatePriorityScore:
    """Test priority score calculation."""

    def test_low_pass_rate_only(self) -> None:
        """Test priority score for low pass rate (<50%)."""
        score = calculate_priority_score(pass_rate=0.4, is_featured=False, cross_language_drift=None)

        assert score == 40  # Low pass rate contributes 40 points

    def test_medium_pass_rate_only(self) -> None:
        """Test priority score for medium pass rate (50-70%)."""
        score = calculate_priority_score(pass_rate=0.6, is_featured=False, cross_language_drift=None)

        assert score == 20  # Medium pass rate contributes 20 points

    def test_high_pass_rate_only(self) -> None:
        """Test priority score for high pass rate (>70%)."""
        score = calculate_priority_score(pass_rate=0.8, is_featured=False, cross_language_drift=None)

        assert score == 0  # High pass rate contributes 0 points

    def test_featured_film_only(self) -> None:
        """Test priority score for featured film."""
        score = calculate_priority_score(pass_rate=1.0, is_featured=True, cross_language_drift=None)

        assert score == 30  # Featured film contributes 30 points

    def test_high_cross_language_drift(self) -> None:
        """Test priority score for high cross-language drift (>10%)."""
        score = calculate_priority_score(pass_rate=1.0, is_featured=False, cross_language_drift=15.0)

        assert score == 30  # High drift contributes 30 points

    def test_medium_cross_language_drift(self) -> None:
        """Test priority score for medium cross-language drift (5-10%)."""
        score = calculate_priority_score(pass_rate=1.0, is_featured=False, cross_language_drift=7.5)

        assert score == 15  # Medium drift contributes 15 points

    def test_low_cross_language_drift(self) -> None:
        """Test priority score for low cross-language drift (<5%)."""
        score = calculate_priority_score(pass_rate=1.0, is_featured=False, cross_language_drift=2.0)

        assert score == 0  # Low drift contributes 0 points

    def test_maximum_priority_score(self) -> None:
        """Test maximum possible priority score."""
        score = calculate_priority_score(pass_rate=0.3, is_featured=True, cross_language_drift=20.0)

        assert score == 100  # 40 (low pass) + 30 (featured) + 30 (high drift)

    def test_combined_factors(self) -> None:
        """Test combination of multiple priority factors."""
        # Medium pass rate + featured film
        score = calculate_priority_score(pass_rate=0.6, is_featured=True, cross_language_drift=None)
        assert score == 50  # 20 + 30

        # Low pass rate + medium drift
        score = calculate_priority_score(pass_rate=0.4, is_featured=False, cross_language_drift=8.0)
        assert score == 55  # 40 + 15


class TestCategorizePriority:
    """Test priority categorization."""

    def test_high_priority_by_score(self) -> None:
        """Test high priority categorization by score."""
        category = categorize_priority(priority_score=70, pass_rate=0.8)

        assert category == "HIGH"

    def test_medium_priority_by_score(self) -> None:
        """Test medium priority categorization by score."""
        category = categorize_priority(priority_score=30, pass_rate=0.8)

        assert category == "MEDIUM"

    def test_medium_priority_by_pass_rate(self) -> None:
        """Test medium priority by pass rate even with low score."""
        category = categorize_priority(priority_score=10, pass_rate=0.65)

        assert category == "MEDIUM"

    def test_low_priority(self) -> None:
        """Test low priority categorization."""
        category = categorize_priority(priority_score=10, pass_rate=0.85)

        assert category == "LOW"

    def test_edge_cases(self) -> None:
        """Test edge case thresholds."""
        # Exactly 50 score = HIGH
        assert categorize_priority(50, 0.8) == "HIGH"

        # Just below 50 score but low pass rate = MEDIUM
        assert categorize_priority(49, 0.65) == "MEDIUM"

        # Exactly 20 score = MEDIUM
        assert categorize_priority(20, 0.8) == "MEDIUM"


class TestFormatFilmTitle:
    """Test film title formatting."""

    def test_simple_title(self) -> None:
        """Test formatting simple slug."""
        assert format_film_title("totoro") == "Totoro"

    def test_multi_word_title(self) -> None:
        """Test formatting multi-word slug."""
        assert format_film_title("spirited_away") == "Spirited Away"

    def test_already_formatted(self) -> None:
        """Test slug that's already in title case."""
        assert format_film_title("princess_mononoke") == "Princess Mononoke"


class TestAnalyzeValidationResults:
    """Test validation results analysis."""

    def test_analyze_single_film(self) -> None:
        """Test analyzing single film."""
        results = {
            "test_film": {
                "per_language": {
                    "en": {"status": "PASS"},
                    "fr": {"status": "FAIL"},
                },
                "cross_language": {"status": "PASS", "max_drift_percent": 2.0},
            }
        }

        summaries = analyze_validation_results(results)

        assert len(summaries) == 1
        assert summaries[0].film_slug == "test_film"
        assert summaries[0].total_languages == 2
        assert summaries[0].languages_passed == 1
        assert summaries[0].pass_rate == 0.5

    def test_analyze_featured_film(self) -> None:
        """Test that featured films are correctly identified."""
        results = {
            "spirited_away": {
                "per_language": {"en": {"status": "PASS"}},
                "cross_language": {"status": "PASS", "max_drift_percent": 1.0},
            }
        }

        summaries = analyze_validation_results(results)

        assert summaries[0].is_featured is True
        assert summaries[0].priority_score >= 30  # Featured film bonus

    def test_analyze_non_featured_film(self) -> None:
        """Test that non-featured films are correctly identified."""
        results = {
            "unknown_film": {
                "per_language": {"en": {"status": "PASS"}},
                "cross_language": {"status": "PASS", "max_drift_percent": 1.0},
            }
        }

        summaries = analyze_validation_results(results)

        assert summaries[0].is_featured is False

    def test_sorting_by_priority(self) -> None:
        """Test that films are sorted by priority score."""
        results = {
            "low_priority": {
                "per_language": {
                    "en": {"status": "PASS"},
                    "fr": {"status": "PASS"},
                },
                "cross_language": {"status": "PASS", "max_drift_percent": 1.0},
            },
            "high_priority": {
                "per_language": {
                    "en": {"status": "FAIL"},
                    "fr": {"status": "FAIL"},
                },
                "cross_language": {"status": "FAIL", "max_drift_percent": 50.0},
            },
        }

        summaries = analyze_validation_results(results)

        # High priority should be first
        assert summaries[0].film_slug == "high_priority"
        assert summaries[1].film_slug == "low_priority"

    def test_empty_results(self) -> None:
        """Test analyzing empty results."""
        results = {}

        summaries = analyze_validation_results(results)

        assert len(summaries) == 0


class TestGeneratePriorityReport:
    """Test priority report generation."""

    def test_generate_report_basic(self, tmp_path: Path) -> None:
        """Test basic report generation."""
        summaries = [
            FilmValidationSummary(
                film_slug="test_film",
                total_languages=2,
                languages_passed=1,
                languages_warned=0,
                languages_failed=1,
                pass_rate=0.5,
                cross_language_status="PASS",
                cross_language_drift=2.0,
                is_featured=False,
                priority_score=20,
                priority_category="MEDIUM",
            )
        ]

        output_file = tmp_path / "report.md"
        generate_priority_report(summaries, output_file)

        # Verify file was created
        assert output_file.exists()

        # Read and verify content
        content = output_file.read_text()
        assert "# Subtitle Improvement Priorities" in content
        assert "Test Film" in content
        assert "Medium Priority Films" in content

    def test_report_with_featured_film(self, tmp_path: Path) -> None:
        """Test report generation with featured film."""
        summaries = [
            FilmValidationSummary(
                film_slug="spirited_away",
                total_languages=2,
                languages_passed=0,
                languages_warned=0,
                languages_failed=2,
                pass_rate=0.0,
                cross_language_status="FAIL",
                cross_language_drift=40.0,
                is_featured=True,
                priority_score=100,
                priority_category="HIGH",
            )
        ]

        output_file = tmp_path / "report.md"
        generate_priority_report(summaries, output_file)

        content = output_file.read_text()
        assert "⭐ **FEATURED FILM**" in content
        assert "Spirited Away" in content
        assert "High Priority Films" in content

    def test_report_creates_parent_directories(self, tmp_path: Path) -> None:
        """Test that report generation creates parent directories."""
        output_file = tmp_path / "nested" / "dirs" / "report.md"

        summaries = [
            FilmValidationSummary(
                film_slug="test_film",
                total_languages=2,
                languages_passed=2,
                languages_warned=0,
                languages_failed=0,
                pass_rate=1.0,
                cross_language_status="PASS",
                cross_language_drift=1.0,
                is_featured=False,
                priority_score=0,
                priority_category="LOW",
            )
        ]

        generate_priority_report(summaries, output_file)

        assert output_file.exists()


class TestConstants:
    """Test that constants are correctly defined."""

    def test_featured_films_list(self) -> None:
        """Test that featured films list contains expected films."""
        expected_films = {
            "spirited_away",
            "princess_mononoke",
            "my_neighbor_totoro",
            "howls_moving_castle",
            "kikis_delivery_service",
        }

        assert FEATURED_FILMS == expected_films

    def test_thresholds(self) -> None:
        """Test that thresholds are reasonable."""
        assert 0 < LOW_PASS_RATE_THRESHOLD < 1
        assert 0 < MEDIUM_PASS_RATE_THRESHOLD < 1
        assert LOW_PASS_RATE_THRESHOLD < MEDIUM_PASS_RATE_THRESHOLD
        assert CROSS_LANG_INCONSISTENCY_THRESHOLD > MEDIUM_INCONSISTENCY_THRESHOLD
        assert MEDIUM_INCONSISTENCY_THRESHOLD > 0
