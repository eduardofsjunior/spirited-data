#!/usr/bin/env python3
"""
Identify priority films for subtitle improvement based on validation results.

This script analyzes subtitle validation results and generates a priority report
to guide subtitle acquisition efforts for maximum ROI.

Usage:
    python scripts/identify_priority_films.py
"""
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Add src to path for potential future imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Featured films with high portfolio impact (will be showcased in Epic 5)
FEATURED_FILMS = {
    "spirited_away",
    "princess_mononoke",
    "my_neighbor_totoro",
    "howls_moving_castle",
    "kikis_delivery_service",
}

# Priority thresholds
LOW_PASS_RATE_THRESHOLD = 0.50  # <50% languages pass = HIGH priority
MEDIUM_PASS_RATE_THRESHOLD = 0.70  # 50-70% = MEDIUM priority
CROSS_LANG_INCONSISTENCY_THRESHOLD = 10.0  # >10% duration variance = HIGH priority
MEDIUM_INCONSISTENCY_THRESHOLD = 5.0  # 5-10% = MEDIUM priority


@dataclass
class FilmValidationSummary:
    """Summary of validation results for a single film."""

    film_slug: str
    total_languages: int
    languages_passed: int
    languages_warned: int
    languages_failed: int
    pass_rate: float
    cross_language_status: str
    cross_language_drift: Optional[float]
    is_featured: bool
    priority_score: int
    priority_category: str


def load_validation_results(results_path: Path) -> Dict:
    """
    Load subtitle validation results from JSON file.

    Args:
        results_path: Path to subtitle_validation_results.json

    Returns:
        Dictionary containing validation results per film

    Raises:
        FileNotFoundError: If results file does not exist
        ValueError: If JSON is malformed
    """
    logger.info(f"Loading validation results from: {results_path}")

    if not results_path.exists():
        raise FileNotFoundError(f"Validation results not found: {results_path}")

    try:
        with open(results_path, "r", encoding="utf-8") as f:
            results = json.load(f)
        logger.info(f"Loaded validation results for {len(results)} films")
        return results
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse JSON: {e}")


def calculate_pass_rate(per_language: Dict) -> Tuple[int, int, int, float]:
    """
    Calculate language pass rate for a film.

    Args:
        per_language: Dictionary of language validation results

    Returns:
        Tuple of (passed_count, warned_count, failed_count, pass_rate)
    """
    total = len(per_language)
    passed = sum(1 for lang_data in per_language.values() if lang_data["status"] == "PASS")
    warned = sum(1 for lang_data in per_language.values() if lang_data["status"] == "WARN")
    failed = total - passed - warned

    pass_rate = passed / total if total > 0 else 0.0

    return passed, warned, failed, pass_rate


def calculate_priority_score(
    pass_rate: float,
    is_featured: bool,
    cross_language_drift: Optional[float],
) -> int:
    """
    Calculate priority score for a film (higher = more important).

    Args:
        pass_rate: Percentage of languages that passed validation (0-1)
        is_featured: Whether film is featured in Epic 5
        cross_language_drift: Cross-language timing drift percentage

    Returns:
        Priority score (0-100)
    """
    score = 0

    # Low pass rate contributes up to 40 points
    if pass_rate < LOW_PASS_RATE_THRESHOLD:
        score += 40
    elif pass_rate < MEDIUM_PASS_RATE_THRESHOLD:
        score += 20

    # Featured film status adds 30 points
    if is_featured:
        score += 30

    # Cross-language inconsistency contributes up to 30 points
    if cross_language_drift is not None:
        if cross_language_drift > CROSS_LANG_INCONSISTENCY_THRESHOLD:
            score += 30
        elif cross_language_drift > MEDIUM_INCONSISTENCY_THRESHOLD:
            score += 15

    return score


def categorize_priority(priority_score: int, pass_rate: float) -> str:
    """
    Categorize film into High/Medium/Low priority.

    Args:
        priority_score: Calculated priority score
        pass_rate: Percentage of languages that passed validation (0-1)

    Returns:
        Priority category: "HIGH", "MEDIUM", or "LOW"
    """
    if priority_score >= 50:
        return "HIGH"
    elif priority_score >= 20 or pass_rate < MEDIUM_PASS_RATE_THRESHOLD:
        return "MEDIUM"
    else:
        return "LOW"


def analyze_validation_results(results: Dict) -> List[FilmValidationSummary]:
    """
    Analyze validation results and generate film summaries.

    Args:
        results: Validation results dictionary

    Returns:
        List of FilmValidationSummary objects, sorted by priority score (descending)
    """
    logger.info("Analyzing validation results and calculating priorities...")

    summaries = []

    for film_slug, film_data in results.items():
        per_language = film_data.get("per_language", {})
        cross_language = film_data.get("cross_language", {})

        # Calculate pass rate
        passed, warned, failed, pass_rate = calculate_pass_rate(per_language)

        # Extract cross-language drift
        cross_lang_status = cross_language.get("status", "UNKNOWN")
        cross_lang_drift = cross_language.get("max_drift_percent")

        # Check if featured
        is_featured = film_slug in FEATURED_FILMS

        # Calculate priority
        priority_score = calculate_priority_score(pass_rate, is_featured, cross_lang_drift)
        priority_category = categorize_priority(priority_score, pass_rate)

        summary = FilmValidationSummary(
            film_slug=film_slug,
            total_languages=len(per_language),
            languages_passed=passed,
            languages_warned=warned,
            languages_failed=failed,
            pass_rate=pass_rate,
            cross_language_status=cross_lang_status,
            cross_language_drift=cross_lang_drift,
            is_featured=is_featured,
            priority_score=priority_score,
            priority_category=priority_category,
        )

        summaries.append(summary)

    # Sort by priority score (descending), then by pass rate (ascending)
    summaries.sort(key=lambda x: (-x.priority_score, x.pass_rate))

    logger.info(f"Generated summaries for {len(summaries)} films")
    return summaries


def format_film_title(film_slug: str) -> str:
    """
    Format film slug into human-readable title.

    Args:
        film_slug: Film identifier (e.g., "spirited_away")

    Returns:
        Formatted title (e.g., "Spirited Away")
    """
    return film_slug.replace("_", " ").title()


def generate_priority_report(
    summaries: List[FilmValidationSummary],
    output_path: Path,
) -> None:
    """
    Generate markdown priority report.

    Args:
        summaries: List of film validation summaries
        output_path: Path to output markdown file
    """
    logger.info(f"Generating priority report: {output_path}")

    # Group by priority
    high_priority = [s for s in summaries if s.priority_category == "HIGH"]
    medium_priority = [s for s in summaries if s.priority_category == "MEDIUM"]
    low_priority = [s for s in summaries if s.priority_category == "LOW"]

    # Calculate overall metrics
    total_films = len(summaries)
    total_langs = sum(s.total_languages for s in summaries)
    total_passed = sum(s.languages_passed for s in summaries)
    overall_pass_rate = total_passed / total_langs if total_langs > 0 else 0

    # Target improvement
    target_films = len(high_priority) + len(medium_priority)

    # Build markdown report
    lines = [
        "# Subtitle Improvement Priorities",
        "",
        "**Generated by:** `scripts/identify_priority_films.py`",
        f"**Source:** `data/processed/subtitle_validation_results.json`",
        "",
        "## Executive Summary",
        "",
        f"- **Total Films Analyzed:** {total_films}",
        f"- **Current Overall Pass Rate:** {overall_pass_rate:.1%}",
        f"- **Target Pass Rate:** 70%+",
        f"- **Films Requiring Improvement:** {target_films} ({len(high_priority)} high priority, {len(medium_priority)} medium priority)",
        "",
        "## Priority Criteria",
        "",
        "Films are prioritized based on:",
        "",
        "1. **Low Pass Rate** (<50% languages pass validation) → HIGH priority",
        "2. **Featured Films** (Epic 5 showcase: Spirited Away, Princess Mononoke, My Neighbor Totoro, Howl's Moving Castle, Kiki's Delivery Service) → HIGH priority",
        "3. **Cross-Language Inconsistency** (>10% duration variance) → HIGH priority",
        "4. **Medium Pass Rate** (50-70% pass) OR moderate inconsistency (5-10% variance) → MEDIUM priority",
        "5. **High Pass Rate** (>70% pass AND <5% cross-language variance) → LOW priority",
        "",
        "---",
        "",
        "## High Priority Films (Acquire ASAP)",
        "",
    ]

    if high_priority:
        for i, film in enumerate(high_priority, 1):
            drift_str = (
                f"{film.cross_language_drift:.1f}%"
                if film.cross_language_drift is not None
                else "N/A"
            )
            featured_str = " ⭐ **FEATURED FILM**" if film.is_featured else ""

            lines.extend(
                [
                    f"### {i}. {format_film_title(film.film_slug)}{featured_str}",
                    "",
                    f"- **Pass Rate:** {film.pass_rate:.1%} ({film.languages_passed}/{film.total_languages} languages)",
                    f"- **Status:** {film.languages_passed} PASS, {film.languages_warned} WARN, {film.languages_failed} FAIL",
                    f"- **Cross-Language Drift:** {drift_str}",
                    f"- **Priority Score:** {film.priority_score}/100",
                    "",
                    "**Recommended Action:** Acquire higher-quality subtitles (priority: English)",
                    "",
                ]
            )
    else:
        lines.append("*No high priority films identified.*")
        lines.append("")

    lines.extend(
        [
            "---",
            "",
            "## Medium Priority Films",
            "",
        ]
    )

    if medium_priority:
        for i, film in enumerate(medium_priority, 1):
            drift_str = (
                f"{film.cross_language_drift:.1f}%"
                if film.cross_language_drift is not None
                else "N/A"
            )
            featured_str = " ⭐ **FEATURED FILM**" if film.is_featured else ""

            lines.extend(
                [
                    f"### {i}. {format_film_title(film.film_slug)}{featured_str}",
                    "",
                    f"- **Pass Rate:** {film.pass_rate:.1%} ({film.languages_passed}/{film.total_languages} languages)",
                    f"- **Cross-Language Drift:** {drift_str}",
                    f"- **Priority Score:** {film.priority_score}/100",
                    "",
                ]
            )
    else:
        lines.append("*No medium priority films identified.*")
        lines.append("")

    lines.extend(
        [
            "---",
            "",
            "## Low Priority Films (Acceptable Quality)",
            "",
        ]
    )

    if low_priority:
        for i, film in enumerate(low_priority, 1):
            lines.append(
                f"{i}. **{format_film_title(film.film_slug)}** - {film.pass_rate:.1%} pass rate ({film.languages_passed}/{film.total_languages})"
            )
        lines.append("")
    else:
        lines.append("*No low priority films identified.*")
        lines.append("")

    lines.extend(
        [
            "---",
            "",
            "## Next Steps",
            "",
            "1. **Review High Priority Films:** Focus on featured films first (Spirited Away, Princess Mononoke, etc.)",
            "2. **Acquire English Subtitles:** Prioritize English subtitles as they have highest portfolio impact",
            "3. **Target 70%+ Pass Rate:** Improving high/medium priority films should achieve target",
            "4. **Version Consistency:** Ensure all languages use same film version (theatrical/Blu-ray/streaming)",
            "",
            f"**Estimated Improvement:** Fixing {target_films} films should increase pass rate from {overall_pass_rate:.1%} to 70%+",
            "",
        ]
    )

    # Write to file
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    logger.info(f"✅ Priority report generated: {output_path}")
    logger.info(f"   - High priority: {len(high_priority)} films")
    logger.info(f"   - Medium priority: {len(medium_priority)} films")
    logger.info(f"   - Low priority: {len(low_priority)} films")


def main() -> int:
    """
    Main execution function.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    try:
        # Define paths
        project_root = Path(__file__).parent.parent
        results_path = project_root / "data" / "processed" / "subtitle_validation_results.json"
        output_path = project_root / "data" / "metadata" / "subtitle_improvement_priorities.md"

        # Load validation results
        results = load_validation_results(results_path)

        # Analyze results
        summaries = analyze_validation_results(results)

        # Generate report
        generate_priority_report(summaries, output_path)

        logger.info("✅ Priority identification complete!")
        return 0

    except FileNotFoundError as e:
        logger.error(f"❌ File not found: {e}")
        return 1
    except ValueError as e:
        logger.error(f"❌ Invalid data: {e}")
        return 1
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
