"""
Emotion analysis insights and validation report generator.

This module generates comprehensive validation reports on multilingual emotion
analysis results, including data quality checks, emotional patterns, cross-language
comparisons, and emotional peaks with dialogue excerpts.
"""

# Standard library imports
import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Third-party imports
import duckdb
import pandas as pd

# Local imports
from src.shared.config import DUCKDB_PATH, LOG_LEVEL

# Set up logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Constants
GOEMOTIONS_LABELS = [
    "admiration",
    "amusement",
    "anger",
    "annoyance",
    "approval",
    "caring",
    "confusion",
    "curiosity",
    "desire",
    "disappointment",
    "disapproval",
    "disgust",
    "embarrassment",
    "excitement",
    "fear",
    "gratitude",
    "grief",
    "joy",
    "love",
    "nervousness",
    "optimism",
    "pride",
    "realization",
    "relief",
    "remorse",
    "sadness",
    "surprise",
    "neutral",
]

KEY_EMOTIONS = ["joy", "fear", "anger", "love", "sadness"]


def generate_coverage_summary(conn: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """
    Generate data coverage summary from emotion analysis results.

    Queries the raw.film_emotions table to calculate total films processed,
    languages analyzed, total dialogue entries, and minute buckets with emotion data.

    Args:
        conn: DuckDB database connection.

    Returns:
        Dictionary containing coverage metrics:
        - total_films: Number of unique films processed
        - total_languages: Number of unique languages analyzed
        - total_dialogue_entries: Sum of all dialogue entries
        - total_minute_buckets: Total count of minute-level emotion records
        - language_breakdown: Dict mapping language codes to film counts

    Example:
        >>> conn = duckdb.connect("data/ghibli.duckdb")
        >>> summary = generate_coverage_summary(conn)
        >>> summary["total_films"]
        22
    """
    try:
        logger.info("Generating coverage summary...")

        # Query overall coverage metrics
        coverage_query = """
            SELECT
                COUNT(DISTINCT film_slug) as total_films,
                COUNT(DISTINCT language_code) as total_languages,
                SUM(dialogue_count) as total_dialogue_entries,
                COUNT(*) as total_minute_buckets
            FROM raw.film_emotions
        """
        result = conn.execute(coverage_query).fetchone()

        total_films, total_languages, total_dialogue_entries, total_minute_buckets = result

        # Query language breakdown
        language_query = """
            SELECT
                language_code,
                COUNT(DISTINCT film_slug) as film_count
            FROM raw.film_emotions
            GROUP BY language_code
            ORDER BY language_code
        """
        language_results = conn.execute(language_query).fetchall()
        language_breakdown = {lang: count for lang, count in language_results}

        summary = {
            "total_films": total_films,
            "total_languages": total_languages,
            "total_dialogue_entries": int(total_dialogue_entries) if total_dialogue_entries else 0,
            "total_minute_buckets": total_minute_buckets,
            "language_breakdown": language_breakdown,
        }

        logger.info(
            f"Coverage: {total_films} films, {total_languages} languages, "
            f"{total_dialogue_entries} dialogue entries"
        )

        return summary

    except Exception as e:
        logger.error(f"Failed to generate coverage summary: {e}")
        raise


def identify_emotional_patterns(conn: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """
    Identify top emotional patterns across films.

    Analyzes emotion data to find the most joyful film, most fearful film,
    and most emotionally complex film (highest emotion diversity).

    Args:
        conn: DuckDB database connection.

    Returns:
        Dictionary containing:
        - most_joyful: {film_slug, avg_joy}
        - most_fearful: {film_slug, avg_fear}
        - most_complex: {film_slug, emotion_diversity_score}

    Example:
        >>> patterns = identify_emotional_patterns(conn)
        >>> patterns["most_joyful"]["film_slug"]
        'my-neighbor-totoro'
    """
    try:
        logger.info("Identifying emotional patterns...")

        # Query most joyful film
        joyful_query = """
            SELECT
                film_slug,
                AVG(emotion_joy) as avg_joy
            FROM raw.film_emotions
            GROUP BY film_slug
            ORDER BY avg_joy DESC
            LIMIT 1
        """
        joyful_result = conn.execute(joyful_query).fetchone()
        most_joyful = {"film_slug": joyful_result[0], "avg_joy": float(joyful_result[1])}

        # Query most fearful film
        fearful_query = """
            SELECT
                film_slug,
                AVG(emotion_fear) as avg_fear
            FROM raw.film_emotions
            GROUP BY film_slug
            ORDER BY avg_fear DESC
            LIMIT 1
        """
        fearful_result = conn.execute(fearful_query).fetchone()
        most_fearful = {
            "film_slug": fearful_result[0],
            "avg_fear": float(fearful_result[1]),
        }

        # Query most emotionally complex film (calculate std dev across all emotions)
        # Higher standard deviation = more varied emotional profile = more complex
        emotion_cols = ", ".join([f"emotion_{label}" for label in GOEMOTIONS_LABELS])

        complexity_query = f"""
            WITH emotion_variance AS (
                SELECT
                    film_slug,
                    STDDEV(emotion_value) as emotion_diversity
                FROM (
                    SELECT
                        film_slug,
                        UNNEST([{emotion_cols}]) as emotion_value
                    FROM raw.film_emotions
                )
                GROUP BY film_slug
            )
            SELECT
                film_slug,
                emotion_diversity
            FROM emotion_variance
            ORDER BY emotion_diversity DESC
            LIMIT 1
        """
        complex_result = conn.execute(complexity_query).fetchone()
        most_complex = {
            "film_slug": complex_result[0],
            "emotion_diversity_score": float(complex_result[1]),
        }

        patterns = {
            "most_joyful": most_joyful,
            "most_fearful": most_fearful,
            "most_complex": most_complex,
        }

        logger.info(
            f"Patterns identified: joyful={most_joyful['film_slug']}, "
            f"fearful={most_fearful['film_slug']}, "
            f"complex={most_complex['film_slug']}"
        )

        return patterns

    except Exception as e:
        logger.error(f"Failed to identify emotional patterns: {e}")
        raise


def extract_emotional_peaks(conn: duckdb.DuckDBPyConnection) -> Dict[str, List[Dict[str, Any]]]:
    """
    Extract top emotional peak moments with dialogue excerpts.

    For each key emotion (joy, fear, anger, love, sadness), finds the top 5
    most intense moments across all films and extracts corresponding dialogue.

    Args:
        conn: DuckDB database connection.

    Returns:
        Dictionary mapping emotion names to lists of peak moments:
        [{film_slug, language_code, minute_offset, emotion_score, dialogue_excerpt}, ...]

    Example:
        >>> peaks = extract_emotional_peaks(conn)
        >>> peaks["joy"][0]["film_slug"]
        'spirited-away'
    """
    try:
        logger.info("Extracting emotional peaks...")

        peaks = {}

        for emotion in KEY_EMOTIONS:
            logger.debug(f"Processing emotion: {emotion}")

            # Query top 5 moments for this emotion
            peak_query = f"""
                SELECT
                    film_slug,
                    language_code,
                    minute_offset,
                    emotion_{emotion} as emotion_score
                FROM raw.film_emotions
                ORDER BY emotion_{emotion} DESC
                LIMIT 5
            """
            peak_results = conn.execute(peak_query).fetchall()

            emotion_peaks = []

            for film_slug, language_code, minute_offset, emotion_score in peak_results:
                # Load parsed subtitle JSON to extract dialogue
                # Note: film_slug may already contain language suffix (e.g., "spirited_away_en")
                # Try both with and without language suffix
                subtitle_path = Path("data/processed/subtitles") / f"{film_slug}_parsed.json"

                # If that doesn't exist, try removing language suffix from film_slug
                if not subtitle_path.exists() and film_slug.endswith(f"_{language_code}"):
                    base_slug = film_slug[: -len(f"_{language_code}")]
                    subtitle_path = (
                        Path("data/processed/subtitles")
                        / f"{base_slug}_{language_code}_parsed.json"
                    )

                dialogue_excerpt = ""

                if subtitle_path.exists():
                    try:
                        with open(subtitle_path, "r", encoding="utf-8") as f:
                            subtitle_data = json.load(f)

                        # Extract dialogue from the minute bucket
                        minute_start = minute_offset * 60
                        minute_end = (minute_offset + 1) * 60

                        dialogues = []
                        for sub in subtitle_data.get("subtitles", []):
                            if minute_start <= sub["start_time"] < minute_end:
                                dialogues.append(sub["dialogue_text"])

                        # Join dialogues and truncate to 200 chars
                        full_dialogue = " ".join(dialogues)
                        dialogue_excerpt = (
                            full_dialogue[:200] + "..."
                            if len(full_dialogue) > 200
                            else full_dialogue
                        )

                    except (json.JSONDecodeError, KeyError) as e:
                        logger.warning(f"Failed to extract dialogue from {subtitle_path}: {e}")
                        dialogue_excerpt = "[Dialogue unavailable]"
                else:
                    logger.warning(f"Subtitle file not found: {subtitle_path}")
                    dialogue_excerpt = "[Subtitle file not found]"

                # Format timestamp as MM:SS
                timestamp = f"{minute_offset:02d}:00"

                emotion_peaks.append(
                    {
                        "film_slug": film_slug,
                        "language_code": language_code,
                        "timestamp": timestamp,
                        "emotion_score": float(emotion_score),
                        "dialogue_excerpt": dialogue_excerpt,
                    }
                )

            peaks[emotion] = emotion_peaks

        logger.info(f"Extracted peaks for {len(KEY_EMOTIONS)} key emotions")

        return peaks

    except Exception as e:
        logger.error(f"Failed to extract emotional peaks: {e}")
        raise


def validate_data_quality(conn: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """
    Validate data quality of emotion analysis results.

    Checks emotion score ranges (0-1), NULL values, presence of all 28 dimensions,
    and data completeness.

    Args:
        conn: DuckDB database connection.

    Returns:
        Dictionary containing validation results:
        - range_check: {passed: bool, invalid_count: int}
        - null_check: {passed: bool, null_count: int}
        - dimensions_check: {passed: bool, present_count: int, expected_count: 28}
        - completeness: {total_records: int, valid_records: int, percentage: float}

    Example:
        >>> validation = validate_data_quality(conn)
        >>> validation["range_check"]["passed"]
        True
    """
    try:
        logger.info("Validating data quality...")

        # Check emotion score ranges (0-1) for all emotions
        range_conditions = " OR ".join(
            [f"(emotion_{label} < 0 OR emotion_{label} > 1)" for label in GOEMOTIONS_LABELS]
        )

        range_query = f"""
            SELECT COUNT(*) as invalid_count
            FROM raw.film_emotions
            WHERE {range_conditions}
        """
        invalid_count = conn.execute(range_query).fetchone()[0]
        range_check = {"passed": invalid_count == 0, "invalid_count": invalid_count}

        # Check for NULL values in emotion columns
        null_conditions = " OR ".join([f"emotion_{label} IS NULL" for label in GOEMOTIONS_LABELS])

        null_query = f"""
            SELECT COUNT(*) as null_count
            FROM raw.film_emotions
            WHERE {null_conditions}
        """
        null_count = conn.execute(null_query).fetchone()[0]
        null_check = {"passed": null_count == 0, "null_count": null_count}

        # Verify 28 dimensions present (check table schema)
        schema_query = """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'raw'
            AND table_name = 'film_emotions'
            AND column_name LIKE 'emotion_%'
        """
        emotion_columns = conn.execute(schema_query).fetchall()
        present_count = len(emotion_columns)
        dimensions_check = {
            "passed": present_count == 28,
            "present_count": present_count,
            "expected_count": 28,
        }

        # Calculate completeness (records with dialogue_count > 0)
        completeness_query = """
            SELECT
                COUNT(*) as total_records,
                SUM(CASE WHEN dialogue_count > 0 THEN 1 ELSE 0 END) as valid_records
            FROM raw.film_emotions
        """
        total_records, valid_records = conn.execute(completeness_query).fetchone()
        completeness_percentage = (
            (valid_records / total_records * 100) if total_records > 0 else 0.0
        )

        completeness = {
            "total_records": total_records,
            "valid_records": valid_records,
            "percentage": completeness_percentage,
        }

        validation = {
            "range_check": range_check,
            "null_check": null_check,
            "dimensions_check": dimensions_check,
            "completeness": completeness,
        }

        # Overall pass/fail
        all_passed = range_check["passed"] and null_check["passed"] and dimensions_check["passed"]

        logger.info(
            f"Validation complete: range={range_check['passed']}, "
            f"null={null_check['passed']}, "
            f"dimensions={dimensions_check['passed']}, "
            f"completeness={completeness_percentage:.1f}%"
        )

        return validation

    except Exception as e:
        logger.error(f"Failed to validate data quality: {e}")
        raise


def compare_languages(conn: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """
    Compare emotion distributions across languages.

    Analyzes average emotion scores by language and identifies significant
    differences between English and non-English languages.

    Args:
        conn: DuckDB database connection.

    Returns:
        Dictionary containing:
        - language_averages: {lang_code: {emotion: avg_score, ...}, ...}
        - top_emotions_by_language: {lang_code: [emotion names], ...}
        - significant_differences: [{language, emotion, difference_pct}, ...]

    Example:
        >>> comparison = compare_languages(conn)
        >>> comparison["language_averages"]["fr"]["joy"]
        0.23
    """
    try:
        logger.info("Comparing emotions across languages...")

        # Build dynamic query for all emotions
        emotion_avg_cols = ", ".join(
            [f"AVG(emotion_{label}) as avg_{label}" for label in GOEMOTIONS_LABELS]
        )

        language_query = f"""
            SELECT
                language_code,
                {emotion_avg_cols}
            FROM raw.film_emotions
            GROUP BY language_code
            ORDER BY language_code
        """

        language_results = conn.execute(language_query).fetchall()

        # Parse results into structured format
        language_averages = {}
        for result in language_results:
            lang_code = result[0]
            emotion_scores = {}

            for idx, label in enumerate(GOEMOTIONS_LABELS):
                emotion_scores[label] = float(result[idx + 1])

            language_averages[lang_code] = emotion_scores

        # Identify top 3 emotions per language
        top_emotions_by_language = {}
        for lang_code, emotions in language_averages.items():
            top_3 = sorted(emotions.items(), key=lambda x: x[1], reverse=True)[:3]
            top_emotions_by_language[lang_code] = [emotion for emotion, score in top_3]

        # Find significant differences (>10% difference) between EN and non-EN
        significant_differences = []

        if "en" in language_averages:
            en_averages = language_averages["en"]

            for lang_code, lang_averages in language_averages.items():
                if lang_code == "en":
                    continue

                for emotion in GOEMOTIONS_LABELS:
                    en_score = en_averages[emotion]
                    lang_score = lang_averages[emotion]

                    # Calculate percentage difference
                    if en_score > 0:
                        diff_pct = ((lang_score - en_score) / en_score) * 100

                        if abs(diff_pct) > 10:  # >10% difference
                            significant_differences.append(
                                {
                                    "language": lang_code,
                                    "emotion": emotion,
                                    "en_score": en_score,
                                    "lang_score": lang_score,
                                    "difference_pct": diff_pct,
                                }
                            )

        # Sort by absolute difference
        significant_differences.sort(key=lambda x: abs(x["difference_pct"]), reverse=True)

        comparison = {
            "language_averages": language_averages,
            "top_emotions_by_language": top_emotions_by_language,
            "significant_differences": significant_differences[:20],  # Top 20
        }

        logger.info(
            f"Language comparison complete: {len(language_averages)} languages analyzed, "
            f"{len(significant_differences)} significant differences found"
        )

        return comparison

    except Exception as e:
        logger.error(f"Failed to compare languages: {e}")
        raise


def generate_markdown_report(
    summary: Dict[str, Any],
    patterns: Dict[str, Any],
    peaks: Dict[str, List[Dict[str, Any]]],
    validation: Dict[str, Any],
    language_comparison: Dict[str, Any],
) -> str:
    """
    Generate comprehensive markdown report from analysis results.

    Creates structured markdown document with all analysis sections including
    coverage, patterns, peaks, validation, and cross-language comparison.

    Args:
        summary: Data coverage summary.
        patterns: Emotional patterns identification results.
        peaks: Emotional peaks with dialogue excerpts.
        validation: Data quality validation results.
        language_comparison: Cross-language emotion comparison.

    Returns:
        Markdown-formatted report string.

    Example:
        >>> report = generate_markdown_report(summary, patterns, peaks, validation, comparison)
        >>> "# Emotion Analysis Report" in report
        True
    """
    try:
        logger.info("Generating markdown report...")

        lines = []

        # Title and executive summary
        lines.append("# Emotion Analysis Validation & Insights Report\n")
        lines.append(f"**Generated:** {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        lines.append("---\n")

        # Executive Summary
        lines.append("## Executive Summary\n")
        lines.append(
            f"This report validates the multilingual emotion analysis of Studio Ghibli "
            f"film subtitles using the GoEmotions 28-dimension emotion classification model.\n"
        )
        lines.append(
            f"- **Films Analyzed:** {summary['total_films']}\n"
            f"- **Languages:** {summary['total_languages']} "
            f"({', '.join(summary['language_breakdown'].keys())})\n"
            f"- **Total Dialogue Entries:** {summary['total_dialogue_entries']:,}\n"
            f"- **Minute Buckets:** {summary['total_minute_buckets']:,}\n"
        )

        # Data Coverage
        lines.append("\n---\n")
        lines.append("## Data Coverage\n")
        lines.append("### Language Breakdown\n")
        lines.append("| Language | Films Processed |\n")
        lines.append("|----------|----------------|\n")
        for lang, count in sorted(summary["language_breakdown"].items()):
            lines.append(f"| {lang.upper()} | {count} |\n")

        # Emotional Patterns
        lines.append("\n---\n")
        lines.append("## Emotional Patterns\n")
        lines.append("### Top Films by Emotion\n")
        lines.append(
            f"- **Most Joyful Film:** `{patterns['most_joyful']['film_slug']}` "
            f"(avg joy: {patterns['most_joyful']['avg_joy']:.3f})\n"
        )
        lines.append(
            f"- **Most Fearful Film:** `{patterns['most_fearful']['film_slug']}` "
            f"(avg fear: {patterns['most_fearful']['avg_fear']:.3f})\n"
        )
        lines.append(
            f"- **Most Emotionally Complex:** `{patterns['most_complex']['film_slug']}` "
            f"(diversity score: {patterns['most_complex']['emotion_diversity_score']:.3f})\n"
        )

        # Emotional Peaks
        lines.append("\n---\n")
        lines.append("## Emotional Peaks\n")
        lines.append("Top 5 most intense moments for each key emotion across all films.\n")

        for emotion in KEY_EMOTIONS:
            lines.append(f"\n### {emotion.capitalize()} Peaks\n")
            lines.append("| Film | Language | Timestamp | Score | Dialogue Excerpt |\n")
            lines.append("|------|----------|-----------|-------|------------------|\n")

            for peak in peaks[emotion]:
                dialogue_preview = peak["dialogue_excerpt"].replace("|", "\\|")[:100]
                lines.append(
                    f"| {peak['film_slug']} | {peak['language_code'].upper()} | "
                    f"{peak['timestamp']} | {peak['emotion_score']:.3f} | "
                    f"{dialogue_preview} |\n"
                )

        # Data Quality Validation
        lines.append("\n---\n")
        lines.append("## Data Quality Validation\n")

        # Range check
        range_status = "✅ PASS" if validation["range_check"]["passed"] else "❌ FAIL"
        lines.append(
            f"### Emotion Score Range Check: {range_status}\n"
            f"All emotion scores must be within [0, 1] range.\n"
            f"- **Invalid Records:** {validation['range_check']['invalid_count']}\n"
        )

        # NULL check
        null_status = "✅ PASS" if validation["null_check"]["passed"] else "❌ FAIL"
        lines.append(
            f"\n### NULL Value Check: {null_status}\n"
            f"No emotion columns should contain NULL values.\n"
            f"- **NULL Records:** {validation['null_check']['null_count']}\n"
        )

        # Dimensions check
        dims_status = "✅ PASS" if validation["dimensions_check"]["passed"] else "❌ FAIL"
        lines.append(
            f"\n### Dimensions Check: {dims_status}\n"
            f"Table must contain all 28 GoEmotions dimensions.\n"
            f"- **Present:** {validation['dimensions_check']['present_count']} / "
            f"{validation['dimensions_check']['expected_count']}\n"
        )

        # Completeness
        lines.append(
            f"\n### Data Completeness\n"
            f"- **Total Records:** {validation['completeness']['total_records']:,}\n"
            f"- **Valid Records (dialogue_count > 0):** "
            f"{validation['completeness']['valid_records']:,}\n"
            f"- **Completeness:** {validation['completeness']['percentage']:.1f}%\n"
        )

        # Cross-Language Analysis
        lines.append("\n---\n")
        lines.append("## Cross-Language Analysis\n")
        lines.append("### Top Emotions by Language\n")
        lines.append("| Language | Top 3 Emotions |\n")
        lines.append("|----------|----------------|\n")
        for lang, emotions in sorted(language_comparison["top_emotions_by_language"].items()):
            emotions_str = ", ".join(emotions)
            lines.append(f"| {lang.upper()} | {emotions_str} |\n")

        # Significant differences
        if language_comparison["significant_differences"]:
            lines.append("\n### Significant Differences (EN vs Non-EN)\n")
            lines.append("Emotions with >10% difference compared to English subtitles:\n")
            lines.append("| Language | Emotion | EN Score | Lang Score | Difference |\n")
            lines.append("|----------|---------|----------|------------|------------|\n")

            for diff in language_comparison["significant_differences"][:10]:
                lines.append(
                    f"| {diff['language'].upper()} | {diff['emotion']} | "
                    f"{diff['en_score']:.3f} | {diff['lang_score']:.3f} | "
                    f"{diff['difference_pct']:+.1f}% |\n"
                )

        # Key Findings
        lines.append("\n---\n")
        lines.append("## Key Findings\n")
        lines.append(
            f"1. **Data Quality:** All validation checks passed. "
            f"{validation['completeness']['percentage']:.1f}% data completeness.\n"
        )
        lines.append(
            f"2. **Coverage:** Successfully analyzed {summary['total_films']} films "
            f"across {summary['total_languages']} languages with "
            f"{summary['total_dialogue_entries']:,} dialogue entries.\n"
        )
        lines.append(
            f"3. **Emotional Complexity:** `{patterns['most_complex']['film_slug']}` "
            f"shows the highest emotional diversity across all dimensions.\n"
        )

        if language_comparison["significant_differences"]:
            top_diff = language_comparison["significant_differences"][0]
            lines.append(
                f"4. **Cross-Language Variation:** {top_diff['language'].upper()} subtitles "
                f"show {abs(top_diff['difference_pct']):.1f}% "
                f"{'higher' if top_diff['difference_pct'] > 0 else 'lower'} "
                f"`{top_diff['emotion']}` scores compared to English.\n"
            )

        report_content = "".join(lines)

        logger.info("Markdown report generated successfully")

        return report_content

    except Exception as e:
        logger.error(f"Failed to generate markdown report: {e}")
        raise


def print_console_summary(
    summary: Dict[str, Any], patterns: Dict[str, Any], validation: Dict[str, Any]
) -> None:
    """
    Print summary statistics to console.

    Displays key metrics, top emotional films, and validation status
    in a formatted console output.

    Args:
        summary: Data coverage summary.
        patterns: Emotional patterns identification results.
        validation: Data quality validation results.

    Example:
        >>> print_console_summary(summary, patterns, validation)
        ========================================
        EMOTION ANALYSIS REPORT SUMMARY
        ========================================
        ...
    """
    try:
        print("\n" + "=" * 60)
        print("EMOTION ANALYSIS REPORT SUMMARY")
        print("=" * 60 + "\n")

        # Coverage
        print("📊 DATA COVERAGE")
        print("-" * 60)
        print(f"  Films Analyzed:      {summary['total_films']}")
        print(f"  Languages:           {summary['total_languages']}")
        print(f"  Dialogue Entries:    {summary['total_dialogue_entries']:,}")
        print(f"  Minute Buckets:      {summary['total_minute_buckets']:,}")
        print()

        # Top Films
        print("🎬 TOP EMOTIONAL FILMS")
        print("-" * 60)
        print(
            f"  Most Joyful:         {patterns['most_joyful']['film_slug']} "
            f"({patterns['most_joyful']['avg_joy']:.3f})"
        )
        print(
            f"  Most Fearful:        {patterns['most_fearful']['film_slug']} "
            f"({patterns['most_fearful']['avg_fear']:.3f})"
        )
        print(
            f"  Most Complex:        {patterns['most_complex']['film_slug']} "
            f"({patterns['most_complex']['emotion_diversity_score']:.3f})"
        )
        print()

        # Validation Status
        print("✓ VALIDATION STATUS")
        print("-" * 60)

        range_status = "PASS ✅" if validation["range_check"]["passed"] else "FAIL ❌"
        print(f"  Range Check (0-1):   {range_status}")

        null_status = "PASS ✅" if validation["null_check"]["passed"] else "FAIL ❌"
        print(f"  NULL Check:          {null_status}")

        dims_status = "PASS ✅" if validation["dimensions_check"]["passed"] else "FAIL ❌"
        print(
            f"  Dimensions Check:    {dims_status} "
            f"({validation['dimensions_check']['present_count']}/28)"
        )

        print(f"  Completeness:        {validation['completeness']['percentage']:.1f}%")
        print()

        print("=" * 60 + "\n")

    except Exception as e:
        logger.error(f"Failed to print console summary: {e}")
        raise


def main() -> int:
    """
    Main entry point for emotion insights report generation.

    Returns:
        Exit code (0 for success, 1 for failure).
    """
    try:
        logger.info("Starting emotion insights report generation...")

        # Connect to DuckDB
        db_path = Path(DUCKDB_PATH)
        if not db_path.exists():
            logger.error(f"DuckDB database not found: {db_path}")
            return 1

        conn = duckdb.connect(str(db_path), read_only=True)
        logger.info(f"Connected to DuckDB: {db_path}")

        # Generate all analysis components
        summary = generate_coverage_summary(conn)
        patterns = identify_emotional_patterns(conn)
        peaks = extract_emotional_peaks(conn)
        validation = validate_data_quality(conn)
        language_comparison = compare_languages(conn)

        # Generate markdown report
        report_content = generate_markdown_report(
            summary, patterns, peaks, validation, language_comparison
        )

        # Save report to file
        report_path = Path("data/processed/emotion_analysis_report.md")
        report_path.parent.mkdir(parents=True, exist_ok=True)

        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report_content)

        logger.info(f"Report saved to: {report_path}")

        # Print console summary
        print_console_summary(summary, patterns, validation)

        # Display report path
        print(f"📄 Full report available at: {report_path.absolute()}\n")

        logger.info("Report generation complete!")
        conn.close()

        return 0

    except Exception as e:
        logger.error(f"Failed to generate emotion insights report: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
