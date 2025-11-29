#!/usr/bin/env python3
"""
Comprehensive Emotion Data Quality Audit (Story 3.6.1).

Audits all 100 film-language combinations to identify emotion timeseries data
that extends beyond actual subtitle duration. Generates comprehensive quality
report to guide downstream Stories 3.6.2-3.6.4.

Usage:
    python scripts/audit_emotion_data_quality.py

Output:
    data/quality_reports/emotion_data_audit_report.md

Author: Story 3.6.1 Implementation
Date: 2025-11-26
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Tuple, Optional
from datetime import datetime

import duckdb
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_subtitle_durations() -> Dict[Tuple[str, str], float]:
    """
    Extract subtitle durations from all parsed subtitle JSON files.

    Prioritizes v2 (improved) subtitle files from Epic 4.X data quality improvements
    when available, falling back to v1 files for films without v2 versions.

    Returns:
        Dictionary mapping (film_title, language_code) -> duration_minutes
    """
    logger.info("Loading subtitle durations from parsed JSON files...")

    durations = {}

    # Priority 1: Load v2 (improved) subtitle files from Epic 4.X
    improved_dir = Path("data/processed/subtitles_improved")
    if improved_dir.exists():
        v2_files = sorted(improved_dir.glob("*_v2_parsed.json"))
        logger.info(f"Found {len(v2_files)} v2 (improved) subtitle files")

        for filepath in v2_files:
            try:
                with open(filepath, 'r') as f:
                    data = json.load(f)

                metadata = data.get('metadata', {})
                film_name = metadata.get('film_name', '').replace(' V2', '')  # Strip V2 suffix
                language_code = metadata.get('language_code', '')
                total_duration_seconds = metadata.get('total_duration', 0)

                if film_name and language_code and total_duration_seconds:
                    duration_minutes = total_duration_seconds / 60.0
                    durations[(film_name, language_code)] = duration_minutes
                    logger.debug(
                        f"Loaded v2: {film_name} ({language_code}): "
                        f"{duration_minutes:.2f} minutes"
                    )

            except Exception as e:
                logger.error(f"Failed to parse {filepath}: {e}")
                continue

    # Priority 2: Load v1 subtitle files for films without v2 versions
    subtitle_dir = Path("data/processed/subtitles")
    v1_files = sorted(subtitle_dir.glob("*_parsed.json"))
    v1_files = [f for f in v1_files if "_v2_" not in f.name]

    logger.info(f"Found {len(v1_files)} v1 subtitle files")

    for filepath in v1_files:
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)

            metadata = data.get('metadata', {})
            film_name = metadata.get('film_name', '')
            language_code = metadata.get('language_code', '')
            total_duration_seconds = metadata.get('total_duration', 0)

            if film_name and language_code and total_duration_seconds:
                # Only use v1 if v2 doesn't exist for this combination
                key = (film_name, language_code)
                if key not in durations:
                    duration_minutes = total_duration_seconds / 60.0
                    durations[key] = duration_minutes
                    logger.debug(
                        f"Loaded v1: {film_name} ({language_code}): "
                        f"{duration_minutes:.2f} minutes"
                    )

        except Exception as e:
            logger.error(f"Failed to parse {filepath}: {e}")
            continue

    logger.info(
        f"Loaded {len(durations)} subtitle duration records "
        f"(prioritizing v2 improved versions)"
    )
    return durations


def query_emotion_extents(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Query emotion data extent (MAX minute_offset) for all film-language combinations.

    Args:
        conn: DuckDB connection to ghibli.duckdb

    Returns:
        DataFrame with columns: film_id, film_title, language_code,
                                emotion_max_minute, total_emotion_minutes
    """
    logger.info("Querying emotion data extents from database...")

    query = """
    SELECT
        film_id,
        film_title,
        language_code,
        MAX(minute_offset) as emotion_max_minute,
        COUNT(DISTINCT minute_offset) as total_emotion_minutes
    FROM main_marts.mart_film_emotion_timeseries
    GROUP BY film_id, film_title, language_code
    ORDER BY film_title, language_code
    """

    df = conn.execute(query).fetchdf()
    logger.info(f"Found {len(df)} film-language combinations in emotion data")

    return df


def classify_runtime_consistency(
    emotion_df: pd.DataFrame,
    subtitle_durations: Dict[Tuple[str, str], float]
) -> pd.DataFrame:
    """
    Cross-validate emotion extent vs subtitle duration and classify each combination.

    Args:
        emotion_df: DataFrame from query_emotion_extents
        subtitle_durations: Dictionary from get_subtitle_durations

    Returns:
        DataFrame with added columns: subtitle_max_minute, overrun_minutes,
                                       classification
    """
    logger.info("Classifying runtime consistency...")

    # Add subtitle duration column
    emotion_df['subtitle_max_minute'] = emotion_df.apply(
        lambda row: subtitle_durations.get(
            (row['film_title'], row['language_code']),
            None
        ),
        axis=1
    )

    # Calculate overrun
    emotion_df['overrun_minutes'] = (
        emotion_df['emotion_max_minute'] -
        emotion_df['subtitle_max_minute']
    )

    # Apply classification logic
    def classify(overrun: Optional[float]) -> str:
        if overrun is None or pd.isna(overrun):
            return 'MISSING'
        elif overrun <= 1.0:
            return 'PASS'
        elif overrun <= 5.0:
            return 'WARN'
        else:
            return 'FAIL'

    emotion_df['classification'] = emotion_df['overrun_minutes'].apply(classify)

    # Log classification counts
    counts = emotion_df['classification'].value_counts()
    for status in ['PASS', 'WARN', 'FAIL', 'MISSING']:
        count = counts.get(status, 0)
        logger.info(f"  {status}: {count} combinations")

    return emotion_df


def check_data_completeness(
    emotion_df: pd.DataFrame,
    subtitle_durations: Dict[Tuple[str, str], float]
) -> Dict[str, List[Tuple[str, str]]]:
    """
    Identify missing or unexpected film-language combinations.

    Args:
        emotion_df: DataFrame from query_emotion_extents
        subtitle_durations: Dictionary from get_subtitle_durations

    Returns:
        Dictionary with 'missing_from_emotions' and 'unexpected_in_emotions' keys
    """
    logger.info("Checking data completeness...")

    # Extract sets of (film_title, language_code)
    emotion_combos = set(
        zip(emotion_df['film_title'], emotion_df['language_code'])
    )
    subtitle_combos = set(subtitle_durations.keys())

    missing_from_emotions = sorted(subtitle_combos - emotion_combos)
    unexpected_in_emotions = sorted(emotion_combos - subtitle_combos)

    logger.info(f"  Missing from emotions: {len(missing_from_emotions)}")
    logger.info(f"  Unexpected in emotions: {len(unexpected_in_emotions)}")

    return {
        'missing_from_emotions': missing_from_emotions,
        'unexpected_in_emotions': unexpected_in_emotions
    }


def calculate_statistics(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Calculate aggregate statistics for the audit report.

    Args:
        df: Classified DataFrame

    Returns:
        Dictionary with statistical metrics
    """
    logger.info("Calculating statistics...")

    total = len(df)
    pass_count = len(df[df['classification'] == 'PASS'])
    warn_count = len(df[df['classification'] == 'WARN'])
    fail_count = len(df[df['classification'] == 'FAIL'])
    missing_count = len(df[df['classification'] == 'MISSING'])

    # Calculate pass rate (excluding MISSING)
    valid_count = total - missing_count
    pass_rate = (pass_count / valid_count * 100) if valid_count > 0 else 0

    # Calculate average overruns
    fail_df = df[df['classification'] == 'FAIL']
    warn_df = df[df['classification'] == 'WARN']

    avg_fail_overrun = fail_df['overrun_minutes'].mean() if len(fail_df) > 0 else 0
    avg_warn_overrun = warn_df['overrun_minutes'].mean() if len(warn_df) > 0 else 0

    # Films requiring re-processing
    reprocess_count = warn_count + fail_count

    return {
        'total': total,
        'pass_count': pass_count,
        'warn_count': warn_count,
        'fail_count': fail_count,
        'missing_count': missing_count,
        'pass_rate': pass_rate,
        'avg_fail_overrun': avg_fail_overrun,
        'avg_warn_overrun': avg_warn_overrun,
        'reprocess_count': reprocess_count
    }


def generate_audit_report(
    df: pd.DataFrame,
    stats: Dict[str, Any],
    completeness: Dict[str, List[Tuple[str, str]]],
    output_path: Path
) -> None:
    """
    Generate comprehensive markdown audit report.

    Args:
        df: Classified DataFrame
        stats: Statistics dictionary
        completeness: Completeness check results
        output_path: Path to save report
    """
    logger.info(f"Generating audit report: {output_path}")

    with open(output_path, 'w') as f:
        # Header
        f.write("# Emotion Data Quality Audit Report\n\n")
        f.write(f"**Audit Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("**Database:** `data/ghibli.duckdb`\n")
        f.write("**Story:** 3.6.1 - Comprehensive Emotion Data Audit\n")
        f.write(f"**Scope:** {stats['total']} film-language combinations\n\n")

        f.write("---\n\n")

        # Executive Summary
        f.write("## Executive Summary\n\n")
        f.write(f"- ✅ **PASS:** {stats['pass_count']} combinations "
                f"({stats['pass_count']/stats['total']*100:.1f}%)\n")
        f.write(f"- ⚠️  **WARN:** {stats['warn_count']} combinations "
                f"({stats['warn_count']/stats['total']*100:.1f}%)\n")
        f.write(f"- ❌ **FAIL:** {stats['fail_count']} combinations "
                f"({stats['fail_count']/stats['total']*100:.1f}%)\n")

        if stats['missing_count'] > 0:
            f.write(f"- ❓ **MISSING:** {stats['missing_count']} combinations "
                    f"(no subtitle metadata found)\n")

        f.write(f"- 🔧 **Total requiring re-processing:** {stats['reprocess_count']} "
                f"combinations (WARN + FAIL)\n\n")

        f.write(f"**Pass Rate:** {stats['pass_rate']:.1f}% "
                f"(excluding MISSING combinations)\n\n")

        # Key Findings
        f.write("### Key Findings\n\n")
        if stats['fail_count'] > 0:
            f.write(f"- {stats['fail_count']} film-language combinations have "
                    f"**major runtime overruns** (>5 minutes)\n")
            f.write(f"  - Average overrun for FAIL cases: "
                    f"{stats['avg_fail_overrun']:.1f} minutes\n")

        if stats['warn_count'] > 0:
            f.write(f"- {stats['warn_count']} combinations have "
                    f"**minor runtime overruns** (1-5 minutes)\n")
            f.write(f"  - Average overrun for WARN cases: "
                    f"{stats['avg_warn_overrun']:.1f} minutes\n")

        if stats['pass_count'] == stats['total'] - stats['missing_count']:
            f.write("- ✅ All valid combinations are within acceptable runtime bounds!\n")

        f.write("\n---\n\n")

        # Top 10 Worst Overruns
        f.write("## Top 10 Worst Runtime Overruns\n\n")

        worst_df = df[df['classification'].isin(['FAIL', 'WARN'])].nlargest(
            10, 'overrun_minutes'
        )

        if len(worst_df) > 0:
            f.write("| Rank | Film Title | Language | Emotion Max (min) | "
                    "Subtitle Max (min) | Overrun (min) | Status |\n")
            f.write("|------|------------|----------|-------------------|"
                    "--------------------|---------------|--------|\n")

            for idx, (rank, row) in enumerate(worst_df.iterrows(), 1):
                emoji = '❌' if row['classification'] == 'FAIL' else '⚠️'
                f.write(
                    f"| {idx} | {row['film_title']} | {row['language_code']} | "
                    f"{row['emotion_max_minute']:.1f} | "
                    f"{row['subtitle_max_minute']:.1f} | "
                    f"{row['overrun_minutes']:.1f} | {emoji} {row['classification']} |\n"
                )
        else:
            f.write("*No runtime overruns detected!*\n")

        f.write("\n---\n\n")

        # Detailed Findings Table
        f.write("## Detailed Findings (All Combinations)\n\n")
        f.write("| Film Title | Language | Emotion Max (min) | "
                "Subtitle Max (min) | Overrun (min) | Status |\n")
        f.write("|------------|----------|-------------------|"
                "--------------------|---------------|--------|\n")

        for _, row in df.iterrows():
            emoji_map = {
                'PASS': '✅',
                'WARN': '⚠️',
                'FAIL': '❌',
                'MISSING': '❓'
            }
            emoji = emoji_map.get(row['classification'], '❓')

            emotion_max = f"{row['emotion_max_minute']:.1f}"
            subtitle_max = (
                f"{row['subtitle_max_minute']:.1f}"
                if pd.notna(row['subtitle_max_minute'])
                else "N/A"
            )
            overrun = (
                f"{row['overrun_minutes']:.1f}"
                if pd.notna(row['overrun_minutes'])
                else "N/A"
            )

            f.write(
                f"| {row['film_title']} | {row['language_code']} | "
                f"{emotion_max} | {subtitle_max} | {overrun} | "
                f"{emoji} {row['classification']} |\n"
            )

        f.write("\n---\n\n")

        # Data Completeness
        f.write("## Data Completeness\n\n")

        missing = completeness['missing_from_emotions']
        unexpected = completeness['unexpected_in_emotions']

        if len(missing) == 0 and len(unexpected) == 0:
            f.write("✅ **Perfect alignment!** All subtitle files have "
                    "corresponding emotion data.\n\n")
        else:
            if len(missing) > 0:
                f.write(f"### Missing from Emotion Data ({len(missing)} combinations)\n\n")
                f.write("These film-language combinations have subtitle files "
                        "but no emotion data:\n\n")
                for film, lang in missing:
                    f.write(f"- {film} ({lang})\n")
                f.write("\n")

            if len(unexpected) > 0:
                f.write(f"### Unexpected in Emotion Data ({len(unexpected)} combinations)\n\n")
                f.write("These film-language combinations have emotion data "
                        "but no matching subtitle files:\n\n")
                for film, lang in unexpected:
                    f.write(f"- {film} ({lang})\n")
                f.write("\n")

        f.write("---\n\n")

        # Films Requiring Re-processing
        f.write("## Films Requiring Re-processing\n\n")

        reprocess_df = df[df['classification'].isin(['FAIL', 'WARN'])].sort_values(
            'overrun_minutes', ascending=False
        )

        if len(reprocess_df) > 0:
            f.write(f"**Total:** {len(reprocess_df)} film-language combinations\n\n")

            # Group by status
            for status in ['FAIL', 'WARN']:
                status_df = reprocess_df[reprocess_df['classification'] == status]
                if len(status_df) > 0:
                    emoji = '❌' if status == 'FAIL' else '⚠️'
                    f.write(f"### {emoji} {status} ({len(status_df)} combinations)\n\n")

                    for _, row in status_df.iterrows():
                        f.write(
                            f"- **{row['film_title']}** ({row['language_code']}): "
                            f"Overrun {row['overrun_minutes']:.1f} minutes\n"
                        )
                    f.write("\n")
        else:
            f.write("✅ **No re-processing required!** All films are within "
                    "acceptable runtime bounds.\n\n")

        f.write("---\n\n")

        # Recommendations
        f.write("## Recommendations for Downstream Stories\n\n")

        f.write("### Story 3.6.2: Root Cause Investigation\n\n")
        if stats['fail_count'] > 0:
            f.write("**Priority films to investigate:**\n\n")
            top_3 = df[df['classification'] == 'FAIL'].nlargest(3, 'overrun_minutes')
            for _, row in top_3.iterrows():
                f.write(
                    f"- {row['film_title']} ({row['language_code']}): "
                    f"{row['overrun_minutes']:.1f} minute overrun\n"
                )
            f.write("\n**Investigation focus:**\n")
            f.write("- Verify `src/nlp/analyze_emotions.py` uses correct duration source\n")
            f.write("- Check if Kaggle metadata duration differs from subtitle duration\n")
            f.write("- Validate emotion pipeline logic for minute_offset calculation\n\n")
        else:
            f.write("✅ No major issues detected - light investigation recommended\n\n")

        f.write("### Story 3.6.3: Pipeline Fix Scope\n\n")
        if stats['reprocess_count'] > 0:
            f.write(f"**Scope:** Fix applies to **all {stats['total']} film-language combinations**\n")
            f.write(f"- {stats['reprocess_count']} combinations require re-processing\n")
            f.write(f"- {stats['pass_count']} combinations already pass (verify fix doesn't regress)\n\n")
        else:
            f.write("✅ No pipeline fixes required\n\n")

        f.write("### Story 3.6.4: Re-processing Targets\n\n")
        if len(reprocess_df) > 0:
            f.write(f"**Re-process these {len(reprocess_df)} combinations:**\n\n")

            # Group by film for easier batch processing
            films = reprocess_df.groupby('film_title')['language_code'].apply(list).to_dict()
            for film, langs in sorted(films.items()):
                f.write(f"- **{film}**: {', '.join(sorted(langs))}\n")

            f.write("\n**Estimated effort:**\n")
            f.write(f"- {len(reprocess_df)} emotion analysis runs\n")
            f.write(f"- ~{len(reprocess_df) * 2} minutes processing time (estimated)\n\n")
        else:
            f.write("✅ No re-processing required\n\n")

        f.write("### Story 3.6.6: Automated Test Baselines\n\n")
        f.write("**Test coverage targets:**\n\n")
        f.write(f"- Validate emotion max_minute <= subtitle_duration for all "
                f"{stats['total']} combinations\n")
        f.write(f"- Target pass rate: 100% (currently {stats['pass_rate']:.1f}%)\n")
        f.write("- Test tolerance: ±1.0 minute buffer acceptable\n\n")

        f.write("---\n\n")

        # Footer
        f.write("## Audit Metadata\n\n")
        f.write(f"- **Script:** `scripts/audit_emotion_data_quality.py`\n")
        f.write(f"- **Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"- **Database:** `data/ghibli.duckdb` (read-only access)\n")
        f.write(f"- **Total combinations audited:** {stats['total']}\n")
        f.write(f"- **Subtitle files scanned:** {len(df)}\n\n")

        f.write("---\n\n")
        f.write("*Report generated by Story 3.6.1: Comprehensive Emotion Data Audit*\n")

    logger.info(f"Report successfully generated: {output_path}")


def main() -> None:
    """Main audit execution."""
    logger.info("="*70)
    logger.info("EMOTION DATA QUALITY AUDIT - Story 3.6.1")
    logger.info("="*70)

    # Validate database exists
    db_path = Path("data/ghibli.duckdb")
    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        logger.error("Please run dbt transformations first: cd src/transformation && dbt run")
        return

    try:
        # Connect to database (read-only)
        logger.info(f"Connecting to database: {db_path}")
        conn = duckdb.connect(str(db_path), read_only=True)

        # Step 1: Load subtitle durations
        subtitle_durations = get_subtitle_durations()

        # Step 2: Query emotion data extents
        emotion_df = query_emotion_extents(conn)

        # Step 3: Classify runtime consistency
        classified_df = classify_runtime_consistency(emotion_df, subtitle_durations)

        # Step 4: Check data completeness
        completeness = check_data_completeness(emotion_df, subtitle_durations)

        # Step 5: Calculate statistics
        stats = calculate_statistics(classified_df)

        # Step 6: Generate report
        output_dir = Path("data/quality_reports")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "emotion_data_audit_report.md"

        generate_audit_report(classified_df, stats, completeness, output_path)

        # Close connection
        conn.close()

        # Print summary
        print("\n" + "="*70)
        print("AUDIT COMPLETE")
        print("="*70)
        print(f"Total combinations audited: {stats['total']}")
        print(f"✅ PASS: {stats['pass_count']} ({stats['pass_count']/stats['total']*100:.1f}%)")
        print(f"⚠️  WARN: {stats['warn_count']} ({stats['warn_count']/stats['total']*100:.1f}%)")
        print(f"❌ FAIL: {stats['fail_count']} ({stats['fail_count']/stats['total']*100:.1f}%)")

        if stats['missing_count'] > 0:
            print(f"❓ MISSING: {stats['missing_count']}")

        print(f"\n🔧 Total requiring re-processing: {stats['reprocess_count']}")
        print(f"📊 Pass rate: {stats['pass_rate']:.1f}%")
        print("="*70)
        print(f"\n📄 Detailed report: {output_path}")

        # Exit with appropriate code
        if stats['fail_count'] > 0:
            logger.warning("Audit found FAIL cases - review required")
            exit(1)
        elif stats['warn_count'] > 0:
            logger.info("Audit found WARN cases - minor issues detected")
            exit(0)
        else:
            logger.info("Audit passed - all films within acceptable bounds")
            exit(0)

    except Exception as e:
        logger.error(f"Audit failed: {e}", exc_info=True)
        exit(2)


if __name__ == "__main__":
    main()
