"""
Subtitle Timing Validation Script.

This module validates subtitle files against documented film versions to ensure
timing accuracy for emotion analysis and RAG system queries.

Validates:
1. Subtitle duration vs documented film runtime (drift < 5% critical, < 2% good)
2. Cross-language consistency (all languages should have similar durations)
3. Common timing issues (negative timestamps, large gaps, subtitles past film end)
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("spiriteddata.validation.validate_subtitle_timing")


def load_film_versions(metadata_path: str = "data/metadata/film_versions.json") -> Dict:
    """
    Load documented film versions from metadata file.

    Args:
        metadata_path: Path to film_versions.json file

    Returns:
        Dictionary mapping film slugs to version metadata

    Raises:
        FileNotFoundError: If metadata file doesn't exist
        json.JSONDecodeError: If file contains invalid JSON
    """
    metadata_file = Path(metadata_path)

    if not metadata_file.exists():
        raise FileNotFoundError(
            f"Film versions metadata not found: {metadata_path}. "
            "Please create this file with documented film runtimes."
        )

    logger.info(f"Loading film versions from: {metadata_path}")

    with open(metadata_file) as f:
        film_versions = json.load(f)

    # Remove _documentation key if present
    film_versions.pop("_documentation", None)

    logger.info(f"Loaded {len(film_versions)} film versions")
    return film_versions


def load_subtitle_metadata(subtitle_path: Path) -> Dict:
    """
    Load parsed subtitle file and extract metadata.

    Args:
        subtitle_path: Path to parsed subtitle JSON file

    Returns:
        Dictionary containing subtitle metadata and entries

    Raises:
        FileNotFoundError: If subtitle file doesn't exist
        json.JSONDecodeError: If file contains invalid JSON
    """
    if not subtitle_path.exists():
        raise FileNotFoundError(f"Subtitle file not found: {subtitle_path}")

    with open(subtitle_path) as f:
        subtitle_data = json.load(f)

    return subtitle_data


def validate_subtitle_timing(
    subtitle_json_path: Path, film_versions: Dict
) -> Dict[str, any]:
    """
    Validate subtitle timing against documented film version.

    Checks:
    - Timing drift (subtitle duration vs film runtime)
    - Last subtitle timestamp (should be near film end)
    - Negative timestamps
    - Large gaps between subtitles

    Args:
        subtitle_json_path: Path to parsed subtitle JSON file
        film_versions: Dictionary of documented film versions

    Returns:
        Dictionary with validation results:
        {
            "status": "PASS|WARN|FAIL",
            "timing_drift_percent": float,
            "last_subtitle_time": float,
            "documented_runtime": float,
            "subtitle_duration": float,
            "issues": List[str],
            "warnings": List[str]
        }
    """
    logger.info(f"Validating timing for: {subtitle_json_path.name}")

    try:
        # Load subtitle data
        subtitle_data = load_subtitle_metadata(subtitle_json_path)

        # Extract film slug (remove language suffix: "spirited_away_en" -> "spirited_away")
        film_slug_with_lang = subtitle_data["metadata"]["film_slug"]
        film_slug = "_".join(film_slug_with_lang.split("_")[:-1])

        # Check if film version is documented
        if film_slug not in film_versions:
            logger.warning(f"No documented film version for: {film_slug}")
            return {
                "status": "FAIL",
                "timing_drift_percent": None,
                "last_subtitle_time": None,
                "documented_runtime": None,
                "subtitle_duration": None,
                "issues": [f"No documented film version for '{film_slug}'"],
                "warnings": [],
            }

        # Get documented runtime
        documented_runtime = film_versions[film_slug]["runtime_seconds"]
        subtitle_duration = subtitle_data["metadata"]["total_duration"]

        # Calculate timing drift
        drift_percent = (
            abs(subtitle_duration - documented_runtime) / documented_runtime * 100
        )

        # Get last subtitle timestamp
        subtitles = subtitle_data["subtitles"]
        if not subtitles:
            return {
                "status": "FAIL",
                "timing_drift_percent": None,
                "last_subtitle_time": None,
                "documented_runtime": documented_runtime,
                "subtitle_duration": subtitle_duration,
                "issues": ["No subtitles found in file"],
                "warnings": [],
            }

        last_subtitle_time = subtitles[-1]["end_time"]

        # Check for timing issues
        issues = []
        warnings = []

        # Drift threshold checks
        if drift_percent > 5.0:
            status = "FAIL"
            issues.append(
                f"Critical timing drift: {drift_percent:.2f}% "
                f"(subtitle: {subtitle_duration:.1f}s, film: {documented_runtime}s)"
            )
        elif drift_percent > 2.0:
            status = "WARN"
            warnings.append(
                f"Timing drift: {drift_percent:.2f}% "
                f"(subtitle: {subtitle_duration:.1f}s, film: {documented_runtime}s)"
            )
        else:
            status = "PASS"

        # Check if last subtitle is too far before film end
        time_before_end = documented_runtime - last_subtitle_time
        if time_before_end > 120:  # More than 2 minutes before end
            msg = (
                f"Last subtitle ends {time_before_end:.1f}s "
                f"({time_before_end/60:.1f} min) before film end (missing final scenes?)"
            )
            if status == "PASS":
                status = "WARN"
            warnings.append(msg)
        elif time_before_end < -120:  # Extends more than 2 minutes past film end
            msg = (
                f"Last subtitle extends {abs(time_before_end):.1f}s past documented film end"
            )
            if status != "FAIL":
                status = "WARN"
            warnings.append(msg)

        # Check for negative timestamps
        negative_timestamps = [s for s in subtitles if s["start_time"] < 0]
        if negative_timestamps:
            warnings.append(
                f"Found {len(negative_timestamps)} subtitles with negative timestamps"
            )

        # Check for large gaps (>2 minutes between subtitles)
        large_gaps = []
        for i in range(1, len(subtitles)):
            gap = subtitles[i]["start_time"] - subtitles[i - 1]["end_time"]
            if gap > 120:  # 2 minutes
                large_gaps.append((i, gap))

        if large_gaps:
            warnings.append(
                f"Found {len(large_gaps)} gaps >2 minutes between subtitles "
                f"(largest: {max(g[1] for g in large_gaps):.1f}s)"
            )

        logger.info(
            f"Validation result for {film_slug_with_lang}: {status} "
            f"(drift: {drift_percent:.2f}%)"
        )

        return {
            "status": status,
            "timing_drift_percent": round(drift_percent, 2),
            "last_subtitle_time": round(last_subtitle_time, 2),
            "documented_runtime": documented_runtime,
            "subtitle_duration": round(subtitle_duration, 2),
            "issues": issues,
            "warnings": warnings,
        }

    except Exception as e:
        logger.error(f"Error validating {subtitle_json_path.name}: {e}", exc_info=True)
        return {
            "status": "FAIL",
            "timing_drift_percent": None,
            "last_subtitle_time": None,
            "documented_runtime": None,
            "subtitle_duration": None,
            "issues": [f"Validation error: {str(e)}"],
            "warnings": [],
        }


def validate_cross_language_consistency(
    film_slug: str, subtitle_dir: str = "data/processed/subtitles"
) -> Dict[str, any]:
    """
    Check if all language versions have consistent durations.

    Cross-language drift >3% indicates different film cuts/versions.

    Args:
        film_slug: Base film slug (without language suffix)
        subtitle_dir: Directory containing parsed subtitle files

    Returns:
        Dictionary with cross-language validation results:
        {
            "status": "PASS|FAIL",
            "max_drift_percent": float,
            "durations": Dict[str, float],
            "issues": List[str],
            "warnings": List[str]
        }
    """
    logger.info(f"Validating cross-language consistency for: {film_slug}")

    subtitle_path = Path(subtitle_dir)
    languages = ["en", "fr", "es", "nl", "ar"]
    durations = {}

    # Load durations for all available languages
    for lang in languages:
        filename = f"{film_slug}_{lang}_parsed.json"
        file_path = subtitle_path / filename

        if file_path.exists():
            try:
                with open(file_path) as f:
                    data = json.load(f)
                    durations[lang] = data["metadata"]["total_duration"]
            except Exception as e:
                logger.warning(f"Failed to load {filename}: {e}")

    # Check if we have any data
    if not durations:
        logger.warning(f"No subtitle files found for {film_slug}")
        return {
            "status": "FAIL",
            "max_drift_percent": None,
            "durations": {},
            "issues": ["No subtitle files found for any language"],
            "warnings": [],
        }

    if len(durations) == 1:
        logger.info(f"Only one language available for {film_slug}, skipping comparison")
        lang = list(durations.keys())[0]
        return {
            "status": "PASS",
            "max_drift_percent": 0.0,
            "durations": durations,
            "issues": [],
            "warnings": [f"Only {lang.upper()} available (no cross-language comparison)"],
        }

    # Calculate average duration and max drift
    avg_duration = sum(durations.values()) / len(durations)
    max_drift = max(abs(d - avg_duration) / avg_duration * 100 for d in durations.values())

    issues = []
    warnings = []

    # Check drift threshold
    if max_drift > 3.0:
        status = "FAIL"
        # Find which languages have the largest drift
        drifts = {
            lang: abs(dur - avg_duration) / avg_duration * 100
            for lang, dur in durations.items()
        }
        worst_lang = max(drifts, key=drifts.get)
        issues.append(
            f"Cross-language drift {max_drift:.2f}% exceeds 3% threshold "
            f"({worst_lang.upper()}: {durations[worst_lang]:.1f}s vs avg: {avg_duration:.1f}s)"
        )
    else:
        status = "PASS"

    logger.info(
        f"Cross-language validation for {film_slug}: {status} (max drift: {max_drift:.2f}%)"
    )

    return {
        "status": status,
        "max_drift_percent": round(max_drift, 2),
        "durations": {lang: round(dur, 2) for lang, dur in durations.items()},
        "issues": issues,
        "warnings": warnings,
    }


def generate_validation_report(
    subtitle_dir: str = "data/processed/subtitles",
    metadata_path: str = "data/metadata/film_versions.json",
    output_path: str = "data/processed/subtitle_validation_report.md",
) -> Dict[str, any]:
    """
    Generate comprehensive subtitle validation report.

    Validates all subtitle files against documented film versions and
    checks cross-language consistency.

    Args:
        subtitle_dir: Directory containing parsed subtitle files
        metadata_path: Path to film_versions.json metadata file
        output_path: Path to save validation report

    Returns:
        Dictionary with validation results for all films
    """
    logger.info("=== Starting Subtitle Validation ===")

    # Load film versions
    try:
        film_versions = load_film_versions(metadata_path)
    except Exception as e:
        logger.error(f"Failed to load film versions: {e}")
        raise

    # Validate all subtitle files
    results = {}
    subtitle_path = Path(subtitle_dir)

    for subtitle_file in sorted(subtitle_path.glob("*_parsed.json")):
        # Extract film slug and language
        filename_parts = subtitle_file.stem.rsplit("_", 2)
        if len(filename_parts) < 3:
            logger.warning(f"Unexpected filename format: {subtitle_file.name}")
            continue

        film_slug = filename_parts[0]
        lang = filename_parts[1]

        # Initialize results structure for this film
        if film_slug not in results:
            results[film_slug] = {"per_language": {}, "cross_language": None}

        # Validate timing for this language
        validation = validate_subtitle_timing(subtitle_file, film_versions)
        results[film_slug]["per_language"][lang] = validation

    # Perform cross-language validation for each film
    logger.info("=== Cross-Language Consistency Check ===")
    for film_slug in results:
        results[film_slug]["cross_language"] = validate_cross_language_consistency(
            film_slug, subtitle_dir
        )

    # Generate report
    logger.info("=== Generating Validation Report ===")
    report_lines = generate_report_content(results, film_versions)

    # Write report to file
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text("\n".join(report_lines))

    logger.info(f"✅ Validation report saved to: {output_path}")

    # Print summary to console
    print_validation_summary(results)

    return results


def generate_report_content(results: Dict, film_versions: Dict) -> List[str]:
    """
    Generate markdown report content from validation results.

    Args:
        results: Validation results for all films
        film_versions: Film version metadata

    Returns:
        List of report lines (markdown format)
    """
    report_lines = [
        "# Subtitle Version Validation Report",
        "",
        f"**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"**Films Validated**: {len(results)}",
        f"**Total Subtitle Files**: {sum(len(r['per_language']) for r in results.values())}",
        "",
        "## Executive Summary",
        "",
    ]

    # Calculate summary statistics
    total_files = sum(len(r["per_language"]) for r in results.values())
    pass_count = sum(
        1
        for r in results.values()
        for v in r["per_language"].values()
        if v["status"] == "PASS"
    )
    warn_count = sum(
        1
        for r in results.values()
        for v in r["per_language"].values()
        if v["status"] == "WARN"
    )
    fail_count = sum(
        1
        for r in results.values()
        for v in r["per_language"].values()
        if v["status"] == "FAIL"
    )

    report_lines.extend(
        [
            f"- **✅ PASS**: {pass_count}/{total_files} files ({pass_count/total_files*100:.1f}%)",
            f"- **⚠️ WARN**: {warn_count}/{total_files} files ({warn_count/total_files*100:.1f}%)",
            f"- **❌ FAIL**: {fail_count}/{total_files} files ({fail_count/total_files*100:.1f}%)",
            "",
        ]
    )

    # Cross-language summary
    cl_pass = sum(1 for r in results.values() if r["cross_language"]["status"] == "PASS")
    cl_fail = len(results) - cl_pass

    report_lines.extend(
        [
            "### Cross-Language Consistency",
            "",
            f"- **✅ Consistent**: {cl_pass}/{len(results)} films ({cl_pass/len(results)*100:.1f}%)",
            f"- **❌ Inconsistent**: {cl_fail}/{len(results)} films ({cl_fail/len(results)*100:.1f}%)",
            "",
            "## Detailed Validation Results",
            "",
        ]
    )

    # Per-film detailed results
    for film_slug, data in sorted(results.items()):
        # Get film title from versions
        film_title = film_versions.get(film_slug, {}).get("title", film_slug.replace("_", " ").title())

        report_lines.extend([f"### {film_title}", ""])

        # Per-language results
        report_lines.append("**Per-Language Timing:**")
        report_lines.append("")

        for lang, validation in sorted(data["per_language"].items()):
            status_emoji = (
                "✅"
                if validation["status"] == "PASS"
                else "⚠️" if validation["status"] == "WARN" else "❌"
            )
            drift = validation["timing_drift_percent"]
            drift_str = f"{drift:.2f}%" if drift is not None else "N/A"

            report_lines.append(
                f"- **{lang.upper()}**: {status_emoji} {validation['status']} "
                f"(drift: {drift_str})"
            )

            # Add issues
            for issue in validation["issues"]:
                report_lines.append(f"  - ❌ {issue}")

            # Add warnings
            for warning in validation["warnings"]:
                report_lines.append(f"  - ⚠️ {warning}")

        # Cross-language consistency
        report_lines.append("")
        report_lines.append("**Cross-Language Consistency:**")
        report_lines.append("")

        cl = data["cross_language"]
        status_emoji = "✅" if cl["status"] == "PASS" else "❌"
        max_drift = cl.get("max_drift_percent", 0.0)

        report_lines.append(
            f"- **Status**: {status_emoji} {cl['status']} (max drift: {max_drift:.2f}%)"
        )

        # Show durations for each language
        if cl.get("durations"):
            report_lines.append("- **Durations by Language:**")
            for lang, duration in sorted(cl["durations"].items()):
                report_lines.append(f"  - {lang.upper()}: {duration:.1f}s ({duration/60:.1f} min)")

        # Add cross-language issues
        for issue in cl["issues"]:
            report_lines.append(f"  - ❌ {issue}")

        # Add cross-language warnings
        for warning in cl["warnings"]:
            report_lines.append(f"  - ⚠️ {warning}")

        report_lines.append("")

    # Recommendations
    report_lines.extend(
        [
            "## Recommendations",
            "",
            "### Critical Issues (❌ FAIL)",
            "",
        ]
    )

    critical_films = [
        (film_slug, data)
        for film_slug, data in results.items()
        if any(v["status"] == "FAIL" for v in data["per_language"].values())
        or data["cross_language"]["status"] == "FAIL"
    ]

    if critical_films:
        report_lines.append(
            f"**{len(critical_films)} films** require immediate attention:"
        )
        report_lines.append("")
        for film_slug, _ in critical_films:
            film_title = film_versions.get(film_slug, {}).get(
                "title", film_slug.replace("_", " ").title()
            )
            report_lines.append(f"- {film_title}")
    else:
        report_lines.append("✅ No critical issues found!")

    report_lines.extend(["", "### Warnings (⚠️ WARN)", ""])

    warn_films = [
        (film_slug, data)
        for film_slug, data in results.items()
        if any(v["status"] == "WARN" for v in data["per_language"].values())
        and not any(v["status"] == "FAIL" for v in data["per_language"].values())
    ]

    if warn_films:
        report_lines.append(f"**{len(warn_films)} films** have warnings:")
        report_lines.append("")
        for film_slug, _ in warn_films:
            film_title = film_versions.get(film_slug, {}).get(
                "title", film_slug.replace("_", " ").title()
            )
            report_lines.append(f"- {film_title}")
    else:
        report_lines.append("✅ No warnings!")

    report_lines.extend(["", "---", "", "*End of Report*", ""])

    return report_lines


def print_validation_summary(results: Dict) -> None:
    """
    Print validation summary to console.

    Args:
        results: Validation results for all films
    """
    total_files = sum(len(r["per_language"]) for r in results.values())
    pass_count = sum(
        1
        for r in results.values()
        for v in r["per_language"].values()
        if v["status"] == "PASS"
    )

    print("\n" + "=" * 60)
    print("SUBTITLE VALIDATION SUMMARY")
    print("=" * 60)
    print(f"Films Validated: {len(results)}")
    print(f"Total Subtitle Files: {total_files}")
    print(f"✅ PASS: {pass_count}/{total_files} ({pass_count/total_files*100:.1f}%)")
    print("=" * 60 + "\n")


def main():
    """Main execution function."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Validate subtitle timing against documented film versions"
    )
    parser.add_argument(
        "--subtitle-dir",
        default="data/processed/subtitles",
        help="Directory containing parsed subtitle files",
    )
    parser.add_argument(
        "--metadata",
        default="data/metadata/film_versions.json",
        help="Path to film versions metadata file",
    )
    parser.add_argument(
        "--output",
        default="data/processed/subtitle_validation_report.md",
        help="Path to save validation report",
    )

    args = parser.parse_args()

    try:
        # Run validation
        results = generate_validation_report(
            subtitle_dir=args.subtitle_dir,
            metadata_path=args.metadata,
            output_path=args.output,
        )

        # Exit with appropriate code
        has_failures = any(
            v["status"] == "FAIL"
            for r in results.values()
            for v in r["per_language"].values()
        )

        if has_failures:
            logger.warning("Validation completed with failures")
            exit(1)
        else:
            logger.info("Validation completed successfully")
            exit(0)

    except Exception as e:
        logger.error(f"Validation failed: {e}", exc_info=True)
        exit(1)


if __name__ == "__main__":
    main()
