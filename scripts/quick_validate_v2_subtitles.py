#!/usr/bin/env python3
"""
Quick validation of improved (v2) subtitle files.

Compares timing characteristics against documented film runtimes without
full parsing pipeline. Used for Story 4.X.2 to quickly assess improvements.
"""

import sys
from pathlib import Path
import json
import pysrt
from typing import Dict, Any, List

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def load_film_versions() -> Dict[str, Any]:
    """Load film version metadata."""
    metadata_path = PROJECT_ROOT / "data/metadata/film_versions.json"
    with open(metadata_path) as f:
        data = json.load(f)
    # Remove _documentation key
    return {k: v for k, v in data.items() if not k.startswith("_")}


def validate_subtitle_file(srt_path: Path, film_versions: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate a single subtitle file timing.

    Returns validation dict with status, drift, and metadata.
    """
    # Extract film slug from filename: {slug}_{lang}_v2.srt -> {slug}
    filename = srt_path.stem  # e.g., "spirited_away_en_v2"
    parts = filename.split("_")

    # Remove "v2" and language code (last 2 parts)
    film_slug = "_".join(parts[:-2])
    lang_code = parts[-2]

    # Load subtitle
    try:
        subs = pysrt.open(str(srt_path), encoding="utf-8")
    except Exception as e:
        return {
            "film_slug": film_slug,
            "language": lang_code,
            "status": "FAIL",
            "error": f"Failed to parse SRT: {e}",
        }

    if not subs:
        return {
            "film_slug": film_slug,
            "language": lang_code,
            "status": "FAIL",
            "error": "Empty subtitle file",
        }

    # Get last subtitle timestamp
    last_sub = subs[-1]
    last_time_seconds = (
        last_sub.end.hours * 3600 +
        last_sub.end.minutes * 60 +
        last_sub.end.seconds +
        last_sub.end.milliseconds / 1000.0
    )

    # Get documented runtime
    if film_slug not in film_versions:
        return {
            "film_slug": film_slug,
            "language": lang_code,
            "status": "FAIL",
            "error": f"No documented version for {film_slug}",
        }

    documented_runtime = film_versions[film_slug]["runtime_seconds"]

    # Calculate drift
    drift_percent = abs(last_time_seconds - documented_runtime) / documented_runtime * 100

    # Determine status
    if drift_percent <= 5.0:
        status = "PASS"
    elif drift_percent <= 10.0:
        status = "WARN"
    else:
        status = "FAIL"

    return {
        "film_slug": film_slug,
        "film_title": film_versions[film_slug]["title"],
        "language": lang_code,
        "status": status,
        "last_subtitle_time": last_time_seconds,
        "documented_runtime": documented_runtime,
        "timing_drift_percent": drift_percent,
        "subtitle_count": len(subs),
    }


def main():
    """Validate all v2 subtitle files."""
    print("="*60)
    print("Quick Subtitle Validation (v2 files)")
    print("Story 4.X.2")
    print("="*60)

    # Load film versions
    film_versions = load_film_versions()
    print(f"Loaded {len(film_versions)} film versions")

    # Find all v2 subtitle files
    v2_dir = PROJECT_ROOT / "data/raw/subtitles_improved"
    v2_files = sorted(v2_dir.glob("*_en_v2.srt"))

    print(f"Found {len(v2_files)} improved subtitle files")
    print("="*60)

    # Validate each file
    results = []
    for srt_path in v2_files:
        result = validate_subtitle_file(srt_path, film_versions)
        results.append(result)

        status_icon = {
            "PASS": "✅",
            "WARN": "⚠️",
            "FAIL": "❌",
        }.get(result["status"], "❓")

        print(f"{status_icon} {result['film_slug']}: {result['status']}", end="")
        if "error" in result:
            print(f" - {result['error']}")
        else:
            print(f" (drift: {result['timing_drift_percent']:.1f}%)")

    # Summary
    print("="*60)
    print("VALIDATION SUMMARY")
    print("="*60)

    pass_count = sum(1 for r in results if r["status"] == "PASS")
    warn_count = sum(1 for r in results if r["status"] == "WARN")
    fail_count = sum(1 for r in results if r["status"] == "FAIL")
    total = len(results)

    print(f"✅ PASS: {pass_count}/{total} ({pass_count/total*100:.1f}%)")
    print(f"⚠️  WARN: {warn_count}/{total} ({warn_count/total*100:.1f}%)")
    print(f"❌ FAIL: {fail_count}/{total} ({fail_count/total*100:.1f}%)")

    # Save results
    output_path = PROJECT_ROOT / "data/processed/subtitle_validation_v2_quick.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump({
            "story": "4.X.2",
            "validation_type": "quick",
            "total_files": total,
            "pass_count": pass_count,
            "warn_count": warn_count,
            "fail_count": fail_count,
            "pass_rate": pass_count / total * 100 if total > 0 else 0,
            "results": results,
        }, f, indent=2)

    print(f"\nResults saved to: {output_path}")
    print("="*60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
