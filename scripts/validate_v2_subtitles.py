#!/usr/bin/env python3
"""
Quick validation for v2 subtitle files (Story 4.X.5).

Validates timing accuracy of acquired multi-language subtitle files.
"""
import json
import logging
from pathlib import Path
from typing import Dict, Any

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def load_film_versions() -> Dict:
    """Load documented film runtimes."""
    with open("data/metadata/film_versions.json", "r") as f:
        versions = json.load(f)
    versions.pop("_documentation", None)
    return versions


def validate_v2_file(parsed_json_path: Path, film_versions: Dict) -> Dict[str, Any]:
    """Validate a single v2 parsed subtitle file."""
    with open(parsed_json_path, "r") as f:
        data = json.load(f)
    
    metadata = data.get("metadata", {})
    subtitles = data.get("subtitles", [])
    
    film_slug = metadata.get("film_slug", "")
    # Extract base film slug (remove _lang_v2 suffix)
    base_slug = film_slug.replace("_en_v2", "").replace("_fr_v2", "").replace(
        "_es_v2", "").replace("_nl_v2", "").replace("_ar_v2", "")
    
    language = metadata.get("language_code", "unknown")
    
    # Get documented runtime
    documented_runtime = None
    if base_slug in film_versions:
        documented_runtime = film_versions[base_slug].get("runtime_seconds")
    
    # Calculate subtitle timing
    if subtitles:
        last_subtitle = max(subtitles, key=lambda s: s.get("end_time", 0))
        last_time = last_subtitle.get("end_time", 0)
        total_duration = metadata.get("total_duration", 0)
    else:
        last_time = 0
        total_duration = 0
    
    # Calculate timing drift
    if documented_runtime and total_duration:
        drift = abs(total_duration - documented_runtime) / documented_runtime * 100
        
        if drift > 5.0:
            status = "FAIL"
        elif drift > 2.0:
            status = "WARN"
        else:
            status = "PASS"
    else:
        drift = None
        status = "UNKNOWN"
    
    return {
        "film_slug": film_slug,
        "base_slug": base_slug,
        "language": language,
        "subtitle_count": len(subtitles),
        "total_duration": total_duration,
        "documented_runtime": documented_runtime,
        "timing_drift_percent": drift,
        "status": status,
    }


def main() -> None:
    """Main execution."""
    logger.info("Validating v2 subtitle files...")
    
    # Load film versions
    film_versions = load_film_versions()
    
    # Find all v2 parsed files
    parsed_dir = Path("data/processed/subtitles")
    v2_files = list(parsed_dir.glob("*_v2_parsed.json"))
    
    logger.info(f"Found {len(v2_files)} v2 parsed files")
    
    # Validate each file
    results = []
    pass_count = 0
    warn_count = 0
    fail_count = 0
    
    for filepath in sorted(v2_files):
        result = validate_v2_file(filepath, film_versions)
        results.append(result)
        
        status = result["status"]
        drift = result["timing_drift_percent"]
        
        if status == "PASS":
            pass_count += 1
            emoji = "✅"
        elif status == "WARN":
            warn_count += 1
            emoji = "⚠️"
        elif status == "FAIL":
            fail_count += 1
            emoji = "❌"
        else:
            emoji = "❓"
        
        drift_str = f"{drift:.2f}%" if drift is not None else "N/A"
        logger.info(f"{emoji} {result['film_slug']} ({result['language'].upper()}): {status} - Drift: {drift_str}")
    
    # Save results
    output = {
        "story": "4.X.5",
        "validation_type": "v2_subtitles",
        "total_files": len(v2_files),
        "pass_count": pass_count,
        "warn_count": warn_count,
        "fail_count": fail_count,
        "pass_rate": (pass_count / len(v2_files) * 100) if v2_files else 0,
        "results": results,
    }
    
    output_path = Path("data/processed/subtitle_validation_v2_quick.json")
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)
    
    # Summary
    print("\n" + "="*60)
    print("V2 SUBTITLE VALIDATION SUMMARY")
    print("="*60)
    print(f"Total files validated: {len(v2_files)}")
    print(f"PASS: {pass_count} ({pass_count/len(v2_files)*100:.1f}%)")
    print(f"WARN: {warn_count} ({warn_count/len(v2_files)*100:.1f}%)")
    print(f"FAIL: {fail_count} ({fail_count/len(v2_files)*100:.1f}%)")
    print(f"Overall pass rate: {pass_count/len(v2_files)*100:.1f}%")
    print("="*60)
    print(f"\nResults saved to: {output_path}")


if __name__ == "__main__":
    main()



