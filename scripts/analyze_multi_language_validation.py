#!/usr/bin/env python3
"""
Analyze multi-language validation results and generate comparison reports.

This script processes validation results from Task 4 and generates comprehensive
reports comparing baseline → Phase 1 (English) → Phase 2 (multi-language) improvements.

Usage:
    python scripts/analyze_multi_language_validation.py
"""
import json
import logging
from pathlib import Path
from typing import Dict, List, Any

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def load_validation_results(path: Path) -> Dict:
    """Load validation results from JSON file."""
    logger.info(f"Loading validation results from {path}")
    
    if not path.exists():
        logger.error(f"Validation file not found: {path}")
        return {}
    
    with open(path, "r") as f:
        return json.load(f)


def calculate_pass_rate_by_language(results: Dict) -> Dict[str, Dict[str, int]]:
    """
    Calculate pass rates broken down by language.
    
    Returns:
        Dict mapping language codes to counts: {lang: {pass: N, warn: N, fail: N}}
    """
    lang_stats = {}
    
    for film_slug, film_data in results.items():
        if film_slug.startswith("_") or film_slug == "film1":
            continue
        
        per_lang = film_data.get("per_language", {})
        
        for lang_code, lang_data in per_lang.items():
            if lang_code not in lang_stats:
                lang_stats[lang_code] = {"pass": 0, "warn": 0, "fail": 0, "total": 0}
            
            status = lang_data.get("status", "UNKNOWN").lower()
            
            if status == "pass":
                lang_stats[lang_code]["pass"] += 1
            elif status == "warn":
                lang_stats[lang_code]["warn"] += 1
            elif status == "fail":
                lang_stats[lang_code]["fail"] += 1
            
            lang_stats[lang_code]["total"] += 1
    
    return lang_stats


def calculate_overall_stats(results: Dict) -> Dict[str, Any]:
    """Calculate overall validation statistics."""
    total_files = 0
    pass_count = 0
    warn_count = 0
    fail_count = 0
    
    timing_drifts = []
    
    for film_slug, film_data in results.items():
        if film_slug.startswith("_") or film_slug == "film1":
            continue
        
        per_lang = film_data.get("per_language", {})
        
        for lang_data in per_lang.values():
            total_files += 1
            
            status = lang_data.get("status", "UNKNOWN").lower()
            drift = lang_data.get("timing_drift_percent")
            
            if status == "pass":
                pass_count += 1
            elif status == "warn":
                warn_count += 1
            elif status == "fail":
                fail_count += 1
            
            if drift is not None:
                timing_drifts.append(drift)
    
    avg_drift = sum(timing_drifts) / len(timing_drifts) if timing_drifts else 0
    pass_rate = (pass_count / total_files * 100) if total_files > 0 else 0
    
    return {
        "total_files": total_files,
        "pass_count": pass_count,
        "warn_count": warn_count,
        "fail_count": fail_count,
        "pass_rate": pass_rate,
        "average_drift": avg_drift,
    }


def calculate_cross_language_consistency(results: Dict) -> Dict[str, Any]:
    """
    Calculate cross-language consistency metrics.
    
    Returns:
        Dict with consistency stats
    """
    consistent_films = 0
    inconsistent_films = 0
    max_drifts = []
    
    for film_slug, film_data in results.items():
        if film_slug.startswith("_") or film_slug == "film1":
            continue
        
        cross_lang = film_data.get("cross_language", {})
        max_drift = cross_lang.get("max_drift_percent")
        cross_status = cross_lang.get("status", "UNKNOWN")
        
        if max_drift is not None:
            max_drifts.append(max_drift)
        
        if cross_status == "PASS":
            consistent_films += 1
        else:
            inconsistent_films += 1
    
    total_films = consistent_films + inconsistent_films
    consistency_rate = (consistent_films / total_films * 100) if total_films > 0 else 0
    avg_max_drift = sum(max_drifts) / len(max_drifts) if max_drifts else 0
    
    return {
        "consistent_films": consistent_films,
        "inconsistent_films": inconsistent_films,
        "consistency_rate": consistency_rate,
        "average_max_drift": avg_max_drift,
    }


def generate_comparison_report(
    baseline_path: Path,
    phase2_path: Path,
    output_path: Path
) -> None:
    """
    Generate comparison report: baseline vs Phase 2.
    
    Args:
        baseline_path: Path to baseline validation results
        phase2_path: Path to Phase 2 validation results
        output_path: Path to save comparison report
    """
    logger.info("Generating comparison report")
    
    # Load results
    baseline = load_validation_results(baseline_path)
    phase2 = load_validation_results(phase2_path)
    
    if not baseline or not phase2:
        logger.error("Could not load validation results")
        return
    
    # Calculate stats
    baseline_stats = calculate_overall_stats(baseline)
    phase2_stats = calculate_overall_stats(phase2)
    
    baseline_lang_stats = calculate_pass_rate_by_language(baseline)
    phase2_lang_stats = calculate_pass_rate_by_language(phase2)
    
    baseline_consistency = calculate_cross_language_consistency(baseline)
    phase2_consistency = calculate_cross_language_consistency(phase2)
    
    # Generate report
    report = f"""# Multi-Language Validation Comparison Report

**Story:** 4.X.5 - Phase 2  
**Generated:** {Path(__file__).stat().st_mtime}  
**Comparison:** Baseline vs Phase 2 (Multi-Language Improvements)

---

## Executive Summary

### Overall Pass Rate

| Phase | Total Files | PASS | WARN | FAIL | Pass Rate | Improvement |
|-------|-------------|------|------|------|-----------|-------------|
| **Baseline** | {baseline_stats['total_files']} | {baseline_stats['pass_count']} | {baseline_stats['warn_count']} | {baseline_stats['fail_count']} | {baseline_stats['pass_rate']:.1f}% | - |
| **Phase 2** | {phase2_stats['total_files']} | {phase2_stats['pass_count']} | {phase2_stats['warn_count']} | {phase2_stats['fail_count']} | {phase2_stats['pass_rate']:.1f}% | +{phase2_stats['pass_rate'] - baseline_stats['pass_rate']:.1f}% |

**Status:** {"✓ TARGET MET" if phase2_stats['pass_rate'] >= 75 else "⚠️  BELOW TARGET (75%+)"}

### Timing Drift

| Phase | Average Drift |
|-------|---------------|
| **Baseline** | {baseline_stats['average_drift']:.2f}% |
| **Phase 2** | {phase2_stats['average_drift']:.2f}% |

**Improvement:** {baseline_stats['average_drift'] - phase2_stats['average_drift']:.2f}% reduction

### Cross-Language Consistency

| Phase | Consistent Films | Inconsistent Films | Consistency Rate |
|-------|------------------|-------------------|------------------|
| **Baseline** | {baseline_consistency['consistent_films']} | {baseline_consistency['inconsistent_films']} | {baseline_consistency['consistency_rate']:.1f}% |
| **Phase 2** | {phase2_consistency['consistent_films']} | {phase2_consistency['inconsistent_films']} | {phase2_consistency['consistency_rate']:.1f}% |

**Improvement:** +{phase2_consistency['consistency_rate'] - baseline_consistency['consistency_rate']:.1f}%

---

## Per-Language Breakdown

### Baseline Results

| Language | Total | PASS | WARN | FAIL | Pass Rate |
|----------|-------|------|------|------|-----------|
"""
    
    for lang in ["en", "fr", "es", "nl", "ar", "ja"]:
        if lang in baseline_lang_stats:
            stats = baseline_lang_stats[lang]
            pass_rate = (stats['pass'] / stats['total'] * 100) if stats['total'] > 0 else 0
            report += f"| {lang.upper()} | {stats['total']} | {stats['pass']} | {stats['warn']} | {stats['fail']} | {pass_rate:.1f}% |\n"
    
    report += "\n### Phase 2 Results\n\n"
    report += "| Language | Total | PASS | WARN | FAIL | Pass Rate | Improvement |\n"
    report += "|----------|-------|------|------|------|-----------|-------------|\n"
    
    for lang in ["en", "fr", "es", "nl", "ar", "ja"]:
        if lang in phase2_lang_stats:
            baseline_stats_lang = baseline_lang_stats.get(lang, {"pass": 0, "total": 1})
            phase2_stats_lang = phase2_lang_stats[lang]
            
            baseline_pass_rate = (baseline_stats_lang['pass'] / baseline_stats_lang['total'] * 100)
            phase2_pass_rate = (phase2_stats_lang['pass'] / phase2_stats_lang['total'] * 100)
            improvement = phase2_pass_rate - baseline_pass_rate
            
            report += f"| {lang.upper()} | {phase2_stats_lang['total']} | {phase2_stats_lang['pass']} | {phase2_stats_lang['warn']} | {phase2_stats_lang['fail']} | {phase2_pass_rate:.1f}% | +{improvement:.1f}% |\n"
    
    report += f"""
---

## Key Insights

### Achievements

"""
    
    # Generate insights based on results
    if phase2_stats['pass_rate'] >= 75:
        report += f"- ✅ **Exceeded 75% pass rate target**: Achieved {phase2_stats['pass_rate']:.1f}%\n"
    
    if phase2_stats['average_drift'] < 2.0:
        report += f"- ✅ **Excellent timing accuracy**: Average drift {phase2_stats['average_drift']:.2f}% (<2%)\n"
    
    if phase2_consistency['consistency_rate'] > baseline_consistency['consistency_rate']:
        report += f"- ✅ **Improved cross-language consistency**: {phase2_consistency['consistency_rate']:.1f}% of films have <3% drift\n"
    
    report += f"""
### Remaining Issues

"""
    
    if phase2_stats['fail_count'] > 0:
        report += f"- ⚠️  **{phase2_stats['fail_count']} files still FAIL**: May require manual refinement\n"
    
    if phase2_stats['warn_count'] > 10:
        report += f"- ⚠️  **{phase2_stats['warn_count']} files with WARN status**: Consider refinement for stretch goal\n"
    
    report += f"""
---

## Recommendations

### For Portfolio Narrative

1. **Highlight Pass Rate Improvement**: {baseline_stats['pass_rate']:.1f}% → {phase2_stats['pass_rate']:.1f}% (+{phase2_stats['pass_rate'] - baseline_stats['pass_rate']:.1f}% absolute)
2. **Emphasize Multi-Language Coverage**: {phase2_stats['pass_count']} validated files across 5 emotion analysis languages
3. **Demonstrate Iterative Approach**: Phase 1 (English priority films) → Phase 2 (Multi-language expansion)

### Next Steps

1. **Re-run emotion analysis** (Task 5) on {phase2_stats['pass_count']} validated files
2. **Update documentation** (Task 6) with Phase 2 metrics
3. **Run full regression tests** (Task 7) to ensure pipeline integrity

---

**Generated by:** `scripts/analyze_multi_language_validation.py`  
**Story:** 4.X.5  
**Date:** 2025-11-18
"""
    
    # Write report
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        f.write(report)
    
    logger.info(f"Comparison report saved to: {output_path}")
    
    # Print summary to console
    print("\n" + "="*60)
    print("VALIDATION COMPARISON SUMMARY")
    print("="*60)
    print(f"Baseline Pass Rate: {baseline_stats['pass_rate']:.1f}%")
    print(f"Phase 2 Pass Rate: {phase2_stats['pass_rate']:.1f}%")
    print(f"Improvement: +{phase2_stats['pass_rate'] - baseline_stats['pass_rate']:.1f}%")
    print(f"Target (75%): {'✓ MET' if phase2_stats['pass_rate'] >= 75 else '✗ NOT MET'}")
    print("="*60)


def main() -> None:
    """Main execution function."""
    # Paths
    baseline_path = Path("data/processed/subtitle_validation_results.json")
    phase2_path = Path("data/processed/subtitle_validation_multi_language_v2.json")
    output_path = Path("data/metadata/multi_language_validation_comparison.md")
    
    # Check if files exist
    if not baseline_path.exists():
        logger.error(f"Baseline validation file not found: {baseline_path}")
        logger.info("Please ensure baseline validation has been run")
        return
    
    if not phase2_path.exists():
        logger.warning(f"Phase 2 validation file not found: {phase2_path}")
        logger.info("This file will be created after running Task 4.1 (subtitle validation)")
        logger.info("For now, generating analysis instructions...")
        
        # Create placeholder instructions
        instructions_path = Path("data/metadata/multi_language_validation_instructions.md")
        with open(instructions_path, "w") as f:
            f.write(f"""# Multi-Language Validation Instructions

**Story:** 4.X.5 - Task 4

---

## Step 1: Run Validation on Acquired Files

After acquiring subtitles (Task 3), run validation:

```bash
python src/validation/validate_subtitle_timing.py \\
    --subtitle-dir data/processed/subtitles/ \\
    --metadata data/metadata/film_versions.json \\
    --output data/processed/subtitle_validation_multi_language_v2.json
```

## Step 2: Generate Comparison Report

After validation completes:

```bash
python scripts/analyze_multi_language_validation.py
```

This will generate:
- `data/metadata/multi_language_validation_comparison.md`

---

**Expected Outcomes:**
- Phase 2 pass rate: 75%+ (100+/134 files)
- Average timing drift: <2%
- Cross-language consistency: <3% drift within films

---
""")
        logger.info(f"Instructions saved to: {instructions_path}")
        return
    
    # Generate comparison report
    generate_comparison_report(baseline_path, phase2_path, output_path)


if __name__ == "__main__":
    main()



