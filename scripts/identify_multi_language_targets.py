#!/usr/bin/env python3
"""
Identify multi-language subtitle targets for Story 4.X.5.

Analyzes validation results to prioritize non-English (FR, ES, NL, AR) subtitle
improvements for maximum ROI on cross-language emotion analysis.

Usage:
    python scripts/identify_multi_language_targets.py
"""
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Featured films (Epic 5 showcase)
FEATURED_FILMS = {
    "spirited_away",
    "princess_mononoke",
    "my_neighbor_totoro",
    "howls_moving_castle",
    "kikis_delivery_service",
}

# Non-English languages for emotion analysis
TARGET_LANGUAGES = ["fr", "es", "nl", "ar"]

# Film metadata for display
FILM_TITLES = {
    "arrietty": "Arrietty",
    "castle_in_the_sky": "Castle in the Sky",
    "earwig_and_the_witch": "Earwig and the Witch",
    "from_up_on_poppy_hill": "From Up on Poppy Hill",
    "grave_of_the_fireflies": "Grave of the Fireflies",
    "howls_moving_castle": "Howl's Moving Castle",
    "kikis_delivery_service": "Kiki's Delivery Service",
    "my_neighbor_totoro": "My Neighbor Totoro",
    "my_neighbors_the_yamadas": "My Neighbors the Yamadas",
    "only_yesterday": "Only Yesterday",
    "pom_poko": "Pom Poko",
    "ponyo": "Ponyo",
    "porco_rosso": "Porco Rosso",
    "princess_mononoke": "Princess Mononoke",
    "spirited_away": "Spirited Away",
    "tales_from_earthsea": "Tales from Earthsea",
    "the_cat_returns": "The Cat Returns",
    "the_red_turtle": "The Red Turtle",
    "the_tale_of_the_princess_kaguya": "The Tale of the Princess Kaguya",
    "the_wind_rises": "The Wind Rises",
    "when_marnie_was_there": "When Marnie Was There",
    "whisper_of_the_heart": "Whisper of the Heart",
}


@dataclass
class LanguageTarget:
    """Individual language+film target for acquisition."""
    film_slug: str
    film_title: str
    language: str
    current_status: str
    timing_drift_percent: float
    priority_score: int
    reason: str


def load_validation_results(path: Path) -> Dict:
    """Load validation results from JSON file."""
    logger.info(f"Loading validation results from {path}")
    with open(path, "r") as f:
        return json.load(f)


def calculate_cross_language_drift(film_data: Dict) -> float:
    """Calculate cross-language timing drift for a film."""
    per_lang = film_data.get("per_language", {})
    
    durations = []
    for lang in TARGET_LANGUAGES + ["en"]:
        if lang in per_lang:
            lang_data = per_lang[lang]
            duration = lang_data.get("subtitle_duration")
            if duration:
                durations.append(duration)
    
    if len(durations) < 2:
        return 0.0
    
    avg_duration = sum(durations) / len(durations)
    max_deviation = max(abs(d - avg_duration) for d in durations)
    
    return (max_deviation / avg_duration * 100) if avg_duration > 0 else 0.0


def prioritize_language_target(
    film_slug: str,
    language: str,
    lang_data: Dict,
    cross_lang_drift: float
) -> int:
    """
    Calculate priority score for a film+language combination.
    
    Priority scoring (0-100):
    - Featured film: +40 points
    - Status FAIL: +30 points
    - Status WARN: +20 points
    - High timing drift (>5%): +15 points
    - Medium timing drift (2-5%): +10 points
    - High cross-language drift (>10%): +10 points
    - Medium cross-language drift (5-10%): +5 points
    
    Returns:
        Priority score (0-100, higher = more urgent)
    """
    score = 0
    
    # Featured film bonus
    if film_slug in FEATURED_FILMS:
        score += 40
    
    # Current status penalty
    status = lang_data.get("status", "UNKNOWN")
    if status == "FAIL":
        score += 30
    elif status == "WARN":
        score += 20
    
    # Timing drift penalty
    drift = lang_data.get("timing_drift_percent", 0.0) or 0.0
    if drift > 5.0:
        score += 15
    elif drift > 2.0:
        score += 10
    
    # Cross-language consistency penalty
    if cross_lang_drift > 10.0:
        score += 10
    elif cross_lang_drift > 5.0:
        score += 5
    
    return min(score, 100)  # Cap at 100


def analyze_multi_language_targets(
    validation_results: Dict
) -> Tuple[List[LanguageTarget], Dict]:
    """
    Analyze validation results and identify multi-language targets.
    
    Returns:
        Tuple of (targets_list, summary_stats)
    """
    targets: List[LanguageTarget] = []
    
    # Statistics
    stats = {
        "total_analyzed": 0,
        "fail_count": 0,
        "warn_count": 0,
        "pass_count": 0,
        "featured_targets": 0,
        "non_featured_targets": 0,
    }
    
    for film_slug, film_data in validation_results.items():
        # Skip test films
        if film_slug.startswith("film"):
            continue
        
        per_lang = film_data.get("per_language", {})
        cross_lang_drift = calculate_cross_language_drift(film_data)
        
        for language in TARGET_LANGUAGES:
            if language not in per_lang:
                continue
            
            lang_data = per_lang[language]
            status = lang_data.get("status", "UNKNOWN")
            drift = lang_data.get("timing_drift_percent", 0.0) or 0.0
            
            stats["total_analyzed"] += 1
            
            # Only target FAIL or WARN files
            if status == "PASS":
                stats["pass_count"] += 1
                continue
            
            if status == "FAIL":
                stats["fail_count"] += 1
            elif status == "WARN":
                stats["warn_count"] += 1
            
            # Calculate priority score
            priority_score = prioritize_language_target(
                film_slug, language, lang_data, cross_lang_drift
            )
            
            # Build reason string
            reasons = []
            if film_slug in FEATURED_FILMS:
                reasons.append("Featured film")
            if status == "FAIL":
                reasons.append(f"FAIL status ({drift:.1f}% drift)")
            elif status == "WARN":
                reasons.append(f"WARN status ({drift:.1f}% drift)")
            if cross_lang_drift > 10.0:
                reasons.append(f"High cross-lang drift ({cross_lang_drift:.1f}%)")
            
            target = LanguageTarget(
                film_slug=film_slug,
                film_title=FILM_TITLES.get(film_slug, film_slug.replace("_", " ").title()),
                language=language.upper(),
                current_status=status,
                timing_drift_percent=drift,
                priority_score=priority_score,
                reason=", ".join(reasons)
            )
            
            targets.append(target)
            
            if film_slug in FEATURED_FILMS:
                stats["featured_targets"] += 1
            else:
                stats["non_featured_targets"] += 1
    
    # Sort by priority score (highest first)
    targets.sort(key=lambda t: t.priority_score, reverse=True)
    
    return targets, stats


def generate_priority_list_markdown(
    targets: List[LanguageTarget],
    stats: Dict,
    output_path: Path
) -> None:
    """Generate priority list markdown file."""
    logger.info(f"Generating priority list: {output_path}")
    
    # Calculate expected improvement
    current_pass_count = 70  # After Phase 1 (Story 4.X.4)
    total_files = 134  # 22 films × 6 languages
    improvement_count = min(len(targets), 50)  # Target 30-50 files
    expected_pass_count = current_pass_count + improvement_count
    expected_pass_rate = expected_pass_count / total_files * 100
    
    content = f"""# Multi-Language Subtitle Priority List

**Story:** 4.X.5 Phase 2 - Multi-Language Subtitle Quality Improvement
**Generated:** 2025-11-18
**Target Languages:** FR, ES, NL, AR (non-English)
**Source:** `data/processed/subtitle_validation_results.json`

---

## Executive Summary

- **Total Non-English Files Analyzed:** {stats['total_analyzed']}
- **Current FAIL/WARN Targets:** {stats['fail_count'] + stats['warn_count']}
- **Current Pass Rate:** 52.2% (70/134 files)
- **Target Pass Rate:** 75%+ (100+/134 files)
- **Expected Improvement:** +{improvement_count} PASS files

### Target Breakdown

- **FAIL Status:** {stats['fail_count']} files
- **WARN Status:** {stats['warn_count']} files
- **Featured Film Targets:** {stats['featured_targets']} files
- **Non-Featured Targets:** {stats['non_featured_targets']} files

---

## Priority Strategy

This list prioritizes multi-language subtitle acquisition based on:

1. **Featured Films (Epic 5 Showcase)**: Spirited Away, Princess Mononoke, My Neighbor Totoro, Howl's Moving Castle, Kiki's Delivery Service
2. **Current Status**: FAIL (>5% drift) > WARN (2-5% drift)
3. **Cross-Language Consistency**: Films with high variance across languages
4. **Portfolio Impact**: High-visibility films for demonstration

**Target:** Acquire 30-50 improved subtitle files to achieve 75%+ overall pass rate.

---

## Priority Targets (Sorted by Score)

"""
    
    # Group targets by priority tier
    tier_1 = [t for t in targets if t.priority_score >= 70]
    tier_2 = [t for t in targets if 50 <= t.priority_score < 70]
    tier_3 = [t for t in targets if t.priority_score < 50]
    
    content += f"### Tier 1: Critical Priority (Score 70-100) — {len(tier_1)} targets\n\n"
    content += "| Film | Language | Status | Drift % | Score | Reason |\n"
    content += "|------|----------|--------|---------|-------|--------|\n"
    
    for target in tier_1:
        content += f"| {target.film_title} | {target.language} | {target.current_status} | {target.timing_drift_percent:.1f}% | {target.priority_score} | {target.reason} |\n"
    
    content += f"\n### Tier 2: High Priority (Score 50-69) — {len(tier_2)} targets\n\n"
    content += "| Film | Language | Status | Drift % | Score | Reason |\n"
    content += "|------|----------|--------|---------|-------|--------|\n"
    
    for target in tier_2:
        content += f"| {target.film_title} | {target.language} | {target.current_status} | {target.timing_drift_percent:.1f}% | {target.priority_score} | {target.reason} |\n"
    
    content += f"\n### Tier 3: Medium Priority (Score <50) — {len(tier_3)} targets\n\n"
    content += "| Film | Language | Status | Drift % | Score | Reason |\n"
    content += "|------|----------|--------|---------|-------|--------|\n"
    
    for target in tier_3:
        content += f"| {target.film_title} | {target.language} | {target.current_status} | {target.timing_drift_percent:.1f}% | {target.priority_score} | {target.reason} |\n"
    
    content += f"""
---

## Acquisition Recommendations

### Phase 2A: Featured Films (Priority 1)
Target all non-English languages for the 5 featured films. This ensures cross-language consistency for Epic 5 showcases.

**Target Count:** {stats['featured_targets']} files

### Phase 2B: High-Impact FAIL Files (Priority 2)
Focus on non-featured films with FAIL status and high timing drift (>10%).

**Target Count:** ~{len([t for t in targets if t.current_status == 'FAIL' and t.film_slug not in FEATURED_FILMS])} files

### Phase 2C: WARN Files for Cross-Language Consistency (Priority 3)
Improve WARN status files to achieve overall 75%+ pass rate.

**Target Count:** ~{len([t for t in targets if t.current_status == 'WARN'])} files

---

## Expected Outcomes

After acquiring top 30-50 prioritized targets:

- **Overall Pass Rate:** 52.2% → 75-80%+
- **Featured Films:** 100% PASS across all emotion analysis languages (EN, FR, ES, NL, AR)
- **Cross-Language Consistency:** <3% drift within same film
- **Portfolio Impact:** Robust cross-language emotion comparison for Epic 5

---

## Next Steps

1. **Review this priority list** with team/stakeholders
2. **Run acquisition script:** `python scripts/fetch_priority_subtitles.py --batch data/metadata/multi_language_priority_list.md --languages fr,es,nl,ar`
3. **Validate acquired files:** Run timing validation after each batch
4. **Re-run emotion analysis:** Process improved files through Epic 3 pipeline
5. **Update metrics:** Document Phase 2 improvement in validation reports

---

**Generated by:** `scripts/identify_multi_language_targets.py`
**Story:** 4.X.5
**Date:** 2025-11-18
"""
    
    # Write to file
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        f.write(content)
    
    logger.info(f"Priority list saved to {output_path}")
    logger.info(f"Total targets identified: {len(targets)}")
    logger.info(f"Tier 1 (Critical): {len(tier_1)}")
    logger.info(f"Tier 2 (High): {len(tier_2)}")
    logger.info(f"Tier 3 (Medium): {len(tier_3)}")


def main() -> None:
    """Main execution function."""
    # Paths
    validation_path = Path("data/processed/subtitle_validation_results.json")
    output_path = Path("data/metadata/multi_language_priority_list.md")
    
    # Load validation results
    validation_results = load_validation_results(validation_path)
    
    # Analyze targets
    targets, stats = analyze_multi_language_targets(validation_results)
    
    # Generate priority list
    generate_priority_list_markdown(targets, stats, output_path)
    
    # Summary stats
    logger.info("")
    logger.info("=" * 60)
    logger.info("MULTI-LANGUAGE TARGET ANALYSIS COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total non-English files analyzed: {stats['total_analyzed']}")
    logger.info(f"Current PASS files: {stats['pass_count']}")
    logger.info(f"Target FAIL files: {stats['fail_count']}")
    logger.info(f"Target WARN files: {stats['warn_count']}")
    logger.info(f"Total improvement targets: {len(targets)}")
    logger.info("")
    logger.info(f"Priority list saved to: {output_path}")
    logger.info("")
    logger.info("Next: Review priority list and run acquisition script")
    logger.info("")


if __name__ == "__main__":
    main()



