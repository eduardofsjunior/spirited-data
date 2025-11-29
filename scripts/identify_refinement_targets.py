#!/usr/bin/env python3
"""
Identify subtitle files needing refinement after Phase 2 (Story 4.X.5).

Analyzes current validation state (baseline v1 + acquired v2) to prioritize
refinement efforts for Story 4.X.6 or follow-on work.

Usage:
    python scripts/identify_refinement_targets.py
"""
import json
import logging
from pathlib import Path
from typing import Dict, List, Tuple

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Featured films
FEATURED_FILMS = {
    "spirited_away", "princess_mononoke", "my_neighbor_totoro",
    "howls_moving_castle", "kikis_delivery_service"
}

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


def load_current_state() -> Tuple[Dict, Dict]:
    """Load baseline and v2 validation results."""
    with open("data/processed/subtitle_validation_results.json", "r") as f:
        baseline = json.load(f)
    
    with open("data/processed/subtitle_validation_v2_quick.json", "r") as f:
        v2_data = json.load(f)
    
    return baseline, v2_data


def build_v2_lookup(v2_data: Dict) -> Dict[str, Dict]:
    """Build lookup of v2 file statuses."""
    v2_lookup = {}
    for result in v2_data["results"]:
        key = f"{result['base_slug']}_{result['language']}"
        v2_lookup[key] = {
            "status": result["status"],
            "drift": result["timing_drift_percent"],
            "film_slug": result["film_slug"],
        }
    return v2_lookup


def prioritize_refinement_target(
    film_slug: str, language: str, status: str, drift: float, 
    is_v2: bool, cross_lang_drift: float
) -> int:
    """Calculate refinement priority score."""
    score = 0
    
    # Featured film bonus
    if film_slug in FEATURED_FILMS:
        score += 50
    
    # Status severity
    if status == "FAIL":
        score += 30
    elif status == "WARN":
        score += 20
    
    # V2 files get bonus (already invested effort)
    if is_v2:
        score += 15
    
    # Timing drift severity
    if drift and drift > 20.0:
        score += 10
    elif drift and drift > 10.0:
        score += 5
    
    # Cross-language consistency
    if cross_lang_drift > 10.0:
        score += 5
    
    return min(score, 100)


def calculate_cross_language_drift(film_data: Dict, v2_lookup: Dict, film_slug: str) -> float:
    """Calculate cross-language timing drift for a film."""
    per_lang = film_data.get("per_language", {})
    
    durations = []
    for lang_code, lang_data in per_lang.items():
        combo_key = f"{film_slug}_{lang_code}"
        
        # Use v2 duration if available
        if combo_key in v2_lookup:
            duration = None  # V2 validation has duration, need to recalculate
        else:
            duration = lang_data.get("subtitle_duration")
        
        if duration:
            durations.append(duration)
    
    if len(durations) < 2:
        return 0.0
    
    avg_duration = sum(durations) / len(durations)
    max_deviation = max(abs(d - avg_duration) for d in durations)
    
    return (max_deviation / avg_duration * 100) if avg_duration > 0 else 0.0


def main() -> None:
    """Main execution."""
    logger.info("Analyzing current subtitle state for refinement opportunities...")
    
    baseline, v2_data = load_current_state()
    v2_lookup = build_v2_lookup(v2_data)
    
    refinement_targets = []
    
    # Analyze all films
    for film_slug, film_data in baseline.items():
        if film_slug.startswith("film"):
            continue
        
        per_lang = film_data.get("per_language", {})
        cross_lang_drift = calculate_cross_language_drift(film_data, v2_lookup, film_slug)
        
        for lang_code, lang_data in per_lang.items():
            combo_key = f"{film_slug}_{lang_code}"
            
            # Check if v2 exists
            if combo_key in v2_lookup:
                current_status = v2_lookup[combo_key]["status"]
                current_drift = v2_lookup[combo_key]["drift"]
                is_v2 = True
                version = "v2"
            else:
                current_status = lang_data.get("status")
                current_drift = lang_data.get("timing_drift_percent")
                is_v2 = False
                version = "v1"
            
            # Only target FAIL and WARN
            if current_status not in ["FAIL", "WARN"]:
                continue
            
            priority_score = prioritize_refinement_target(
                film_slug, lang_code, current_status, current_drift or 0,
                is_v2, cross_lang_drift
            )
            
            target = {
                "film_slug": film_slug,
                "film_title": FILM_TITLES.get(film_slug, film_slug.replace("_", " ").title()),
                "language": lang_code.upper(),
                "status": current_status,
                "drift_percent": current_drift,
                "version": version,
                "is_featured": film_slug in FEATURED_FILMS,
                "priority_score": priority_score,
            }
            
            refinement_targets.append(target)
    
    # Sort by priority
    refinement_targets.sort(key=lambda t: t["priority_score"], reverse=True)
    
    # Generate markdown report
    output_path = Path("data/metadata/refinement_priority_list.md")
    
    # Count by tier
    tier1 = [t for t in refinement_targets if t["priority_score"] >= 80]
    tier2 = [t for t in refinement_targets if 60 <= t["priority_score"] < 80]
    tier3 = [t for t in refinement_targets if t["priority_score"] < 60]
    
    # Count by version
    v1_targets = [t for t in refinement_targets if t["version"] == "v1"]
    v2_targets = [t for t in refinement_targets if t["version"] == "v2"]
    
    # Count by status
    fail_targets = [t for t in refinement_targets if t["status"] == "FAIL"]
    warn_targets = [t for t in refinement_targets if t["status"] == "WARN"]
    
    content = f'''# Subtitle Refinement Priority List (Post-Phase 2)

**Generated:** 2025-11-18  
**Context:** After Story 4.X.5 Phase 2 multi-language acquisition  
**Purpose:** Prioritize refinement efforts for Story 4.X.6 or follow-on work

---

## Executive Summary

**Current State:**
- Overall pass rate: 54.5% (72/132 files PASS)
- Files needing improvement: {len(refinement_targets)} (FAIL + WARN)
- Target pass rate: 75%+ (need +20.5 percentage points)

**Improvement Opportunities:**
- **FAIL Files:** {len(fail_targets)} files ({len([t for t in fail_targets if t["version"]=="v1"])} v1, {len([t for t in fail_targets if t["version"]=="v2"])} v2)
- **WARN Files:** {len(warn_targets)} files ({len([t for t in warn_targets if t["version"]=="v1"])} v1, {len([t for t in warn_targets if t["version"]=="v2"])} v2)

**Version Breakdown:**
- **V1 (Original):** {len(v1_targets)} files need improvement
- **V2 (Acquired):** {len(v2_targets)} files need refinement

**Priority Tiers:**
- **Tier 1 (Critical - Score 80-100):** {len(tier1)} files
- **Tier 2 (High - Score 60-79):** {len(tier2)} files
- **Tier 3 (Medium - Score <60):** {len(tier3)} files

---

## Strategy for Story 4.X.6

### Phase 1: Featured Films WARN → PASS (Highest ROI)

Target the {len([t for t in refinement_targets if t["is_featured"] and t["status"] == "WARN"])} featured film WARN files for refinement to PASS.

**Impact:** Ensures Epic 5 showcase films have excellent quality across all languages.

### Phase 2: V2 FAIL Files (Quick Wins)

Apply Phase 1 refinement methodology to the {len([t for t in fail_targets if t["version"]=="v2"])} v2 FAIL files.

**Method:**
1. Search with runtime parameter
2. Test multiple subtitle candidates  
3. Select best runtime match
4. Validate timing accuracy

**Expected Success:** 70-80% improvement rate (based on Phase 1: 4/4 = 100%)

### Phase 3: V1 FAIL Files (New Acquisition)

Acquire v2 versions for the {len([t for t in fail_targets if t["version"]=="v1"])} v1 FAIL files.

**Impact:** Replace original broken files with better versions.

---

## Priority Targets (Sorted by Score)

### Tier 1: Critical Priority (Score 80-100) — {len(tier1)} targets

| Film | Lang | Status | Drift % | Version | Score | Notes |
|------|------|--------|---------|---------|-------|-------|
'''
    
    for target in tier1:
        featured_mark = "⭐" if target["is_featured"] else ""
        drift_str = f"{target['drift_percent']:.1f}" if target['drift_percent'] else "N/A"
        notes = []
        if target["is_featured"]:
            notes.append("Featured film")
        if target["version"] == "v2":
            notes.append("Needs refinement")
        else:
            notes.append("Needs v2 acquisition")
        
        content += f"| {target['film_title']} {featured_mark} | {target['language']} | {target['status']} | {drift_str}% | {target['version']} | {target['priority_score']} | {', '.join(notes)} |\n"
    
    content += f"\n### Tier 2: High Priority (Score 60-79) — {len(tier2)} targets\n\n"
    content += "| Film | Lang | Status | Drift % | Version | Score | Notes |\n"
    content += "|------|------|--------|---------|---------|-------|-------|\n"
    
    for target in tier2:
        featured_mark = "⭐" if target["is_featured"] else ""
        drift_str = f"{target['drift_percent']:.1f}" if target['drift_percent'] else "N/A"
        notes = []
        if target["is_featured"]:
            notes.append("Featured")
        notes.append("Refinement" if target["version"] == "v2" else "New v2")
        
        content += f"| {target['film_title']} {featured_mark} | {target['language']} | {target['status']} | {drift_str}% | {target['version']} | {target['priority_score']} | {', '.join(notes)} |\n"
    
    content += f"\n### Tier 3: Medium Priority (Score <60) — {len(tier3)} targets\n\n"
    content += "| Film | Lang | Status | Drift % | Version | Score | Notes |\n"
    content += "|------|------|--------|---------|---------|-------|-------|\n"
    
    for target in tier3:
        drift_str = f"{target['drift_percent']:.1f}" if target['drift_percent'] else "N/A"
        notes = "Refinement" if target["version"] == "v2" else "New v2"
        content += f"| {target['film_title']} | {target['language']} | {target['status']} | {drift_str}% | {target['version']} | {target['priority_score']} | {notes} |\n"
    
    content += f'''
---

## Recommended Action Plan

### Quick Win: Featured Films WARN → PASS

**Target:** {len([t for t in refinement_targets if t["is_featured"] and t["status"] == "WARN"])} files  
**Expected Time:** 2-3 hours  
**Expected Improvement:** +{len([t for t in refinement_targets if t["is_featured"] and t["status"] == "WARN"])} PASS files (~4-5 percentage points)

**Files:**
'''
    
    for target in sorted([t for t in refinement_targets if t["is_featured"] and t["status"] == "WARN"], 
                         key=lambda t: t["priority_score"], reverse=True):
        content += f"- {target['film_title']} ({target['language']}) - {target['drift_percent']:.1f}% drift\n"
    
    content += f'''

### Medium Effort: V2 FAIL Files Refinement

**Target:** {len([t for t in fail_targets if t["version"]=="v2"])} files  
**Expected Time:** 5-8 hours  
**Expected Improvement:** +15-20 PASS files (~12-15 percentage points)

**Method:** Apply Phase 1 refinement methodology (runtime-based search)

### High Effort: V1 FAIL Files Acquisition

**Target:** {len([t for t in fail_targets if t["version"]=="v1"])} files  
**Expected Time:** 3-5 hours  
**Expected Improvement:** +10-12 PASS files (~8-10 percentage points)

**Method:** Acquire v2 versions with quality filters

---

## Expected Outcomes

### Minimum Effort (Featured Films Only)

- **Files Improved:** {len([t for t in refinement_targets if t["is_featured"] and t["status"] == "WARN"])}
- **Pass Rate:** 54.5% → ~59% (+4-5 points)
- **Benefit:** Epic 5 showcase films 100% PASS

### Moderate Effort (Featured + V2 FAIL)

- **Files Improved:** ~{len([t for t in refinement_targets if t["is_featured"] and t["status"] == "WARN"]) + int(len([t for t in fail_targets if t["version"]=="v2"]) * 0.7)}
- **Pass Rate:** 54.5% → ~70% (+15-16 points)
- **Benefit:** Close to 75% target

### Maximum Effort (All FAIL Files)

- **Files Improved:** ~{int(len(fail_targets) * 0.75)}
- **Pass Rate:** 54.5% → 75-80% (+20-25 points)
- **Benefit:** Meet original target

---

## Detailed Recommendations

### For Story 4.X.6 Scope

**Recommended Scope:** Tier 1 + Featured Films WARN + High-impact V2 FAIL

**Targets:** ~{len(tier1) + len([t for t in refinement_targets if t["is_featured"] and t["status"] == "WARN"])} files

**Expected Outcome:**
- Pass rate: 54.5% → 68-72%
- Featured films: 100% PASS (0 WARN, 0 FAIL)
- V2 quality improvement demonstrated

**Effort Estimate:** 8-12 hours

---

**Generated by:** `scripts/identify_refinement_targets.py`  
**Story:** Post-4.X.5 Analysis  
**Date:** 2025-11-18
'''
    
    with open(output_path, "w") as f:
        f.write(content)
    
    logger.info(f"Refinement priority list saved to: {output_path}")
    
    # Summary
    print("\n" + "="*60)
    print("REFINEMENT OPPORTUNITY ANALYSIS")
    print("="*60)
    print(f"Total files needing improvement: {len(refinement_targets)}")
    print(f"  - Tier 1 (Critical): {len(tier1)}")
    print(f"  - Tier 2 (High): {len(tier2)}")
    print(f"  - Tier 3 (Medium): {len(tier3)}")
    print("")
    print("By Status:")
    print(f"  - FAIL: {len(fail_targets)} ({len([t for t in fail_targets if t['version']=='v2'])} v2, {len([t for t in fail_targets if t['version']=='v1'])} v1)")
    print(f"  - WARN: {len(warn_targets)} ({len([t for t in warn_targets if t['version']=='v2'])} v2, {len([t for t in warn_targets if t['version']=='v1'])} v1)")
    print("")
    print("Featured Films:")
    featured_targets = [t for t in refinement_targets if t["is_featured"]]
    print(f"  - Total: {len(featured_targets)}")
    print(f"  - FAIL: {len([t for t in featured_targets if t['status']=='FAIL'])}")
    print(f"  - WARN: {len([t for t in featured_targets if t['status']=='WARN'])}")
    print("")
    print("Quick Win Opportunity:")
    quick_wins = [t for t in refinement_targets if t["is_featured"] and t["status"] == "WARN"]
    print(f"  - Featured WARN files: {len(quick_wins)}")
    print(f"  - Expected improvement: +4-5 percentage points")
    print(f"  - Estimated effort: 2-3 hours")
    print("="*60)
    print(f"\nPriority list saved to: {output_path}")


if __name__ == "__main__":
    main()



