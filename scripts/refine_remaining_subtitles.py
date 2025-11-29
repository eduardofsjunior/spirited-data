#!/usr/bin/env python3
"""
Refine remaining WARN and FAIL files (Story 4.X.5 final push).

Prioritizes WARN files (closer to threshold) for quick wins.

Usage:
    python scripts/refine_remaining_subtitles.py --limit 10  # Test with 10 files
    python scripts/refine_remaining_subtitles.py             # Refine all remaining
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

from refine_fail_subtitles import refine_subtitle
from fetch_priority_subtitles import login_and_get_token


def main():
    """Main execution."""
    parser = argparse.ArgumentParser(description="Refine remaining WARN/FAIL subtitle files")
    parser.add_argument("--limit", type=int, help="Limit number of files to refine")
    parser.add_argument("--warn-only", action="store_true", help="Only refine WARN files (skip FAIL)")
    args = parser.parse_args()
    
    # Get credentials
    api_key = os.getenv("OPEN_SUBTITLES_API_KEY")
    username = os.getenv("OPEN_SUBTITLES_USERNAME")
    password = os.getenv("OPEN_SUBTITLES_PASSWORD")
    
    if not all([api_key, username, password]):
        print("ERROR: Missing OpenSubtitles credentials")
        return 1
    
    # Login
    token = login_and_get_token(api_key, username, password)
    if not token:
        print("ERROR: Authentication failed")
        return 1
    
    # Load targets
    with open("data/metadata/remaining_improvements.json", "r") as f:
        data = json.load(f)
    
    targets = data["targets"]
    
    # Filter if warn-only
    if args.warn_only:
        targets = [t for t in targets if t["status"] == "WARN"]
        print(f"Mode: WARN-only (skipping FAIL files)")
    
    # Apply limit
    if args.limit:
        targets = targets[:args.limit]
    
    print("="*60)
    print("SUBTITLE REFINEMENT (WARN + FAIL)")
    print("="*60)
    print(f"Files to refine: {len(targets)}")
    print(f"  WARN: {len([t for t in targets if t['status'] == 'WARN'])}")
    print(f"  FAIL: {len([t for t in targets if t['status'] == 'FAIL'])}")
    print("="*60)
    
    # Refine each file
    results = []
    improved_count = 0
    warn_to_pass = 0
    fail_to_pass = 0
    fail_to_warn = 0
    
    for idx, target in enumerate(targets, 1):
        status_emoji = "⚠️" if target["status"] == "WARN" else "❌"
        print(f"\n[{idx}/{len(targets)}] {status_emoji}")
        
        result = refine_subtitle(
            api_key, token,
            target["film_slug"],
            target["language"],
            target.get("current_drift", 0) or 0
        )
        
        result["film_slug"] = target["film_slug"]
        result["language"] = target["language"]
        result["old_status"] = target["status"]
        
        # Determine new status
        if result.get("success"):
            new_drift = result["new_drift"]
            if new_drift < 2.0:
                result["new_status"] = "PASS"
                improved_count += 1
                if target["status"] == "WARN":
                    warn_to_pass += 1
                else:
                    fail_to_pass += 1
            elif new_drift < 5.0:
                result["new_status"] = "WARN"
                if target["status"] == "FAIL":
                    fail_to_warn += 1
                    improved_count += 1
            else:
                result["new_status"] = "FAIL"
        else:
            result["new_status"] = target["status"]
        
        results.append(result)
        time.sleep(2)  # Rate limiting
    
    # Summary
    print("\n" + "="*60)
    print("REFINEMENT SUMMARY")
    print("="*60)
    print(f"Files processed: {len(results)}")
    print(f"Successfully improved: {improved_count}")
    print(f"No improvement found: {len(results) - improved_count}")
    print("")
    print("Quality Improvements:")
    print(f"  WARN → PASS: {warn_to_pass}")
    print(f"  FAIL → PASS: {fail_to_pass}")
    print(f"  FAIL → WARN: {fail_to_warn}")
    print("")
    print(f"Overall success rate: {improved_count}/{len(results)} ({100*improved_count/len(results):.1f}%)")
    print("="*60)
    
    # Save results
    output = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "targets_processed": len(results),
        "improved_count": improved_count,
        "warn_to_pass": warn_to_pass,
        "fail_to_pass": fail_to_pass,
        "fail_to_warn": fail_to_warn,
        "success_rate": 100 * improved_count / len(results) if results else 0,
        "results": results,
    }
    
    with open("data/metadata/remaining_refinement_results.json", "w") as f:
        json.dump(output, f, indent=2)
    
    print(f"\nResults saved to: data/metadata/remaining_refinement_results.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())

