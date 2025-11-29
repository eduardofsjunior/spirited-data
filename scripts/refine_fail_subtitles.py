#!/usr/bin/env python3
"""
Refine FAIL subtitle files using runtime-based search (Story 4.X.6).

Applies Phase 1 refinement methodology: prioritize runtime accuracy over download count.

Usage:
    python scripts/refine_fail_subtitles.py --limit 5  # Test with 5 worst files
    python scripts/refine_fail_subtitles.py            # Refine all 27 FAIL files
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import List, Dict, Optional, Any
import pysrt

sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    import requests
except ImportError:
    print("ERROR: requests not found")
    sys.exit(1)

# Configuration
API_BASE_URL = "https://api.opensubtitles.com/api/v1"
OUTPUT_DIR = Path("data/raw/subtitles_improved")

FILM_METADATA = {
    "spirited_away": {"title": "Spirited Away", "year": 2001, "runtime": 7500},
    "princess_mononoke": {"title": "Princess Mononoke", "year": 1997, "runtime": 8100},
    "howls_moving_castle": {"title": "Howl's Moving Castle", "year": 2004, "runtime": 7140},
    "kikis_delivery_service": {"title": "Kiki's Delivery Service", "year": 1989, "runtime": 6180},
    "my_neighbor_totoro": {"title": "My Neighbor Totoro", "year": 1988, "runtime": 5160},
    "castle_in_the_sky": {"title": "Castle in the Sky", "year": 1986, "runtime": 7440},
    "earwig_and_the_witch": {"title": "Earwig and the Witch", "year": 2020, "runtime": 4920},
    "ponyo": {"title": "Ponyo", "year": 2008, "runtime": 6060},
    "the_red_turtle": {"title": "The Red Turtle", "year": 2016, "runtime": 4800},
    "grave_of_the_fireflies": {"title": "Grave of the Fireflies", "year": 1988, "runtime": 5340},
    "whisper_of_the_heart": {"title": "Whisper of the Heart", "year": 1995, "runtime": 6660},
    "only_yesterday": {"title": "Only Yesterday", "year": 1991, "runtime": 7080},
    "pom_poko": {"title": "Pom Poko", "year": 1994, "runtime": 7140},
    "the_wind_rises": {"title": "The Wind Rises", "year": 2013, "runtime": 7560},
    "arrietty": {"title": "Arrietty", "year": 2010, "runtime": 5640},
    "from_up_on_poppy_hill": {"title": "From Up on Poppy Hill", "year": 2011, "runtime": 5520},
    "my_neighbors_the_yamadas": {"title": "My Neighbors the Yamadas", "year": 1999, "runtime": 6240},
    "porco_rosso": {"title": "Porco Rosso", "year": 1992, "runtime": 5640},
    "tales_from_earthsea": {"title": "Tales from Earthsea", "year": 2006, "runtime": 6900},
    "the_cat_returns": {"title": "The Cat Returns", "year": 2002, "runtime": 4500},
    "the_tale_of_the_princess_kaguya": {"title": "The Tale of the Princess Kaguya", "year": 2013, "runtime": 8220},
    "when_marnie_was_there": {"title": "When Marnie Was There", "year": 2014, "runtime": 6180},
}

LANGUAGE_TARGETS = {
    "en": ["en", "eng"],
    "fr": ["fr", "fre"],
    "es": ["es", "spa"],
    "nl": ["nl", "dut"],
    "ar": ["ar", "ara"],
}


def get_subtitle_runtime(content: bytes) -> Optional[float]:
    """Extract runtime from subtitle file content."""
    try:
        # Write to temp file
        temp_path = Path("/tmp/temp_subtitle.srt")
        temp_path.write_bytes(content)
        
        # Parse with pysrt
        subs = pysrt.open(str(temp_path), encoding='utf-8')
        
        if not subs:
            return None
        
        # Get last subtitle end time (in seconds)
        last_sub = subs[-1]
        runtime = (last_sub.end.hours * 3600 + 
                  last_sub.end.minutes * 60 + 
                  last_sub.end.seconds + 
                  last_sub.end.milliseconds / 1000.0)
        
        temp_path.unlink()
        return runtime
    except Exception as e:
        print(f"    ✗ Could not parse subtitle runtime: {e}")
        return None


def search_and_rank_by_runtime(
    api_key: str, token: str, film_title: str, film_year: int,
    lang_codes: List[str], target_runtime: float
) -> List[Dict]:
    """Search subtitles and rank by runtime accuracy."""
    url = f"{API_BASE_URL}/subtitles"
    headers = {
        "Api-Key": api_key,
        "Authorization": f"Bearer {token}",
        "User-Agent": "EduardoSubsFetcher/2.0",
    }
    
    params = {
        "query": film_title,
        "year": film_year,
        "languages": ",".join(lang_codes),
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        if response.status_code != 200:
            return []
        
        results = response.json().get("data", [])
        return results
    
    except Exception as e:
        print(f"    ✗ Search error: {e}")
        return []


def download_and_test_subtitle(
    api_key: str, token: str, file_id: int, target_runtime: float
) -> Optional[tuple]:
    """Download subtitle and test runtime match. Returns (content, runtime, drift)."""
    url = f"{API_BASE_URL}/download"
    headers = {
        "Api-Key": api_key,
        "Authorization": f"Bearer {token}",
        "User-Agent": "EduardoSubsFetcher/2.0",
    }
    
    try:
        response = requests.post(url, headers=headers, json={"file_id": file_id}, timeout=60)
        
        if response.status_code != 200:
            return None
        
        download_url = response.json().get("link")
        if not download_url:
            return None
        
        file_response = requests.get(download_url, timeout=60)
        if file_response.status_code != 200:
            return None
        
        content = file_response.content
        
        # Handle ZIP files
        if content[:2] == b"PK":
            import zipfile
            import io
            try:
                with zipfile.ZipFile(io.BytesIO(content)) as zf:
                    srt_files = [f for f in zf.namelist() if f.endswith(".srt")]
                    if srt_files:
                        content = zf.read(srt_files[0])
            except:
                return None
        
        # Get runtime
        runtime = get_subtitle_runtime(content)
        if not runtime:
            return None
        
        # Calculate drift
        drift = abs(runtime - target_runtime) / target_runtime * 100
        
        return (content, runtime, drift)
    
    except Exception as e:
        return None


def refine_subtitle(
    api_key: str, token: str, film_slug: str, language: str,
    current_drift: float
) -> Dict[str, Any]:
    """Refine a single FAIL subtitle file."""
    
    if film_slug not in FILM_METADATA:
        return {"success": False, "reason": "No film metadata"}
    
    meta = FILM_METADATA[film_slug]
    film_title = meta["title"]
    film_year = meta["year"]
    target_runtime = meta["runtime"]
    
    print(f"\n{'='*60}")
    print(f"Refining: {film_title} ({language.upper()})")
    print(f"Current drift: {current_drift:.1f}%")
    print(f"Target runtime: {target_runtime}s ({target_runtime/60:.1f} min)")
    print(f"{'='*60}")
    
    # Search for subtitles
    lang_codes = LANGUAGE_TARGETS.get(language, [language])
    print(f"  → Searching for alternatives...")
    results = search_and_rank_by_runtime(
        api_key, token, film_title, film_year, lang_codes, target_runtime
    )
    
    if not results:
        print(f"  ✗ No results found")
        return {"success": False, "reason": "No results"}
    
    print(f"  ✓ Found {len(results)} candidates")
    print(f"  → Testing runtime accuracy for top candidates...")
    
    # Test up to 10 candidates
    best_match = None
    best_drift = current_drift
    tested_count = 0
    
    for idx, result in enumerate(results[:10], 1):
        attrs = result.get("attributes", {})
        files = attrs.get("files", [{}])
        file_id = files[0].get("file_id")
        
        if not file_id:
            continue
        
        print(f"    Testing candidate {idx}/{min(10, len(results))} (file_id: {file_id})...", end="")
        
        test_result = download_and_test_subtitle(api_key, token, file_id, target_runtime)
        tested_count += 1
        
        if test_result:
            content, runtime, drift = test_result
            print(f" runtime={runtime:.1f}s, drift={drift:.2f}%")
            
            if drift < best_drift:
                best_drift = drift
                best_match = {
                    "content": content,
                    "runtime": runtime,
                    "drift": drift,
                    "file_id": file_id,
                    "uploader": attrs.get("uploader", {}).get("name", "unknown"),
                    "downloads": attrs.get("download_count", 0),
                }
                print(f"      ✓ NEW BEST: {drift:.2f}% drift")
        else:
            print(f" ✗ failed")
        
        time.sleep(0.5)  # Rate limiting
    
    if best_match and best_match["drift"] < current_drift:
        # Save improved file
        output_path = OUTPUT_DIR / f"{film_slug}_{language}_v2.srt"
        output_path.write_bytes(best_match["content"])
        
        improvement = current_drift - best_match["drift"]
        print(f"\n  ✓ IMPROVED: {current_drift:.1f}% → {best_match['drift']:.2f}% (-{improvement:.1f} points)")
        print(f"  ✓ Saved to: {output_path}")
        
        return {
            "success": True,
            "old_drift": current_drift,
            "new_drift": best_match["drift"],
            "improvement": improvement,
            "file_id": best_match["file_id"],
            "runtime": best_match["runtime"],
        }
    else:
        print(f"\n  ✗ No better match found (tested {tested_count} candidates)")
        return {"success": False, "reason": f"No improvement after {tested_count} tests"}


def main():
    """Main execution."""
    parser = argparse.ArgumentParser(description="Refine FAIL subtitle files")
    parser.add_argument("--limit", type=int, help="Limit number of files to refine (for testing)")
    args = parser.parse_args()
    
    # Get credentials
    api_key = os.getenv("OPEN_SUBTITLES_API_KEY")
    username = os.getenv("OPEN_SUBTITLES_USERNAME")
    password = os.getenv("OPEN_SUBTITLES_PASSWORD")
    
    if not all([api_key, username, password]):
        print("ERROR: Missing OpenSubtitles credentials")
        return 1
    
    # Login
    from fetch_priority_subtitles import login_and_get_token
    sys.path.insert(0, str(Path(__file__).parent))
    token = login_and_get_token(api_key, username, password)
    
    if not token:
        print("ERROR: Authentication failed")
        return 1
    
    # Load FAIL targets
    with open("data/metadata/fail_files_for_refinement.json", "r") as f:
        data = json.load(f)
    
    targets = data["targets"]
    
    # Sort by drift (worst first)
    targets_sorted = sorted(targets, key=lambda t: t.get("current_drift", 0) or 0, reverse=True)
    
    if args.limit:
        targets_sorted = targets_sorted[:args.limit]
    
    print("="*60)
    print("SUBTITLE REFINEMENT")
    print("="*60)
    print(f"Total FAIL files to refine: {len(targets_sorted)}")
    print("="*60)
    
    # Refine each file
    results = []
    improved_count = 0
    
    for idx, target in enumerate(targets_sorted, 1):
        print(f"\n[{idx}/{len(targets_sorted)}]")
        
        result = refine_subtitle(
            api_key, token,
            target["film_slug"],
            target["language"],
            target.get("current_drift", 0) or 0
        )
        
        result["film_slug"] = target["film_slug"]
        result["language"] = target["language"]
        results.append(result)
        
        if result.get("success"):
            improved_count += 1
        
        time.sleep(2)  # Rate limiting
    
    # Summary
    print("\n" + "="*60)
    print("REFINEMENT SUMMARY")
    print("="*60)
    print(f"Files processed: {len(results)}")
    print(f"Successfully improved: {improved_count}")
    print(f"No improvement found: {len(results) - improved_count}")
    print(f"Success rate: {improved_count}/{len(results)} ({100*improved_count/len(results):.1f}%)")
    print("="*60)
    
    # Save results
    output = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "targets_processed": len(results),
        "improved_count": improved_count,
        "success_rate": 100 * improved_count / len(results) if results else 0,
        "results": results,
    }
    
    with open("data/metadata/refinement_results.json", "w") as f:
        json.dump(output, f, indent=2)
    
    print(f"\nResults saved to: data/metadata/refinement_results.json")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())



