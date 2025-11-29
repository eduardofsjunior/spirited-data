#!/usr/bin/env python3
"""
Refine failing subtitle files by searching for better runtime matches.

Targets the 3 films that failed validation in Story 4.X.2 and attempts
to find subtitles that better match documented runtimes.

Failed films:
- Earwig and the Witch: Need ~82 min (4920s), currently have 96.8 min (17.9% drift)
- The Red Turtle: Need ~80 min (4800s), currently have 104.6 min (30.8% drift)
- The Wind Rises: Need ~126 min (7560s), currently have 84.2 min (33.2% drift)
"""

import os
import sys
import time
import json
from pathlib import Path
from typing import List, Dict, Optional, Any
import zipfile
import io

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    import requests
    import pysrt
except ImportError:
    print("ERROR: Missing dependencies. Install with: pip install requests pysrt")
    sys.exit(1)


# Configuration
API_BASE_URL = "https://api.opensubtitles.com/api/v1"
OUTPUT_DIR = Path("data/raw/subtitles_improved")
METADATA_DIR = Path("data/metadata")

# Films that failed validation with target runtimes
FAILING_FILMS = [
    {
        "title": "Earwig and the Witch",
        "year": 2020,
        "slug": "earwig_and_the_witch",
        "target_runtime_seconds": 4920,
        "target_runtime_display": "82 min",
        "current_drift": 17.9,
    },
    {
        "title": "The Red Turtle",
        "year": 2016,
        "slug": "the_red_turtle",
        "target_runtime_seconds": 4800,
        "target_runtime_display": "80 min",
        "current_drift": 30.8,
    },
    {
        "title": "The Wind Rises",
        "year": 2013,
        "slug": "the_wind_rises",
        "target_runtime_seconds": 7560,
        "target_runtime_display": "126 min",
        "current_drift": 33.2,
    },
]


def get_credentials():
    """Get API credentials from environment."""
    api_key = os.getenv("OPEN_SUBTITLES_API_KEY")
    username = os.getenv("OPEN_SUBTITLES_USERNAME")
    password = os.getenv("OPEN_SUBTITLES_PASSWORD")

    if not api_key:
        print("ERROR: OPEN_SUBTITLES_API_KEY not set")
        sys.exit(1)
    if not username or not password:
        print("ERROR: OPEN_SUBTITLES_USERNAME and OPEN_SUBTITLES_PASSWORD required")
        sys.exit(1)

    return api_key, username, password


def login_and_get_token(api_key: str, username: str, password: str) -> Optional[str]:
    """Login to OpenSubtitles API."""
    url = f"{API_BASE_URL}/login"
    headers = {
        "Api-Key": api_key,
        "User-Agent": "EduardoSubsFetcher/2.0",
        "Content-Type": "application/json",
    }

    payload = {"username": username, "password": password}

    try:
        print("  → Authenticating...")
        response = requests.post(url, headers=headers, json=payload, timeout=30)

        if response.status_code == 200:
            token = response.json().get("token")
            if token:
                print("  ✓ Authenticated")
                return token

        print(f"  ✗ Login failed: {response.status_code}")
        return None
    except Exception as e:
        print(f"  ✗ Login error: {e}")
        return None


def search_all_subtitles(
    api_key: str,
    token: str,
    film_title: str,
    film_year: int,
    max_results: int = 100
) -> List[Dict[str, Any]]:
    """
    Search for ALL available subtitles for a film.

    Returns up to max_results subtitles sorted by download count.
    """
    url = f"{API_BASE_URL}/subtitles"
    headers = {
        "Api-Key": api_key,
        "Authorization": f"Bearer {token}",
        "User-Agent": "EduardoSubsFetcher/2.0",
    }

    params = {
        "query": film_title,
        "year": film_year,
        "languages": "en",
        "order_by": "download_count",
        "order_direction": "desc",
    }

    try:
        print(f"  → Searching for {film_title} ({film_year})...")
        response = requests.get(url, headers=headers, params=params, timeout=30)

        if response.status_code == 200:
            results = response.json().get("data", [])
            print(f"  ✓ Found {len(results)} total results")
            return results[:max_results]
        else:
            print(f"  ✗ Search failed: {response.status_code}")
            return []
    except Exception as e:
        print(f"  ✗ Search error: {e}")
        return []


def calculate_subtitle_runtime(file_content: bytes) -> Optional[float]:
    """
    Parse subtitle content and calculate runtime in seconds.

    Returns last subtitle timestamp or None on error.
    """
    try:
        # Save to temp file for pysrt
        import tempfile
        with tempfile.NamedTemporaryFile(mode='wb', suffix='.srt', delete=False) as f:
            f.write(file_content)
            temp_path = f.name

        subs = pysrt.open(temp_path, encoding="utf-8")
        os.unlink(temp_path)

        if not subs:
            return None

        last_sub = subs[-1]
        runtime = (
            last_sub.end.hours * 3600 +
            last_sub.end.minutes * 60 +
            last_sub.end.seconds +
            last_sub.end.milliseconds / 1000.0
        )

        return runtime
    except Exception as e:
        print(f"    ⚠️  Parse error: {e}")
        return None


def download_subtitle(api_key: str, token: str, file_id: int) -> Optional[bytes]:
    """Download subtitle file content."""
    url = f"{API_BASE_URL}/download"
    headers = {
        "Api-Key": api_key,
        "Authorization": f"Bearer {token}",
        "User-Agent": "EduardoSubsFetcher/2.0",
    }

    payload = {"file_id": file_id}

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=60)

        if response.status_code == 200:
            download_url = response.json().get("link")
            if not download_url:
                return None

            file_response = requests.get(download_url, timeout=60)
            if file_response.status_code == 200:
                content = file_response.content

                # Handle ZIP files
                if content[:2] == b"PK":
                    with zipfile.ZipFile(io.BytesIO(content)) as zf:
                        srt_files = [f for f in zf.namelist() if f.endswith(".srt")]
                        if srt_files:
                            content = zf.read(srt_files[0])
                        else:
                            return None

                return content

        return None
    except Exception as e:
        print(f"    ⚠️  Download error: {e}")
        return None


def find_best_runtime_match(
    api_key: str,
    token: str,
    film_title: str,
    film_year: int,
    target_runtime: float,
    tolerance_percent: float = 5.0
) -> Optional[Dict[str, Any]]:
    """
    Search all subtitles and find the one with runtime closest to target.

    Args:
        tolerance_percent: Maximum acceptable drift percentage

    Returns:
        Dict with file_id, runtime, drift, etc. or None if no good match
    """
    results = search_all_subtitles(api_key, token, film_title, film_year)

    if not results:
        return None

    print(f"  → Analyzing {len(results)} candidates for runtime match...")
    print(f"  → Target runtime: {target_runtime/60:.1f} min ({target_runtime}s)")

    candidates = []

    for idx, result in enumerate(results):
        attrs = result.get("attributes", {})
        files = attrs.get("files", [{}])
        file_id = files[0].get("file_id")

        if not file_id:
            continue

        # Download and check runtime
        print(f"    Checking candidate {idx+1}/{len(results)} (file_id: {file_id})...", end=" ")

        content = download_subtitle(api_key, token, file_id)
        if not content:
            print("❌ download failed")
            continue

        runtime = calculate_subtitle_runtime(content)
        if not runtime:
            print("❌ parse failed")
            continue

        drift_percent = abs(runtime - target_runtime) / target_runtime * 100

        print(f"Runtime: {runtime/60:.1f}min ({drift_percent:.1f}% drift)", end=" ")

        if drift_percent <= tolerance_percent:
            print("✅ GOOD MATCH!")
        else:
            print("❌ too much drift")

        uploader = attrs.get("uploader", {}).get("name", "unknown")
        download_count = attrs.get("download_count", 0)

        candidates.append({
            "file_id": file_id,
            "runtime": runtime,
            "drift_percent": drift_percent,
            "uploader": uploader,
            "download_count": download_count,
            "content": content,
        })

        # Rate limiting
        time.sleep(1)

        # Stop early if we found a perfect match
        if drift_percent <= 1.0:
            print(f"  ✓ Found excellent match, stopping search")
            break

        # Limit to first 20 candidates to avoid API abuse
        if idx >= 19:
            print(f"  ⚠️  Reached limit of 20 candidates")
            break

    if not candidates:
        print("  ✗ No valid candidates found")
        return None

    # Sort by drift (best match first)
    candidates.sort(key=lambda x: x["drift_percent"])
    best = candidates[0]

    print("\n  " + "="*60)
    print(f"  BEST MATCH:")
    print(f"  - File ID: {best['file_id']}")
    print(f"  - Runtime: {best['runtime']/60:.1f} min ({best['runtime']:.1f}s)")
    print(f"  - Drift: {best['drift_percent']:.2f}%")
    print(f"  - Uploader: {best['uploader']}")
    print(f"  - Downloads: {best['download_count']:,}")
    print("  " + "="*60)

    return best


def main():
    """Main execution."""
    print("="*60)
    print("Refine Failing Subtitles - Better Runtime Matches")
    print("Story 4.X.2 - Improvement Iteration")
    print("="*60)

    # Setup
    api_key, username, password = get_credentials()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Login
    token = login_and_get_token(api_key, username, password)
    if not token:
        return 1

    # Process each failing film
    results = []

    for film in FAILING_FILMS:
        print(f"\n{'#'*60}")
        print(f"Film: {film['title']} ({film['year']})")
        print(f"Target Runtime: {film['target_runtime_display']}")
        print(f"Current Drift: {film['current_drift']}%")
        print(f"{'#'*60}\n")

        best_match = find_best_runtime_match(
            api_key,
            token,
            film['title'],
            film['year'],
            film['target_runtime_seconds'],
            tolerance_percent=5.0
        )

        if best_match and best_match['drift_percent'] < film['current_drift']:
            # Save improved subtitle
            output_path = OUTPUT_DIR / f"{film['slug']}_en_v2.srt"

            # Backup old version
            if output_path.exists():
                backup_path = OUTPUT_DIR / f"{film['slug']}_en_v2_old.srt"
                output_path.rename(backup_path)
                print(f"\n  → Backed up old version to: {backup_path}")

            # Save new version
            output_path.write_bytes(best_match['content'])
            print(f"  ✓ Saved improved subtitle to: {output_path}")

            results.append({
                "film": film['title'],
                "slug": film['slug'],
                "improved": True,
                "old_drift": film['current_drift'],
                "new_drift": best_match['drift_percent'],
                "improvement": film['current_drift'] - best_match['drift_percent'],
                "file_id": best_match['file_id'],
                "uploader": best_match['uploader'],
            })
        else:
            print(f"\n  ✗ No better match found (keeping current version)")
            results.append({
                "film": film['title'],
                "slug": film['slug'],
                "improved": False,
                "old_drift": film['current_drift'],
            })

    # Summary
    print("\n" + "="*60)
    print("REFINEMENT SUMMARY")
    print("="*60)

    improved_count = sum(1 for r in results if r.get("improved", False))
    print(f"Films improved: {improved_count}/3")

    for result in results:
        if result.get("improved"):
            print(f"\n✅ {result['film']}")
            print(f"   Old drift: {result['old_drift']:.1f}%")
            print(f"   New drift: {result['new_drift']:.1f}%")
            print(f"   Improvement: -{result['improvement']:.1f}%")
        else:
            print(f"\n❌ {result['film']} - No improvement found")

    # Save metadata
    metadata_path = METADATA_DIR / "refinement_log.json"
    with open(metadata_path, "w") as f:
        json.dump({
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "films_processed": len(FAILING_FILMS),
            "films_improved": improved_count,
            "results": results,
        }, f, indent=2)

    print(f"\nMetadata saved to: {metadata_path}")
    print("="*60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
