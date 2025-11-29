#!/usr/bin/env python3
"""
Final refinement for Pom Poko to achieve 100% pass rate.

Current status: WARN (6.7% drift)
Target runtime: 119 min (7140s)
Current runtime: 111.1 min (6663s)

Goal: Find subtitle with <5% drift to achieve PASS status.
"""

import os
import sys
import time
import json
from pathlib import Path
from typing import List, Dict, Optional, Any
import zipfile
import io
import tempfile

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

# Target film
FILM = {
    "title": "Pom Poko",
    "year": 1994,
    "slug": "pom_poko",
    "target_runtime_seconds": 7140,
    "target_runtime_display": "119 min (1h 59m)",
    "current_drift": 6.7,
    "current_runtime": 6663,
}


def get_credentials():
    """Get API credentials from environment."""
    api_key = os.getenv("OPEN_SUBTITLES_API_KEY")
    username = os.getenv("OPEN_SUBTITLES_USERNAME")
    password = os.getenv("OPEN_SUBTITLES_PASSWORD")

    if not api_key or not username or not password:
        print("ERROR: API credentials not set")
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
) -> List[Dict[str, Any]]:
    """Search for ALL available subtitles."""
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
            return results
        else:
            print(f"  ✗ Search failed: {response.status_code}")
            return []
    except Exception as e:
        print(f"  ✗ Search error: {e}")
        return []


def calculate_subtitle_runtime(file_content: bytes) -> Optional[float]:
    """Parse subtitle content and calculate runtime in seconds."""
    try:
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
        return None


def main():
    """Main execution."""
    print("="*70)
    print("Final Refinement: Pom Poko - Achieving 100% Pass Rate")
    print("="*70)
    print(f"Target Runtime: {FILM['target_runtime_display']} ({FILM['target_runtime_seconds']}s)")
    print(f"Current Runtime: {FILM['current_runtime']/60:.1f} min ({FILM['current_runtime']}s)")
    print(f"Current Drift: {FILM['current_drift']}% (WARN status)")
    print(f"Goal: Find subtitle with <5% drift for PASS status")
    print("="*70)

    # Setup
    api_key, username, password = get_credentials()

    # Login
    token = login_and_get_token(api_key, username, password)
    if not token:
        return 1

    # Search all subtitles
    results = search_all_subtitles(api_key, token, FILM['title'], FILM['year'])

    if not results:
        print("\n✗ No results found")
        return 1

    print(f"\n  → Analyzing {len(results)} candidates for runtime match...")
    print(f"  → Looking for runtime close to {FILM['target_runtime_seconds']/60:.1f} min")

    candidates = []
    target_runtime = FILM['target_runtime_seconds']

    # Check each candidate
    for idx, result in enumerate(results):
        attrs = result.get("attributes", {})
        files = attrs.get("files", [{}])
        file_id = files[0].get("file_id")

        if not file_id:
            continue

        print(f"\n  [{idx+1}/{len(results)}] Checking file_id: {file_id}...", end=" ")

        content = download_subtitle(api_key, token, file_id)
        if not content:
            print("❌ download failed")
            time.sleep(1)
            continue

        runtime = calculate_subtitle_runtime(content)
        if not runtime:
            print("❌ parse failed")
            time.sleep(1)
            continue

        drift_percent = abs(runtime - target_runtime) / target_runtime * 100

        uploader = attrs.get("uploader", {}).get("name", "unknown")
        download_count = attrs.get("download_count", 0)

        print(f"Runtime: {runtime/60:.1f}min ({runtime:.0f}s), Drift: {drift_percent:.2f}%", end=" ")

        if drift_percent < 5.0:
            print("✅ PASS!")
            candidates.append({
                "file_id": file_id,
                "runtime": runtime,
                "drift_percent": drift_percent,
                "uploader": uploader,
                "download_count": download_count,
                "content": content,
                "status": "PASS"
            })
        elif drift_percent < FILM['current_drift']:
            print("🟡 Better than current")
            candidates.append({
                "file_id": file_id,
                "runtime": runtime,
                "drift_percent": drift_percent,
                "uploader": uploader,
                "download_count": download_count,
                "content": content,
                "status": "BETTER"
            })
        else:
            print("❌ worse")

        time.sleep(1)

        # Stop early if we found a perfect match
        if drift_percent <= 1.0:
            print(f"\n  ✓ Found excellent match (<1% drift), stopping search")
            break

        # Limit to 30 candidates
        if idx >= 29:
            print(f"\n  ⚠️  Reached limit of 30 candidates")
            break

    if not candidates:
        print("\n✗ No better match found")
        return 1

    # Sort by drift (best match first)
    candidates.sort(key=lambda x: x["drift_percent"])
    best = candidates[0]

    print("\n" + "="*70)
    print("BEST MATCH FOUND:")
    print("="*70)
    print(f"  File ID: {best['file_id']}")
    print(f"  Runtime: {best['runtime']/60:.1f} min ({best['runtime']:.0f}s)")
    print(f"  Drift: {best['drift_percent']:.2f}%")
    print(f"  Status: {best['status']}")
    print(f"  Uploader: {best['uploader']}")
    print(f"  Downloads: {best['download_count']:,}")
    print("="*70)

    if best['drift_percent'] < FILM['current_drift']:
        # Save improved subtitle
        output_path = OUTPUT_DIR / f"{FILM['slug']}_en_v2.srt"

        # Backup old version
        if output_path.exists():
            backup_path = OUTPUT_DIR / f"{FILM['slug']}_en_v2_old.srt"
            output_path.rename(backup_path)
            print(f"\n  → Backed up old version to: {backup_path}")

        # Save new version
        output_path.write_bytes(best['content'])
        print(f"  ✓ Saved improved subtitle to: {output_path}")

        # Show improvement
        improvement = FILM['current_drift'] - best['drift_percent']
        print(f"\n  📊 Improvement: {FILM['current_drift']:.1f}% → {best['drift_percent']:.2f}% (reduced by {improvement:.2f}%)")

        if best['status'] == 'PASS':
            print(f"  ✅ STATUS: WARN → PASS (100% pass rate achieved!)")
        else:
            print(f"  🟡 STATUS: Still WARN but improved")

        return 0
    else:
        print(f"\n  ✗ No better match found (keeping current version)")
        return 1


if __name__ == "__main__":
    sys.exit(main())
