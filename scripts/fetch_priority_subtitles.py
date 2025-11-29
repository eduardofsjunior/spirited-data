#!/usr/bin/env python3
"""
OpenSubtitles API subtitle fetcher for PRIORITY FILMS (Story 4.X.2).

Modified from fetch_subtitles_test.py to:
1. Target only high-priority films from Story 4.X.1
2. Output to data/raw/subtitles_improved/ directory
3. Add _v2 suffix to filenames for version tracking
4. Focus on English subtitles first (highest portfolio impact)

Usage:
    # Fetch English subtitles for priority films
    python scripts/fetch_priority_subtitles.py --languages en

    # Fetch multiple languages
    python scripts/fetch_priority_subtitles.py --languages en fr es
"""

import os
import sys
import time
import json
import argparse
from pathlib import Path
from typing import List, Dict, Optional, Any
import zipfile
import io

# Add project root to path for imports
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    import requests
except ImportError:
    print("ERROR: requests library not found. Install with: pip install requests")
    sys.exit(1)


# Configuration
API_BASE_URL = "https://api.opensubtitles.com/api/v1"
OUTPUT_DIR = Path("data/raw/subtitles_improved")  # MODIFIED: Output to improved directory
METADATA_DIR = Path("data/metadata")

# Complete Ghibli film metadata for all 22 films
FILM_METADATA = {
    "spirited_away": {"title": "Spirited Away", "year": 2001},
    "princess_mononoke": {"title": "Princess Mononoke", "year": 1997},
    "howls_moving_castle": {"title": "Howl's Moving Castle", "year": 2004},
    "kikis_delivery_service": {"title": "Kiki's Delivery Service", "year": 1989},
    "my_neighbor_totoro": {"title": "My Neighbor Totoro", "year": 1988},
    "castle_in_the_sky": {"title": "Castle in the Sky", "year": 1986},
    "earwig_and_the_witch": {"title": "Earwig and the Witch", "year": 2020},
    "ponyo": {"title": "Ponyo", "year": 2008},
    "the_red_turtle": {"title": "The Red Turtle", "year": 2016},
    "grave_of_the_fireflies": {"title": "Grave of the Fireflies", "year": 1988},
    "whisper_of_the_heart": {"title": "Whisper of the Heart", "year": 1995},
    "only_yesterday": {"title": "Only Yesterday", "year": 1991},
    "pom_poko": {"title": "Pom Poko", "year": 1994},
    "the_wind_rises": {"title": "The Wind Rises", "year": 2013},
    "arrietty": {"title": "Arrietty", "year": 2010},
    "from_up_on_poppy_hill": {"title": "From Up on Poppy Hill", "year": 2011},
    "my_neighbors_the_yamadas": {"title": "My Neighbors the Yamadas", "year": 1999},
    "porco_rosso": {"title": "Porco Rosso", "year": 1992},
    "tales_from_earthsea": {"title": "Tales from Earthsea", "year": 2006},
    "the_cat_returns": {"title": "The Cat Returns", "year": 2002},
    "the_tale_of_the_princess_kaguya": {"title": "The Tale of the Princess Kaguya", "year": 2013},
    "when_marnie_was_there": {"title": "When Marnie Was There", "year": 2014},
}

# PRIORITY FILMS ONLY (14 high priority films from Story 4.X.1)
# Ordered by priority score (highest first)
PRIORITY_FILMS = [
    # Featured films (highest portfolio impact)
    {"title": "Spirited Away", "year": 2001, "slug": "spirited_away", "priority": 100},
    {"title": "Princess Mononoke", "year": 1997, "slug": "princess_mononoke", "priority": 85},
    {"title": "Howl's Moving Castle", "year": 2004, "slug": "howls_moving_castle", "priority": 80},
    {"title": "Kiki's Delivery Service", "year": 1989, "slug": "kikis_delivery_service", "priority": 80},
    {"title": "My Neighbor Totoro", "year": 1988, "slug": "my_neighbor_totoro", "priority": 50},

    # High priority non-featured
    {"title": "Castle in the Sky", "year": 1986, "slug": "castle_in_the_sky", "priority": 70},
    {"title": "Earwig and the Witch", "year": 2020, "slug": "earwig_and_the_witch", "priority": 70},
    {"title": "Ponyo", "year": 2008, "slug": "ponyo", "priority": 70},
    {"title": "The Red Turtle", "year": 2016, "slug": "the_red_turtle", "priority": 70},
    {"title": "Grave of the Fireflies", "year": 1988, "slug": "grave_of_the_fireflies", "priority": 70},
    {"title": "Whisper of the Heart", "year": 1995, "slug": "whisper_of_the_heart", "priority": 70},
    {"title": "Only Yesterday", "year": 1991, "slug": "only_yesterday", "priority": 50},
    {"title": "Pom Poko", "year": 1994, "slug": "pom_poko", "priority": 50},
    {"title": "The Wind Rises", "year": 2013, "slug": "the_wind_rises", "priority": 50},
]

# Language mapping: target_name -> [preferred_codes]
LANGUAGE_TARGETS = {
    "en": ["en", "eng"],           # English - PRIMARY TARGET
    "fr": ["fr", "fre"],           # French - secondary
    "es": ["es", "spa"],           # Spanish - secondary
    "nl": ["nl", "dut"],           # Dutch - NEW for Story 4.X.5
    "ar": ["ar", "ara"],           # Arabic - NEW for Story 4.X.5
}


def get_api_key() -> str:
    """Get API key from environment."""
    api_key = os.getenv("OPEN_SUBTITLES_API_KEY")
    if not api_key:
        print("ERROR: OPEN_SUBTITLES_API_KEY environment variable not set")
        print("Set it with: export OPEN_SUBTITLES_API_KEY='your_key_here'")
        sys.exit(1)
    return api_key


def get_credentials() -> tuple:
    """Get username and password from environment (if needed)."""
    username = os.getenv("OPEN_SUBTITLES_USERNAME", "")
    password = os.getenv("OPEN_SUBTITLES_PASSWORD", "")
    return username, password


def login_and_get_token(api_key: str, username: str = "", password: str = "") -> Optional[str]:
    """
    Login to OpenSubtitles API and get JWT token.

    Args:
        api_key: OpenSubtitles API key
        username: Optional username for API authentication
        password: Optional password for API authentication

    Returns:
        JWT token or None on failure
    """
    url = f"{API_BASE_URL}/login"
    headers = {
        "Api-Key": api_key,
        "User-Agent": "EduardoSubsFetcher/2.0",
        "Content-Type": "application/json",
    }

    # Build payload with username/password if provided
    payload = {}
    if username and password:
        payload = {"username": username, "password": password}
        print("  → Authenticating with API key + username/password...")
    else:
        print("  → Authenticating with API key only...")

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)

        if response.status_code == 200:
            data = response.json()
            token = data.get("token")
            if token:
                print(f"  ✓ Authenticated successfully")
                return token
            else:
                print(f"  ✗ No token in response: {data}")
                return None
        else:
            print(f"  ✗ Login failed: {response.status_code}")
            print(f"    Response: {response.text}")
            return None

    except requests.RequestException as e:
        print(f"  ✗ Login error: {e}")
        return None


def search_subs(
    api_key: str,
    token: str,
    film_title: str,
    film_year: int,
    lang_codes: List[str],
    max_retries: int = 3
) -> List[Dict[str, Any]]:
    """
    Search for subtitles with retry logic for rate limiting.

    Returns:
        List of subtitle results (empty if none found)
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
        "languages": ",".join(lang_codes),
        "order_by": "download_count",  # Quality filter: most downloaded first
        "order_direction": "desc",
    }

    for attempt in range(max_retries):
        try:
            print(f"  → Searching for {film_title} ({film_year}) - lang: {','.join(lang_codes)}...")
            response = requests.get(url, headers=headers, params=params, timeout=30)

            # Handle rate limiting
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 5))
                print(f"  ⚠️  Rate limited. Waiting {retry_after}s...")
                time.sleep(retry_after)
                continue

            if response.status_code == 200:
                data = response.json()
                results = data.get("data", [])
                print(f"  ✓ Found {len(results)} results")
                return results
            else:
                print(f"  ✗ Search failed: {response.status_code}")
                print(f"    Response: {response.text[:200]}")
                return []

        except requests.RequestException as e:
            print(f"  ✗ Search error (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff

    return []


def pick_best(results: List[Dict[str, Any]], target_lang: str) -> Optional[Dict[str, Any]]:
    """
    Pick best subtitle from search results based on quality filters.

    Quality criteria (from Story 4.X.2 AC1):
    - High download count (already sorted by API)
    - Verified uploaders preferred
    - Non-hearing impaired preferred
    - Exact language match

    Args:
        results: List of search results from API
        target_lang: Target language code

    Returns:
        Best subtitle dict or None
    """
    if not results:
        return None

    print(f"  → Filtering {len(results)} results for quality...")

    candidates = []

    for result in results:
        attrs = result.get("attributes", {})

        # Language matching
        lang = attrs.get("language")
        if not lang or lang.lower() not in [target_lang.lower(), f"{target_lang.lower()}g"]:
            continue

        # Japanese special filtering (exclude bilingual)
        if target_lang == "ja":
            feature_details = attrs.get("feature_details", {})
            movie_name = feature_details.get("movie_name", "").lower()
            if any(term in movie_name for term in ["dual", "bilingual", "eng+jpn", "jpn+eng"]):
                print(f"    ✗ Skipping bilingual Japanese subtitle")
                continue

        # Extract quality metrics
        files = attrs.get("files", [{}])
        file_id = files[0].get("file_id") if files else None

        if not file_id:
            continue

        uploader = attrs.get("uploader", {})
        uploader_name = uploader.get("name", "unknown")

        download_count = attrs.get("download_count", 0)
        hearing_impaired = attrs.get("hearing_impaired", False)

        # Score candidate
        score = download_count
        if not hearing_impaired:
            score += 10000  # Strongly prefer non-HI

        candidates.append({
            "result": result,
            "score": score,
            "uploader": uploader_name,
            "downloads": download_count,
            "hearing_impaired": hearing_impaired,
            "file_id": file_id,
        })

    if not candidates:
        print(f"  ✗ No suitable candidates after filtering")
        return None

    # Sort by score and pick best
    candidates.sort(key=lambda x: x["score"], reverse=True)
    best = candidates[0]

    print(f"  ✓ Selected subtitle:")
    print(f"    - Uploader: {best['uploader']}")
    print(f"    - Downloads: {best['downloads']:,}")
    print(f"    - Hearing Impaired: {'Yes' if best['hearing_impaired'] else 'No'}")
    print(f"    - File ID: {best['file_id']}")

    return best["result"]


def download_sub(api_key: str, token: str, file_id: int) -> Optional[bytes]:
    """
    Download subtitle file content.

    Args:
        api_key: API key
        token: JWT token
        file_id: Subtitle file ID

    Returns:
        File content as bytes or None on failure
    """
    url = f"{API_BASE_URL}/download"
    headers = {
        "Api-Key": api_key,
        "Authorization": f"Bearer {token}",
        "User-Agent": "EduardoSubsFetcher/2.0",
    }

    payload = {"file_id": file_id}

    try:
        print(f"  → Downloading file_id {file_id}...")
        response = requests.post(url, headers=headers, json=payload, timeout=60)

        if response.status_code == 200:
            data = response.json()
            download_url = data.get("link")

            if not download_url:
                print(f"  ✗ No download link in response")
                return None

            # Download actual file
            print(f"  → Fetching from download URL...")
            file_response = requests.get(download_url, timeout=60)

            if file_response.status_code == 200:
                content = file_response.content

                # Handle ZIP files
                if content[:2] == b"PK":  # ZIP magic bytes
                    print(f"  → Extracting from ZIP archive...")
                    try:
                        with zipfile.ZipFile(io.BytesIO(content)) as zf:
                            # Find .srt file
                            srt_files = [f for f in zf.namelist() if f.endswith(".srt")]
                            if srt_files:
                                content = zf.read(srt_files[0])
                                print(f"  ✓ Extracted: {srt_files[0]}")
                            else:
                                print(f"  ✗ No .srt file in ZIP")
                                return None
                    except zipfile.BadZipFile:
                        print(f"  ✗ Invalid ZIP file")
                        return None

                print(f"  ✓ Downloaded {len(content):,} bytes")
                return content
            else:
                print(f"  ✗ File download failed: {file_response.status_code}")
                return None

        else:
            print(f"  ✗ Download request failed: {response.status_code}")
            print(f"    Response: {response.text}")
            return None

    except requests.RequestException as e:
        print(f"  ✗ Download error: {e}")
        return None


def save_file(content: bytes, path: Path) -> bool:
    """Save content to file."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        print(f"  ✓ Saved to: {path}")
        return True
    except Exception as e:
        print(f"  ✗ Save error: {e}")
        return False


def fetch_subtitle_for_language(
    api_key: str,
    token: str,
    film_title: str,
    film_year: int,
    film_slug: str,
    lang_key: str,
    lang_codes: List[str]
) -> Optional[Dict[str, Any]]:
    """
    Fetch subtitle for a specific language.

    Returns metadata dict with keys: lang, path, file_id, size, uploader
    """
    # MODIFIED: Add _v2 suffix for version tracking
    target_path = OUTPUT_DIR / f"{film_slug}_{lang_key}_v2.srt"

    if target_path.exists():
        file_size = target_path.stat().st_size
        print(f"\n{'='*60}")
        print(f"Fetching {lang_key.upper()} subtitles for {film_title} ({film_year})...")
        print(f"{'='*60}")
        print(f"  ⚡ Already exists: {target_path} ({file_size:,} bytes) - SKIPPING")
        return {
            "film": film_title,
            "lang": lang_key,
            "path": str(target_path),
            "file_id": "cached",
            "size": file_size,
            "source_lang": lang_key,
            "uploader": "cached",
        }

    print(f"\n{'='*60}")
    print(f"Fetching {lang_key.upper()} subtitles for {film_title} ({film_year})...")
    print(f"{'='*60}")

    # Try each language code in preference order
    for lang_code in lang_codes:
        results = search_subs(api_key, token, film_title, film_year, [lang_code])

        if results:
            best = pick_best(results, lang_code)

            if best:
                attrs = best.get("attributes", {})
                files = attrs.get("files", [{}])
                file_id = files[0].get("file_id")
                uploader = attrs.get("uploader", {}).get("name", "unknown")

                if not file_id:
                    print(f"  ✗ No file_id found in result")
                    continue

                # Download
                content = download_sub(api_key, token, file_id)

                if content:
                    # Save with _v2 suffix
                    if save_file(content, target_path):
                        return {
                            "film": film_title,
                            "lang": lang_key,
                            "path": str(target_path),
                            "file_id": file_id,
                            "size": len(content),
                            "source_lang": lang_code,
                            "uploader": uploader,
                            "download_count": attrs.get("download_count", 0),
                        }

        # Delay between language code attempts
        time.sleep(1)

    print(f"  ✗ Failed to fetch {lang_key.upper()} subtitles")
    return None


def parse_batch_file(batch_file_path: Path) -> List[Dict[str, Any]]:
    """
    Parse multi-language priority list markdown file.
    
    Extracts film_slug and language targets from markdown table rows.
    
    Args:
        batch_file_path: Path to priority list markdown file
        
    Returns:
        List of dicts with keys: film_slug, film_title, language, priority_score
    """
    print(f"  → Parsing batch file: {batch_file_path}")
    
    if not batch_file_path.exists():
        print(f"  ✗ Batch file not found: {batch_file_path}")
        return []
    
    targets = []
    with open(batch_file_path, "r") as f:
        lines = f.readlines()
    
    # Parse markdown table rows (format: | Film | Language | Status | Drift % | Score | Reason |)
    for line in lines:
        line = line.strip()
        
        # Skip non-table rows
        if not line.startswith("|") or line.startswith("|---"):
            continue
        
        # Skip header rows
        if "Film" in line and "Language" in line and "Status" in line:
            continue
        
        # Parse table row
        parts = [p.strip() for p in line.split("|")]
        
        # Expecting: ['', 'Film', 'Language', 'Status', 'Drift %', 'Score', 'Reason', '']
        if len(parts) < 7:
            continue
        
        film_title = parts[1]
        language = parts[2].lower()
        priority_score = parts[5]
        
        # Skip empty rows
        if not film_title or not language:
            continue
        
        # Convert film title to slug
        film_slug = film_title.lower().replace(" ", "_").replace("'", "")
        
        # Parse priority score
        try:
            score = int(priority_score)
        except ValueError:
            score = 0
        
        targets.append({
            "film_slug": film_slug,
            "film_title": film_title,
            "language": language,
            "priority_score": score,
        })
    
    print(f"  ✓ Parsed {len(targets)} targets from batch file")
    return targets


def load_progress_log(log_path: Path) -> Dict[str, List[str]]:
    """
    Load progress log to enable resume capability.
    
    Returns:
        Dict mapping film_slug to list of completed languages
    """
    if not log_path.exists():
        return {}
    
    try:
        with open(log_path, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def save_progress(log_path: Path, film_slug: str, language: str) -> None:
    """Save progress after successful subtitle acquisition."""
    progress = load_progress_log(log_path)
    
    if film_slug not in progress:
        progress[film_slug] = []
    
    if language not in progress[film_slug]:
        progress[film_slug].append(language)
    
    try:
        with open(log_path, "w") as f:
            json.dump(progress, f, indent=2)
    except IOError as e:
        print(f"  ⚠️  Failed to save progress: {e}")


def get_film_metadata(film_slug: str) -> Optional[Dict[str, Any]]:
    """
    Get film metadata (title, year) from FILM_METADATA dictionary.
    
    Args:
        film_slug: Film slug identifier
        
    Returns:
        Dict with keys: title, year, slug, priority
    """
    # Look up in comprehensive metadata
    if film_slug in FILM_METADATA:
        meta = FILM_METADATA[film_slug]
        return {
            "title": meta["title"],
            "year": meta["year"],
            "slug": film_slug,
            "priority": 50,  # Default priority for batch mode
        }
    
    # Fallback: generate from slug
    print(f"  ⚠️  Unknown film slug: {film_slug} (using fallback metadata)")
    title = film_slug.replace("_", " ").title()
    
    return {
        "title": title,
        "year": 2000,  # Default year
        "slug": film_slug,
        "priority": 50,
    }


def main():
    """Main execution flow."""
    parser = argparse.ArgumentParser(
        description="Fetch priority film subtitles (Story 4.X.2 + 4.X.5 multi-language)"
    )
    parser.add_argument(
        "--languages",
        nargs="+",
        default=["en"],
        choices=list(LANGUAGE_TARGETS.keys()),
        help="Languages to fetch (default: en)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of films to fetch (for testing)"
    )
    parser.add_argument(
        "--batch",
        type=str,
        default=None,
        help="Batch mode: read targets from priority list file (e.g., data/metadata/multi_language_priority_list.md)"
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from previous batch run (skips already completed targets)"
    )
    args = parser.parse_args()

    print("="*60)
    print("OpenSubtitles Priority Film Subtitle Fetcher")
    print("Story 4.X.2 + 4.X.5: Multi-Language Acquisition")
    print("="*60)
    
    # Determine processing mode
    if args.batch:
        print(f"Mode: BATCH (from {args.batch})")
        batch_path = Path(args.batch)
        batch_targets = parse_batch_file(batch_path)
        
        if not batch_targets:
            print("✗ No targets found in batch file")
            return 1
        
        print(f"Batch targets: {len(batch_targets)}")
    else:
        print(f"Mode: STANDARD (predefined priority films)")
        print(f"Target films: {len(PRIORITY_FILMS)} priority films")
        print(f"Languages: {', '.join(args.languages)}")
        batch_targets = None
    
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Resume mode: {'enabled' if args.resume else 'disabled'}")
    print("="*60)

    # Setup
    api_key = get_api_key()
    username, password = get_credentials()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    METADATA_DIR.mkdir(parents=True, exist_ok=True)
    
    # Progress tracking
    progress_log_path = METADATA_DIR / "multi_language_acquisition_progress.json"
    progress = load_progress_log(progress_log_path) if args.resume else {}

    # Login
    token = login_and_get_token(api_key, username, password)
    if not token:
        print("\n✗ Failed to authenticate.")
        print("If API key auth fails, try setting username/password:")
        print("  export OPEN_SUBTITLES_USERNAME='your_username'")
        print("  export OPEN_SUBTITLES_PASSWORD='your_password'")
        return 1

    # Fetch subtitles
    results = []
    skipped_count = 0
    failed_count = 0
    
    if args.batch:
        # BATCH MODE: Process targets from batch file
        total_targets = len(batch_targets)
        
        for idx, target in enumerate(batch_targets, 1):
            film_slug = target["film_slug"]
            language = target["language"]
            priority_score = target["priority_score"]
            
            # Check if already completed (resume capability)
            if args.resume and film_slug in progress and language in progress[film_slug]:
                print(f"\n[{idx}/{total_targets}] {film_slug} ({language.upper()}) - SKIPPED (already completed)")
                skipped_count += 1
                continue
            
            # Get film metadata
            film_meta = get_film_metadata(film_slug)
            if not film_meta:
                print(f"\n[{idx}/{total_targets}] {film_slug} ({language.upper()}) - SKIPPED (no metadata)")
                skipped_count += 1
                continue
            
            film_title = film_meta["title"]
            film_year = film_meta["year"]
            
            print(f"\n{'#'*60}")
            print(f"[{idx}/{total_targets}] Film: {film_title} ({film_year})")
            print(f"Language: {language.upper()} | Priority Score: {priority_score}")
            print(f"{'#'*60}")
            
            # Fetch subtitle
            lang_codes = LANGUAGE_TARGETS.get(language, [language])
            result = fetch_subtitle_for_language(
                api_key, token, film_title, film_year, film_slug, language, lang_codes
            )
            
            if result:
                results.append(result)
                # Save progress
                save_progress(progress_log_path, film_slug, language)
            else:
                failed_count += 1
            
            # Rate limiting delay between requests
            time.sleep(2)
    
    else:
        # STANDARD MODE: Process predefined films
        films_to_fetch = PRIORITY_FILMS[:args.limit] if args.limit else PRIORITY_FILMS
        
        for film in films_to_fetch:
            film_title = film["title"]
            film_year = film["year"]
            film_slug = film["slug"]
            priority = film["priority"]

            print(f"\n{'#'*60}")
            print(f"Film: {film_title} ({film_year}) - Priority: {priority}/100")
            print(f"{'#'*60}")

            for lang_key in args.languages:
                # Check resume
                if args.resume and film_slug in progress and lang_key in progress[film_slug]:
                    print(f"  → {lang_key.upper()} - SKIPPED (already completed)")
                    skipped_count += 1
                    continue
                
                lang_codes = LANGUAGE_TARGETS[lang_key]

                result = fetch_subtitle_for_language(
                    api_key, token, film_title, film_year, film_slug, lang_key, lang_codes
                )

                if result:
                    results.append(result)
                    save_progress(progress_log_path, film_slug, lang_key)
                else:
                    failed_count += 1

                # Rate limiting delay between requests
                time.sleep(2)

    # Summary
    total_attempted = len(results) + failed_count
    success_rate = (len(results) / total_attempted * 100) if total_attempted > 0 else 0
    
    print("\n" + "="*60)
    print("ACQUISITION SUMMARY")
    print("="*60)
    print(f"Total subtitles acquired: {len(results)}")
    print(f"Skipped (resume): {skipped_count}")
    print(f"Failed: {failed_count}")
    print(f"Success rate: {len(results)}/{total_attempted} ({success_rate:.1f}%)")
    
    if args.batch:
        print(f"Batch file: {args.batch}")
        print(f"Targets processed: {len(batch_targets)}")
    else:
        print(f"Languages: {', '.join(args.languages)}")
        print(f"Films processed: {len(films_to_fetch) if not args.batch else len(set(t['film_slug'] for t in batch_targets))}")

    # Save acquisition metadata
    story_id = "4.X.5" if args.batch else "4.X.2"
    metadata_path = METADATA_DIR / f"acquisition_log_{story_id.replace('.', '_')}.json"
    metadata = {
        "story": story_id,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "mode": "batch" if args.batch else "standard",
        "batch_file": args.batch if args.batch else None,
        "languages": args.languages if not args.batch else list(set(t["language"] for t in batch_targets)),
        "targets_attempted": total_attempted,
        "subtitles_acquired": len(results),
        "skipped": skipped_count,
        "failed": failed_count,
        "success_rate": success_rate,
        "results": results,
    }

    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"\nMetadata saved to: {metadata_path}")
    print("="*60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
