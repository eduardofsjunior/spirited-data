#!/usr/bin/env python3
"""
OpenSubtitles API subtitle fetcher for Spirited Away (2001).
Downloads EN, PT-BR, and JA subtitles.
"""

import os
import sys
import time
import json
from pathlib import Path
from typing import List, Dict, Optional, Any
import zipfile
import io

try:
    import requests
except ImportError:
    print("ERROR: requests library not found. Install with: pip install requests")
    sys.exit(1)


# Configuration
API_BASE_URL = "https://api.opensubtitles.com/api/v1"
OUTPUT_DIR = Path("data/raw/subtitles")

# Films to fetch (all 22 Ghibli films)
FILMS = [
    {"title": "Castle in the Sky", "year": 1986, "slug": "castle_in_the_sky"},
    {"title": "Grave of the Fireflies", "year": 1988, "slug": "grave_of_the_fireflies"},
    {"title": "My Neighbor Totoro", "year": 1988, "slug": "my_neighbor_totoro"},
    {"title": "Kiki's Delivery Service", "year": 1989, "slug": "kikis_delivery_service"},
    {"title": "Only Yesterday", "year": 1991, "slug": "only_yesterday"},
    {"title": "Porco Rosso", "year": 1992, "slug": "porco_rosso"},
    {"title": "Pom Poko", "year": 1994, "slug": "pom_poko"},
    {"title": "Whisper of the Heart", "year": 1995, "slug": "whisper_of_the_heart"},
    {"title": "Princess Mononoke", "year": 1997, "slug": "princess_mononoke"},
    {"title": "My Neighbors the Yamadas", "year": 1999, "slug": "my_neighbors_the_yamadas"},
    {"title": "Spirited Away", "year": 2001, "slug": "spirited_away"},
    {"title": "The Cat Returns", "year": 2002, "slug": "the_cat_returns"},
    {"title": "Howl's Moving Castle", "year": 2004, "slug": "howls_moving_castle"},
    {"title": "Tales from Earthsea", "year": 2006, "slug": "tales_from_earthsea"},
    {"title": "Ponyo", "year": 2008, "slug": "ponyo"},
    {"title": "Arrietty", "year": 2010, "slug": "arrietty"},
    {"title": "From Up on Poppy Hill", "year": 2011, "slug": "from_up_on_poppy_hill"},
    {"title": "The Wind Rises", "year": 2013, "slug": "the_wind_rises"},
    {"title": "The Tale of the Princess Kaguya", "year": 2013, "slug": "the_tale_of_the_princess_kaguya"},
    {"title": "When Marnie Was There", "year": 2014, "slug": "when_marnie_was_there"},
    {"title": "The Red Turtle", "year": 2016, "slug": "the_red_turtle"},
    {"title": "Earwig and the Witch", "year": 2021, "slug": "earwig_and_the_witch"},
]

# Language mapping: target_name -> [preferred_codes]
LANGUAGE_TARGETS = {
    "en": ["en", "eng"],           # English
    "pt": ["pob", "por", "pt"],    # PT-BR preferred, fallback to PT-PT
    "ja": ["ja", "jpn"],           # Japanese
}

# Additional languages for coverage assessment
ASSESSMENT_LANGUAGES = {
    "fr": ["fr", "fre"],           # French
    "es": ["es", "spa"],           # Spanish
    "nl": ["nl", "dut"],           # Dutch
    "ar": ["ar", "ara"],           # Arabic
}


def get_api_key() -> str:
    """Get API key from environment."""
    api_key = os.getenv("OPEN_SUBTITLES_API_KEY")
    if not api_key:
        print("ERROR: OPEN_SUBTITLES_API_KEY environment variable not set")
        print("Set it with: export OPEN_SUBTITLES_API_KEY='your_key_here'")
        sys.exit(1)
    return api_key


def login_and_get_token(api_key: str) -> Optional[str]:
    """
    Login to OpenSubtitles API and get JWT token.

    Args:
        api_key: OpenSubtitles API key

    Returns:
        JWT token or None on failure
    """
    url = f"{API_BASE_URL}/login"
    headers = {
        "Api-Key": api_key,
        "User-Agent": "EduardoSubsFetcher/1.0",
        "Content-Type": "application/json",
    }

    # Login with API key only (no username/password needed for API key mode)
    payload = {}

    try:
        print("  → Authenticating with API key...")
        response = requests.post(url, json=payload, headers=headers, timeout=30)

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
            print(f"  ✗ Login failed ({response.status_code}): {response.text}")
            return None

    except requests.RequestException as e:
        print(f"  ✗ Login request error: {e}")
        return None


def get_headers(api_key: str, token: Optional[str] = None) -> Dict[str, str]:
    """Build request headers."""
    headers = {
        "Api-Key": api_key,
        "User-Agent": "EduardoSubsFetcher/1.0",
        "Content-Type": "application/json",
    }

    if token:
        headers["Authorization"] = f"Bearer {token}"

    return headers


def search_subs(
    api_key: str,
    token: str,
    title: str,
    year: int,
    language_codes: List[str],
    max_retries: int = 3
) -> List[Dict[str, Any]]:
    """
    Search for subtitles via OpenSubtitles API.

    Args:
        api_key: OpenSubtitles API key
        token: JWT authentication token
        title: Film title
        year: Release year
        language_codes: List of language codes to search
        max_retries: Maximum retry attempts for rate limiting

    Returns:
        List of subtitle entries from API
    """
    url = f"{API_BASE_URL}/subtitles"

    params = {
        "query": title,
        "year": year,
        "type": "movie",
        "languages": ",".join(language_codes),
        "order_by": "download_count",
        "order_direction": "desc",
    }

    headers = get_headers(api_key, token)

    for attempt in range(max_retries):
        try:
            print(f"  → Searching: {title} ({year}) | langs={','.join(language_codes)}")
            response = requests.get(url, params=params, headers=headers, timeout=30)

            if response.status_code == 200:
                data = response.json()
                results = data.get("data", [])
                print(f"  ✓ Found {len(results)} results")
                return results

            elif response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 5))
                print(f"  ⚠ Rate limited (429). Retrying after {retry_after}s...")
                time.sleep(retry_after)
                continue

            elif response.status_code in [401, 403]:
                print(f"  ✗ Auth error ({response.status_code}): {response.text}")
                return []

            else:
                print(f"  ✗ Search failed ({response.status_code}): {response.text}")
                return []

        except requests.RequestException as e:
            print(f"  ✗ Request error (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
                continue
            return []

    return []


def pick_best(
    entries: List[Dict[str, Any]],
    preferred_lang: str
) -> Optional[Dict[str, Any]]:
    """
    Pick best subtitle entry for target language.

    Prefers:
    - Japanese-only (not bilingual) for JA subtitles
    - Non-hearing-impaired (hearing_impaired=False)
    - Highest download_count
    - Matching language code

    Args:
        entries: List of subtitle entries from API
        preferred_lang: Preferred language code (e.g., "pob", "en")

    Returns:
        Best subtitle entry or None
    """
    if not entries:
        return None

    # Filter by language if specified
    matching = [
        e for e in entries
        if e.get("attributes", {}).get("language") == preferred_lang
    ]

    if not matching:
        # Fallback to any entry (already sorted by download_count)
        matching = entries

    # For Japanese, filter out bilingual subtitles (those with English in file name)
    if preferred_lang in ["ja", "jpn"]:
        ja_only = []
        for e in matching:
            file_info = e.get("attributes", {}).get("files", [{}])[0] if e.get("attributes", {}).get("files") else {}
            file_name = file_info.get("file_name", "").lower()
            # Exclude if file name contains english indicators
            if not any(indicator in file_name for indicator in ["eng", "english", "en+", "+en", "kanji+hir+eng"]):
                ja_only.append(e)

        if ja_only:
            print(f"  → Filtered {len(ja_only)}/{len(matching)} Japanese-only subtitles (excluding bilingual)")
            matching = ja_only

    # Prefer non-HI
    non_hi = [e for e in matching if not e.get("attributes", {}).get("hearing_impaired", False)]
    candidates = non_hi if non_hi else matching

    if not candidates:
        return None

    # Already sorted by download_count, pick first
    best = candidates[0]

    attrs = best.get("attributes", {})
    file_info = attrs.get("files", [{}])[0] if attrs.get("files") else {}

    print(f"  ✓ Selected: {attrs.get('language')} | downloads={attrs.get('download_count', 0)} | "
          f"file={file_info.get('file_name', 'N/A')} | uploader={attrs.get('uploader', {}).get('name', 'unknown')}")

    return best


def download_sub(
    api_key: str,
    token: str,
    file_id: int,
    max_retries: int = 3
) -> Optional[bytes]:
    """
    Download subtitle file via OpenSubtitles API.

    Args:
        api_key: OpenSubtitles API key
        token: JWT authentication token
        file_id: Subtitle file ID
        max_retries: Maximum retry attempts

    Returns:
        Subtitle file content as bytes, or None on failure
    """
    url = f"{API_BASE_URL}/download"
    headers = get_headers(api_key, token)
    payload = {"file_id": file_id}

    for attempt in range(max_retries):
        try:
            # POST to get download link
            response = requests.post(url, json=payload, headers=headers, timeout=30)

            if response.status_code == 200:
                data = response.json()
                download_link = data.get("link")

                if not download_link:
                    print(f"  ✗ No download link in response")
                    return None

                # Follow download link
                print(f"  → Downloading from temporary link...")
                download_response = requests.get(download_link, timeout=60)

                if download_response.status_code == 200:
                    print(f"  ✓ Downloaded {len(download_response.content)} bytes")
                    return download_response.content
                else:
                    print(f"  ✗ Download failed ({download_response.status_code})")
                    return None

            elif response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 5))
                print(f"  ⚠ Rate limited (429). Retrying after {retry_after}s...")
                time.sleep(retry_after)
                continue

            elif response.status_code in [401, 403]:
                print(f"  ✗ Auth error ({response.status_code}): {response.text}")
                return None

            else:
                print(f"  ✗ Download request failed ({response.status_code}): {response.text}")
                return None

        except requests.RequestException as e:
            print(f"  ✗ Request error (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                continue
            return None

    return None


def save_file(
    content: bytes,
    target_path: Path
) -> bool:
    """
    Save subtitle file to disk, handling .zip extraction.

    Args:
        content: File content as bytes
        target_path: Target file path

    Returns:
        True if saved successfully, False otherwise
    """
    # Create parent directory
    target_path.parent.mkdir(parents=True, exist_ok=True)

    # Check if content is a ZIP file
    if content[:2] == b'PK':  # ZIP magic bytes
        print(f"  → Content is .zip, extracting...")
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                # Find .srt file in archive
                srt_files = [name for name in zf.namelist() if name.endswith('.srt')]

                if srt_files:
                    # Extract first .srt file
                    srt_content = zf.read(srt_files[0])
                    target_path.write_bytes(srt_content)
                    print(f"  ✓ Extracted {srt_files[0]} → {target_path}")
                    return True
                else:
                    # No .srt, check for .ass or other formats
                    print(f"  ⚠ No .srt in archive, files: {zf.namelist()}")
                    # Save first file
                    if zf.namelist():
                        first_file = zf.namelist()[0]
                        file_content = zf.read(first_file)
                        target_path.write_bytes(file_content)
                        print(f"  ⚠ Saved {first_file} → {target_path} (may need conversion)")
                        return True
                    return False

        except zipfile.BadZipFile:
            print(f"  ✗ Invalid ZIP file")
            return False
    else:
        # Not a ZIP, save directly
        target_path.write_bytes(content)
        print(f"  ✓ Saved → {target_path}")
        return True


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

    Returns metadata dict with keys: lang, path, file_id, size
    """
    # Check if file already exists
    target_path = OUTPUT_DIR / f"{film_slug}_{lang_key}.srt"
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
                file_id = best.get("attributes", {}).get("files", [{}])[0].get("file_id")

                if not file_id:
                    print(f"  ✗ No file_id found in result")
                    continue

                # Download
                content = download_sub(api_key, token, file_id)

                if content:
                    # Save
                    target_path = OUTPUT_DIR / f"{film_slug}_{lang_key}.srt"
                    if save_file(content, target_path):
                        return {
                            "film": film_title,
                            "lang": lang_key,
                            "path": str(target_path),
                            "file_id": file_id,
                            "size": len(content),
                            "source_lang": lang_code,
                        }

    print(f"  ✗ Failed to fetch {lang_key.upper()} subtitles")
    return None


def assess_coverage(api_key: str, token: str, languages: Dict[str, List[str]]) -> Dict[str, Any]:
    """
    Assess subtitle availability for target languages without downloading.

    Args:
        api_key: OpenSubtitles API key
        token: JWT authentication token
        languages: Dictionary of language_key -> [language_codes]

    Returns:
        Coverage report dictionary
    """
    print("\n" + "="*60)
    print("COVERAGE ASSESSMENT MODE")
    print("="*60)
    print(f"Films: {len(FILMS)}")
    print(f"Languages to assess: {', '.join(languages.keys()).upper()}")
    print("="*60)

    coverage_data = {lang: {"available": [], "missing": []} for lang in languages.keys()}

    for film in FILMS:
        print(f"\n📽️  {film['title']} ({film['year']})")

        for lang_key, lang_codes in languages.items():
            # Search for subtitles
            found = False
            for lang_code in lang_codes:
                results = search_subs(api_key, token, film['title'], film['year'], [lang_code])

                if results:
                    # Check if we can find a valid subtitle
                    best = pick_best(results, lang_code)
                    if best:
                        coverage_data[lang_key]["available"].append(film['title'])
                        print(f"  ✓ {lang_key.upper()}: Available")
                        found = True
                        break

                # Small delay to avoid rate limiting
                time.sleep(0.5)

            if not found:
                coverage_data[lang_key]["missing"].append(film['title'])
                print(f"  ✗ {lang_key.upper()}: Not found")

    # Print summary
    print("\n" + "="*60)
    print("COVERAGE ASSESSMENT SUMMARY")
    print("="*60)

    print(f"\n📊 Coverage by Language:")
    print(f"   Total films: {len(FILMS)}\n")

    # Sort languages by coverage percentage (descending)
    lang_coverage = []
    for lang_key, data in coverage_data.items():
        available_count = len(data["available"])
        percentage = (available_count / len(FILMS)) * 100
        lang_coverage.append((lang_key, available_count, percentage, data))

    lang_coverage.sort(key=lambda x: x[2], reverse=True)

    for lang_key, count, percentage, data in lang_coverage:
        status = "🟢" if percentage >= 90 else "🟡" if percentage >= 70 else "🔴"
        print(f"   {status} {lang_key.upper()}: {count}/{len(FILMS)} films ({percentage:.1f}%)")

    # Show missing films for each language
    print(f"\n❌ Missing Films by Language:")
    for lang_key, _, percentage, data in lang_coverage:
        if data["missing"]:
            print(f"\n   {lang_key.upper()} - Missing {len(data['missing'])} films:")
            for title in data["missing"][:5]:  # Show first 5
                print(f"      - {title}")
            if len(data["missing"]) > 5:
                print(f"      ... and {len(data['missing']) - 5} more")

    # Recommendations
    print(f"\n💡 Recommendations:")
    tier1 = [lang for lang, _, pct, _ in lang_coverage if pct >= 90]
    tier2 = [lang for lang, _, pct, _ in lang_coverage if 70 <= pct < 90]
    tier3 = [lang for lang, _, pct, _ in lang_coverage if pct < 70]

    if tier1:
        print(f"   ✅ Tier 1 (≥90% coverage): {', '.join([l.upper() for l in tier1])}")
        print(f"      → Recommended for acquisition")
    if tier2:
        print(f"   ⚠️  Tier 2 (70-90% coverage): {', '.join([l.upper() for l in tier2])}")
        print(f"      → Acceptable with documented gaps")
    if tier3:
        print(f"   ❌ Tier 3 (<70% coverage): {', '.join([l.upper() for l in tier3])}")
        print(f"      → Consider alternative sources or drop")

    print("\n" + "="*60)

    return coverage_data


def main():
    """Main execution flow."""
    import argparse

    parser = argparse.ArgumentParser(description="OpenSubtitles subtitle fetcher and coverage assessor")
    parser.add_argument(
        "--assess-coverage",
        action="store_true",
        help="Assess subtitle availability without downloading (for FR, ES, NL, AR)"
    )
    parser.add_argument(
        "--languages",
        nargs="+",
        choices=list(LANGUAGE_TARGETS.keys()) + list(ASSESSMENT_LANGUAGES.keys()),
        help="Specific languages to fetch (default: all in LANGUAGE_TARGETS)"
    )

    args = parser.parse_args()

    print("\n" + "="*60)
    print("OpenSubtitles Subtitle Fetcher")
    print("="*60)
    print(f"Films: {len(FILMS)}")

    # Determine which languages to process
    if args.assess_coverage:
        # Assessment mode - check new languages only
        languages_to_process = ASSESSMENT_LANGUAGES
        print(f"Mode: COVERAGE ASSESSMENT")
        print(f"Languages: {', '.join(languages_to_process.keys()).upper()}")
    elif args.languages:
        # User specified languages
        languages_to_process = {}
        for lang in args.languages:
            if lang in LANGUAGE_TARGETS:
                languages_to_process[lang] = LANGUAGE_TARGETS[lang]
            elif lang in ASSESSMENT_LANGUAGES:
                languages_to_process[lang] = ASSESSMENT_LANGUAGES[lang]
        print(f"Mode: DOWNLOAD")
        print(f"Languages: {', '.join(languages_to_process.keys()).upper()}")
    else:
        # Default: use original LANGUAGE_TARGETS
        languages_to_process = LANGUAGE_TARGETS
        print(f"Mode: DOWNLOAD")
        print(f"Languages: {', '.join(languages_to_process.keys()).upper()}")

    print(f"Output: {OUTPUT_DIR}")
    print("="*60)

    # Get API key
    api_key = get_api_key()
    print(f"✓ API key loaded: {api_key[:8]}...{api_key[-4:]}")

    # Note: OpenSubtitles API allows downloads with just API key (no login token needed)
    token = ""  # Empty token, API key in header is sufficient

    # Assessment mode - check availability without downloading
    if args.assess_coverage:
        assess_coverage(api_key, token, languages_to_process)
        return

    # Download mode
    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"✓ Output directory ready: {OUTPUT_DIR}")

    # Fetch subtitles for each film and language
    all_results = []

    for film in FILMS:
        print(f"\n{'#'*60}")
        print(f"# Processing: {film['title']} ({film['year']})")
        print(f"{'#'*60}")

        film_results = []
        for lang_key, lang_codes in languages_to_process.items():
            result = fetch_subtitle_for_language(
                api_key,
                token,
                film['title'],
                film['year'],
                film['slug'],
                lang_key,
                lang_codes
            )
            if result:
                film_results.append(result)
                all_results.append(result)

        print(f"\n→ {film['title']}: {len(film_results)}/{len(languages_to_process)} languages downloaded")

    # Print summary
    print("\n" + "="*60)
    print("DOWNLOAD SUMMARY")
    print("="*60)

    total_expected = len(FILMS) * len(languages_to_process)

    # Calculate coverage statistics
    coverage_by_lang = {}
    for lang_key in languages_to_process.keys():
        lang_results = [r for r in all_results if r['lang'] == lang_key]
        coverage_by_lang[lang_key] = len(lang_results)

    print(f"\n📊 Coverage Statistics:")
    print(f"   Total films: {len(FILMS)}")
    print(f"   Total expected files: {total_expected}")
    print(f"   Files obtained: {len(all_results)} ({len(all_results)/total_expected*100:.1f}%)")
    print(f"\n   By language:")
    for lang_key, count in coverage_by_lang.items():
        percentage = count / len(FILMS) * 100
        print(f"   - {lang_key.upper()}: {count}/{len(FILMS)} films ({percentage:.1f}%)")

    # Show films with complete coverage
    complete_films = []
    partial_films = []
    missing_films = []

    for film in FILMS:
        film_subs = [r for r in all_results if r['film'] == film['title']]
        if len(film_subs) == len(languages_to_process):
            complete_films.append(film)
        elif len(film_subs) > 0:
            partial_films.append((film, film_subs))
        else:
            missing_films.append(film)

    if complete_films:
        print(f"\n✅ Complete coverage ({len(languages_to_process)} languages): {len(complete_films)} films")
        for film in complete_films[:5]:  # Show first 5
            print(f"   - {film['title']} ({film['year']})")
        if len(complete_films) > 5:
            print(f"   ... and {len(complete_films) - 5} more")

    if partial_films:
        print(f"\n⚠️  Partial coverage: {len(partial_films)} films")
        for film, subs in partial_films[:5]:  # Show first 5
            langs = ', '.join([s['lang'].upper() for s in subs])
            print(f"   - {film['title']} ({film['year']}): {langs}")
        if len(partial_films) > 5:
            print(f"   ... and {len(partial_films) - 5} more")

    if missing_films:
        print(f"\n❌ No subtitles found: {len(missing_films)} films")
        for film in missing_films[:5]:  # Show first 5
            print(f"   - {film['title']} ({film['year']})")
        if len(missing_films) > 5:
            print(f"   ... and {len(missing_films) - 5} more")

    print("\n" + "="*60)


if __name__ == "__main__":
    main()
