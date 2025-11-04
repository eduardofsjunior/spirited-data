"""
Subtitle file parsing module.

This module provides functions for parsing .srt subtitle files into structured
dialogue data for text analysis.
"""
# Standard library imports
import argparse
import json
import logging
import os
import random
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Third-party imports
import pysrt

# Local imports
from src.shared.config import LOG_LEVEL

# Set up logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _open_srt_with_encoding_detection(
    filepath: str, expected_language: Optional[str] = None
) -> Tuple[pysrt.SubRipFile, str]:
    """
    Open .srt file with automatic encoding detection.

    Tries UTF-8 first, falls back to Latin-1 if UTF-8 fails (for English files only).
    For Japanese files, UTF-8 is required (no fallback to Latin-1).
    Logs the encoding used for audit trail.

    Args:
        filepath: Path to .srt subtitle file
        expected_language: Optional language code ('en' or 'ja') for encoding preference

    Returns:
        Tuple of (SubRipFile object, encoding_used)

    Raises:
        UnicodeDecodeError: If file encoding cannot be detected after fallbacks
    """
    encoding_used = "utf-8"
    subtitles: Optional[pysrt.SubRipFile] = None

    # Detect language from filename if not provided
    if expected_language is None:
        if filepath.endswith("_ja.srt"):
            expected_language = "ja"
        elif filepath.endswith("_en.srt"):
            expected_language = "en"
        elif filepath.endswith("_fr.srt"):
            expected_language = "fr"
        elif filepath.endswith("_es.srt"):
            expected_language = "es"
        elif filepath.endswith("_nl.srt"):
            expected_language = "nl"
        elif filepath.endswith("_ar.srt"):
            expected_language = "ar"

    try:
        subtitles = pysrt.open(filepath, encoding="utf-8")
    except UnicodeDecodeError:
        # For Japanese files, UTF-8 is required - don't fallback to Latin-1
        if expected_language == "ja":
            logger.error(f"UTF-8 encoding failed for Japanese file {filepath}. Japanese files must be UTF-8 encoded.")
            raise
        # For all other languages (EN, FR, ES, NL, AR), try Latin-1 fallback
        logger.warning(f"UTF-8 encoding failed for {filepath} (language: {expected_language}), trying Latin-1")
        try:
            encoding_used = "latin-1"
            subtitles = pysrt.open(filepath, encoding="latin-1")
        except UnicodeDecodeError as e:
            logger.error(f"Failed to decode {filepath} with UTF-8 and Latin-1: {e}")
            raise

    if subtitles is None:
        raise ValueError(f"Failed to open subtitle file: {filepath}")

    language_info = f" (language: {expected_language})" if expected_language else ""
    logger.info(f"Parsing {filepath}{language_info} with encoding: {encoding_used}")
    return subtitles, encoding_used


def parse_srt_file(filepath: str, expected_language: Optional[str] = None) -> Tuple[List[Dict[str, Any]], int]:
    """
    Parse .srt subtitle file and extract structured data.

    Uses pysrt library to load subtitle file with encoding detection and
    handles malformed entries gracefully. Extracts:
    - subtitle_index: Sequential subtitle number (1-based)
    - start_time: Start time in seconds (float)
    - end_time: End time in seconds (float)
    - duration: Duration in seconds (end_time - start_time)
    - dialogue_text: Raw dialogue text from subtitle entry

    Args:
        filepath: Path to .srt subtitle file
        expected_language: Optional language code ('en' or 'ja') for encoding detection

    Returns:
        Tuple of:
        - List of dictionaries, each containing subtitle data
        - Count of skipped entries due to malformed timestamps

    Raises:
        FileNotFoundError: If subtitle file doesn't exist
        UnicodeDecodeError: If file encoding cannot be detected after fallbacks
    """
    logger.info(f"Parsing subtitle file: {filepath}")

    # Open file with encoding detection
    subtitles, encoding_used = _open_srt_with_encoding_detection(filepath, expected_language)

    result: List[Dict[str, Any]] = []
    skipped_count = 0

    for subtitle in subtitles:
        try:
            # Extract subtitle_index from subtitle.index property
            subtitle_index = subtitle.index

            # Extract start_time in seconds: convert SubRipTime to total seconds
            # Using ordinal property which gives milliseconds since start
            start_time = subtitle.start.ordinal / 1000.0

            # Extract end_time in seconds: convert SubRipTime to total seconds
            end_time = subtitle.end.ordinal / 1000.0

            # Calculate duration: end_time - start_time (in seconds)
            duration = end_time - start_time

            # Extract dialogue_text: use subtitle.text property and clean it
            dialogue_text = clean_dialogue_text(subtitle.text)

            result.append(
                {
                    "subtitle_index": subtitle_index,
                    "start_time": start_time,
                    "end_time": end_time,
                    "duration": duration,
                    "dialogue_text": dialogue_text,
                }
            )

        except (AttributeError, ValueError, TypeError) as e:
            # Handle malformed timestamps or other parsing errors
            skipped_count += 1
            logger.warning(
                f"Subtitle {subtitle.index if hasattr(subtitle, 'index') else 'unknown'} "
                f"has malformed timestamp, skipping: {e}"
            )
            continue

    logger.info(
        f"Successfully parsed {len(result)} subtitles from {filepath} "
        f"(skipped {skipped_count} malformed entries)"
    )
    return result, skipped_count


def extract_film_metadata(
    filepath: str, subtitles: List[Dict[str, Any]], language_code: Optional[str] = None
) -> Dict[str, Any]:
    """
    Extract film metadata from filepath and subtitle data.

    Extracts film_slug from filename, converts to film_name, detects language code,
    and calculates statistics about the subtitle file.

    Args:
        filepath: Path to .srt subtitle file
        subtitles: List of parsed subtitle dictionaries
        language_code: Optional language code ('en' or 'ja'). If not provided, auto-detected from film_slug

    Returns:
        Dictionary containing metadata:
        {
            "film_name": str,  # Human-readable film name (e.g., "Spirited Away")
            "film_slug": str,  # File slug (e.g., "spirited_away_ja")
            "language_code": str,  # Language code ('en' or 'ja')
            "total_subtitles": int,  # Count of subtitle entries
            "total_duration": float,  # Total duration in seconds
            "parse_timestamp": str  # ISO 8601 timestamp
        }
    """
    # Extract film_slug from file name: Path(filepath).stem (e.g., "spirited_away_ja")
    film_slug = Path(filepath).stem

    # Auto-detect language from film_slug if not provided
    if language_code is None:
        if film_slug.endswith("_ja"):
            language_code = "ja"
        elif film_slug.endswith("_fr"):
            language_code = "fr"
        elif film_slug.endswith("_es"):
            language_code = "es"
        elif film_slug.endswith("_nl"):
            language_code = "nl"
        elif film_slug.endswith("_ar"):
            language_code = "ar"
        else:
            language_code = "en"  # Default to 'en' if no language suffix detected

    # Extract film_name from slug: convert underscores to spaces, capitalize words
    # Remove language suffix (_en, _ja, _fr, _es, _nl, _ar) before processing
    name_part = film_slug.replace("_en", "").replace("_ja", "").replace("_fr", "").replace("_es", "").replace("_nl", "").replace("_ar", "")
    film_name = " ".join(word.capitalize() for word in name_part.split("_"))

    # Calculate total_subtitles: count of parsed subtitle entries
    total_subtitles = len(subtitles)

    # Calculate total_duration: max(end_time) - min(start_time) (in seconds)
    if subtitles:
        start_times = [sub["start_time"] for sub in subtitles]
        end_times = [sub["end_time"] for sub in subtitles]
        total_duration = max(end_times) - min(start_times)
    else:
        total_duration = 0.0

    # Add parse_timestamp: datetime.now().isoformat() (ISO 8601 format)
    parse_timestamp = datetime.now().isoformat()

    return {
        "film_name": film_name,
        "film_slug": film_slug,
        "language_code": language_code,
        "total_subtitles": total_subtitles,
        "total_duration": total_duration,
        "parse_timestamp": parse_timestamp,
    }


def clean_dialogue_text(text: str) -> str:
    """
    Clean dialogue text by removing HTML tags and normalizing whitespace.

    Removes HTML formatting tags (e.g., <i>, <b>, <u>) and normalizes
    whitespace by collapsing multiple spaces/newlines into single spaces.

    Args:
        text: Raw dialogue text from subtitle entry (may contain HTML tags, newlines)

    Returns:
        Cleaned dialogue text with HTML tags removed and whitespace normalized

    Example:
        >>> clean_dialogue_text("<i>Hello</i>\\nworld")
        'Hello world'
        >>> clean_dialogue_text("  Multiple   spaces  ")
        'Multiple spaces'
    """
    if not text:
        return ""

    # Remove HTML tags using regex: removes <i>, <b>, <u>, etc.
    cleaned = re.sub(r"<[^>]+>", "", text)

    # Normalize whitespace: join multiple lines/spaces with single space
    # This handles multi-line dialogue and extra whitespace
    cleaned = " ".join(cleaned.split())

    return cleaned


def save_parsed_subtitles(
    subtitles: List[Dict[str, Any]], metadata: Dict[str, Any], output_path: str
) -> None:
    """
    Save parsed subtitle data as JSON file.

    Creates output directory if needed and saves structured JSON with
    metadata and subtitle entries.

    Args:
        subtitles: List of parsed subtitle dictionaries
        metadata: Film metadata dictionary
        output_path: Path where JSON file should be saved

    Raises:
        OSError: If output directory cannot be created or file cannot be written
    """
    # Create output directory if needed
    output_dir = Path(output_path).parent
    os.makedirs(output_dir, exist_ok=True)

    # Create output data structure
    data = {
        "metadata": metadata,
        "subtitles": subtitles,
    }

    # Save JSON file with indentation for readability
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    logger.info(f"Saved parsed subtitles to {output_path}")


def process_all_subtitles(
    subtitle_dir: Path, film_filter: Optional[List[str]] = None, language: str = "en"
) -> List[Dict[str, Any]]:
    """
    Process all .srt subtitle files in directory.

    Discovers subtitle files based on language filter and processes each one,
    saving parsed JSON output.

    Args:
        subtitle_dir: Directory containing .srt subtitle files
        film_filter: Optional list of film slugs to process (if None, process all)
        language: Language to process: 'en' (English), 'ja' (Japanese), or 'all' (both)

    Returns:
        List of processing results:
        [
            {
                "film_slug": str,
                "success": bool,
                "error_message": Optional[str],
                "output_path": Optional[str]
            },
            ...
        ]
    """
    results: List[Dict[str, Any]] = []

    # Discover all .srt files in directory
    all_srt_files = list(subtitle_dir.glob("*.srt"))

    # Filter files based on language parameter
    if language == "en":
        # Filter for English files only: *_en.srt pattern
        filtered_files = [f for f in all_srt_files if f.name.endswith("_en.srt")]
    elif language == "ja":
        # Filter for Japanese files only: *_ja.srt pattern
        filtered_files = [f for f in all_srt_files if f.name.endswith("_ja.srt")]
    elif language == "fr":
        # Filter for French files only: *_fr.srt pattern
        filtered_files = [f for f in all_srt_files if f.name.endswith("_fr.srt")]
    elif language == "es":
        # Filter for Spanish files only: *_es.srt pattern
        filtered_files = [f for f in all_srt_files if f.name.endswith("_es.srt")]
    elif language == "nl":
        # Filter for Dutch files only: *_nl.srt pattern
        filtered_files = [f for f in all_srt_files if f.name.endswith("_nl.srt")]
    elif language == "ar":
        # Filter for Arabic files only: *_ar.srt pattern
        filtered_files = [f for f in all_srt_files if f.name.endswith("_ar.srt")]
    elif language == "all":
        # Filter for all six languages
        filtered_files = [
            f for f in all_srt_files
            if f.name.endswith(("_en.srt", "_ja.srt", "_fr.srt", "_es.srt", "_nl.srt", "_ar.srt"))
        ]
    else:
        logger.error(f"Invalid language parameter: {language}. Must be 'en', 'ja', 'fr', 'es', 'nl', 'ar', or 'all'")
        return results

    # Apply film filter if provided
    if film_filter:
        filtered_files = [
            f for f in filtered_files if Path(f).stem in film_filter
        ]

    total_files = len(filtered_files)
    logger.info(f"Found {total_files} {language} subtitle files to process")

    # Determine language for each file (needed for processing)
    # If language == 'all', we need to detect from filename
    for count, filepath in enumerate(filtered_files, 1):
        film_slug = Path(filepath).stem
        
        # Detect file language from filename
        if filepath.name.endswith("_ja.srt"):
            file_language = "ja"
        elif filepath.name.endswith("_fr.srt"):
            file_language = "fr"
        elif filepath.name.endswith("_es.srt"):
            file_language = "es"
        elif filepath.name.endswith("_nl.srt"):
            file_language = "nl"
        elif filepath.name.endswith("_ar.srt"):
            file_language = "ar"
        else:
            file_language = "en"
        
        logger.info(f"Processing {count}/{total_files}: {film_slug} ({file_language})")

        try:
            # Parse subtitle file with language detection
            subtitles, skipped_count = parse_srt_file(str(filepath), expected_language=file_language)

            # Extract metadata with language code
            metadata = extract_film_metadata(str(filepath), subtitles, language_code=file_language)

            # Build output path: data/processed/subtitles/{film_slug}_parsed.json
            output_path = f"data/processed/subtitles/{film_slug}_parsed.json"

            # Save parsed JSON
            save_parsed_subtitles(subtitles, metadata, output_path)

            results.append(
                {
                    "film_slug": film_slug,
                    "success": True,
                    "error_message": None,
                    "output_path": output_path,
                    "skipped_count": skipped_count,
                }
            )

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Failed to process {film_slug}: {error_msg}")
            results.append(
                {
                    "film_slug": film_slug,
                    "success": False,
                    "error_message": error_msg,
                    "output_path": None,
                }
            )

    return results


def validate_parsed_subtitles(
    srt_filepath: str, json_filepath: str
) -> Dict[str, Any]:
    """
    Validate parsed subtitles by comparing .srt file with JSON output.

    Compares subtitle counts and performs spot-check on random entries
    to verify parsing accuracy. Detects language from JSON metadata for
    language-aware encoding detection.

    Args:
        srt_filepath: Path to original .srt subtitle file
        json_filepath: Path to parsed JSON file

    Returns:
        Validation report dictionary:
        {
            "matched": bool,  # True if counts match and spot-checks pass
            "srt_count": int,  # Subtitle count in .srt file
            "json_count": int,  # Subtitle count in JSON file
            "spot_check_results": List[Dict]  # Results of 5 random spot-checks
        }
    """
    # Load JSON to detect language from metadata
    with open(json_filepath, "r", encoding="utf-8") as f:
        json_data = json.load(f)
    
    language = json_data.get("metadata", {}).get("language_code", "en")
    
    logger.info(f"Validating {language} subtitles: {srt_filepath} vs {json_filepath}")

    # Count subtitle entries in original .srt file (with language-aware encoding detection)
    srt_subtitles, _ = _open_srt_with_encoding_detection(srt_filepath, expected_language=language)

    srt_count = len(srt_subtitles)

    # Count subtitle entries in parsed JSON file (already loaded above)
    json_count = json_data["metadata"]["total_subtitles"]

    # Compare counts: should match
    counts_match = srt_count == json_count

    logger.info(f"Count comparison: SRT={srt_count}, JSON={json_count}, Match={counts_match}")

    # Spot-check 5 random subtitle entries
    spot_check_results: List[Dict[str, Any]] = []
    if json_count > 0:
        # Select 5 random indices (or all if less than 5)
        num_checks = min(5, json_count)
        random_indices = random.sample(range(json_count), num_checks)

        validated_count = 0

        for list_idx in random_indices:
            # Extract parsed subtitle from JSON file (0-based list index)
            parsed_subtitle = json_data["subtitles"][list_idx]
            parsed_index = parsed_subtitle["subtitle_index"]

            # Find corresponding subtitle in .srt file (pysrt uses 1-based index)
            # pysrt subtitles are accessed by list index (0-based), not subtitle.index
            if list_idx < len(srt_subtitles):
                srt_subtitle = srt_subtitles[list_idx]
                original_text = srt_subtitle.text
                original_start = srt_subtitle.start.ordinal / 1000.0

                # Verify: subtitle_index matches, start_time matches (within 0.1s tolerance),
                # dialogue_text matches (after cleaning)
                index_match = srt_subtitle.index == parsed_subtitle["subtitle_index"]
                time_match = abs(original_start - parsed_subtitle["start_time"]) < 0.1
                text_match = clean_dialogue_text(original_text) == parsed_subtitle["dialogue_text"]

                check_passed = index_match and time_match and text_match

                if check_passed:
                    validated_count += 1

                spot_check_results.append(
                    {
                        "index": parsed_index,
                        "index_match": index_match,
                        "time_match": time_match,
                        "text_match": text_match,
                        "passed": check_passed,
                    }
                )
            else:
                logger.warning(f"Index {list_idx} out of range for SRT file")
                spot_check_results.append(
                    {
                        "index": parsed_index,
                        "index_match": False,
                        "time_match": False,
                        "text_match": False,
                        "passed": False,
                    }
                )

        logger.info(f"Validation: {validated_count}/{num_checks} entries verified")
    else:
        logger.warning("No subtitles to validate")

    matched = counts_match and all(
        result["passed"] for result in spot_check_results
    )

    return {
        "matched": matched,
        "srt_count": srt_count,
        "json_count": json_count,
        "spot_check_results": spot_check_results,
    }


def main() -> None:
    """
    Main entry point for subtitle parsing script.

    Parses .srt subtitle files from input directory and saves structured
    JSON output to processed directory.
    """
    parser = argparse.ArgumentParser(
        description="Parse .srt subtitle files into structured JSON data"
    )
    parser.add_argument(
        "--directory",
        type=str,
        default="data/raw/subtitles",
        help="Directory containing .srt subtitle files (default: data/raw/subtitles)",
    )
    parser.add_argument(
        "--films",
        type=str,
        nargs="+",
        help="Optional list of film slugs to process (e.g., spirited_away_en princess_mononoke_en)",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Enable validation after parsing (compare .srt vs JSON)",
    )
    parser.add_argument(
        "--language",
        type=str,
        choices=["en", "ja", "fr", "es", "nl", "ar", "all"],
        default="en",
        help="Language to process: 'en' (English, default), 'ja' (Japanese), 'fr' (French), 'es' (Spanish), 'nl' (Dutch), 'ar' (Arabic), or 'all' (all six languages)",
    )

    args = parser.parse_args()

    # Validate language parameter value (should be handled by choices, but double-check)
    if args.language not in ["en", "ja", "fr", "es", "nl", "ar", "all"]:
        logger.error(f"Invalid language parameter: {args.language}. Must be 'en', 'ja', 'fr', 'es', 'nl', 'ar', or 'all'")
        sys.exit(1)

    logger.info(f"Processing language: {args.language}")

    subtitle_dir = Path(args.directory)

    # Validate directory exists
    if not subtitle_dir.exists():
        logger.error(f"Subtitle directory does not exist: {subtitle_dir}")
        sys.exit(1)

    if not subtitle_dir.is_dir():
        logger.error(f"Path is not a directory: {subtitle_dir}")
        sys.exit(1)

    try:
        # Process all subtitle files with language parameter
        results = process_all_subtitles(subtitle_dir, args.films, language=args.language)

        # Print summary
        successful = [r for r in results if r["success"]]
        failed = [r for r in results if not r["success"]]

        logger.info(f"Processing complete: {len(successful)} successful, {len(failed)} failed")

        # Run validation if requested
        if args.validate:
            logger.info("Running validation checks...")
            validation_results = []

            for result in successful:
                if result["output_path"]:
                    # Find corresponding .srt file
                    film_slug = result["film_slug"]
                    srt_filepath = subtitle_dir / f"{film_slug}.srt"

                    if srt_filepath.exists():
                        try:
                            validation = validate_parsed_subtitles(
                                str(srt_filepath), result["output_path"]
                            )
                            validation_results.append(
                                {
                                    "film_slug": film_slug,
                                    "matched": validation["matched"],
                                    "srt_count": validation["srt_count"],
                                    "json_count": validation["json_count"],
                                }
                            )
                        except Exception as e:
                            logger.error(f"Validation failed for {film_slug}: {e}")
                    else:
                        logger.warning(f"SRT file not found for validation: {srt_filepath}")

            # Print validation summary
            if validation_results:
                all_matched = all(r["matched"] for r in validation_results)
                logger.info(f"Validation complete: {sum(1 for r in validation_results if r['matched'])}/{len(validation_results)} files passed")

                if not all_matched:
                    logger.warning("Some files failed validation checks")
                    for result in validation_results:
                        if not result["matched"]:
                            logger.warning(
                                f"  {result['film_slug']}: SRT={result['srt_count']}, "
                                f"JSON={result['json_count']}"
                            )

        # Exit with error code if any failures
        if failed:
            logger.error(f"Failed to process {len(failed)} files")
            sys.exit(1)

        sys.exit(0)

    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

