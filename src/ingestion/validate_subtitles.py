#!/usr/bin/env python3
"""
Subtitle validation script for Studio Ghibli subtitle files.

Validates .srt format integrity and extracts statistics for subtitle files.
Implements validation rules from Story 1.4:
- Sequential numbering
- Timestamp format validation
- Non-empty text content
- Statistics extraction (entry count, word count, duration)

Usage:
    python src/ingestion/validate_subtitles.py
    python src/ingestion/validate_subtitles.py --directory data/raw/subtitles/
    python src/ingestion/validate_subtitles.py --verbose
    python src/ingestion/validate_subtitles.py --output validation_report.json
"""

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Dict, List, Any, Optional

# Regex patterns for .srt validation
SUBTITLE_NUMBER_PATTERN = re.compile(r'^\d+$')
TIMESTAMP_PATTERN = re.compile(r'^\d{2}:\d{2}:\d{2},\d{3} --> \d{2}:\d{2}:\d{2},\d{3}$')
TIME_EXTRACT_PATTERN = re.compile(r'(\d{2}):(\d{2}):(\d{2}),(\d{3})')

# Default directory for subtitle files
DEFAULT_SUBTITLE_DIR = Path("data/raw/subtitles")


def validate_srt_format(file_path: str) -> Dict[str, Any]:
    """
    Validate .srt subtitle file format integrity.

    Checks for:
    - Sequential subtitle numbering (1, 2, 3, ...)
    - Valid timestamp format (HH:MM:SS,mmm --> HH:MM:SS,mmm)
    - Non-empty text content for each subtitle entry

    Args:
        file_path: Absolute or relative path to .srt file

    Returns:
        Validation result dictionary with keys:
        - valid (bool): True if all checks pass
        - errors (List[str]): List of error messages (empty if valid)
        - file_name (str): Name of file validated
        - entry_count (int): Number of subtitle entries found

    Raises:
        FileNotFoundError: If file does not exist at specified path
        UnicodeDecodeError: If file encoding cannot be determined

    Example:
        >>> result = validate_srt_format("data/raw/subtitles/spirited_away_en.srt")
        >>> result["valid"]
        True
        >>> result["entry_count"]
        1158
    """
    file_path_obj = Path(file_path)

    if not file_path_obj.exists():
        raise FileNotFoundError(f"Subtitle file not found: {file_path}")

    errors = []
    entry_count = 0

    # Try UTF-8 first, fallback to Latin-1
    content = None
    encoding_used = "utf-8"

    try:
        content = file_path_obj.read_text(encoding='utf-8')
    except UnicodeDecodeError:
        try:
            content = file_path_obj.read_text(encoding='latin-1')
            encoding_used = "latin-1"
        except Exception as e:
            raise UnicodeDecodeError(
                'unknown', b'', 0, 0,
                f"Cannot decode file {file_path}: {e}"
            )

    lines = content.split('\n')

    # Parse subtitle entries
    i = 0
    expected_number = 1

    while i < len(lines):
        line = lines[i].strip()

        # Skip empty lines
        if not line:
            i += 1
            continue

        # Check if line is a subtitle number
        if SUBTITLE_NUMBER_PATTERN.match(line):
            subtitle_num = int(line)

            # Validate sequential numbering
            if subtitle_num != expected_number:
                errors.append(
                    f"Line {i+1}: Non-sequential subtitle number. "
                    f"Expected {expected_number}, got {subtitle_num}"
                )

            expected_number += 1
            entry_count += 1

            # Check next line for timestamp
            if i + 1 >= len(lines):
                errors.append(f"Line {i+1}: Subtitle {subtitle_num} missing timestamp")
                break

            timestamp_line = lines[i + 1].strip()

            if not TIMESTAMP_PATTERN.match(timestamp_line):
                errors.append(
                    f"Line {i+2}: Invalid timestamp format: '{timestamp_line}'"
                )

            # Check for text content (at least one non-empty line after timestamp)
            has_text = False
            j = i + 2

            while j < len(lines):
                text_line = lines[j].strip()

                if not text_line:
                    break  # End of this subtitle entry

                if text_line:
                    has_text = True
                    break

                j += 1

            if not has_text:
                errors.append(f"Line {i+1}: Subtitle {subtitle_num} has no text content")

            # Move past this entry
            i += 2
            while i < len(lines) and lines[i].strip():
                i += 1
        else:
            i += 1

    return {
        "valid": len(errors) == 0,
        "errors": errors,
        "file_name": file_path_obj.name,
        "entry_count": entry_count,
        "encoding": encoding_used,
    }


def extract_subtitle_stats(file_path: str) -> Dict[str, Any]:
    """
    Extract statistics from .srt subtitle file.

    Args:
        file_path: Absolute or relative path to .srt file

    Returns:
        Statistics dictionary with keys:
        - file_name (str): Name of file
        - entry_count (int): Number of subtitle entries
        - word_count (int): Total words in all dialogue
        - time_span_minutes (float): Duration from first to last subtitle
        - film_slug (str): Extracted from file name

    Example:
        >>> stats = extract_subtitle_stats("data/raw/subtitles/spirited_away_en.srt")
        >>> stats["entry_count"]
        1158
        >>> stats["word_count"]
        8935
        >>> stats["time_span_minutes"]
        124.8
    """
    file_path_obj = Path(file_path)

    if not file_path_obj.exists():
        raise FileNotFoundError(f"Subtitle file not found: {file_path}")

    # Try UTF-8 first, fallback to Latin-1
    try:
        content = file_path_obj.read_text(encoding='utf-8')
    except UnicodeDecodeError:
        content = file_path_obj.read_text(encoding='latin-1')

    lines = content.split('\n')

    entry_count = 0
    word_count = 0
    first_timestamp_ms = None
    last_timestamp_ms = None

    i = 0
    while i < len(lines):
        line = lines[i].strip()

        if not line:
            i += 1
            continue

        # Check if subtitle number
        if SUBTITLE_NUMBER_PATTERN.match(line):
            entry_count += 1

            # Get timestamp from next line
            if i + 1 < len(lines):
                timestamp_line = lines[i + 1].strip()

                # Extract start timestamp
                match = TIME_EXTRACT_PATTERN.search(timestamp_line)
                if match:
                    hours = int(match.group(1))
                    minutes = int(match.group(2))
                    seconds = int(match.group(3))
                    milliseconds = int(match.group(4))

                    total_ms = (
                        hours * 3600000 +
                        minutes * 60000 +
                        seconds * 1000 +
                        milliseconds
                    )

                    if first_timestamp_ms is None:
                        first_timestamp_ms = total_ms

                    last_timestamp_ms = total_ms

            # Count words in text content
            j = i + 2
            while j < len(lines):
                text_line = lines[j].strip()

                if not text_line:
                    break

                # Remove HTML tags for word counting
                clean_text = re.sub(r'<[^>]+>', '', text_line)
                words = clean_text.split()
                word_count += len(words)

                j += 1

            i = j
        else:
            i += 1

    # Calculate time span
    time_span_minutes = 0.0
    if first_timestamp_ms is not None and last_timestamp_ms is not None:
        time_span_minutes = (last_timestamp_ms - first_timestamp_ms) / 60000.0

    # Extract film slug from file name
    film_slug = file_path_obj.stem

    return {
        "file_name": file_path_obj.name,
        "entry_count": entry_count,
        "word_count": word_count,
        "time_span_minutes": round(time_span_minutes, 1),
        "film_slug": film_slug,
    }


def main() -> int:
    """
    Main execution flow.

    Discovers all .srt files in subtitle directory, validates format,
    extracts statistics, and generates validation report.

    Returns:
        Exit code (0 for success, 1 for failures)
    """
    parser = argparse.ArgumentParser(
        description="Validate Studio Ghibli subtitle files (.srt format)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python src/ingestion/validate_subtitles.py
  python src/ingestion/validate_subtitles.py --directory data/raw/subtitles/
  python src/ingestion/validate_subtitles.py --verbose
  python src/ingestion/validate_subtitles.py --output report.json
        """
    )

    parser.add_argument(
        '--directory',
        type=Path,
        default=DEFAULT_SUBTITLE_DIR,
        help=f'Directory containing subtitle files (default: {DEFAULT_SUBTITLE_DIR})'
    )

    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging with detailed validation results'
    )

    parser.add_argument(
        '--output',
        type=Path,
        help='Save validation report as JSON to specified file'
    )

    args = parser.parse_args()

    subtitle_dir = args.directory

    if not subtitle_dir.exists():
        print(f"❌ Error: Directory not found: {subtitle_dir}")
        return 1

    # Discover .srt files
    srt_files = sorted(subtitle_dir.glob("*.srt"))

    if not srt_files:
        print(f"⚠️  Warning: No .srt files found in {subtitle_dir}")
        return 0

    print("\n" + "="*80)
    print("🎬 Validating Subtitle Files")
    print("="*80)
    print(f"Directory: {subtitle_dir}")
    print(f"Files found: {len(srt_files)}")
    print("="*80 + "\n")

    valid_count = 0
    invalid_count = 0
    results = []

    for srt_file in srt_files:
        try:
            # Validate format
            validation_result = validate_srt_format(str(srt_file))

            # Extract statistics
            stats = extract_subtitle_stats(str(srt_file))

            # Combine results
            result = {**validation_result, **stats}
            results.append(result)

            if result["valid"]:
                valid_count += 1
                print(f"📄 {result['file_name']}")
                print(f"   ✓ Valid format")
                print(f"   - Entries: {result['entry_count']}")
                print(f"   - Words: {result['word_count']}")
                print(f"   - Duration: {result['time_span_minutes']} minutes")
                print(f"   - Encoding: {result['encoding']}")

                if args.verbose and result.get('errors'):
                    print(f"   - Warnings: {len(result['errors'])}")
                    for error in result['errors'][:3]:
                        print(f"     • {error}")
            else:
                invalid_count += 1
                print(f"📄 {result['file_name']}")
                print(f"   ✗ Invalid format")
                print(f"   - Errors: {len(result['errors'])}")

                for error in result['errors'][:5]:
                    print(f"     • {error}")

                if len(result['errors']) > 5:
                    print(f"     ... and {len(result['errors']) - 5} more errors")

            print()

        except Exception as e:
            invalid_count += 1
            print(f"📄 {srt_file.name}")
            print(f"   ✗ Failed to validate: {e}")
            print()

            results.append({
                "file_name": srt_file.name,
                "valid": False,
                "errors": [str(e)],
                "entry_count": 0,
                "word_count": 0,
                "time_span_minutes": 0.0,
                "film_slug": srt_file.stem,
            })

    # Print summary
    print("="*80)
    print("📊 Validation Summary")
    print("="*80)
    print(f"Total files: {len(srt_files)}")
    print(f"✓ Valid: {valid_count}")
    print(f"✗ Invalid: {invalid_count}")
    print(f"Success rate: {valid_count/len(srt_files)*100:.1f}%")
    print("="*80 + "\n")

    # Save report if requested
    if args.output:
        report = {
            "directory": str(subtitle_dir),
            "total_files": len(srt_files),
            "valid_count": valid_count,
            "invalid_count": invalid_count,
            "files": results,
        }

        args.output.write_text(json.dumps(report, indent=2))
        print(f"💾 Report saved to: {args.output}\n")

    return 0 if invalid_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
