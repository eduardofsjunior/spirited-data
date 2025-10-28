"""
Kaggle Studio Ghibli Films CSV Data Loading Module.

This module loads and validates the Kaggle Studio Ghibli Films CSV dataset,
performs data cleaning, type conversions, and cross-references with Ghibli API data.

Example:
    $ python src/ingestion/load_kaggle_data.py
    $ python src/ingestion/load_kaggle_data.py --input data/raw/kaggle/studio_ghibli_films.csv
    $ python src/ingestion/load_kaggle_data.py --verbose
"""

import argparse
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from src.shared.exceptions import DataValidationError

# Constants
DEFAULT_INPUT_PATH = "data/raw/kaggle/studio_ghibli_films.csv"
DEFAULT_OUTPUT_PATH = "data/processed/kaggle_cleaned.csv"
SUMMARY_OUTPUT_PATH = "data/processed/kaggle_validation_summary.json"
API_FILMS_PATH = "data/raw/ghibli_api_cache/films.json"

# Required columns in the Kaggle CSV (actual structure)
REQUIRED_COLUMNS = ["Name", "Year", "Director"]

# Title variations mapping (handles full vs short names)
# Key: normalized Kaggle title → Value: normalized API title
TITLE_VARIATIONS = {
    "the secret world of arrietty": "arrietty",
}

# Known films not in both datasets (for documentation)
KNOWN_EXCLUSIONS = {
    "kaggle_only": [
        "The Boy and the Heron (2023) - Too recent, not in Ghibli API at time of data collection",
        "Nausicaä of the Valley of the Wind (1984) - Pre-Studio Ghibli film, not in official API",
        "Ocean Waves (1993) - TV movie, not included in Ghibli API's main film catalog",
    ],
    "api_only": [
        "Earwig and the Witch (2020) - Not in Kaggle dataset (dataset cutoff date)",
        "The Red Turtle (2016) - Co-production with Dutch studio, not in Kaggle dataset",
    ],
    "explanation": {
        "kaggle_dataset_scope": "Community-curated dataset may include pre-Ghibli works and TV movies",
        "api_scope": "Official Ghibli API focuses on core theatrical Studio Ghibli productions",
        "temporal_mismatch": "Datasets collected at different times (Kaggle: ~2023, API: varies)",
    }
}

# Logger
logger = logging.getLogger(__name__)


def load_kaggle_csv(file_path: str) -> pd.DataFrame:
    """
    Load Kaggle Studio Ghibli Films CSV with encoding handling.

    Tries multiple encodings (UTF-8, Latin-1, CP1252) with fallback logic
    to handle various CSV encodings and BOM markers.

    Args:
        file_path: Path to Kaggle CSV file

    Returns:
        DataFrame with raw CSV data

    Raises:
        FileNotFoundError: If CSV file does not exist
        ValueError: If CSV cannot be decoded with any encoding
        pd.errors.EmptyDataError: If CSV is empty

    Example:
        >>> df = load_kaggle_csv("data/raw/kaggle/studio_ghibli_films.csv")
        >>> df.shape
        (41, 10)
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(
            f"Kaggle CSV not found at {file_path}\n"
            f"Download from: https://www.kaggle.com/datasets/priyankapanga/studio-ghibli-films-dataset\n"
            f"Place at: {file_path}"
        )

    encodings = ["utf-8", "latin-1", "cp1252"]
    logger.info(f"Loading Kaggle CSV from {file_path}")

    for encoding in encodings:
        try:
            df = pd.read_csv(file_path, encoding=encoding)
            logger.info(f"✓ Loaded {len(df)} films from Kaggle CSV (encoding: {encoding})")
            return df
        except UnicodeDecodeError:
            logger.debug(f"Failed to decode with {encoding}, trying next encoding...")
            continue
        except pd.errors.EmptyDataError:
            logger.error(f"CSV file is empty: {file_path}")
            raise

    raise ValueError(
        f"Could not decode {file_path} with any of these encodings: {encodings}"
    )


def validate_required_columns(df: pd.DataFrame) -> bool:
    """
    Validate that all required columns exist in DataFrame.

    Args:
        df: DataFrame to validate

    Returns:
        True if all required columns present

    Raises:
        DataValidationError: If any required columns are missing
    """
    missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]

    if missing_columns:
        raise DataValidationError(
            f"Missing required columns: {missing_columns}. "
            f"Found columns: {list(df.columns)}"
        )

    logger.info(f"✓ All required columns present: {REQUIRED_COLUMNS}")
    return True


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle missing values in DataFrame with appropriate fill strategies.

    Numeric fields (Budget, Revenue, Year) filled with 0.
    String fields (Name, Director, Screenplay, Genre 1/2/3) filled with "Unknown".

    Args:
        df: DataFrame with potential missing values

    Returns:
        DataFrame with missing values handled

    Example:
        >>> df = pd.DataFrame({"Name": ["Film A", None], "Year": [2000, None]})
        >>> cleaned = handle_missing_values(df)
        >>> cleaned["Name"].iloc[1]
        'Unknown'
        >>> cleaned["Year"].iloc[1]
        0
    """
    # Count missing values before handling
    missing_before = df.isnull().sum()
    missing_counts = {col: count for col, count in missing_before.items() if count > 0}

    if missing_counts:
        logger.info(f"Missing values before handling: {missing_counts}")

    # Fill numeric fields with 0
    numeric_columns = ["Budget", "Revenue", "Year"]
    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    # Fill string fields with "Unknown"
    string_columns = ["Name", "Director", "Screenplay", "Genre 1", "Genre 2", "Genre 3", "Duration"]
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].fillna("Unknown")

    missing_after = df.isnull().sum()
    total_filled = (missing_before - missing_after).sum()

    logger.info(f"✓ Filled {total_filled} missing values")

    return df


def clean_currency(value: Any) -> float:
    """
    Remove currency symbols and convert to float.

    Args:
        value: Currency string (e.g., "$289900000", "10000000.00")

    Returns:
        Cleaned float value, or 0.0 if conversion fails
    """
    if pd.isna(value) or value == "" or value == "Unknown":
        return 0.0

    try:
        # Remove $, commas, spaces
        cleaned = str(value).replace("$", "").replace(",", "").strip()
        return float(cleaned)
    except (ValueError, AttributeError):
        logger.warning(f"Could not convert '{value}' to float, using 0.0")
        return 0.0


def safe_int_convert(value: Any, default: int = 0) -> int:
    """
    Convert value to int with fallback to default.

    Args:
        value: Value to convert
        default: Default value if conversion fails

    Returns:
        Integer value or default
    """
    if pd.isna(value) or value == "" or value == "Unknown":
        return default

    try:
        return int(float(value))  # Handle "2001.0" strings
    except (ValueError, TypeError, AttributeError):
        logger.warning(f"Could not convert '{value}' to int, using {default}")
        return default


def convert_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert DataFrame columns to appropriate data types.

    - Year → int
    - Budget → float (cleaned currency)
    - Revenue → float (cleaned currency)

    Args:
        df: DataFrame with raw types

    Returns:
        DataFrame with proper types
    """
    # Convert Year to integer
    if "Year" in df.columns:
        df["Year"] = df["Year"].apply(safe_int_convert)
        logger.debug(f"✓ Converted Year to int")

    # Convert Budget to float (clean currency)
    if "Budget" in df.columns:
        df["Budget"] = df["Budget"].apply(clean_currency)
        logger.debug(f"✓ Converted Budget to float")

    # Convert Revenue to float (clean currency)
    if "Revenue" in df.columns:
        df["Revenue"] = df["Revenue"].apply(clean_currency)
        logger.debug(f"✓ Converted Revenue to float")

    logger.info(f"✓ Data type conversions completed")

    return df


def normalize_title(title: str) -> str:
    """
    Normalize film title for comparison.

    Handles real-world data quality issues found in Kaggle dataset:
    - Embedded newlines: "Spirited Away\\n       (2001)"
    - Year suffixes with/without parentheses: "(2001)" or just "2001"
    - Extra whitespace and mixed casing
    - Special characters (Nausicaä → nausicaa)

    Args:
        title: Film title (potentially malformed)

    Returns:
        Normalized title (lowercase, no year, no extra whitespace)

    Examples:
        >>> normalize_title("Spirited Away\\n       (2001)")
        'spirited away'
        >>> normalize_title("Ocean Waves\\n       (1994)")
        'ocean waves'
        >>> normalize_title("The Wind Rises (2013)")
        'the wind rises'
    """
    if pd.isna(title):
        return ""

    # Step 1: Remove newlines and collapse whitespace
    normalized = " ".join(str(title).split())

    # Step 2: Remove year in parentheses at the end: "(2001)"
    normalized = re.sub(r'\s*\(\d{4}\)\s*$', '', normalized)

    # Step 3: Remove standalone year at the end
    # Matches patterns like "Ocean Waves 1994" after removing newlines
    normalized = re.sub(r'\s+\d{4}\s*$', '', normalized)

    # Step 4: Convert to lowercase
    normalized = normalized.lower()

    # Step 5: Remove common special characters that cause mismatches
    # Replace special chars with ASCII equivalents: ä→a, é→e, etc.
    special_char_map = str.maketrans({
        'ä': 'a', 'ö': 'o', 'ü': 'u',
        'é': 'e', 'è': 'e', 'ê': 'e',
        'à': 'a', 'â': 'a',
        'î': 'i', 'ï': 'i',
        'ç': 'c',
    })
    normalized = normalized.translate(special_char_map)

    return normalized.strip()


def cross_reference_with_ghibli_api(kaggle_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Cross-reference Kaggle titles with Ghibli API films.

    Performs intelligent title matching with:
    - Normalization (newlines, years, special chars)
    - Title variation mapping (full vs short names)
    - Known exclusions documentation

    Args:
        kaggle_df: DataFrame with Kaggle film data

    Returns:
        Match report dictionary with:
        - matched_count: Number of films found in both sources
        - total_kaggle: Total films in Kaggle CSV
        - total_api: Total films from API
        - match_percentage: Percentage of API films matched
        - kaggle_only: List of films only in Kaggle
        - api_only: List of films only in API
        - variations_used: Number of title variations applied

    Raises:
        FileNotFoundError: If API films.json not found
    """
    if not os.path.exists(API_FILMS_PATH):
        raise FileNotFoundError(
            f"Ghibli API films cache not found at {API_FILMS_PATH}. "
            f"Run Story 1.2 (fetch_ghibli_api.py) first."
        )

    logger.info(f"Loading Ghibli API films from {API_FILMS_PATH}")

    with open(API_FILMS_PATH, "r", encoding="utf-8") as f:
        api_films = json.load(f)

    # Normalize titles for comparison
    api_titles = {normalize_title(film["title"]): film["title"] for film in api_films}
    kaggle_titles = {
        normalize_title(title): title for title in kaggle_df["Name"]
    }

    # Step 1: Direct matches after normalization
    matched = set(api_titles.keys()) & set(kaggle_titles.keys())

    # Step 2: Apply title variations mapping for known cases
    variations_used = 0
    kaggle_remaining = set(kaggle_titles.keys()) - matched
    api_remaining = set(api_titles.keys()) - matched

    for kaggle_norm, api_norm in TITLE_VARIATIONS.items():
        if kaggle_norm in kaggle_remaining and api_norm in api_remaining:
            matched.add(api_norm)  # Add to matched set using API normalized form
            kaggle_remaining.remove(kaggle_norm)
            api_remaining.remove(api_norm)
            variations_used += 1
            logger.debug(
                f"Applied title variation: '{kaggle_titles[kaggle_norm]}' → "
                f"'{api_titles[api_norm]}'"
            )

    # Step 3: Calculate final unmatched
    kaggle_only = kaggle_remaining
    api_only = api_remaining

    match_percentage = (len(matched) / len(api_titles)) * 100 if api_titles else 0

    report = {
        "matched_count": len(matched),
        "total_kaggle": len(kaggle_titles),
        "total_api": len(api_titles),
        "match_percentage": round(match_percentage, 1),
        "kaggle_only": sorted([kaggle_titles[t] for t in kaggle_only]),
        "api_only": sorted([api_titles[t] for t in api_only]),
        "variations_used": variations_used,
        "known_exclusions": KNOWN_EXCLUSIONS,
    }

    logger.info(
        f"✓ Cross-reference: {report['matched_count']}/{report['total_api']} films matched "
        f"({report['match_percentage']:.1f}%)"
    )

    if variations_used > 0:
        logger.info(f"Applied {variations_used} title variation(s)")

    if report["kaggle_only"]:
        logger.info(f"Films only in Kaggle: {report['kaggle_only']}")
    if report["api_only"]:
        logger.info(f"Films only in API: {report['api_only']}")

    return report


def save_cleaned_data(df: pd.DataFrame, output_path: str, cross_ref: Dict[str, Any]) -> None:
    """
    Save cleaned DataFrame and validation summary.

    Exports cleaned CSV and JSON summary with metadata.

    Args:
        df: Cleaned DataFrame
        output_path: Path for cleaned CSV
        cross_ref: Cross-reference report
    """
    # Ensure output directory exists
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # Save cleaned CSV
    df.to_csv(output_path, index=False, encoding="utf-8")
    logger.info(f"✓ Saved cleaned data to {output_path}")

    # Create validation summary
    summary = {
        "source_file": DEFAULT_INPUT_PATH,
        "processed_at": datetime.now().isoformat(),
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": list(df.columns),
        "data_types": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "cross_reference": cross_ref,
    }

    # Save validation summary
    summary_path = Path(SUMMARY_OUTPUT_PATH)
    summary_path.parent.mkdir(parents=True, exist_ok=True)

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    logger.info(f"✓ Saved validation summary to {summary_path}")


def main() -> None:
    """
    Main execution flow for Kaggle CSV data loading pipeline.

    Orchestrates all steps:
    1. Load CSV
    2. Validate columns
    3. Handle missing values
    4. Convert data types
    5. Cross-reference with API
    6. Save cleaned data
    """
    # Load CSV
    df = load_kaggle_csv(DEFAULT_INPUT_PATH)

    # Validate required columns
    validate_required_columns(df)

    # Handle missing values
    df = handle_missing_values(df)

    # Convert data types
    df = convert_data_types(df)

    # Cross-reference with Ghibli API
    cross_ref_report = cross_reference_with_ghibli_api(df)

    # Save cleaned data and validation summary
    save_cleaned_data(df, DEFAULT_OUTPUT_PATH, cross_ref_report)

    logger.info(
        f"✓ Pipeline complete: Processed {len(df)} films, "
        f"{cross_ref_report['matched_count']} matches with Ghibli API"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load and validate Kaggle Studio Ghibli Films CSV dataset"
    )
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT_PATH,
        help=f"Input CSV file path (default: {DEFAULT_INPUT_PATH})",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT_PATH,
        help=f"Output cleaned CSV path (default: {DEFAULT_OUTPUT_PATH})",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Update paths if provided
    if args.input != DEFAULT_INPUT_PATH:
        globals()["DEFAULT_INPUT_PATH"] = args.input
    if args.output != DEFAULT_OUTPUT_PATH:
        globals()["DEFAULT_OUTPUT_PATH"] = args.output

    main()
