"""
Multilingual emotion analysis module.

This module provides functions for performing emotion analysis on parsed subtitle
dialogue using HuggingFace transformers with the AnasAlokla/multilingual_go_emotions
model. Supports 5 languages (EN, FR, ES, NL, AR) with 28 GoEmotions labels.
"""

# Standard library imports
import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

# Third-party imports
import duckdb
import pandas as pd

# Local imports
from src.shared.config import LOG_LEVEL
from transformers import pipeline

# Validation buffer for subtitle timing variations (Story 3.6.4)
# Industry-standard tolerance for timing drift across different film cuts/versions.
# Analysis showed 12 of 14 validation failures had overruns < 6 minutes (legitimate drift).
# Only major outliers (Earwig: 23+ min) indicated actual data quality issues.
# See: docs/stories/3.6.4-regeneration-report.md
VALIDATION_BUFFER_MINUTES = 10.0

# Set up logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Constants
GOEMOTIONS_LABELS = [
    "admiration",
    "amusement",
    "anger",
    "annoyance",
    "approval",
    "caring",
    "confusion",
    "curiosity",
    "desire",
    "disappointment",
    "disapproval",
    "disgust",
    "embarrassment",
    "excitement",
    "fear",
    "gratitude",
    "grief",
    "joy",
    "love",
    "nervousness",
    "optimism",
    "pride",
    "realization",
    "relief",
    "remorse",
    "sadness",
    "surprise",
    "neutral",
]

SUPPORTED_LANGUAGES = ["en", "fr", "es", "nl", "ar"]


class DataValidationError(Exception):
    """Raised when emotion data validation fails."""

    pass


def load_emotion_model() -> pipeline:
    """
    Load HuggingFace emotion classification model.

    Loads the AnasAlokla/multilingual_go_emotions model with bert-base-multilingual-cased
    backbone. Model is cached to ~/.cache/huggingface/ on first download.

    Returns:
        HuggingFace text-classification pipeline configured for multi-label
        emotion classification (returns all 28 GoEmotions labels).

    Raises:
        OSError: If model download fails due to network error.
        RuntimeError: If model loading fails.
    """
    try:
        logger.info("Loading emotion model: AnasAlokla/multilingual_go_emotions")
        classifier = pipeline(
            "text-classification",
            model="AnasAlokla/multilingual_go_emotions",
            top_k=None,  # Return all 28 emotion scores (multi-label classification)
        )
        logger.info("Successfully loaded emotion model")
        return classifier
    except OSError as e:
        logger.error(f"Failed to download model (network error): {e}")
        raise
    except RuntimeError as e:
        logger.error(f"Failed to load model: {e}")
        raise


def detect_language_from_filename(filepath: Path) -> str:
    """
    Detect language code from parsed subtitle filename.

    Extracts language code from filename suffix pattern:
    - Standard: {film_slug}_{lang_code}_parsed.json (e.g., spirited_away_en_parsed.json → "en")
    - V2: {film_slug}_{lang_code}_v2_parsed.json (e.g., spirited_away_en_v2_parsed.json → "en")

    Args:
        filepath: Path to parsed JSON file

    Returns:
        ISO 639-1 language code (e.g., "en", "fr", "es", "nl", "ar")

    Raises:
        ValueError: If language code cannot be extracted or is not supported.
    """
    filename = filepath.stem  # Remove .json extension
    # Pattern: {film_slug}_{lang_code}_parsed OR {film_slug}_{lang_code}_v2_parsed
    parts = filename.split("_")

    if len(parts) < 3 or parts[-1] != "parsed":
        raise ValueError(
            f"Invalid filename pattern: expected {{film_slug}}_{{lang_code}}_parsed.json or "
            f"{{film_slug}}_{{lang_code}}_v2_parsed.json, got {filepath.name}"
        )

    # Check if v2 pattern: last 3 parts are "v2_parsed" or just "parsed"
    if len(parts) >= 4 and parts[-2] == "v2":
        # V2 pattern: {film_slug}_{lang_code}_v2_parsed
        lang_code = parts[-3].lower()  # Third-to-last part before "_v2_parsed"
    else:
        # Standard pattern: {film_slug}_{lang_code}_parsed
        lang_code = parts[-2].lower()  # Second-to-last part before "_parsed"

    if lang_code not in SUPPORTED_LANGUAGES:
        raise ValueError(
            f"Unsupported language code: {lang_code}. "
            f"Supported languages: {', '.join(SUPPORTED_LANGUAGES)}"
        )

    logger.info(f"Detected language: {lang_code} from {filepath.name}")
    return lang_code


def analyze_dialogue_emotions(text: str, model: pipeline) -> Dict[str, float]:
    """
    Analyze dialogue text and return emotion scores for 28 GoEmotions labels.

    Performs multi-label emotion classification on dialogue text using HuggingFace
    pipeline. Returns scores (0-1 range) for all 28 GoEmotions labels.

    Args:
        text: Dialogue text from subtitle entry
        model: HuggingFace text-classification pipeline

    Returns:
        Dictionary mapping emotion labels to scores (0-1 range).
        Example: {"admiration": 0.23, "amusement": 0.18, ..., "neutral": 0.12}
        All 28 labels are guaranteed to be present.

    Raises:
        ValueError: If text is empty or model returns unexpected format.
    """
    if not text or not text.strip():
        logger.warning("Empty dialogue text, returning zero emotions")
        return {label: 0.0 for label in GOEMOTIONS_LABELS}

    # Truncate to ~450 words if needed (BERT limit is 512 tokens)
    # Conservative estimate: 450 words ≈ 500 tokens
    words = text.split()
    if len(words) > 450:
        truncated_text = " ".join(words[:450])
        logger.debug(f"Truncated long dialogue from {len(words)} to 450 words")
        text = truncated_text

    # Retry logic for transient model inference failures
    max_retries = 3
    retry_delay = 1.0  # seconds

    for attempt in range(max_retries):
        try:
            # Call HuggingFace pipeline
            results = model(text)

            # Handle nested list structure: model returns [[{...}, {...}]] for single text
            # Extract the inner list if results is a list of lists
            if results and isinstance(results[0], list):
                results = results[0]

            # Parse results: list of dicts with {"label": "admiration", "score": 0.85}
            emotion_scores: Dict[str, float] = {}
            for result in results:
                label = result.get("label", "")
                score = result.get("score", 0.0)
                if label:
                    emotion_scores[label] = float(score)

            # Ensure all 28 labels present (fill missing with 0.0)
            for label in GOEMOTIONS_LABELS:
                emotion_scores.setdefault(label, 0.0)

            # Log max emotion for debugging
            max_emotion = max(emotion_scores.items(), key=lambda x: x[1])
            logger.debug(
                f"Analyzed dialogue: {text[:50]}... → {max_emotion[0]} ({max_emotion[1]:.2f})"
            )

            return emotion_scores

        except Exception as e:
            is_last_attempt = attempt == max_retries - 1
            error_msg = (
                f"Error analyzing dialogue emotions (attempt {attempt + 1}/{max_retries}): {e}"
            )

            if is_last_attempt:
                logger.error(error_msg)
                raise ValueError(
                    f"Failed to analyze emotions after {max_retries} attempts: {e}"
                ) from e
            else:
                # Exponential backoff: 1s, 2s, 4s
                delay = retry_delay * (2**attempt)
                logger.warning(f"{error_msg} - Retrying in {delay}s...")
                time.sleep(delay)


def process_film_subtitles(
    parsed_json_path: Path, model: pipeline, subtitle_version: str = "v1"
) -> List[Dict[str, Any]]:
    """
    Process film subtitle file and analyze emotions for each dialogue entry.

    Loads parsed JSON file, analyzes emotions for each dialogue entry, and groups
    results by minute-level time buckets. Validates that emotion data does not
    extend beyond subtitle duration.

    Args:
        parsed_json_path: Path to parsed subtitle JSON file
        model: HuggingFace emotion classification pipeline
        subtitle_version: Version tag for subtitle source (e.g., "v1", "v2")

    Returns:
        List of dictionaries, each containing:
        - film_slug: Film slug identifier
        - language_code: Language code
        - minute_offset: Integer minute bucket (e.g., 12 for 12th minute)
        - emotions: List of emotion dicts for aggregation
        - dialogue_count: Number of dialogue entries in this minute bucket
        - subtitle_version: Version tag for subtitle source

    Raises:
        DataValidationError: If emotion data extends beyond subtitle duration
                            or required metadata is missing
    """
    logger.info(f"Processing film subtitles: {parsed_json_path.name}")

    # Load parsed JSON file
    with open(parsed_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    metadata = data.get("metadata", {})
    film_slug = metadata.get("film_slug", "")
    language_code = metadata.get("language_code", "")
    subtitles = data.get("subtitles", [])

    if not film_slug:
        raise ValueError(f"Missing film_slug in metadata: {parsed_json_path}")

    # Check for missing duration (AC3)
    total_duration = metadata.get("total_duration")
    if total_duration is None:
        raise DataValidationError(
            f"Missing total_duration in subtitle metadata for {film_slug}_{language_code}. "
            "Re-parse subtitle file."
        )

    # Check for empty subtitles (AC4)
    if not subtitles:
        logger.warning(f"Skipping {film_slug}_{language_code}: No subtitles found")
        return []  # Return empty list, don't raise error

    logger.info(f"Processing {len(subtitles)} subtitles for {film_slug} ({language_code})")

    # Track emotions by minute bucket
    minute_emotions: Dict[int, List[Dict[str, float]]] = {}

    # Process each subtitle entry
    total_subtitles = len(subtitles)
    for idx, subtitle in enumerate(subtitles):
        if (idx + 1) % 100 == 0:
            logger.info(f"Processed {idx + 1}/{total_subtitles} subtitles")

        start_time = subtitle.get("start_time", 0.0)
        dialogue_text = subtitle.get("dialogue_text", "")

        if not dialogue_text:
            continue

        # Calculate minute offset (convert seconds to minute bucket)
        minute_offset = int(start_time // 60)

        # Analyze emotions for this dialogue
        emotions = analyze_dialogue_emotions(dialogue_text, model)

        # Accumulate emotions by minute bucket
        if minute_offset not in minute_emotions:
            minute_emotions[minute_offset] = []
        minute_emotions[minute_offset].append(emotions)

    # Convert to list format for aggregation
    emotion_entries = []
    for minute_offset, emotion_list in minute_emotions.items():
        emotion_entries.append(
            {
                "film_slug": film_slug,
                "language_code": language_code,
                "minute_offset": minute_offset,
                "emotions": emotion_list,
                "dialogue_count": len(emotion_list),
                "subtitle_version": subtitle_version,
            }
        )

    # Validate duration bounds (AC1, AC5)
    if minute_emotions:
        max_minute = max(minute_emotions.keys())
        expected_duration_minutes = total_duration / 60.0

        # Allow buffer for subtitle timing variations across different film cuts/versions
        # (VALIDATION_BUFFER_MINUTES = 10.0, defined at module level)
        if max_minute > expected_duration_minutes + VALIDATION_BUFFER_MINUTES:
            overrun = max_minute - expected_duration_minutes
            error_msg = (
                f"Emotion data extends {overrun:.1f} min beyond subtitle duration. "
                f"Film: {film_slug}_{language_code}, Max minute: {max_minute}, "
                f"Expected duration: {expected_duration_minutes:.1f} min"
            )
            logger.error(f"Validation FAIL: {error_msg}")
            raise DataValidationError(error_msg)
        elif max_minute < expected_duration_minutes - 1.0:
            # Negative overrun is VALID (emotion ends early)
            logger.info(
                f"Emotion data ends {expected_duration_minutes - max_minute:.1f} min "
                f"before subtitle duration (valid) - {film_slug}_{language_code}"
            )
        else:
            # Within bounds
            logger.info(f"Validation PASS: {film_slug}_{language_code}")

    logger.info(f"Processed {total_subtitles} subtitles into {len(emotion_entries)} minute buckets")

    return emotion_entries


def aggregate_emotions_by_minute(
    emotion_entries: List[Dict[str, Any]], smoothing_window: int = 10
) -> pd.DataFrame:
    """
    Aggregate emotion scores by minute with rolling average smoothing.

    Groups emotion entries by minute_offset, calculates average scores per minute,
    and applies configurable rolling average for temporal smoothing (default: 10 minutes).

    Smoothing analysis shows:
    - window=3: 44% noise reduction (previous default)
    - window=7: 75% noise reduction
    - window=10: 82% noise reduction (new default - good balance)
    - window=15: 88% noise reduction (may over-smooth)

    Args:
        emotion_entries: List of emotion entry dicts from process_film_subtitles()
        smoothing_window: Rolling average window size in minutes (default: 10)

    Returns:
        DataFrame with columns:
        - minute_offset: Integer minute bucket
        - dialogue_count: Number of dialogue entries in this minute
        - emotion_admiration, emotion_amusement, ..., emotion_neutral: 28 emotion columns
    """
    if not emotion_entries:
        logger.warning("No emotion entries to aggregate")
        return pd.DataFrame()

    # Convert to DataFrame for easier aggregation
    rows = []
    for entry in emotion_entries:
        minute_offset = entry["minute_offset"]
        dialogue_count = entry["dialogue_count"]
        emotions_list = entry["emotions"]  # List of emotion dicts for this minute

        # Sum all emotion scores for this minute
        emotion_sums: Dict[str, float] = {label: 0.0 for label in GOEMOTIONS_LABELS}
        for emotion_dict in emotions_list:
            for label in GOEMOTIONS_LABELS:
                emotion_sums[label] += emotion_dict.get(label, 0.0)

        # Calculate average scores (divide by dialogue_count)
        row = {"minute_offset": minute_offset, "dialogue_count": dialogue_count}
        for label in GOEMOTIONS_LABELS:
            avg_score = emotion_sums[label] / dialogue_count if dialogue_count > 0 else 0.0
            row[f"emotion_{label}"] = avg_score

        rows.append(row)

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    # Sort by minute_offset for rolling average
    df = df.sort_values("minute_offset").reset_index(drop=True)

    # Apply rolling average for smoothing (centered window)
    emotion_columns = [f"emotion_{label}" for label in GOEMOTIONS_LABELS]
    for col in emotion_columns:
        df[col] = (
            df[col]
            .rolling(window=smoothing_window, center=True, min_periods=1)
            .mean()
            .fillna(df[col])
        )

    total_dialogue = df["dialogue_count"].sum()
    logger.info(f"Aggregated {len(df)} minute buckets with {total_dialogue} dialogue entries")

    return df


def create_emotions_table(conn: duckdb.DuckDBPyConnection, drop_if_exists: bool = False) -> None:
    """
    Create raw.film_emotions table with 28 emotion columns and metadata.

    Creates table with wide format schema (28 emotion columns + metadata).
    Includes indexes for efficient querying by film_id and language_code.
    Adds new metadata columns for subtitle version tracking.

    Args:
        conn: DuckDB connection object
        drop_if_exists: If True, drop existing table. If False, only create if not exists (default).

    Raises:
        Exception: If table creation fails
    """
    logger.info("Creating/updating raw.film_emotions table...")

    try:
        if drop_if_exists:
            # Drop existing table if exists (idempotent loading)
            conn.execute("DROP TABLE IF EXISTS raw.film_emotions")

        # Check if table exists
        table_exists = (
            conn.execute(
                """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'raw' AND table_name = 'film_emotions'
        """
            ).fetchone()[0]
            > 0
        )

        if not table_exists or drop_if_exists:
            # Create table with wide format schema including new metadata columns
            create_table_sql = """
            CREATE TABLE raw.film_emotions (
                film_slug VARCHAR NOT NULL,
                film_id VARCHAR,
                language_code VARCHAR(2) NOT NULL,
                minute_offset INTEGER NOT NULL,
                dialogue_count INTEGER,
                emotion_admiration DOUBLE,
                emotion_amusement DOUBLE,
                emotion_anger DOUBLE,
                emotion_annoyance DOUBLE,
                emotion_approval DOUBLE,
                emotion_caring DOUBLE,
                emotion_confusion DOUBLE,
                emotion_curiosity DOUBLE,
                emotion_desire DOUBLE,
                emotion_disappointment DOUBLE,
                emotion_disapproval DOUBLE,
                emotion_disgust DOUBLE,
                emotion_embarrassment DOUBLE,
                emotion_excitement DOUBLE,
                emotion_fear DOUBLE,
                emotion_gratitude DOUBLE,
                emotion_grief DOUBLE,
                emotion_joy DOUBLE,
                emotion_love DOUBLE,
                emotion_nervousness DOUBLE,
                emotion_optimism DOUBLE,
                emotion_pride DOUBLE,
                emotion_realization DOUBLE,
                emotion_relief DOUBLE,
                emotion_remorse DOUBLE,
                emotion_sadness DOUBLE,
                emotion_surprise DOUBLE,
                emotion_neutral DOUBLE,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                subtitle_version VARCHAR DEFAULT 'v1',
                subtitle_timing_validated BOOLEAN,
                timing_drift_percent DOUBLE,
                PRIMARY KEY (film_slug, language_code, minute_offset)
            );
            """
            conn.execute(create_table_sql)

            # Create indexes for efficient querying
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_film_emotions_film_id "
                "ON raw.film_emotions(film_id);"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_film_emotions_language "
                "ON raw.film_emotions(language_code);"
            )

            logger.info("Created raw.film_emotions table with 28 emotion columns and metadata")
        else:
            # Table exists - add new columns if they don't exist
            logger.info("Table exists, checking for new metadata columns...")

            # Check and add subtitle_version column
            try:
                conn.execute(
                    """
                    ALTER TABLE raw.film_emotions
                    ADD COLUMN IF NOT EXISTS subtitle_version VARCHAR DEFAULT 'v1'
                """
                )
                logger.info("Added subtitle_version column")
            except Exception as e:
                logger.debug(f"Column subtitle_version may already exist: {e}")

            # Check and add subtitle_timing_validated column
            try:
                conn.execute(
                    """
                    ALTER TABLE raw.film_emotions
                    ADD COLUMN IF NOT EXISTS subtitle_timing_validated BOOLEAN
                """
                )
                logger.info("Added subtitle_timing_validated column")
            except Exception as e:
                logger.debug(f"Column subtitle_timing_validated may already exist: {e}")

            # Check and add timing_drift_percent column
            try:
                conn.execute(
                    """
                    ALTER TABLE raw.film_emotions
                    ADD COLUMN IF NOT EXISTS timing_drift_percent DOUBLE
                """
                )
                logger.info("Added timing_drift_percent column")
            except Exception as e:
                logger.debug(f"Column timing_drift_percent may already exist: {e}")

            logger.info("Schema migration complete")

    except Exception as e:
        logger.error(f"Failed to create/update emotions table: {e}")
        raise


def resolve_film_id(
    film_slug: str, conn: duckdb.DuckDBPyConnection, film_name: Optional[str] = None
) -> Optional[str]:
    """
    Resolve film_id from film_slug by querying DuckDB films table.

    Uses film_name from parsed JSON metadata if available (more reliable),
    otherwise extracts film name from slug by removing language suffix and
    converting to title format.

    Args:
        film_slug: Film slug from parsed JSON (e.g., "spirited_away_en")
        conn: DuckDB connection object
        film_name: Optional film name from parsed JSON metadata (preferred)

    Returns:
        Film ID (UUID string) if found, None otherwise
    """
    # Use film_name from metadata if available (more reliable)
    if film_name:
        film_title = film_name
    else:
        # Remove language suffix from slug (e.g., "spirited_away_en" → "spirited_away")
        # Split on last underscore, take prefix
        parts = film_slug.rsplit("_", 1)
        if len(parts) == 2 and parts[1] in SUPPORTED_LANGUAGES:
            base_slug = parts[0]
        else:
            # If no language suffix found, use slug as-is
            base_slug = film_slug

        # Convert slug to title: replace underscores with spaces and title-case
        # e.g., "spirited_away" → "Spirited Away"
        film_title = base_slug.replace("_", " ").title()

    try:
        # Query DuckDB for matching film_id
        result = conn.execute(
            "SELECT id FROM raw.films WHERE LOWER(title) = LOWER(?)",
            [film_title],
        ).fetchone()

        if result:
            film_id = result[0]
            logger.info(f"Resolved {film_slug} → {film_id} (title: {film_title})")
            return film_id
        else:
            logger.warning(f"Could not resolve film_id for {film_slug} (title: {film_title})")
            return None

    except Exception as e:
        logger.error(f"Error resolving film_id for {film_slug}: {e}")
        return None


def load_emotions_to_duckdb(
    film_slug: str,
    film_id: Optional[str],
    language_code: str,
    emotions_df: pd.DataFrame,
    conn: duckdb.DuckDBPyConnection,
    subtitle_version: str = "v1",
    subtitle_timing_validated: Optional[bool] = None,
    timing_drift_percent: Optional[float] = None,
    replace_existing: bool = False,
) -> None:
    """
    Load aggregated emotion data into DuckDB raw.film_emotions table.

    Adds film metadata columns to DataFrame and inserts using pandas integration.
    Handles duplicate key errors gracefully (skips duplicates).
    Supports replacement mode to delete old records before inserting new ones.

    Args:
        film_slug: Film slug (e.g., "spirited_away_en")
        film_id: Film ID from raw.films table (can be None)
        language_code: ISO 639-1 language code (e.g., "en")
        emotions_df: DataFrame with minute_offset, dialogue_count, and 28 emotion columns
        conn: DuckDB connection object
        subtitle_version: Version tag for subtitle source (default: "v1")
        subtitle_timing_validated: Whether subtitle timing was validated (optional)
        timing_drift_percent: Timing drift percentage from validation (optional)
        replace_existing: If True, delete existing records before inserting (default: False)

    Raises:
        Exception: If data loading fails
    """
    if emotions_df.empty:
        logger.warning(f"No emotion data to load for {film_slug} ({language_code})")
        return

    # Add metadata columns to DataFrame
    emotions_df = emotions_df.copy()
    emotions_df["film_slug"] = film_slug
    emotions_df["film_id"] = film_id
    emotions_df["language_code"] = language_code
    emotions_df["subtitle_version"] = subtitle_version
    emotions_df["subtitle_timing_validated"] = subtitle_timing_validated
    emotions_df["timing_drift_percent"] = timing_drift_percent

    # Reorder columns: metadata first, then emotions, then new metadata
    # Note: loaded_at is auto-generated by DuckDB, don't include in DataFrame
    column_order = (
        ["film_slug", "film_id", "language_code", "minute_offset", "dialogue_count"]
        + [f"emotion_{label}" for label in GOEMOTIONS_LABELS]
        + ["subtitle_version", "subtitle_timing_validated", "timing_drift_percent"]
    )
    emotions_df = emotions_df[column_order]

    try:
        # Replacement logic: delete existing records first
        if replace_existing:
            logger.info(
                f"Replacement mode: deleting existing records for {film_slug} ({language_code})"
            )

            delete_sql = """
            DELETE FROM raw.film_emotions
            WHERE film_slug = ? AND language_code = ?
            """
            result = conn.execute(delete_sql, [film_slug, language_code])
            deleted_count = result.fetchone()
            logger.info(f"Deleted {deleted_count if deleted_count else 0} existing records")

        # Register DataFrame as temporary table
        conn.register("emotions_df", emotions_df)

        # Insert data using INSERT SELECT
        # Handle duplicates: use INSERT OR IGNORE to skip duplicates gracefully
        # Explicitly list columns (excluding loaded_at which has DEFAULT)
        column_list = ", ".join(column_order)
        insert_sql = f"""
        INSERT OR IGNORE INTO raw.film_emotions ({column_list})
        SELECT {column_list} FROM emotions_df
        """
        conn.execute(insert_sql)

        # Verify insertion count
        inserted_count = len(emotions_df)
        logger.info(
            f"Loaded {inserted_count} emotion records for {film_slug} ({language_code}) "
            f"[version: {subtitle_version}]"
        )

    except Exception as e:
        # Check if error is due to duplicate key constraint
        error_msg = str(e).lower()
        if "primary key" in error_msg or "duplicate" in error_msg:
            logger.warning(
                f"Duplicate records detected for {film_slug} ({language_code}), skipping"
            )
        else:
            logger.error(f"Failed to load emotions data: {e}")
            raise


def validate_emotion_data(emotions_df: pd.DataFrame, parsed_json_path: Path) -> Dict[str, Any]:
    """
    Validate emotion data quality and consistency.

    Checks emotion scores are in [0, 1] range, all 28 dimensions present,
    and dialogue counts match between DataFrame and parsed JSON.

    Args:
        emotions_df: DataFrame with emotion data
        parsed_json_path: Path to original parsed JSON file

    Returns:
        Dictionary with validation results:
        - valid: bool - Overall validation status
        - dialogue_count_match: bool - Whether dialogue counts match
        - emotion_stats: dict - Min/max/mean scores per emotion dimension
    """
    validation_results: Dict[str, Any] = {
        "valid": True,
        "dialogue_count_match": False,
        "emotion_stats": {},
    }

    if emotions_df.empty:
        logger.warning("Cannot validate empty DataFrame")
        validation_results["valid"] = False
        return validation_results

    # Check all 28 emotion dimensions present
    expected_emotion_columns = [f"emotion_{label}" for label in GOEMOTIONS_LABELS]
    missing_columns = [col for col in expected_emotion_columns if col not in emotions_df.columns]
    if missing_columns:
        logger.error(f"Missing emotion columns: {missing_columns}")
        validation_results["valid"] = False
        return validation_results

    # Check emotion scores are in [0, 1] range
    for col in expected_emotion_columns:
        min_score = emotions_df[col].min()
        max_score = emotions_df[col].max()
        mean_score = emotions_df[col].mean()

        validation_results["emotion_stats"][col] = {
            "min": float(min_score),
            "max": float(max_score),
            "mean": float(mean_score),
        }

        if min_score < 0.0 or max_score > 1.0:
            logger.error(f"Emotion scores out of range for {col}: min={min_score}, max={max_score}")
            validation_results["valid"] = False

    # Load original parsed JSON and count dialogue entries
    try:
        with open(parsed_json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        subtitles = data.get("subtitles", [])
        json_dialogue_count = len(subtitles)

        # Count dialogue entries in DataFrame
        df_dialogue_count = int(emotions_df["dialogue_count"].sum())

        # Compare counts
        if json_dialogue_count == df_dialogue_count:
            validation_results["dialogue_count_match"] = True
            logger.info(
                f"Validation passed: {df_dialogue_count} dialogue entries, "
                f"emotion scores in [0, 1]"
            )
        else:
            logger.warning(
                f"Dialogue count mismatch: JSON={json_dialogue_count}, "
                f"DataFrame={df_dialogue_count}"
            )
            validation_results["dialogue_count_match"] = False
            # Don't fail validation for count mismatch (may be expected due to filtering)

    except Exception as e:
        logger.error(f"Error loading parsed JSON for validation: {e}")
        validation_results["valid"] = False

    return validation_results


def build_subtitle_priority_map(v1_dir: Path, v2_dir: Path) -> Dict[str, tuple[Path, str]]:
    """
    Build priority map of subtitle files (v2 preferred over v1).

    Scans both v1 (standard) and v2 (improved) subtitle directories and builds
    a priority map where v2 files override v1 files for the same film-language
    combination. This ensures highest quality subtitles are used.

    Args:
        v1_dir: Directory containing v1 subtitle files (data/processed/subtitles)
        v2_dir: Directory containing v2 improved subtitle files (data/processed/subtitles_improved)

    Returns:
        Dictionary mapping "{film_slug}_{lang_code}" to tuple of (filepath, version).
        Example: {"spirited_away_en": (Path("...spirited_away_en_v2_parsed.json"), "v2")}

    Example:
        >>> priority_map = build_subtitle_priority_map(
        ...     Path("data/processed/subtitles"),
        ...     Path("data/processed/subtitles_improved")
        ... )
        >>> # The Red Turtle v2 overrides v1
        >>> priority_map["the_red_turtle_en"]
        (Path(".../the_red_turtle_en_v2_parsed.json"), "v2")
    """
    priority_map: Dict[str, tuple[Path, str]] = {}

    # First, add all v1 subtitles
    if v1_dir.exists():
        for v1_file in v1_dir.glob("*_parsed.json"):
            # Exclude v2 files if they're in the same directory
            if "_v2_" not in v1_file.name:
                # Extract film_slug from filename: {film_slug}_{lang}_parsed.json
                # Remove "_parsed" suffix to get "{film_slug}_{lang}"
                film_slug_lang = v1_file.stem.replace("_parsed", "")
                priority_map[film_slug_lang] = (v1_file, "v1")
                logger.debug(f"Found v1 subtitle: {film_slug_lang}")

    # Then, override with v2 if available (v2 takes precedence)
    if v2_dir.exists():
        for v2_file in v2_dir.glob("*_v2_parsed.json"):
            # Extract film_slug from filename: {film_slug}_{lang}_v2_parsed.json
            # Remove "_v2_parsed" suffix to get "{film_slug}_{lang}"
            film_slug_lang = v2_file.stem.replace("_v2_parsed", "")
            priority_map[film_slug_lang] = (v2_file, "v2")
            logger.info(f"Using v2 improved subtitle for {film_slug_lang}")

    # Log v1 files that don't have v2 versions
    v1_count = sum(1 for _, version in priority_map.values() if version == "v1")
    v2_count = sum(1 for _, version in priority_map.values() if version == "v2")
    logger.info(
        f"Subtitle priority map built: {len(priority_map)} total " f"({v1_count} v1, {v2_count} v2)"
    )

    return priority_map


def process_all_films(
    subtitle_dir: Path,
    db_path: Path,
    film_filter: Optional[List[str]] = None,
    language_filter: Optional[List[str]] = None,
    smoothing_window: int = 10,
    subtitle_version: str = "v1",
    replace_existing: bool = False,
    validation_data_path: Optional[Path] = None,
) -> List[Dict[str, Any]]:
    """
    Process all film subtitle files and load emotion data into DuckDB.

    Orchestrates the full pipeline: discovers parsed JSON files, filters by
    film/language, loads emotion model, processes each file, and loads to DuckDB.

    Args:
        subtitle_dir: Directory containing parsed subtitle JSON files
        db_path: Path to DuckDB database file
        film_filter: Optional list of film slugs to process (e.g., ["spirited_away"])
        language_filter: Optional list of language codes (default: all supported languages)
        smoothing_window: Rolling average window size in minutes (default: 10)
        subtitle_version: Version tag for subtitle source (default: "v1")
        replace_existing: If True, delete existing records before inserting (default: False)
        validation_data_path: Path to validation JSON file with timing metrics (optional)

    Returns:
        List of result dictionaries, each containing:
        - film_slug: Film slug processed
        - language_code: Language code
        - success: bool - Whether processing succeeded
        - error_message: str - Error message if failed
        - records_loaded: int - Number of records loaded
        - validation_results: dict - Validation report
    """
    # Default language filter: all supported languages (exclude JA)
    if language_filter is None:
        language_filter = SUPPORTED_LANGUAGES

    logger.info(f"Starting batch emotion analysis from {subtitle_dir}")

    # Load validation data if provided (for timing_drift_percent metadata)
    validation_data: Dict[str, Dict[str, Any]] = {}
    if validation_data_path and validation_data_path.exists():
        try:
            with open(validation_data_path, "r", encoding="utf-8") as f:
                validation_json = json.load(f)

            # Handle both flat dict and results array structure
            if "results" in validation_json:
                # Array structure: {"results": [{film_slug, status, ...}]}
                for result in validation_json["results"]:
                    film_slug = result.get("film_slug")
                    if film_slug:
                        validation_data[film_slug] = result
            else:
                # Flat dict structure: {film_slug: {metrics}}
                for film_slug, metrics in validation_json.items():
                    validation_data[film_slug] = metrics

            logger.info(f"Loaded validation data for {len(validation_data)} films")
        except Exception as e:
            logger.warning(f"Failed to load validation data from {validation_data_path}: {e}")
            logger.info("Continuing without validation metadata")

    # Build priority map with v2 improved subtitles (AC2)
    v2_dir = subtitle_dir.parent / "subtitles_improved"
    priority_map = build_subtitle_priority_map(subtitle_dir, v2_dir)

    # Filter priority map by film and language filters
    filtered_priority_map: Dict[str, tuple[Path, str]] = {}
    for film_slug_lang, (filepath, version) in priority_map.items():
        try:
            lang_code = detect_language_from_filename(filepath)

            # Check language filter
            if lang_code not in language_filter:
                continue

            # Extract base film slug (without language suffix) for film filter
            # Pattern: {film_slug}_{lang_code}
            parts = film_slug_lang.rsplit("_", 1)
            if len(parts) >= 2:
                film_slug_base = parts[0]
                # Check film filter
                if film_filter and film_slug_base not in film_filter:
                    continue

            filtered_priority_map[film_slug_lang] = (filepath, version)

        except ValueError as e:
            logger.debug(f"Skipping file {filepath.name}: {e}")
            continue

    logger.info(f"Processing {len(filtered_priority_map)} files after filtering")

    # Load emotion model once (reused for all films)
    try:
        model = load_emotion_model()
    except Exception as e:
        logger.error(f"Failed to load emotion model: {e}")
        return [
            {
                "film_slug": "ALL",
                "language_code": "ALL",
                "success": False,
                "error_message": f"Model loading failed: {e}",
                "records_loaded": 0,
                "validation_results": {},
            }
        ]

    # Connect to DuckDB and create table
    # Use provided db_path or fall back to config
    try:
        conn = duckdb.connect(str(db_path))
        # Create schemas if they don't exist
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
        conn.execute("CREATE SCHEMA IF NOT EXISTS marts")
        create_emotions_table(conn)
    except Exception as e:
        logger.error(f"Failed to create emotions table: {e}")
        conn.close()
        return [
            {
                "film_slug": "ALL",
                "language_code": "ALL",
                "success": False,
                "error_message": f"Table creation failed: {e}",
                "records_loaded": 0,
                "validation_results": {},
            }
        ]

    # Process each file
    results = []
    success_count = 0
    failed_films: List[tuple[str, str]] = []

    for film_slug_lang, (filepath, version) in filtered_priority_map.items():
        try:
            # Detect language from filename
            language_code = detect_language_from_filename(filepath)

            # Load parsed JSON to get metadata
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)

            metadata = data.get("metadata", {})
            film_slug = metadata.get("film_slug", "")
            film_name = metadata.get("film_name", "")

            if not film_slug:
                raise ValueError(f"Missing film_slug in metadata: {filepath}")

            logger.info(f"Processing {film_slug} ({language_code}) with {version} subtitle...")

            # Process film subtitles → emotion entries (AC3: Pass version parameter)
            emotion_entries = process_film_subtitles(filepath, model, version)

            # Aggregate by minute → DataFrame
            emotions_df = aggregate_emotions_by_minute(emotion_entries, smoothing_window)

            if emotions_df.empty:
                logger.warning(f"No emotion data generated for {film_slug}")
                results.append(
                    {
                        "film_slug": film_slug,
                        "language_code": language_code,
                        "success": False,
                        "error_message": "No emotion data generated",
                        "records_loaded": 0,
                        "validation_results": {},
                    }
                )
                continue

            # Resolve film_id from slug
            film_id = resolve_film_id(film_slug, conn, film_name)

            # Get validation metadata if available
            # Strip language and version suffix to match validation data keys
            # film_slug like "castle_in_the_sky_en_v2" -> "castle_in_the_sky"
            base_slug = film_slug.replace("_en_v2", "").replace("_en", "")

            subtitle_timing_validated = None
            timing_drift_percent = None
            if base_slug in validation_data:
                v_metrics = validation_data[base_slug]
                # Check validation status (PASS/FAIL)
                subtitle_timing_validated = v_metrics.get("status") == "PASS"
                # Get timing drift if available
                timing_drift_percent = v_metrics.get("timing_drift_percent")
                logger.info(
                    f"Validation metadata for {base_slug}: validated={subtitle_timing_validated}, "
                    f"drift={timing_drift_percent:.2f}%"
                )

            # Load to DuckDB with metadata (AC6: Pass version from priority map)
            load_emotions_to_duckdb(
                film_slug,
                film_id,
                language_code,
                emotions_df,
                conn,
                subtitle_version=version,  # Use version from priority map, not CLI arg
                subtitle_timing_validated=subtitle_timing_validated,
                timing_drift_percent=timing_drift_percent,
                replace_existing=replace_existing,
            )

            # Validate data
            validation_results = validate_emotion_data(emotions_df, filepath)

            records_loaded = len(emotions_df)
            success_count += 1

            results.append(
                {
                    "film_slug": film_slug,
                    "language_code": language_code,
                    "success": True,
                    "error_message": None,
                    "records_loaded": records_loaded,
                    "validation_results": validation_results,
                }
            )

            logger.info(
                f"✓ Successfully processed {film_slug} ({language_code}): "
                f"{records_loaded} records loaded"
            )

        except DataValidationError as e:
            # Validation failure - log and continue to next film (AC7, AC8)
            logger.error(f"Validation failed for {film_slug_lang}: {e}")
            failed_films.append((film_slug_lang, str(e)))
            results.append(
                {
                    "film_slug": film_slug_lang,
                    "language_code": language_code if "language_code" in locals() else "unknown",
                    "success": False,
                    "error_message": f"Validation error: {e}",
                    "records_loaded": 0,
                    "validation_results": {},
                }
            )
        except Exception as e:
            # Unexpected error - log and continue to next film (AC7, AC8)
            logger.error(f"Failed to process {filepath.name}: {e}")
            failed_films.append((film_slug_lang, str(e)))
            results.append(
                {
                    "film_slug": film_slug_lang,
                    "language_code": "unknown",
                    "success": False,
                    "error_message": str(e),
                    "records_loaded": 0,
                    "validation_results": {},
                }
            )

    conn.close()

    # Print summary of failed films (AC7, AC8)
    if failed_films:
        logger.warning(f"Failed to process {len(failed_films)} films:")
        for film, error in failed_films:
            logger.warning(f"  - {film}: {error}")

    logger.info(f"Processed {success_count}/{len(filtered_priority_map)} films successfully")
    return results


def main() -> None:
    """Main entry point for emotion analysis script with CLI."""
    parser = argparse.ArgumentParser(
        description="Perform multilingual emotion analysis on parsed subtitle dialogue"
    )
    parser.add_argument(
        "--subtitle-dir",
        type=Path,
        default=Path("data/processed/subtitles"),
        help="Directory containing parsed subtitle JSON files (default: data/processed/subtitles)",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("data/ghibli.duckdb"),
        help="Path to DuckDB database file (default: data/ghibli.duckdb)",
    )
    parser.add_argument(
        "--films",
        type=str,
        default=None,
        help=(
            "Comma-separated list of film slugs to process "
            "(e.g., 'spirited_away,princess_mononoke')"
        ),
    )
    parser.add_argument(
        "--languages",
        type=str,
        default="en,fr,es,nl,ar",
        help="Comma-separated list of language codes (default: en,fr,es,nl,ar)",
    )
    parser.add_argument(
        "--validate",
        type=bool,
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Enable validation (default: True)",
    )
    parser.add_argument(
        "--smoothing-window",
        type=int,
        default=10,
        help=(
            "Rolling average window size in minutes for noise reduction "
            "(default: 10, range: 1-15)"
        ),
    )
    parser.add_argument(
        "--subtitle-version",
        type=str,
        default="v1",
        help="Version tag for subtitle source (e.g., 'v1', 'v2_improved', default: 'v1')",
    )
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help="Delete existing emotion records before inserting new ones (replacement mode)",
    )
    parser.add_argument(
        "--validation-data",
        type=Path,
        default=None,
        help="Path to validation JSON file with timing metrics (optional)",
    )

    args = parser.parse_args()

    # Parse film filter
    film_filter = None
    if args.films:
        film_filter = [f.strip() for f in args.films.split(",")]

    # Parse language filter
    language_filter = [lang.strip() for lang in args.languages.split(",")]

    # Validate paths
    if not args.subtitle_dir.exists():
        logger.error(f"Subtitle directory not found: {args.subtitle_dir}")
        sys.exit(1)

    if not args.db_path.parent.exists():
        logger.error(f"Database directory not found: {args.db_path.parent}")
        sys.exit(1)

    try:
        # Log configuration
        logger.info(f"Using smoothing window: {args.smoothing_window} minutes")
        logger.info(f"Subtitle version: {args.subtitle_version}")
        logger.info(f"Replace existing: {args.replace_existing}")
        if args.validation_data:
            logger.info(f"Validation data: {args.validation_data}")

        # Process all films
        results = process_all_films(
            subtitle_dir=args.subtitle_dir,
            db_path=args.db_path,
            film_filter=film_filter,
            language_filter=language_filter,
            smoothing_window=args.smoothing_window,
            subtitle_version=args.subtitle_version,
            replace_existing=args.replace_existing,
            validation_data_path=args.validation_data,
        )

        # Print summary
        success_count = sum(1 for r in results if r["success"])
        total_count = len(results)
        total_records = sum(r["records_loaded"] for r in results)

        logger.info("=" * 60)
        logger.info(f"Summary: {success_count}/{total_count} films processed successfully")
        logger.info(f"Total records loaded: {total_records}")

        # Check for failures
        failures = [r for r in results if not r["success"]]
        if failures:
            logger.warning(f"{len(failures)} films failed:")
            for failure in failures:
                logger.warning(
                    f"  - {failure['film_slug']} ({failure['language_code']}): "
                    f"{failure['error_message']}"
                )
            sys.exit(1)

        logger.info("✓ All films processed successfully")
        sys.exit(0)

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
