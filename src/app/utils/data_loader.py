"""
DuckDB connection and emotion mart data loaders.

Provides cached connections and query functions for Streamlit app.

[Source: architecture/12-security-and-performance.md#1-caching]
"""

import streamlit as st
import duckdb
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional

from .config import DUCKDB_PATH, EMOTION_MARTS, EMOTION_LABELS


@st.cache_resource
def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    """
    Create persistent DuckDB connection (cached at app startup).

    Returns:
        DuckDB connection object

    Raises:
        FileNotFoundError: If database file does not exist

    [Source: architecture/12-security-and-performance.md#1-caching]
    """
    if not DUCKDB_PATH.exists():
        raise FileNotFoundError(
            f"DuckDB database not found at {DUCKDB_PATH}. "
            "Run data pipeline first: dbt run && python src/nlp/analyze_emotions.py"
        )

    return duckdb.connect(str(DUCKDB_PATH), read_only=True)


@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_emotion_mart(mart_name: str) -> pd.DataFrame:
    """
    Load emotion mart table from DuckDB.

    Args:
        mart_name: Mart key from config.EMOTION_MARTS

    Returns:
        DataFrame with mart data

    Raises:
        ValueError: If mart_name is not recognized

    [Source: architecture/database-schema.md#marts-schema-dbt-tables]
    """
    conn = get_duckdb_connection()

    if mart_name not in EMOTION_MARTS:
        raise ValueError(
            f"Unknown mart: {mart_name}. Valid options: {list(EMOTION_MARTS.keys())}"
        )

    table_name = EMOTION_MARTS[mart_name]

    query = f"SELECT * FROM {table_name}"

    return conn.execute(query).fetch_df()


@st.cache_data(ttl=3600)
def get_film_list() -> pd.DataFrame:
    """
    Get list of all films with metadata.

    Returns:
        DataFrame with columns: film_id, title, director, release_year, rt_score

    [Source: architecture/database-schema.md#staging-schema-dbt-views]
    """
    conn = get_duckdb_connection()

    query = """
    SELECT
        id as film_id,
        title,
        director,
        release_year,
        rt_score
    FROM main_staging.stg_films
    ORDER BY release_year DESC
    """

    return conn.execute(query).fetch_df()


@st.cache_data(ttl=3600)
def get_film_emotions(film_id: int, language_code: str) -> pd.DataFrame:
    """
    Get minute-level emotion data for a specific film and language.

    Args:
        film_id: Film ID from stg_kaggle_films
        language_code: Language code (en, fr, es, nl, ar)

    Returns:
        DataFrame with columns: minute_offset, emotion_* (28 emotions)

    Raises:
        ValueError: If language_code is not in supported languages

    [Source: architecture/database-schema.md#raw-schema-epic-1-data-ingestion]
    """
    # Input validation to prevent SQL injection
    from .config import DATA_STATS

    if language_code not in DATA_STATS["language_codes"]:
        raise ValueError(
            f"Invalid language_code: {language_code}. "
            f"Valid options: {DATA_STATS['language_codes']}"
        )

    conn = get_duckdb_connection()

    # Build dynamic column list from config
    emotion_cols = ", ".join([f"emotion_{label}" for label in EMOTION_LABELS])

    # Use parameterized query to prevent SQL injection
    query = f"""
    SELECT
        minute_offset,
        {emotion_cols}
    FROM raw.film_emotions
    WHERE film_id = ?
      AND language_code = ?
    ORDER BY minute_offset
    """

    return conn.execute(query, [film_id, language_code]).fetch_df()


# ============================================================================
# Epic 5.2: Home Page Data Functions
# ============================================================================

@st.cache_data(ttl=3600)
def get_hero_stats() -> Dict[str, any]:
    """
    Get hero statistics for home page dashboard.

    Returns:
        Dict with keys: film_count, emotion_data_points, languages_count, dialogue_entries

    [Source: Story 5.2 - AC2]
    """
    conn = get_duckdb_connection()

    query = """
    SELECT
        (SELECT COUNT(DISTINCT id) FROM main_staging.stg_films) as film_count,
        (SELECT COUNT(*) FROM main_marts.mart_film_emotion_timeseries) as emotion_data_points,
        (SELECT COUNT(DISTINCT language_code) FROM main_marts.mart_film_emotion_timeseries) as languages_count,
        (SELECT SUM(dialogue_count) FROM main_marts.mart_film_emotion_timeseries) as dialogue_entries
    """

    result = conn.execute(query).fetch_df().iloc[0]

    return {
        "film_count": int(result["film_count"]),
        "emotion_data_points": int(result["emotion_data_points"]),
        "languages_count": int(result["languages_count"]),
        "dialogue_entries": int(result["dialogue_entries"])
    }


@st.cache_data(ttl=3600)
def get_top_joyful_film() -> Dict[str, any]:
    """
    Get film with highest average joy score.

    Returns:
        Dict with keys: film_title, joy_score

    [Source: Story 5.2 - AC3]
    """
    conn = get_duckdb_connection()

    query = """
    SELECT
        film_title,
        ROUND(emotion_joy, 4) as joy_score
    FROM main_marts.mart_film_emotion_summary
    WHERE language_code = 'en'
    ORDER BY emotion_joy DESC
    LIMIT 1
    """

    result = conn.execute(query).fetch_df().iloc[0]

    return {
        "film_title": result["film_title"],
        "joy_score": float(result["joy_score"])
    }


@st.cache_data(ttl=3600)
def get_top_fearful_film() -> Dict[str, any]:
    """
    Get film with highest average fear score.

    Returns:
        Dict with keys: film_title, fear_score

    [Source: Story 5.2 - AC3]
    """
    conn = get_duckdb_connection()

    query = """
    SELECT
        film_title,
        ROUND(emotion_fear, 4) as fear_score
    FROM main_marts.mart_film_emotion_summary
    WHERE language_code = 'en'
    ORDER BY emotion_fear DESC
    LIMIT 1
    """

    result = conn.execute(query).fetch_df().iloc[0]

    return {
        "film_title": result["film_title"],
        "fear_score": float(result["fear_score"])
    }


@st.cache_data(ttl=3600)
def get_director_comparison() -> Dict[str, Dict[str, any]]:
    """
    Get emotional style comparison between Miyazaki and Takahata.

    Returns:
        Dict with keys: miyazaki, takahata
        Each containing: diversity, joy, sadness, style_label

    [Source: Story 5.2 - AC3]
    """
    conn = get_duckdb_connection()

    query = """
    SELECT
        director,
        emotion_diversity,
        avg_emotion_joy,
        avg_emotion_sadness,
        film_count
    FROM main_marts.mart_director_emotion_profile
    WHERE director IN ('Hayao Miyazaki', 'Isao Takahata')
    """

    df = conn.execute(query).fetch_df()

    result = {}
    for _, row in df.iterrows():
        key = "miyazaki" if row["director"] == "Hayao Miyazaki" else "takahata"

        # Determine style based on metrics
        diversity = float(row["emotion_diversity"])
        joy = float(row["avg_emotion_joy"])
        sadness = float(row["avg_emotion_sadness"])

        # Miyazaki: Higher diversity, more adventurous
        # Takahata: Higher sadness/joy, more emotional depth
        if key == "miyazaki":
            style_label = "Adventurous & Diverse"
        else:
            style_label = "Emotionally Intense"

        result[key] = {
            "diversity": diversity,
            "joy": joy,
            "sadness": sadness,
            "film_count": int(row["film_count"]),
            "style_label": style_label
        }

    return result


@st.cache_data(ttl=3600)
def get_film_emotion_timeseries(film_title: str, language_code: str) -> pd.DataFrame:
    """
    Get complete emotion timeline for a film in a specific language.

    Args:
        film_title: Film title (e.g., "Spirited Away")
        language_code: Language code (en, fr, es, nl, ar)

    Returns:
        DataFrame with columns: minute_offset, emotion_* (28 emotions)

    [Source: Story 5.2 - AC4]
    """
    from .config import DATA_STATS

    if language_code not in DATA_STATS["language_codes"]:
        raise ValueError(
            f"Invalid language_code: {language_code}. "
            f"Valid options: {DATA_STATS['language_codes']}"
        )

    conn = get_duckdb_connection()

    # Build dynamic column list from config
    emotion_cols = ", ".join([f"emotion_{label}" for label in EMOTION_LABELS])

    query = f"""
    SELECT
        minute_offset,
        {emotion_cols}
    FROM main_marts.mart_film_emotion_timeseries
    WHERE film_title = ?
      AND language_code = ?
    ORDER BY minute_offset
    """

    return conn.execute(query, [film_title, language_code]).fetch_df()


# ============================================================================
# Epic 5.3: Film Explorer (The Spirit Archives) Data Functions
# ============================================================================

@st.cache_data(ttl=3600)
def get_film_list_with_metadata() -> List[Dict[str, any]]:
    """
    Get list of all films with metadata formatted for film selector.

    IMPROVED (2025-11-23): Now queries films directly from emotion data to include
    all films with emotion analysis, not just those in Kaggle dataset.

    Returns:
        List of dicts with keys: film_id, title, display_name, release_year, director

    [Source: Story 5.3 - Task 1.1, AC1, Enhanced to include The Red Turtle]
    """
    conn = get_duckdb_connection()

    query = """
    SELECT DISTINCT
        e.film_id,
        e.film_title as title,
        COALESCE(k.release_year, f.release_year) as release_year,
        COALESCE(k.director, f.director) as director
    FROM main_marts.mart_film_emotion_timeseries e
    LEFT JOIN main_staging.stg_kaggle_films k ON e.film_id = k.film_id
    LEFT JOIN main_staging.stg_films f ON e.film_id = f.id
    WHERE e.film_id IS NOT NULL
    ORDER BY release_year DESC, title
    """

    df = conn.execute(query).fetch_df()

    # Format display names for selector
    films = []
    for _, row in df.iterrows():
        films.append({
            "film_id": str(row["film_id"]),  # UUID string
            "title": row["title"],
            "display_name": f"{row['title']} ({row['release_year']}) - {row['director']}",
            "release_year": int(row["release_year"]),
            "director": row["director"]
        })

    return films


@st.cache_data(ttl=3600)
def get_film_emotion_timeseries_by_id(film_id: str, language_code: str) -> pd.DataFrame:
    """
    Get smoothed emotion timeline for a film by ID in a specific language.

    Args:
        film_id: Film ID (UUID string) from stg_kaggle_films
        language_code: Language code (en, fr, es, nl, ar)

    Returns:
        DataFrame with columns: minute_offset, dialogue_count, emotion_* (28 emotions)

    [Source: Story 5.3 - Task 1.2, AC2]
    """
    from .config import DATA_STATS

    if language_code not in DATA_STATS["language_codes"]:
        raise ValueError(
            f"Invalid language_code: {language_code}. "
            f"Valid options: {DATA_STATS['language_codes']}"
        )

    conn = get_duckdb_connection()

    # Build dynamic column list from config
    emotion_cols = ", ".join([f"emotion_{label}" for label in EMOTION_LABELS])

    query = f"""
    SELECT
        minute_offset,
        dialogue_count,
        {emotion_cols}
    FROM main_marts.mart_film_emotion_timeseries
    WHERE film_id = ?
      AND language_code = ?
    ORDER BY minute_offset
    """

    return conn.execute(query, [film_id, language_code]).fetch_df()


@st.cache_data(ttl=3600)
def get_raw_emotion_peaks(film_id: str, language_code: str) -> pd.DataFrame:
    """
    Get raw dialogue-level emotion data for a film (non-smoothed).

    Args:
        film_id: Film ID (UUID string) from stg_kaggle_films
        language_code: Language code (en, fr, es, nl, ar)

    Returns:
        DataFrame with columns: minute_offset, dialogue_count, emotion_* (28 emotions)

    [Source: Story 5.3 - Task 1.3, AC5]
    """
    from .config import DATA_STATS

    if language_code not in DATA_STATS["language_codes"]:
        raise ValueError(
            f"Invalid language_code: {language_code}. "
            f"Valid options: {DATA_STATS['language_codes']}"
        )

    conn = get_duckdb_connection()

    # Build dynamic column list from config
    emotion_cols = ", ".join([f"emotion_{label}" for label in EMOTION_LABELS])

    # Use raw.film_emotions table which has the raw, non-smoothed data
    query = f"""
    SELECT
        minute_offset,
        dialogue_count,
        {emotion_cols}
    FROM raw.film_emotions
    WHERE film_id = ?
      AND language_code = ?
    ORDER BY minute_offset
    """

    return conn.execute(query, [film_id, language_code]).fetch_df()


@st.cache_data(ttl=3600)
def get_film_emotion_summary_by_id(film_id: str, language_code: str) -> Dict[str, float]:
    """
    Get aggregated emotion summary for a film.

    Args:
        film_id: Film ID (UUID string) from stg_kaggle_films
        language_code: Language code (en, fr, es, nl, ar)

    Returns:
        Dict with emotion labels as keys and average scores as values

    [Source: Story 5.3 - Task 1.4, AC4]
    """
    from .config import DATA_STATS

    if language_code not in DATA_STATS["language_codes"]:
        raise ValueError(
            f"Invalid language_code: {language_code}. "
            f"Valid options: {DATA_STATS['language_codes']}"
        )

    conn = get_duckdb_connection()

    # Build dynamic column list from config
    emotion_cols = ", ".join([f"emotion_{label}" for label in EMOTION_LABELS])

    query = f"""
    SELECT
        {emotion_cols}
    FROM main_marts.mart_film_emotion_summary
    WHERE film_id = ?
      AND language_code = ?
    """

    df = conn.execute(query, [film_id, language_code]).fetch_df()

    if df.empty:
        return {}

    # Convert to dict format
    row = df.iloc[0]
    return {label: float(row[f"emotion_{label}"]) for label in EMOTION_LABELS}


@st.cache_data(ttl=3600)
def get_emotion_peaks_with_scenes(film_id: str, language_code: str) -> pd.DataFrame:
    """
    Get emotion peaks with scene descriptions for marker annotations.

    Args:
        film_id: Film ID (UUID string) from stg_kaggle_films
        language_code: Language code (en, fr, es, nl, ar)

    Returns:
        DataFrame with columns: emotion_type, peak_minute_offset, intensity_score, scene_description

    [Source: Story 5.3 - Enhancement: Peak emotion markers]
    """
    from .config import DATA_STATS

    if language_code not in DATA_STATS["language_codes"]:
        raise ValueError(
            f"Invalid language_code: {language_code}. "
            f"Valid options: {DATA_STATS['language_codes']}"
        )

    conn = get_duckdb_connection()

    query = """
    SELECT
        emotion_type,
        peak_minute_offset,
        intensity_score,
        scene_description,
        peak_rank
    FROM main_marts.mart_emotion_peaks_smoothed
    WHERE film_id = ?
      AND language_code = ?
      AND peak_rank <= 3
    ORDER BY emotion_type, peak_rank
    """

    return conn.execute(query, [film_id, language_code]).fetch_df()


@st.cache_data(ttl=3600)
def get_film_slug_from_id(film_id: str, language_code: str) -> Optional[str]:
    """
    Get film slug (base name without language suffix) from film ID.

    Args:
        film_id: Film ID (UUID string) from stg_kaggle_films
        language_code: Language code (en, fr, es, nl, ar)

    Returns:
        Base film slug (e.g., "spirited_away") or None if not found

    [Source: Story 5.3 - QA Fix CODE-001: Extract hardcoded query]
    """
    from .config import DATA_STATS, DUCKDB_PATH
    import duckdb
    import logging

    logger = logging.getLogger(__name__)

    if language_code not in DATA_STATS["language_codes"]:
        logger.warning(f"Invalid language_code: {language_code}")
        return None

    try:
        conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        result = conn.execute(
            "SELECT DISTINCT film_slug FROM raw.film_emotions WHERE film_id = ? AND language_code = ? LIMIT 1",
            [film_id, language_code]
        ).fetchone()

        if result:
            # film_slug includes language (e.g., "spirited_away_en"), strip language suffix
            film_slug_with_lang = result[0]
            film_slug_base = film_slug_with_lang.rsplit('_', 1)[0]
            return film_slug_base
        else:
            logger.info(f"No film_slug found for film_id={film_id}, language={language_code}")
            return None

    except Exception as e:
        logger.error(f"Failed to get film_slug for film_id={film_id}: {e}", exc_info=True)
        return None


@st.cache_data(ttl=3600)
def get_peak_dialogues(film_slug: str, language_code: str, peaks_df: pd.DataFrame) -> List[Dict]:
    """
    Load actual dialogue text from parsed subtitle files for emotion peaks.
    Groups consecutive peaks of same emotion into time ranges for diversity.

    IMPROVEMENT (2025-11-23): Uses ±15 second window centered on highest intensity peak
    with proximity-based dialogue ranking for better contextual accuracy.

    Args:
        film_slug: Film slug (e.g., "spirited_away")
        language_code: Language code (en, fr, es, nl, ar)
        peaks_df: DataFrame with peak_minute_offset column

    Returns:
        List of dicts with keys: emotion_type, minute_range, dialogue_lines (list of strings)
        dialogue_lines contains top 3 dialogues closest to peak center (sorted by proximity)

    [Source: Story 5.3 - Enhancement: Peak dialogue sidebar with improved accuracy]
    """
    import json
    from pathlib import Path

    # Construct path to parsed subtitle file (try v2 improved first, then original)
    parsed_file_v2 = Path(f"data/processed/subtitles_improved/{film_slug}_{language_code}_v2_parsed.json")
    parsed_file_v1 = Path(f"data/processed/subtitles/{film_slug}_{language_code}_parsed.json")

    if parsed_file_v2.exists():
        parsed_file = parsed_file_v2
    elif parsed_file_v1.exists():
        parsed_file = parsed_file_v1
    else:
        # Fallback: return empty list if neither file found
        return []

    # Load parsed subtitles
    with open(parsed_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    subtitles = data.get("subtitles", [])

    # Group peaks by emotion type (exclude neutral - not informative)
    emotion_peaks = {}
    for _, peak_row in peaks_df.iterrows():
        emotion_type = peak_row["emotion_type"]

        # Skip neutral emotion
        if emotion_type == "neutral":
            continue

        if emotion_type not in emotion_peaks:
            emotion_peaks[emotion_type] = []
        emotion_peaks[emotion_type].append({
            "minute": int(peak_row["peak_minute_offset"]),
            "intensity": float(peak_row["intensity_score"]),
            "rank": int(peak_row["peak_rank"])
        })

    # Process each emotion's peaks - group consecutive minutes
    peak_dialogues = []

    for emotion_type, peaks in emotion_peaks.items():
        # Sort by minute
        peaks_sorted = sorted(peaks, key=lambda x: x["minute"])

        # Group consecutive minutes (within 5 minutes = likely same scene due to rolling avg)
        grouped_peaks = []
        current_group = [peaks_sorted[0]]

        for peak in peaks_sorted[1:]:
            if peak["minute"] - current_group[-1]["minute"] <= 5:
                # Consecutive peak - add to current group
                current_group.append(peak)
            else:
                # New peak cluster - save current group and start new one
                grouped_peaks.append(current_group)
                current_group = [peak]

        # Don't forget the last group
        grouped_peaks.append(current_group)

        # For each group, create a single entry with time range
        for group in grouped_peaks:
            # Find highest intensity peak in group
            max_intensity_peak = max(group, key=lambda x: x["intensity"])

            # Calculate time range for display
            min_minute = min(p["minute"] for p in group)
            max_minute = max(p["minute"] for p in group)

            # IMPROVED: Use tighter window centered on highest intensity peak (±15 seconds)
            # This reduces contextually inaccurate dialogue compared to old ±30s from edges
            peak_center_sec = max_intensity_peak["minute"] * 60
            peak_start_sec = peak_center_sec - 15
            peak_end_sec = peak_center_sec + 15

            # Extract dialogues from this time window with proximity scoring
            dialogue_candidates = []
            for sub in subtitles:
                if peak_start_sec <= sub["start_time"] <= peak_end_sec:
                    # Calculate proximity score (closer to peak center = better)
                    distance_from_peak = abs(sub["start_time"] - peak_center_sec)
                    dialogue_candidates.append({
                        "text": sub["dialogue_text"],
                        "distance": distance_from_peak
                    })

            # Sort by proximity to peak and select best representative lines
            dialogue_candidates.sort(key=lambda x: x["distance"])
            best_dialogues = [d["text"] for d in dialogue_candidates[:3]]  # Top 3 closest

            # Only include if we found dialogues
            if best_dialogues:
                # Format time range display
                if min_minute == max_minute:
                    minute_display = min_minute
                    minute_range = f"min {min_minute}"
                else:
                    minute_display = min_minute  # For sorting
                    minute_range = f"min {min_minute}-{max_minute}"

                peak_dialogues.append({
                    "emotion_type": emotion_type,
                    "minute": minute_display,  # For sorting
                    "minute_range": minute_range,  # For display
                    "intensity": max_intensity_peak["intensity"],
                    "peak_rank": max_intensity_peak["rank"],
                    "dialogue_lines": best_dialogues  # Best representative lines
                })

    # Sort by minute (chronological order to follow film's narrative)
    peak_dialogues.sort(key=lambda x: x["minute"])

    return peak_dialogues


# ============================================================================
# Epic 3.6: Data Quality Validation Functions
# ============================================================================

@st.cache_data(ttl=3600)
def get_validation_status(film_id: str, language_code: str) -> Optional[Dict[str, any]]:
    """
    Get data quality validation status for a specific film-language combination.

    Checks against dbt validation model to determine if subtitle data extends
    beyond film runtime + 10-minute buffer (indicating data quality issues).

    Args:
        film_id: Film ID (UUID string) from stg_kaggle_films
        language_code: Language code (en, fr, es, nl, ar)

    Returns:
        Dict with keys: validation_status ('PASS'/'FAIL'/'UNKNOWN'),
                       overrun_minutes (float), film_title (str)
        None if no validation data found

    [Source: Story 3.6.5 - Data Quality Validation Layer, Epic 3.6 UX Enhancement]
    """
    from .config import DATA_STATS

    if language_code not in DATA_STATS["language_codes"]:
        return None

    conn = get_duckdb_connection()

    # First get film_slug from film_id (validation model uses film_slug, not film_id)
    slug_query = """
    SELECT DISTINCT film_slug
    FROM raw.film_emotions
    WHERE film_id = ? AND language_code = ?
    LIMIT 1
    """

    slug_result = conn.execute(slug_query, [film_id, language_code]).fetch_df()

    if slug_result.empty:
        return None

    film_slug = slug_result.iloc[0]["film_slug"]

    # Query validation model using film_slug
    query = """
    SELECT
        validation_status,
        overrun_minutes,
        film_title,
        max_minute_offset,
        expected_duration_minutes
    FROM main_intermediate.int_emotion_data_quality_checks
    WHERE film_slug = ?
      AND language_code = ?
      AND film_slug != '_SUMMARY_'
    """

    result = conn.execute(query, [film_slug, language_code]).fetch_df()

    if result.empty:
        return None

    row = result.iloc[0]
    return {
        "validation_status": row["validation_status"],
        "overrun_minutes": float(row["overrun_minutes"]) if pd.notna(row["overrun_minutes"]) else None,
        "film_title": row["film_title"],
        "max_minute_offset": float(row["max_minute_offset"]) if pd.notna(row["max_minute_offset"]) else None,
        "expected_duration_minutes": float(row["expected_duration_minutes"]) if pd.notna(row["expected_duration_minutes"]) else None
    }
