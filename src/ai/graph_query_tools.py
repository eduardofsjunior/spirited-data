"""
LangChain function calling tools for structured database queries.

This module provides LangChain Tools that enable the RAG system to answer
questions requiring graph traversal, sentiment analysis, and aggregation.

All tools return standardized ToolResponse format with answer text, data sources,
visualization data, and suggested followups.
"""
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, TypedDict

import duckdb
import numpy as np
from langchain.tools import tool
from scipy.stats import pearsonr

from src.shared.database import get_duckdb_connection
from src.validation.chart_utils import load_dialogue_excerpts
import json
import os

# Configure logging
logger = logging.getLogger("spiriteddata.ai.graph_query_tools")

# Constants
GRAPH_TABLES_WHITELIST = [
    "main_marts.mart_graph_nodes",
    "main_marts.mart_graph_edges",
    "staging.stg_films",
    "staging.stg_people",
    "staging.stg_kaggle_films",
    "raw.film_emotions",
]
MAX_QUERY_RESULTS = 100
DANGEROUS_SQL_KEYWORDS = [
    "DROP",
    "DELETE",
    "UPDATE",
    "INSERT",
    "ALTER",
    "TRUNCATE",
    "CREATE",
]


# TypedDict definitions for standardized response format
class DataSourceCitation(TypedDict):
    """Structured data provenance information."""

    tables: List[str]  # DuckDB tables queried
    functions: List[str]  # Python/SQL functions called
    computation_method: Optional[str]  # Tool used (e.g., "NetworkX", "VADER", "SQL aggregation")
    row_count: Optional[int]  # Number of rows analyzed
    timestamp: str  # ISO 8601 timestamp


class ToolResponse(TypedDict):
    """Standardized response format for all custom tools."""

    answer: str  # Human-readable response text
    data_sources: DataSourceCitation
    visualization_data: Optional[Dict[str, Any]]  # Chart data if applicable
    suggested_followups: Optional[List[str]]  # Contextual next questions


# Helper functions
def _get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    """Get DuckDB connection from shared database module."""
    return get_duckdb_connection(read_only=True)


def _fuzzy_match_film_title(title: str, conn: duckdb.DuckDBPyConnection) -> Optional[str]:
    """
    Fuzzy match film title to find exact match in database.

    Args:
        title: Film title to match
        conn: DuckDB connection

    Returns:
        Exact film title from database, or None if not found
    """
    query = """
        SELECT title
        FROM main_staging.stg_films
        WHERE LOWER(title) = LOWER(?)
        LIMIT 1
    """
    result = conn.execute(query, [title]).fetchone()
    return result[0] if result else None


def _sanitize_sql_keywords(sql: str) -> bool:
    """
    Check if SQL contains dangerous keywords.

    Args:
        sql: SQL query string

    Returns:
        True if SQL is safe (no dangerous keywords), False otherwise
    """
    sql_upper = sql.upper()
    for keyword in DANGEROUS_SQL_KEYWORDS:
        if keyword in sql_upper:
            return False
    return True


def _translate_technical_to_narrative(tables: List[str], computation_method: Optional[str]) -> str:
    """
    Translate technical details into portfolio-friendly narrative explanations.

    Maps database tables and methods to "archive features" that sound magical and accessible.
    """
    feature_map = {
        "raw.film_emotions": "🎭 Emotion Archive (subtitle sentiment analysis)",
        "main_staging.stg_films": "📚 Film Catalog",
        "main_staging.stg_kaggle_films": "💰 Box Office Records",
        "main_marts.mart_sentiment_success_correlation": "📊 Pattern Discovery Tools (sentiment-success correlations)",
        "main_marts.mart_film_sentiment_summary": "🎭 Emotion Archive (aggregated film emotions)",
        "main_marts.mart_film_success_metrics": "🎯 Success Metrics Collection",
    }

    features_used = []
    for table in tables:
        if table in feature_map:
            features_used.append(feature_map[table])
        elif "emotion" in table.lower():
            features_used.append("🎭 Emotion Archive")
        elif "sentiment" in table.lower():
            features_used.append("📊 Pattern Discovery Tools")

    # Deduplicate while preserving order
    features_used = list(dict.fromkeys(features_used))

    if not features_used:
        features_used = ["📚 Film Archive"]

    return ", ".join(features_used)


def load_dialogue_with_emotions(
    film_slug: str,
    language_code: str,
    minute_offsets: List[int],
    conn: Optional[duckdb.DuckDBPyConnection] = None
) -> Dict[int, Dict[str, Any]]:
    """
    Load dialogue excerpts WITH emotion scores for interpretation.

    This enhanced version fetches both dialogue text AND the emotion breakdown
    for each minute, enabling Sora to explain WHY moments are emotional.

    Args:
        film_slug: Film identifier (e.g., "spirited_away")
        language_code: Language code (e.g., "en")
        minute_offsets: List of minute offsets to load
        conn: Optional database connection

    Returns:
        Dict mapping minute to {"dialogue": [...], "emotions": {...}, "compound": float}
    """
    close_conn = False
    if conn is None:
        conn = get_duckdb_connection()
        close_conn = True

    try:
        # First, load dialogue from parsed subtitle files
        dialogue_by_minute = load_dialogue_excerpts(film_slug, language_code, minute_offsets)

        # Then, load emotion scores from database
        film_slug_full = f"{film_slug}_{language_code}"
        placeholders = ", ".join(["?"] * len(minute_offsets))

        query = f"""
        SELECT
            minute_offset,
            emotion_joy, emotion_sadness, emotion_anger, emotion_fear,
            emotion_love, emotion_surprise, emotion_disgust, emotion_admiration,
            emotion_excitement, emotion_optimism, emotion_caring, emotion_relief,
            emotion_nervousness, emotion_grief, emotion_disappointment,
            (emotion_admiration + emotion_amusement + emotion_approval + emotion_caring +
             emotion_desire + emotion_excitement + emotion_gratitude + emotion_joy +
             emotion_love + emotion_optimism + emotion_pride + emotion_relief) -
            (emotion_anger + emotion_annoyance + emotion_confusion + emotion_disappointment +
             emotion_disapproval + emotion_disgust + emotion_embarrassment + emotion_fear +
             emotion_grief + emotion_nervousness + emotion_remorse + emotion_sadness) as compound_sentiment
        FROM raw.film_emotions
        WHERE film_slug = ?
        AND minute_offset IN ({placeholders})
        ORDER BY minute_offset
        """

        params = [film_slug_full] + minute_offsets
        emotion_data = conn.execute(query, params).fetchall()

        # Combine dialogue + emotions
        result = {}
        emotion_dict = {row[0]: row[1:] for row in emotion_data}  # minute -> emotion scores

        for minute in minute_offsets:
            dialogue_lines = dialogue_by_minute.get(minute, [])

            if minute in emotion_dict:
                emotions_tuple = emotion_dict[minute]
                # Top emotions (those > 0.3) for this minute
                emotion_names = [
                    "joy", "sadness", "anger", "fear", "love", "surprise",
                    "disgust", "admiration", "excitement", "optimism", "caring",
                    "relief", "nervousness", "grief", "disappointment"
                ]

                emotion_scores = {
                    name: score
                    for name, score in zip(emotion_names, emotions_tuple[:-1])
                    if score > 0.01  # Include emotions above 1% (low threshold for nuanced detection)
                }

                compound = emotions_tuple[-1]  # Last value is compound_sentiment

                result[minute] = {
                    "dialogue": dialogue_lines,
                    "emotions": emotion_scores,
                    "compound": compound
                }
            else:
                # No emotion data for this minute
                result[minute] = {
                    "dialogue": dialogue_lines,
                    "emotions": {},
                    "compound": 0.0
                }

        return result

    finally:
        if close_conn:
            conn.close()


def _format_tool_response(
    answer: str,
    tables: List[str],
    functions: List[str],
    computation_method: Optional[str] = None,
    row_count: Optional[int] = None,
    visualization_data: Optional[Dict[str, Any]] = None,
    suggested_followups: Optional[List[str]] = None,
) -> ToolResponse:
    """
    Create standardized ToolResponse format with portfolio-friendly narrative.

    Args:
        answer: Human-readable answer text
        tables: List of tables queried
        functions: List of functions called
        computation_method: Description of computation method
        row_count: Number of rows analyzed
        visualization_data: Optional chart data
        suggested_followups: Optional list of followup questions

    Returns:
        ToolResponse dict with all required fields
    """
    # Add narrative explanation of which features powered this analysis
    archive_features = _translate_technical_to_narrative(tables, computation_method)

    # Enhance answer with feature attribution if not already narrative
    if answer and not answer.startswith(("I discovered", "I found", "I analyzed", "Through my")):
        narrative_intro = f"\n\n✨ **Archive Features Used**: {archive_features}"
        answer = answer + narrative_intro

    return ToolResponse(
        answer=answer,
        data_sources=DataSourceCitation(
            tables=tables,
            functions=functions,
            computation_method=computation_method,
            row_count=row_count,
            timestamp=datetime.now().isoformat(),
        ),
        visualization_data=visualization_data,
        suggested_followups=suggested_followups,
    )


def _calculate_compound_sentiment(emotion_row: Dict[str, Any]) -> float:
    """
    Calculate compound sentiment score from emotion row.

    Compound = (positive emotions) - (negative emotions)
    Positive: admiration, amusement, approval, caring, excitement, gratitude, joy, love, optimism, pride, relief
    Negative: anger, annoyance, disappointment, disapproval, disgust, embarrassment, fear, grief, nervousness, remorse, sadness

    Args:
        emotion_row: Dict with emotion_* keys

    Returns:
        Compound sentiment score (positive - negative)
    """
    positive_emotions = [
        "emotion_admiration",
        "emotion_amusement",
        "emotion_approval",
        "emotion_caring",
        "emotion_excitement",
        "emotion_gratitude",
        "emotion_joy",
        "emotion_love",
        "emotion_optimism",
        "emotion_pride",
        "emotion_relief",
    ]
    negative_emotions = [
        "emotion_anger",
        "emotion_annoyance",
        "emotion_disappointment",
        "emotion_disapproval",
        "emotion_disgust",
        "emotion_embarrassment",
        "emotion_fear",
        "emotion_grief",
        "emotion_nervousness",
        "emotion_remorse",
        "emotion_sadness",
    ]

    positive_sum = sum(emotion_row.get(emotion, 0.0) for emotion in positive_emotions)
    negative_sum = sum(emotion_row.get(emotion, 0.0) for emotion in negative_emotions)

    return positive_sum - negative_sum


# Tool implementations
def _interpret_positive_emotions(emotion_names: List[str], dialogue_lines: List[str]) -> str:
    """
    Generate interpretation for positive emotional moments based on dominant emotions.

    Args:
        emotion_names: List of top 2 emotion names (e.g., ["joy", "excitement"])
        dialogue_lines: Dialogue from this moment for context

    Returns:
        Interpretation string describing what might be happening narratively
    """
    # Analyze emotion combinations to suggest narrative context
    emotion_set = set(emotion_names)

    if "joy" in emotion_set and "excitement" in emotion_set:
        return "triumph or celebration - perhaps a character achieving a goal or experiencing a breakthrough."
    elif "joy" in emotion_set and "love" in emotion_set:
        return "connection and warmth - maybe a reunion, friendship moment, or emotional bonding between characters."
    elif "relief" in emotion_set:
        return "resolution after tension - the easing of conflict or escape from danger."
    elif "admiration" in emotion_set:
        return "witnessing something beautiful or impressive - a moment of wonder or appreciation."
    elif "excitement" in emotion_set and "optimism" in emotion_set:
        return "anticipation and hope - characters moving toward positive change or new possibilities."
    elif "caring" in emotion_set:
        return "tenderness and compassion - acts of kindness or protective behavior between characters."
    elif "joy" in emotion_set:
        return "happiness and positivity - characters experiencing contentment or success."
    else:
        return f"{' and '.join(emotion_names)} - positive emotional energy suggesting uplifting character development."


def _interpret_negative_emotions(emotion_names: List[str], dialogue_lines: List[str]) -> str:
    """
    Generate interpretation for negative emotional moments based on dominant emotions.

    Args:
        emotion_names: List of top 2 emotion names (e.g., ["anger", "fear"])
        dialogue_lines: Dialogue from this moment for context

    Returns:
        Interpretation string describing what might be happening narratively
    """
    # Analyze emotion combinations to suggest narrative context
    emotion_set = set(emotion_names)

    # Check dialogue content for additional context
    dialogue_text = " ".join(dialogue_lines).lower() if dialogue_lines else ""

    if "anger" in emotion_set and "fear" in emotion_set:
        return "confrontation under threat - a character standing up to danger despite being afraid."
    elif "sadness" in emotion_set and "grief" in emotion_set:
        return "loss or separation - processing deep emotional pain or saying goodbye."
    elif "sadness" in emotion_set and "caring" in emotion_set:
        # Check dialogue for context clues
        if any(word in dialogue_text for word in ["hurt", "sick", "sorry", "care about"]):
            return "worried compassion - caring for someone who's suffering or in danger, mixing concern with helplessness."
        else:
            return "bittersweet tenderness - caring deeply while facing painful circumstances."
    elif "fear" in emotion_set and "nervousness" in emotion_set:
        return "anxiety and uncertainty - facing the unknown or entering dangerous territory."
    elif "anger" in emotion_set and "disgust" in emotion_set:
        return "moral outrage - witnessing or confronting something ethically wrong or unjust."
    elif "disappointment" in emotion_set:
        return "unmet expectations - plans falling apart or hopes being dashed."
    elif "sadness" in emotion_set and "fear" in emotion_set:
        return "vulnerable despair - feeling trapped, helpless, or without options."
    elif "anger" in emotion_set:
        return "conflict and defiance - characters clashing or resisting external forces."
    elif "fear" in emotion_set:
        return "danger or threat - characters in peril or facing intimidating challenges."
    elif "sadness" in emotion_set:
        return "melancholy or regret - characters processing difficult emotions or past choices."
    else:
        return f"{' and '.join(emotion_names)} - difficult emotions suggesting character struggles or obstacles."


@tool
def get_film_sentiment(film_title: str, compact: bool = False) -> Dict[str, Any]:
    """
    Get sentiment analysis for a film including overall sentiment, peaks, and emotional arc.

    This tool queries emotion data to calculate:
    - Overall average compound sentiment score
    - Top 3 most positive moments (with timestamps)
    - Top 3 most negative moments (with timestamps)
    - Emotional arc summary

    Args:
        film_title: Title of the film (e.g., "Spirited Away", "My Neighbor Totoro")
        compact: If True, returns abbreviated response (for multi-film comparisons). Default: False.

    Returns:
        ToolResponse dict with:
        - answer: Sentiment summary and arc description
        - data_sources: Tables and functions used
        - visualization_data: Line chart data for sentiment timeline
        - suggested_followups: Related questions

    Example:
        >>> result = get_film_sentiment("Spirited Away")
        >>> result["answer"]
        "Spirited Away has an overall positive sentiment (0.15). Emotional peaks occur..."
    """
    logger.info(f"Executing get_film_sentiment (compact={compact})", extra={"film_title": film_title})

    try:
        # Input validation
        if not film_title or not isinstance(film_title, str):
            return _format_tool_response(
                answer="Error: film_title must be a non-empty string",
                tables=[],
                functions=[],
            )

        conn = _get_duckdb_connection()

        # Find film (fuzzy match)
        exact_title = _fuzzy_match_film_title(film_title, conn)
        if not exact_title:
            return _format_tool_response(
                answer=f"Film '{film_title}' not found in the database.",
                tables=["staging.stg_films"],
                functions=["SQL SELECT"],
                row_count=0,
                suggested_followups=["List all available films", "Search for similar film titles"],
            )

        # Query emotion data for the film (use English by default, or aggregate across languages)
        # Calculate compound sentiment from emotion columns
        emotion_query = """
            SELECT 
                minute_offset,
                dialogue_count,
                emotion_admiration, emotion_amusement, emotion_approval, emotion_caring,
                emotion_excitement, emotion_gratitude, emotion_joy, emotion_love,
                emotion_optimism, emotion_pride, emotion_relief,
                emotion_anger, emotion_annoyance, emotion_disappointment, emotion_disapproval,
                emotion_disgust, emotion_embarrassment, emotion_fear, emotion_grief,
                emotion_nervousness, emotion_remorse, emotion_sadness,
                emotion_confusion, emotion_curiosity, emotion_desire, emotion_realization,
                emotion_surprise, emotion_neutral
            FROM raw.film_emotions
            WHERE film_slug LIKE ?
            ORDER BY minute_offset
        """
        # Match film slug pattern (e.g., "spirited_away_%")
        film_slug_pattern = film_title.lower().replace(" ", "_").replace("'", "") + "_%"
        emotion_result = conn.execute(emotion_query, [film_slug_pattern]).fetchall()

        if not emotion_result:
            return _format_tool_response(
                answer=f"No emotion data found for '{exact_title}'. Emotion analysis may not have been run yet.",
                tables=["raw.film_emotions"],
                functions=["SQL SELECT"],
                row_count=0,
                suggested_followups=["List films with emotion data available", "Run emotion analysis for this film"],
            )

        # Calculate compound sentiment for each minute
        sentiment_data = []
        for row in emotion_result:
            minute_offset = row[0]
            dialogue_count = row[1]
            emotion_dict = {
                "emotion_admiration": row[2],
                "emotion_amusement": row[3],
                "emotion_approval": row[4],
                "emotion_caring": row[5],
                "emotion_excitement": row[6],
                "emotion_gratitude": row[7],
                "emotion_joy": row[8],
                "emotion_love": row[9],
                "emotion_optimism": row[10],
                "emotion_pride": row[11],
                "emotion_relief": row[12],
                "emotion_anger": row[13],
                "emotion_annoyance": row[14],
                "emotion_disappointment": row[15],
                "emotion_disapproval": row[16],
                "emotion_disgust": row[17],
                "emotion_embarrassment": row[18],
                "emotion_fear": row[19],
                "emotion_grief": row[20],
                "emotion_nervousness": row[21],
                "emotion_remorse": row[22],
                "emotion_sadness": row[23],
                "emotion_confusion": row[24],
                "emotion_curiosity": row[25],
                "emotion_desire": row[26],
                "emotion_realization": row[27],
                "emotion_surprise": row[28],
                "emotion_neutral": row[29],
            }
            compound = _calculate_compound_sentiment(emotion_dict)
            sentiment_data.append(
                {
                    "minute_offset": minute_offset,
                    "compound": compound,
                    "dialogue_count": dialogue_count,
                }
            )

        if not sentiment_data:
            return _format_tool_response(
                answer=f"No valid sentiment data found for '{exact_title}'.",
                tables=["raw.film_emotions"],
                functions=["SQL SELECT"],
                row_count=0,
            )

        # Calculate overall average
        overall_sentiment = sum(s["compound"] for s in sentiment_data) / len(sentiment_data)

        # Find top 3 positive and negative moments
        sorted_by_compound = sorted(sentiment_data, key=lambda x: x["compound"], reverse=True)
        positive_peaks = sorted_by_compound[:5]  # Get top 5 for dialogue loading
        negative_peaks = sorted(sorted_by_compound, key=lambda x: x["compound"])[:5]

        # Load dialogue excerpts WITH EMOTION SCORES for the emotional peaks
        # Extract film slug from pattern (e.g., "spirited_away_en" -> "spirited_away")
        film_slug_base = film_slug_pattern.rstrip("%").rsplit("_", 1)[0] if "_" in film_slug_pattern else film_slug_pattern.rstrip("%")
        language_code = "en"  # Default to English

        # For each peak, load dialogue from a 5-minute window (±2 minutes) for context
        context_window_minutes = []
        for peak in positive_peaks + negative_peaks:
            peak_minute = peak["minute_offset"]
            # Add the peak minute and ±2 minutes around it
            for offset in range(max(0, peak_minute - 2), peak_minute + 3):
                if offset not in context_window_minutes:
                    context_window_minutes.append(offset)

        # Use enhanced function that loads dialogue + emotions
        dialogue_with_emotions = load_dialogue_with_emotions(
            film_slug=film_slug_base,
            language_code=language_code,
            minute_offsets=context_window_minutes,
            conn=conn
        )

        # Generate arc summary
        if overall_sentiment > 0.1:
            arc_direction = "positive"
        elif overall_sentiment < -0.1:
            arc_direction = "negative"
        else:
            arc_direction = "neutral"

        peak_minute = positive_peaks[0]["minute_offset"] if positive_peaks else None
        dip_minute = negative_peaks[0]["minute_offset"] if negative_peaks else None

        # Build response based on compact mode
        if compact:
            # COMPACT MODE: Brief summary for multi-film comparisons
            answer_parts = [
                f"**{exact_title}**: Overall sentiment {overall_sentiment:.2f} ({arc_direction})",
            ]

            # Show only top emotional peaks (no dialogue/interpretations)
            if positive_peaks:
                top_pos = positive_peaks[0]
                answer_parts.append(f"  🌟 Peak: minute {top_pos['minute_offset']} ({top_pos['compound']:.2f})")

            if negative_peaks:
                top_neg = negative_peaks[0]
                answer_parts.append(f"  😔 Valley: minute {top_neg['minute_offset']} ({top_neg['compound']:.2f})")

            # Get top emotion for characterization
            if dialogue_with_emotions and positive_peaks:
                peak_minute = positive_peaks[0]['minute_offset']
                peak_data = dialogue_with_emotions.get(peak_minute, {})
                peak_emotions = peak_data.get("emotions", {})
                if peak_emotions:
                    top_emotion = max(peak_emotions.items(), key=lambda x: x[1])
                    answer_parts.append(f"  Dominant emotion: {top_emotion[0]} ({top_emotion[1]:.3f})")

        else:
            # FULL MODE: Detailed analysis with dialogue and interpretations
            # Calculate sentiment variance for explicit metric mention
            sentiment_values = [s['compound'] for s in sentiment_data]
            sentiment_variance = np.var(sentiment_values) if sentiment_values else 0.0
            emotional_range = max(sentiment_values) - min(sentiment_values) if sentiment_values else 0.0
            
            # Calculate beginning and ending sentiment
            beginning_sentiment = sentiment_data[0]['compound'] if sentiment_data else 0.0
            ending_sentiment = sentiment_data[-1]['compound'] if sentiment_data else 0.0
            
            answer_parts = [
                f"{exact_title} has an overall {arc_direction} compound_sentiment ({overall_sentiment:.2f}) "
                f"based on analysis of n={len(sentiment_data)} minutes. "
                f"The sentiment_variance is {sentiment_variance:.3f} and the emotional_range spans {emotional_range:.3f}. "
                f"Beginning_sentiment starts at {beginning_sentiment:.2f} and ending_sentiment concludes at {ending_sentiment:.2f}. "
            ]

            if peak_minute is not None:
                peak_sentiment = next((s['compound'] for s in sentiment_data if s['minute_offset'] == peak_minute), 0.0)
                answer_parts.append(f"Peak_positive_sentiment occurs at minute {peak_minute} with compound_sentiment {peak_sentiment:.2f}.")
            if dip_minute is not None:
                dip_sentiment = next((s['compound'] for s in sentiment_data if s['minute_offset'] == dip_minute), 0.0)
                answer_parts.append(f"Lowest point (most negative compound_sentiment) occurs at minute {dip_minute} with value {dip_sentiment:.2f}.")
            
            # Add citation
            answer_parts.append(
                f"\nThis analysis comes from my **Emotion Archive**, where I've analyzed {len(sentiment_data)} minutes "
                f"of dialogue across multiple language translations."
            )

            # Top positive moments with dialogue, emotions, and interpretation
            answer_parts.append("\n🌟 Top positive moments:")
            for i, peak in enumerate(positive_peaks[:3], 1):
                minute = peak['minute_offset']
                score = peak['compound']
                # Convert minute to HH:MM:SS timestamp
                hours = minute // 60
                minutes = minute % 60
                timestamp = f"{hours:02d}:{minutes:02d}:00"
                answer_parts.append(f"\n{i}. **Minute {minute} ({timestamp})** (sentiment: {score:.2f})")

                # Get enriched data for this minute
                minute_data = dialogue_with_emotions.get(minute, {})
                dialogue_lines = minute_data.get("dialogue", [])
                emotions = minute_data.get("emotions", {})

                # Show top dialogue with emotion annotations
                if dialogue_lines:
                    answer_parts.append("   **Key dialogue:**")
                    # Get top 2 emotions for annotation
                    top_2_emotions = sorted(emotions.items(), key=lambda x: x[1], reverse=True)[:2] if emotions else []

                    for line in dialogue_lines[:2]:  # Top 2 lines
                        # Format emotions as (emotion: score, ...) - show only top 2
                        if top_2_emotions:
                            emotion_str = ", ".join([f"{k}: {v:.3f}" for k, v in top_2_emotions])
                            answer_parts.append(f'   - "{line}" ({emotion_str})')
                        else:
                            answer_parts.append(f'   - "{line}"')

                # Add interpretation based on dominant emotions
                if emotions:
                    top_emotions = sorted(emotions.items(), key=lambda x: x[1], reverse=True)[:2]
                    emotion_names = [e[0] for e in top_emotions]

                    answer_parts.append(f"\n   **Why this moment feels positive:**")
                    answer_parts.append(f"   The dialogue here shows strong {' and '.join(emotion_names)} emotions.")
                    answer_parts.append(f"   Based on this emotional signature, I imagine this could be a moment of")
                    answer_parts.append(f"   {_interpret_positive_emotions(emotion_names, dialogue_lines)}")

            # Top negative moments with dialogue, emotions, and interpretation
            answer_parts.append("\n😔 Top negative moments:")
            for i, peak in enumerate(negative_peaks[:3], 1):
                minute = peak['minute_offset']
                score = peak['compound']
                # Convert minute to HH:MM:SS timestamp
                hours = minute // 60
                minutes = minute % 60
                timestamp = f"{hours:02d}:{minutes:02d}:00"
                answer_parts.append(f"\n{i}. **Minute {minute} ({timestamp})** (sentiment: {score:.2f})")

                # Get enriched data for this minute
                minute_data = dialogue_with_emotions.get(minute, {})
                dialogue_lines = minute_data.get("dialogue", [])
                emotions = minute_data.get("emotions", {})

                # Show top dialogue with emotion annotations
                if dialogue_lines:
                    answer_parts.append("   **Key dialogue:**")
                    # Get top 2 emotions for annotation
                    top_2_emotions = sorted(emotions.items(), key=lambda x: x[1], reverse=True)[:2] if emotions else []

                    for line in dialogue_lines[:2]:  # Top 2 lines
                        # Format emotions as (emotion: score, ...) - show only top 2
                        if top_2_emotions:
                            emotion_str = ", ".join([f"{k}: {v:.3f}" for k, v in top_2_emotions])
                            answer_parts.append(f'   - "{line}" ({emotion_str})')
                        else:
                            answer_parts.append(f'   - "{line}"')

                # Add interpretation based on dominant emotions
                if emotions:
                    top_emotions = sorted(emotions.items(), key=lambda x: x[1], reverse=True)[:2]
                    emotion_names = [e[0] for e in top_emotions]

                    answer_parts.append(f"\n   **Why this moment feels negative:**")
                    answer_parts.append(f"   The dialogue here shows strong {' and '.join(emotion_names)} emotions.")
                    answer_parts.append(f"   Based on this emotional signature, I imagine this could be a moment of")
                    answer_parts.append(f"   {_interpret_negative_emotions(emotion_names, dialogue_lines)}")

        answer = "\n".join(answer_parts)

        # Create visualization data
        minute_offsets = [s["minute_offset"] for s in sentiment_data]
        compound_scores = [s["compound"] for s in sentiment_data]

        visualization_data = {
            "chart_type": "line",
            "x": minute_offsets,
            "y": compound_scores,
            "title": f"Sentiment Arc: {exact_title}",
            "xlabel": "Minute Offset",
            "ylabel": "Compound Sentiment Score",
        }

        followups = [
            f"Compare sentiment with other films",
            f"Show most emotional moments in {exact_title}",
            f"Analyze sentiment correlation with box office for {exact_title}",
        ]

        return _format_tool_response(
            answer=answer,
            tables=["raw.film_emotions", "staging.stg_films"],
            functions=["AVG()", "ORDER BY", "SQL aggregation"],
            computation_method="SQL aggregation on emotion data with compound sentiment calculation",
            row_count=len(sentiment_data),
            visualization_data=visualization_data,
            suggested_followups=followups,
        )

    except ValueError as e:
        logger.warning(f"Validation error in get_film_sentiment: {e}")
        return _format_tool_response(
            answer=f"Validation error: {str(e)}",
            tables=[],
            functions=[],
        )
    except duckdb.Error as e:
        logger.error(f"Database error in get_film_sentiment: {e}", exc_info=True)
        return _format_tool_response(
            answer="Database query failed. Please try again.",
            tables=[],
            functions=[],
        )
    except Exception as e:
        logger.error(f"Unexpected error in get_film_sentiment: {e}", exc_info=True)
        return _format_tool_response(
            answer="An unexpected error occurred. Please try again.",
            tables=[],
            functions=[],
        )


@tool
def query_graph_database(sql: str) -> Dict[str, Any]:
    """
    Execute a safe SELECT query against graph mart tables (read-only).

    This tool allows the LLM to execute custom SQL queries for flexible data exploration.
    Only SELECT statements are allowed, and queries are restricted to whitelisted tables.

    Args:
        sql: SQL SELECT query string. Must start with SELECT and only reference whitelisted tables.

    Returns:
        ToolResponse dict with:
        - answer: Summary of query results
        - data_sources: Tables and functions used
        - visualization_data: Optional chart data if query returns numeric columns
        - suggested_followups: Related questions

    Example:
        >>> result = query_graph_database("SELECT name FROM main_marts.mart_graph_nodes WHERE node_type = 'film' LIMIT 5")
        >>> result["answer"]
        "Query returned 5 rows: Spirited Away, My Neighbor Totoro..."
    """
    logger.info(f"Executing query_graph_database", extra={"sql_preview": sql[:100] if len(sql) > 100 else sql})

    try:
        # Input validation
        if not sql or not isinstance(sql, str):
            return _format_tool_response(
                answer="Error: sql must be a non-empty string",
                tables=[],
                functions=[],
            )

        sql_upper = sql.strip().upper()

        # Validate SQL starts with SELECT
        if not sql_upper.startswith("SELECT"):
            return _format_tool_response(
                answer="Error: Only SELECT queries are allowed. Query must start with SELECT.",
                tables=[],
                functions=[],
            )

        # Check for dangerous keywords
        if not _sanitize_sql_keywords(sql):
            return _format_tool_response(
                answer="Error: Query contains dangerous keywords (DROP, DELETE, UPDATE, INSERT, ALTER, TRUNCATE, CREATE). Only SELECT queries are allowed.",
                tables=[],
                functions=[],
            )

        # Validate table references (basic check - check if whitelisted tables are mentioned)
        sql_lower = sql.lower()
        has_whitelisted_table = any(table.lower() in sql_lower for table in GRAPH_TABLES_WHITELIST)

        if not has_whitelisted_table:
            return _format_tool_response(
                answer=f"Error: Query must reference one of the whitelisted tables: {', '.join(GRAPH_TABLES_WHITELIST)}",
                tables=[],
                functions=[],
            )

        # Execute query with limit
        conn = _get_duckdb_connection()

        # Add LIMIT if not present
        if "LIMIT" not in sql_upper:
            sql = f"{sql.rstrip(';')} LIMIT {MAX_QUERY_RESULTS}"

        try:
            result = conn.execute(sql).fetchall()
            columns = [desc[0] for desc in conn.description] if conn.description else []

            # Convert to list of dicts
            rows = [dict(zip(columns, row)) for row in result]

            # Generate answer
            if not rows:
                answer = "Query returned no results."
            else:
                answer = f"Query returned {len(rows)} row(s)."
                if len(rows) <= 5:
                    # Show sample data
                    answer += "\n\nSample results:"
                    for i, row in enumerate(rows, 1):
                        answer += f"\n{i}. {row}"

            # Try to create visualization data if numeric columns exist
            visualization_data = None
            if rows and len(rows) > 0:
                numeric_cols = [col for col in columns if any(isinstance(row.get(col), (int, float)) for row in rows)]
                if numeric_cols and len(numeric_cols) >= 2:
                    # Use first two numeric columns for scatter plot
                    x_col, y_col = numeric_cols[0], numeric_cols[1]
                    visualization_data = {
                        "chart_type": "scatter",
                        "x": [row.get(x_col) for row in rows],
                        "y": [row.get(y_col) for row in rows],
                        "title": f"Query Results: {x_col} vs {y_col}",
                        "xlabel": x_col,
                        "ylabel": y_col,
                    }

            # Extract table names from SQL (simple regex-like extraction)
            tables_queried = [table for table in GRAPH_TABLES_WHITELIST if table.lower() in sql_lower]

            followups = [
                "Refine this query with additional filters",
                "Show related data from other tables",
                "Export these results",
            ]

            return _format_tool_response(
                answer=answer,
                tables=tables_queried,
                functions=["User-provided SQL query"],
                computation_method="User-provided SQL query",
                row_count=len(rows),
                visualization_data=visualization_data,
                suggested_followups=followups,
            )

        except duckdb.Error as e:
            logger.warning(f"SQL syntax error: {e}")
            return _format_tool_response(
                answer=f"SQL syntax error: {str(e)}. Please check your query syntax.",
                tables=[],
                functions=[],
            )

    except ValueError as e:
        logger.warning(f"Validation error in query_graph_database: {e}")
        return _format_tool_response(
            answer=f"Validation error: {str(e)}",
            tables=[],
            functions=[],
        )
    except Exception as e:
        logger.error(f"Unexpected error in query_graph_database: {e}", exc_info=True)
        return _format_tool_response(
            answer="An unexpected error occurred. Please try again.",
            tables=[],
            functions=[],
        )


@tool
def find_films_by_criteria(
    director: Optional[str] = None,
    min_year: Optional[int] = None,
    min_rating: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Find films matching specified criteria (director, release year, Rotten Tomatoes score).

    This tool queries the films table with optional filters to find matching films.

    Args:
        director: Director name (exact match, case-insensitive)
        min_year: Minimum release year (inclusive)
        min_rating: Minimum Rotten Tomatoes score (0-100, inclusive)

    Returns:
        ToolResponse dict with:
        - answer: List of matching films with details
        - data_sources: Tables and functions used
        - visualization_data: Optional table visualization
        - suggested_followups: Related questions

    Example:
        >>> result = find_films_by_criteria(director="Hayao Miyazaki", min_year=2000)
        >>> result["answer"]
        "Found 3 films matching criteria: Spirited Away (2001, RT: 97)..."
    """
    logger.info(
        f"Executing find_films_by_criteria",
        extra={"director": director, "min_year": min_year, "min_rating": min_rating},
    )

    try:
        # Input validation: at least one filter must be provided
        if not any([director, min_year is not None, min_rating is not None]):
            return _format_tool_response(
                answer="Error: At least one filter parameter (director, min_year, min_rating) must be provided",
                tables=[],
                functions=[],
            )

        # Validate min_rating range
        if min_rating is not None and (min_rating < 0 or min_rating > 100):
            return _format_tool_response(
                answer="Error: min_rating must be between 0 and 100",
                tables=[],
                functions=[],
            )

        conn = _get_duckdb_connection()

        # Build dynamic SQL query
        where_clauses = []
        params = []

        if director:
            where_clauses.append("LOWER(director) = LOWER(?)")
            params.append(director)

        if min_year is not None:
            where_clauses.append("release_year >= ?")
            params.append(min_year)

        if min_rating is not None:
            where_clauses.append("rt_score >= ?")
            params.append(min_rating)

        where_sql = " AND ".join(where_clauses)

        query = f"""
            SELECT id, title, release_year, director, rt_score
            FROM main_staging.stg_films
            WHERE {where_sql}
            ORDER BY release_year DESC
            LIMIT {MAX_QUERY_RESULTS}
        """

        result = conn.execute(query, params).fetchall()

        if not result:
            filter_desc = []
            if director:
                filter_desc.append(f"director='{director}'")
            if min_year is not None:
                filter_desc.append(f"year>={min_year}")
            if min_rating is not None:
                filter_desc.append(f"rating>={min_rating}")

            return _format_tool_response(
                answer=f"No films found matching criteria: {', '.join(filter_desc)}",
                tables=["staging.stg_films"],
                functions=["SQL WHERE filters"],
                computation_method="DuckDB SQL filtering",
                row_count=0,
                suggested_followups=["List all available films", "Try different filter criteria"],
            )

        # Convert to list of dicts
        films = [
            {
                "id": row[0],
                "title": row[1],
                "release_year": row[2],
                "director": row[3],
                "rt_score": row[4],
            }
            for row in result
        ]

        # Generate answer
        answer_parts = [f"Found {len(films)} film(s) matching criteria:"]
        for film in films:
            answer_parts.append(
                f"- {film['title']} ({film['release_year']}, Director: {film['director']}, RT Score: {film['rt_score']})"
            )

        answer = "\n".join(answer_parts)

        followups = [
            f"Show sentiment analysis for these films",
            "Find similar films",
            "Compare ratings across these films",
        ]

        return _format_tool_response(
            answer=answer,
            tables=["staging.stg_films"],
            functions=["SQL WHERE filters", "ORDER BY"],
            computation_method="DuckDB SQL filtering",
            row_count=len(films),
            suggested_followups=followups,
        )

    except ValueError as e:
        logger.warning(f"Validation error in find_films_by_criteria: {e}")
        return _format_tool_response(
            answer=f"Validation error: {str(e)}",
            tables=[],
            functions=[],
        )
    except duckdb.Error as e:
        logger.error(f"Database error in find_films_by_criteria: {e}", exc_info=True)
        return _format_tool_response(
            answer="Database query failed. Please try again.",
            tables=[],
            functions=[],
        )
    except Exception as e:
        logger.error(f"Unexpected error in find_films_by_criteria: {e}", exc_info=True)
        return _format_tool_response(
            answer="An unexpected error occurred. Please try again.",
            tables=[],
            functions=[],
        )


@tool
def correlate_metrics(metric_x: str, metric_y: str, compact: bool = False) -> Dict[str, Any]:
    """
    Calculate Pearson correlation between two film metrics.

    Supported metrics:
    - sentiment: Average compound sentiment score from emotion analysis
    - box_office: Box office revenue in USD (requires revenue_usd IS NOT NULL)
    - rt_score: Rotten Tomatoes critic score (0-100)
    - tmdb_rating: TMDB audience rating (0-10, requires tmdb_rating IS NOT NULL)

    This tool calculates Pearson correlation coefficient and p-value to measure
    the strength and significance of the relationship between two metrics.

    Args:
        metric_x: First metric name (must be one of: sentiment, box_office, rt_score, tmdb_rating)
        metric_y: Second metric name (must be one of: sentiment, box_office, rt_score, tmdb_rating)
        compact: If True, returns only summary statistics (r, p, n) without film lists. Default: False.

    Returns:
        ToolResponse dict with:
        - answer: Correlation interpretation and summary
        - data_sources: Tables and functions used
        - visualization_data: Scatter plot data (omitted in compact mode)
        - suggested_followups: Related questions

    Example:
        >>> result = correlate_metrics("sentiment", "box_office")
        >>> result["answer"]
        "Correlation between sentiment and box_office: r=0.45 (moderate positive), p=0.07, n=12 films"
    """
    logger.info(f"Executing correlate_metrics (compact={compact})", extra={"metric_x": metric_x, "metric_y": metric_y})

    try:
        # Input validation
        supported_metrics = ["sentiment", "box_office", "rt_score", "tmdb_rating"]
        if metric_x not in supported_metrics:
            return _format_tool_response(
                answer=f"Error: Invalid metric_x '{metric_x}'. Supported metrics: {', '.join(supported_metrics)}",
                tables=[],
                functions=[],
            )
        if metric_y not in supported_metrics:
            return _format_tool_response(
                answer=f"Error: Invalid metric_y '{metric_y}'. Supported metrics: {', '.join(supported_metrics)}",
                tables=[],
                functions=[],
            )

        if metric_x == metric_y:
            return _format_tool_response(
                answer="Error: metric_x and metric_y must be different",
                tables=[],
                functions=[],
            )

        conn = _get_duckdb_connection()

        # Build dynamic SQL query based on metrics
        # Determine which joins are needed
        needs_sentiment_join = metric_x == "sentiment" or metric_y == "sentiment"
        needs_kaggle_join = metric_x == "box_office" or metric_y == "box_office"

        # Build SELECT clause for x_value
        if metric_x == "sentiment":
            x_select = "sentiment.avg_compound"
        elif metric_x == "box_office":
            x_select = "k.revenue"
        elif metric_x == "rt_score":
            x_select = "f.rt_score"
        elif metric_x == "tmdb_rating":
            # Check if tmdb_rating column exists
            try:
                conn.execute("SELECT tmdb_rating FROM main_staging.stg_films LIMIT 1").fetchone()
                x_select = "f.tmdb_rating"
            except duckdb.BinderException:
                return _format_tool_response(
                    answer=f"Error: Column 'tmdb_rating' not available. Try using 'rt_score' instead. Available metrics: sentiment, box_office, rt_score",
                    tables=[],
                    functions=[],
                )
        else:
            x_select = "NULL"

        # Build SELECT clause for y_value
        if metric_y == "sentiment":
            y_select = "sentiment.avg_compound"
        elif metric_y == "box_office":
            y_select = "k.revenue"
        elif metric_y == "rt_score":
            y_select = "f.rt_score"
        elif metric_y == "tmdb_rating":
            # Check if tmdb_rating column exists
            try:
                conn.execute("SELECT tmdb_rating FROM main_staging.stg_films LIMIT 1").fetchone()
                y_select = "f.tmdb_rating"
            except duckdb.BinderException:
                return _format_tool_response(
                    answer=f"Error: Column 'tmdb_rating' not available. Try using 'rt_score' instead. Available metrics: sentiment, box_office, rt_score",
                    tables=[],
                    functions=[],
                )
        else:
            y_select = "NULL"

        # Build FROM clause with necessary JOINs
        from_parts = ["FROM main_staging.stg_films f"]

        if needs_sentiment_join:
            from_parts.append(
                """LEFT JOIN (
                    SELECT
                        film_slug,
                        AVG(compound_sentiment) as avg_compound
                    FROM (
                        SELECT
                            film_slug,
                            minute_offset,
                            (emotion_admiration + emotion_amusement + emotion_approval + emotion_caring +
                             emotion_excitement + emotion_gratitude + emotion_joy + emotion_love +
                             emotion_optimism + emotion_pride + emotion_relief -
                             emotion_anger - emotion_annoyance - emotion_disappointment - emotion_disapproval -
                             emotion_disgust - emotion_embarrassment - emotion_fear - emotion_grief -
                             emotion_nervousness - emotion_remorse - emotion_sadness) as compound_sentiment
                        FROM raw.film_emotions
                    ) sub
                    GROUP BY film_slug
                ) sentiment ON sentiment.film_slug LIKE CONCAT('%', REPLACE(LOWER(f.title), ' ', '_'), '%')"""
            )

        if needs_kaggle_join:
            from_parts.append("LEFT JOIN main_staging.stg_kaggle_films k ON f.id = k.film_id")

        from_clause = " ".join(from_parts)

        # Build WHERE clause
        where_clauses = []
        if needs_kaggle_join:
            where_clauses.append("k.revenue IS NOT NULL")
        if metric_x == "tmdb_rating" or metric_y == "tmdb_rating":
            where_clauses.append("f.tmdb_rating IS NOT NULL")

        where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        # Build final query
        query = f"""
            SELECT 
                f.id,
                f.title,
                {x_select} as x_value,
                {y_select} as y_value
            {from_clause}
            {where_sql}
        """

        result = conn.execute(query).fetchall()

        if not result:
            return _format_tool_response(
                answer=f"No data found for {metric_x} and {metric_y} correlation.",
                tables=["staging.stg_films", "raw.film_emotions", "staging.stg_kaggle_films"],
                functions=["AVG()", "pearsonr()"],
                row_count=0,
            )

        # Extract data pairs, filtering out NULLs
        data_pairs = []
        for row in result:
            film_id = row[0]
            film_title = row[1]
            x_val = row[2] if len(row) > 2 else None
            y_val = row[3] if len(row) > 3 else None

            if x_val is not None and y_val is not None:
                data_pairs.append({"title": film_title, "x_value": float(x_val), "y_value": float(y_val)})

        # Validate sample size
        if len(data_pairs) < 2:
            return _format_tool_response(
                answer=f"Insufficient data for correlation (need at least 2 films with both {metric_x} and {metric_y} data, found {len(data_pairs)})",
                tables=["staging.stg_films", "raw.film_emotions", "staging.stg_kaggle_films"],
                functions=["AVG()"],
                row_count=len(data_pairs),
            )

        # Calculate correlation
        x_values = [pair["x_value"] for pair in data_pairs]
        y_values = [pair["y_value"] for pair in data_pairs]

        try:
            correlation, p_value = pearsonr(x_values, y_values)
        except Exception as e:
            logger.error(f"Correlation calculation error: {e}")
            return _format_tool_response(
                answer=f"Error calculating correlation: {str(e)}",
                tables=["staging.stg_films", "raw.film_emotions", "staging.stg_kaggle_films"],
                functions=["pearsonr()"],
                row_count=len(data_pairs),
            )

        # Generate interpretation
        abs_corr = abs(correlation)
        if abs_corr < 0.3:
            strength = "weak"
        elif abs_corr < 0.7:
            strength = "moderate"
        else:
            strength = "strong"

        direction = "positive" if correlation > 0 else "negative"

        significance = "statistically significant" if p_value < 0.05 else "not statistically significant"

        # Build answer with explicit statistical terms and sentiment metrics
        # Use explicit metric names - if sentiment is involved, use "compound_sentiment"
        x_metric_name = "compound_sentiment" if metric_x == "sentiment" else metric_x.replace("_", " ")
        y_metric_name = "compound_sentiment" if metric_y == "sentiment" else metric_y.replace("_", " ")

        if compact:
            # COMPACT MODE: Only summary statistics
            answer_parts = [
                f"Correlation: r={correlation:.3f} ({strength} {direction}), "
                f"p={p_value:.3f}, n={len(data_pairs)}"
            ]
        else:
            # FULL MODE: Detailed interpretation
            answer_parts = [
                f"Correlation between {x_metric_name} and {y_metric_name}: "
                f"r={correlation:.3f} ({strength} {direction}), "
                f"p-value={p_value:.3f} ({significance}), "
                f"n={len(data_pairs)} films"
            ]

            # Add interpretation for sentiment-related correlations
            if metric_x == "sentiment" or metric_y == "sentiment":
                answer_parts.append(
                    f"\nThis analysis comes from my **Pattern Discovery Tools**, which correlate "
                    f"compound_sentiment scores from my **Emotion Archive** with success metrics. "
                )
                if abs_corr > 0.3 and p_value < 0.05:
                    answer_parts.append(
                        f"Based on this {strength} {direction} correlation, I imagine that films with "
                        f"{'higher' if direction == 'positive' else 'lower'} emotional sentiment tend to "
                        f"{'achieve better' if direction == 'positive' else 'struggle with'} {metric_y if metric_x == 'sentiment' else metric_x} performance. "
                        f"The statistical significance (p < 0.05) suggests this pattern is meaningful, not random."
                    )
                elif abs_corr > 0.3:
                    answer_parts.append(
                        f"While the correlation is {strength}, it's not statistically significant (p > 0.05), "
                        f"so I interpret this as an interesting pattern that needs more data to confirm."
                    )
                else:
                    answer_parts.append(
                        f"The weak correlation suggests that {metric_x} and {metric_y} are largely independent. "
                        f"My interpretation is that emotional sentiment and {metric_y if metric_x == 'sentiment' else metric_x} "
                        f"don't strongly influence each other in these films."
                    )

        answer = "".join(answer_parts)

        # Create visualization data (omit in compact mode to save tokens)
        visualization_data = None
        if not compact:
            visualization_data = {
                "chart_type": "scatter",
                "x": x_values,
                "y": y_values,
                "title": f"{metric_x} vs {metric_y} Correlation",
                "xlabel": metric_x.replace("_", " ").title(),
                "ylabel": metric_y.replace("_", " ").title(),
            }

        followups = [
            f"Analyze outliers in {metric_x} vs {metric_y}",
            f"Compare {metric_x} with other metrics",
            f"Compare {metric_y} with other metrics",
            "Show individual film data",
        ]

        tables_used = ["staging.stg_films"]
        if needs_sentiment_join:
            tables_used.append("raw.film_emotions")
        if metric_x == "box_office" or metric_y == "box_office":
            tables_used.append("staging.stg_kaggle_films")

        return _format_tool_response(
            answer=answer,
            tables=tables_used,
            functions=["AVG()" if needs_sentiment_join else "", "pearsonr()"],
            computation_method="scipy.stats Pearson correlation",
            row_count=len(data_pairs),
            visualization_data=visualization_data,
            suggested_followups=followups,
        )

    except ValueError as e:
        logger.warning(f"Validation error in correlate_metrics: {e}")
        return _format_tool_response(
            answer=f"Validation error: {str(e)}",
            tables=[],
            functions=[],
        )
    except duckdb.Error as e:
        logger.error(f"Database error in correlate_metrics: {e}", exc_info=True)
        return _format_tool_response(
            answer="Database query failed. Please try again.",
            tables=[],
            functions=[],
        )
    except Exception as e:
        logger.error(f"Unexpected error in correlate_metrics: {e}", exc_info=True)
        return _format_tool_response(
            answer="An unexpected error occurred. Please try again.",
            tables=[],
            functions=[],
        )


@tool
def compare_sentiment_arcs_across_languages(
    film_title: str,
    language_codes: Optional[List[str]] = None,
    emotion_dimension: str = "compound",
    compact: bool = False,
) -> Dict[str, Any]:
    """
    Compare emotional trajectories of the same film across different language translations.

    This tool analyzes how emotional arcs differ between translations by:
    - Calculating correlation matrix between language pairs
    - Identifying peak moments and their alignment
    - Finding divergence points where translations differ emotionally
    - Extracting dialogue quotes at key moments to explain differences

    Args:
        film_title: Title of the film to compare (e.g., "Spirited Away")
        language_codes: List of language codes to compare (default: all available: en, fr, es, nl, ar)
        emotion_dimension: Emotion to analyze - "compound", "dominant", or specific emotion name (e.g., "joy", "fear")
        compact: If True, returns divergence summary and top 3 divergence points only. Default: False.

    Returns:
        ToolResponse dict with:
        - answer: Comprehensive comparison summary with quote-driven explanations (or brief summary if compact)
        - data_sources: Tables and functions used
        - visualization_data: Multi-line chart data for arc comparison (omitted in compact mode)
        - suggested_followups: Related questions
        - quote_analysis: Optional dict with peak quotes and divergence analysis (omitted in compact mode)

    Example:
        >>> result = compare_sentiment_arcs_across_languages("Spirited Away", ["en", "fr", "es"])
        >>> result["answer"]
        "Comparing Spirited Away across 3 languages: EN-FR correlation 0.85, peak alignment differs..."
    """
    logger.info(
        f"Executing compare_sentiment_arcs_across_languages (compact={compact})",
        extra={"film_title": film_title, "language_codes": language_codes, "emotion_dimension": emotion_dimension},
    )

    try:
        # Input validation
        if not film_title or not isinstance(film_title, str):
            return _format_tool_response(
                answer="Error: film_title must be a non-empty string",
                tables=[],
                functions=[],
            )

        supported_languages = ["en", "fr", "es", "nl", "ar"]
        if language_codes is None:
            language_codes = supported_languages
        else:
            invalid_langs = [lang for lang in language_codes if lang not in supported_languages]
            if invalid_langs:
                return _format_tool_response(
                    answer=f"Error: Invalid language code(s): {invalid_langs}. Supported: {', '.join(supported_languages)}",
                    tables=[],
                    functions=[],
                )

        supported_emotions = [
            "compound",
            "dominant",
            "joy",
            "fear",
            "sadness",
            "anger",
            "love",
            "surprise",
            "admiration",
            "amusement",
            "approval",
            "caring",
            "excitement",
            "gratitude",
            "optimism",
            "pride",
            "relief",
            "annoyance",
            "disappointment",
            "disapproval",
            "disgust",
            "embarrassment",
            "grief",
            "nervousness",
            "remorse",
            "confusion",
            "curiosity",
            "desire",
            "realization",
            "neutral",
        ]
        if emotion_dimension not in supported_emotions:
            return _format_tool_response(
                answer=f"Error: Invalid emotion_dimension '{emotion_dimension}'. Supported: compound, dominant, or specific emotion name",
                tables=[],
                functions=[],
            )

        if len(language_codes) < 2:
            return _format_tool_response(
                answer=f"Error: Need at least 2 languages for comparison. Provided: {language_codes}",
                tables=[],
                functions=[],
            )

        conn = _get_duckdb_connection()

        # Find film
        exact_title = _fuzzy_match_film_title(film_title, conn)
        if not exact_title:
            return _format_tool_response(
                answer=f"Film '{film_title}' not found in the database.",
                tables=["staging.stg_films"],
                functions=["SQL SELECT"],
                row_count=0,
            )

        # Build film slug pattern
        film_slug_base = film_title.lower().replace(" ", "_").replace("'", "")

        # Query emotion data for all requested languages
        if emotion_dimension == "compound":
            # Calculate compound score
            emotion_select = """
                (emotion_admiration + emotion_amusement + emotion_approval + emotion_caring +
                 emotion_excitement + emotion_gratitude + emotion_joy + emotion_love +
                 emotion_optimism + emotion_pride + emotion_relief -
                 emotion_anger - emotion_annoyance - emotion_disappointment - emotion_disapproval -
                 emotion_disgust - emotion_embarrassment - emotion_fear - emotion_grief -
                 emotion_nervousness - emotion_remorse - emotion_sadness) as emotion_score
            """
        elif emotion_dimension == "dominant":
            # Find dominant emotion (highest scoring)
            emotion_select = """
                GREATEST(
                    emotion_admiration, emotion_amusement, emotion_approval, emotion_caring,
                    emotion_excitement, emotion_gratitude, emotion_joy, emotion_love,
                    emotion_optimism, emotion_pride, emotion_relief,
                    emotion_anger, emotion_annoyance, emotion_disappointment, emotion_disapproval,
                    emotion_disgust, emotion_embarrassment, emotion_fear, emotion_grief,
                    emotion_nervousness, emotion_remorse, emotion_sadness,
                    emotion_confusion, emotion_curiosity, emotion_desire, emotion_realization,
                    emotion_surprise, emotion_neutral
                ) as emotion_score
            """
        else:
            # Specific emotion
            emotion_col = f"emotion_{emotion_dimension}"
            emotion_select = f"{emotion_col} as emotion_score"

        # Build language filter
        lang_placeholders = ",".join(["?" for _ in language_codes])

        query = f"""
            SELECT
                language_code,
                minute_offset,
                {emotion_select}
            FROM raw.film_emotions
            WHERE film_slug LIKE ? AND language_code IN ({lang_placeholders})
            ORDER BY language_code, minute_offset
        """

        params = [f"{film_slug_base}_%"] + language_codes
        result = conn.execute(query, params).fetchall()

        if not result:
            return _format_tool_response(
                answer=f"No emotion data found for '{exact_title}' in languages: {', '.join(language_codes)}",
                tables=["raw.film_emotions"],
                functions=["SQL SELECT"],
                row_count=0,
            )

        # Organize data by language
        arcs_by_language = {}
        for row in result:
            lang_code, minute_offset, emotion_score = row
            if lang_code not in arcs_by_language:
                arcs_by_language[lang_code] = []
            arcs_by_language[lang_code].append((minute_offset, emotion_score))

        # Check we have at least 2 languages with data
        if len(arcs_by_language) < 2:
            return _format_tool_response(
                answer=f"Insufficient data: Need at least 2 languages with emotion data. Found: {list(arcs_by_language.keys())}",
                tables=["raw.film_emotions"],
                functions=["SQL SELECT"],
                row_count=len(result),
            )

        # Align arcs by minute_offset (fill missing minutes with None)
        all_minutes = set()
        for lang_data in arcs_by_language.values():
            all_minutes.update([m for m, _ in lang_data])
        all_minutes = sorted(all_minutes)

        aligned_arcs = {}
        for lang_code in arcs_by_language:
            lang_dict = dict(arcs_by_language[lang_code])
            aligned_arcs[lang_code] = [lang_dict.get(minute, None) for minute in all_minutes]

        # Calculate correlation matrix
        correlation_matrix = {}
        lang_list = list(aligned_arcs.keys())
        for i, lang1 in enumerate(lang_list):
            for lang2 in lang_list[i + 1 :]:
                # Filter out None values
                pairs = [
                    (aligned_arcs[lang1][j], aligned_arcs[lang2][j])
                    for j in range(len(all_minutes))
                    if aligned_arcs[lang1][j] is not None and aligned_arcs[lang2][j] is not None
                ]
                if len(pairs) >= 2:
                    x_vals = [p[0] for p in pairs]
                    y_vals = [p[1] for p in pairs]
                    try:
                        corr, _ = pearsonr(x_vals, y_vals)
                        correlation_matrix[(lang1, lang2)] = corr
                    except Exception:
                        correlation_matrix[(lang1, lang2)] = None

        # Find peak moments per language (top 3)
        peak_moments = {}
        for lang_code, scores in aligned_arcs.items():
            valid_scores = [(all_minutes[i], score) for i, score in enumerate(scores) if score is not None]
            valid_scores.sort(key=lambda x: x[1], reverse=True)
            peak_moments[lang_code] = valid_scores[:3]

        # Calculate variance per minute (divergence points)
        variance_by_minute = []
        for i, minute in enumerate(all_minutes):
            scores_at_minute = [
                aligned_arcs[lang][i] for lang in aligned_arcs.keys() if aligned_arcs[lang][i] is not None
            ]
            if len(scores_at_minute) > 1:
                variance = np.var(scores_at_minute)
                variance_by_minute.append((minute, variance, scores_at_minute))
        variance_by_minute.sort(key=lambda x: x[1], reverse=True)
        divergence_points = variance_by_minute[:3]  # Top 3 divergence points

        # Calculate per-language summary statistics (avg, min, max sentiment per language)
        per_language_stats = {}
        for lang_code, scores in aligned_arcs.items():
            valid_scores = [s for s in scores if s is not None]
            if valid_scores:
                per_language_stats[lang_code] = {
                    "avg": np.mean(valid_scores),
                    "min": min(valid_scores),
                    "max": max(valid_scores),
                    "n": len(valid_scores)
                }

        # Generate answer text
        avg_correlation = np.mean([v for v in correlation_matrix.values() if v is not None])

        if compact:
            # COMPACT MODE: Brief summary only
            answer_parts = [
                f"Comparing '{exact_title}' across {len(lang_list)} languages: "
                f"avg correlation={avg_correlation:.3f}"
            ]

            # Most similar and divergent pairs (brief)
            if correlation_matrix:
                max_corr_pair = max(correlation_matrix.items(), key=lambda x: x[1] if x[1] is not None else -1)
                min_corr_pair = min(correlation_matrix.items(), key=lambda x: x[1] if x[1] is not None else 1)
                answer_parts.append(
                    f", most similar: {max_corr_pair[0][0].upper()}-{max_corr_pair[0][1].upper()} (r={max_corr_pair[1]:.3f}), "
                    f"most divergent: {min_corr_pair[0][0].upper()}-{min_corr_pair[0][1].upper()} (r={min_corr_pair[1]:.3f})"
                )

            # Top 3 divergence points
            if divergence_points:
                div_minutes = [str(d[0]) for d in divergence_points[:3]]
                answer_parts.append(f". Divergence at minutes: {', '.join(div_minutes)}")

        else:
            # FULL MODE: Detailed analysis with quotes
            answer_parts = [
                f"⚠️ IMPORTANT: Subtitle timing varies across language sources. The same minute offset may represent different scenes. Results use 10-minute smoothing to reduce misalignment effects.",
                f"\nComparing '{exact_title}' across {len(lang_list)} languages ({', '.join(lang_list).upper()}) using {emotion_dimension} dimension:",
                f"Average correlation across language pairs: {avg_correlation:.3f}",
            ]

            # Add per-language summary statistics
            if per_language_stats:
                answer_parts.append("\n\nPer-language sentiment metrics:")
                for lang_code, stats in per_language_stats.items():
                    answer_parts.append(
                        f"  {lang_code.upper()}: avg compound_sentiment={stats['avg']:.3f}, "
                        f"min={stats['min']:.3f}, max={stats['max']:.3f}, n={stats['n']}"
                    )

            # Most similar and divergent pairs
            if correlation_matrix:
                max_corr_pair = max(correlation_matrix.items(), key=lambda x: x[1] if x[1] is not None else -1)
                min_corr_pair = min(correlation_matrix.items(), key=lambda x: x[1] if x[1] is not None else 1)
                answer_parts.append(f"\nMost similar pair: {max_corr_pair[0][0].upper()}-{max_corr_pair[0][1].upper()} (r={max_corr_pair[1]:.3f})")
                answer_parts.append(f"Most divergent pair: {min_corr_pair[0][0].upper()}-{min_corr_pair[0][1].upper()} (r={min_corr_pair[1]:.3f})")

            if divergence_points:
                answer_parts.append(f"\nKey divergence points:")
                for minute, variance, scores in divergence_points:
                    answer_parts.append(f"  Minute {minute}: variance={variance:.3f}")

        # Extract quotes at peak moments and divergence points (skip in compact mode)
        quote_analysis = None
        film_slug_for_quotes = film_slug_base

        if not compact:
            try:
                # Extract quotes at peak moments per language
                peak_quotes = {}
                for lang_code, peaks in peak_moments.items():
                    peak_minutes = [p[0] for p in peaks]
                    excerpts = load_dialogue_excerpts(film_slug_for_quotes, lang_code, peak_minutes)
                
                    peak_quotes[lang_code] = []
                    for minute, score in peaks:
                        quotes = excerpts.get(minute, ["[Dialogue unavailable]"])
                        # Use first quote as primary quote
                        primary_quote = quotes[0] if quotes else "[No dialogue at this moment]"
                        peak_quotes[lang_code].append({
                            "minute": minute,
                            "quote": primary_quote,
                            "emotion_score": float(score),
                            "explanation": f"Emotional peak at minute {minute} with {emotion_dimension} score {score:.3f}"
                        })

                # Extract quotes at divergence points for all languages
                divergence_quotes = []
                for minute, variance, scores_at_minute in divergence_points:
                    quotes_by_language = {}
                    emotion_scores_by_language = {}

                    for lang_code in lang_list:
                        # Get emotion score at this minute for this language
                        minute_idx = all_minutes.index(minute)
                        if aligned_arcs[lang_code][minute_idx] is not None:
                            emotion_scores_by_language[lang_code] = float(aligned_arcs[lang_code][minute_idx])

                            # Load quotes for this language at this minute
                            excerpts = load_dialogue_excerpts(film_slug_for_quotes, lang_code, [minute])
                            quotes = excerpts.get(minute, ["[Dialogue unavailable]"])
                            quotes_by_language[lang_code] = quotes
                        else:
                            emotion_scores_by_language[lang_code] = None
                            quotes_by_language[lang_code] = ["[No data for this minute]"]

                    # Generate explanation comparing quotes across languages
                    if len([s for s in emotion_scores_by_language.values() if s is not None]) >= 2:
                        # Find languages with highest and lowest scores
                        valid_scores = {k: v for k, v in emotion_scores_by_language.items() if v is not None}
                        if valid_scores:
                            max_lang = max(valid_scores.items(), key=lambda x: x[1])[0]
                            min_lang = min(valid_scores.items(), key=lambda x: x[1])[0]

                            max_quote = quotes_by_language[max_lang][0] if quotes_by_language[max_lang] else "[No quote]"
                            min_quote = quotes_by_language[min_lang][0] if quotes_by_language[min_lang] else "[No quote]"

                            explanation = (
                                f"At minute {minute}, translations diverge significantly (variance: {variance:.3f}). "
                                f"{max_lang.upper()} uses '{max_quote}' (emotion score: {emotion_scores_by_language[max_lang]:.3f}), "
                                f"while {min_lang.upper()} uses '{min_quote}' (emotion score: {emotion_scores_by_language[min_lang]:.3f}). "
                                f"This difference likely stems from translation choices or cultural adaptation."
                            )
                        else:
                            explanation = f"At minute {minute}, translations diverge significantly (variance: {variance:.3f})."
                    else:
                        explanation = f"At minute {minute}, translations diverge significantly (variance: {variance:.3f})."

                    divergence_quotes.append({
                        "minute": minute,
                        "quotes_by_language": quotes_by_language,
                        "emotion_scores_by_language": emotion_scores_by_language,
                        "explanation": explanation
                    })

                # Identify language-specific drivers (quotes that drive peaks in one language but not others)
                language_specific_drivers = {}
                for lang_code, peaks in peak_moments.items():
                    drivers = []
                    for minute, score in peaks:
                        # Check if this minute has significantly different scores in other languages
                        minute_idx = all_minutes.index(minute)
                        other_scores = [
                            aligned_arcs[other_lang][minute_idx]
                            for other_lang in lang_list
                            if other_lang != lang_code and aligned_arcs[other_lang][minute_idx] is not None
                        ]

                        if other_scores:
                            avg_other_score = np.mean(other_scores)
                            # If this language's score is significantly higher than others
                            if score > avg_other_score + 0.1:  # Threshold for "significantly different"
                                excerpts = load_dialogue_excerpts(film_slug_for_quotes, lang_code, [minute])
                                quote = excerpts.get(minute, ["[Dialogue unavailable]"])[0] if excerpts.get(minute) else "[No dialogue]"

                                why_different = (
                                    f"In {lang_code.upper()}, the emotional peak at minute {minute} "
                                    f"(score: {score:.3f}) is driven by dialogue '{quote}'. "
                                    f"This peak is less pronounced in other languages "
                                    f"(average score: {avg_other_score:.3f}), suggesting the translation "
                                    f"in {lang_code.upper()} emphasizes different emotional tones."
                                )

                                drivers.append({
                                    "minute": minute,
                                    "quote": quote,
                                    "why_different": why_different
                                })

                    if drivers:
                        language_specific_drivers[lang_code] = drivers

                # Build quote_analysis structure
                if peak_quotes or divergence_quotes or language_specific_drivers:
                    quote_analysis = {
                        "peak_quotes": peak_quotes,
                        "divergence_quotes": divergence_quotes,
                        "language_specific_drivers": language_specific_drivers
                    }

                    # Enhance answer text with quote-driven explanations
                    if language_specific_drivers:
                        answer_parts.append("\n\nLanguage-specific emotional drivers:")
                        for lang_code, drivers in language_specific_drivers.items():
                            for driver in drivers[:2]:  # Show top 2 per language
                                answer_parts.append(f"\n{lang_code.upper()}: {driver['why_different']}")

                    if divergence_quotes:
                        answer_parts.append("\n\nDivergence point analysis:")
                        for div_quote in divergence_quotes[:2]:  # Show top 2 divergence points
                            answer_parts.append(f"\n{div_quote['explanation']}")

            except Exception as e:
                logger.warning(f"Failed to extract quotes: {e}", exc_info=True)
                # Continue without quotes - non-blocking feature

        answer = "\n".join(answer_parts)

        # Create visualization data (omit in compact mode to save tokens)
        visualization_data = None
        if not compact:
            visualization_data = {
                "chart_type": "line",
                "x": all_minutes,
                "y": {lang: aligned_arcs[lang] for lang in aligned_arcs.keys()},
                "title": f"Sentiment Arc Comparison: {exact_title} ({emotion_dimension})",
                "xlabel": "Minute Offset",
                "ylabel": f"{emotion_dimension.capitalize()} Score",
            }

        followups = [
            f"Compare arcs for other films",
            f"Analyze {emotion_dimension} in detail",
            "Show dialogue excerpts at divergence points",
            "Compare with other emotion dimensions",
        ]

        # Build response with quote_analysis if available
        response = _format_tool_response(
            answer=answer,
            tables=["raw.film_emotions"],
            functions=["AVG()", "pearsonr()", "STDDEV()"],
            computation_method="Multilingual emotion arc comparison with temporal alignment",
            row_count=len(result),
            visualization_data=visualization_data,
            suggested_followups=followups,
        )

        # Add quote_analysis to response if available
        if quote_analysis:
            response["quote_analysis"] = quote_analysis

        return response

    except ValueError as e:
        logger.warning(f"Validation error in compare_sentiment_arcs_across_languages: {e}")
        return _format_tool_response(
            answer=f"Validation error: {str(e)}",
            tables=[],
            functions=[],
        )
    except duckdb.Error as e:
        logger.error(f"Database error in compare_sentiment_arcs_across_languages: {e}", exc_info=True)
        return _format_tool_response(
            answer="Database query failed. Please try again.",
            tables=[],
            functions=[],
        )
    except Exception as e:
        logger.error(f"Unexpected error in compare_sentiment_arcs_across_languages: {e}", exc_info=True)
        return _format_tool_response(
            answer="An unexpected error occurred. Please try again.",
            tables=[],
            functions=[],
        )

