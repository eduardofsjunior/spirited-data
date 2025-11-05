"""
Charting utilities for validation dashboard.

Provides Plotly chart generation functions for emotion analysis visualization,
including sentiment timeline animations, peak annotations, and dialogue linking.
"""

import json
import logging
import os
import pickle
from pathlib import Path
from typing import Dict, List, Optional, Any

import duckdb
import networkx as nx
import pandas as pd
import plotly.graph_objects as go

from src.graph.build_graph import (
    build_networkx_graph,
    load_edges_from_duckdb,
    load_nodes_from_duckdb,
)

logger = logging.getLogger(__name__)

# Emotion categorization for compound sentiment score
POSITIVE_EMOTIONS = [
    "admiration",
    "amusement",
    "approval",
    "caring",
    "excitement",
    "gratitude",
    "joy",
    "love",
    "optimism",
    "pride",
    "relief",
]

NEGATIVE_EMOTIONS = [
    "anger",
    "annoyance",
    "disappointment",
    "disapproval",
    "disgust",
    "embarrassment",
    "fear",
    "grief",
    "nervousness",
    "remorse",
    "sadness",
]


def calculate_compound_score(emotion_row: pd.Series) -> float:
    """
    Calculate compound sentiment from 28 GoEmotions dimensions.

    Computes positive score (avg of 11 positive emotions) minus negative score
    (avg of 11 negative emotions) to produce a balanced sentiment polarity score.

    Args:
        emotion_row: Series containing emotion_* columns with scores 0-1

    Returns:
        Compound sentiment score in range [-1, 1] where:
        - +1 = very positive
        - 0 = neutral
        - -1 = very negative

    Example:
        >>> row = pd.Series({'emotion_joy': 0.8, 'emotion_anger': 0.2})
        >>> calculate_compound_score(row)
        0.05454  # More positive than negative
    """
    positive_score = sum(
        emotion_row.get(f"emotion_{e}", 0.0) for e in POSITIVE_EMOTIONS
    ) / len(POSITIVE_EMOTIONS)

    negative_score = sum(
        emotion_row.get(f"emotion_{e}", 0.0) for e in NEGATIVE_EMOTIONS
    ) / len(NEGATIVE_EMOTIONS)

    return positive_score - negative_score


def calculate_dominant_emotion(emotion_row: pd.Series) -> Dict[str, Any]:
    """
    Calculate dominant emotion approach - strongest single emotion at each moment.

    Instead of averaging, this finds the single most intense emotion and its
    polarity, providing higher variance and clearer emotional peaks.

    Args:
        emotion_row: Series containing emotion_* columns with scores 0-1

    Returns:
        Dictionary with:
        - score: Signed intensity (-1 to +1, negative for negative emotions)
        - emotion: Name of dominant emotion
        - intensity: Raw intensity value (0-1)
        - polarity: 'positive' or 'negative'

    Example:
        >>> row = pd.Series({'emotion_joy': 0.8, 'emotion_anger': 0.2})
        >>> calculate_dominant_emotion(row)
        {'score': 0.8, 'emotion': 'joy', 'intensity': 0.8, 'polarity': 'positive'}
    """
    # Find strongest positive emotion
    max_positive_val = 0.0
    max_positive_emotion = None
    for emotion in POSITIVE_EMOTIONS:
        val = emotion_row.get(f"emotion_{emotion}", 0.0)
        if val > max_positive_val:
            max_positive_val = val
            max_positive_emotion = emotion

    # Find strongest negative emotion
    max_negative_val = 0.0
    max_negative_emotion = None
    for emotion in NEGATIVE_EMOTIONS:
        val = emotion_row.get(f"emotion_{emotion}", 0.0)
        if val > max_negative_val:
            max_negative_val = val
            max_negative_emotion = emotion

    # Return the dominant one
    if max_positive_val > max_negative_val:
        return {
            "score": max_positive_val,
            "emotion": max_positive_emotion,
            "intensity": max_positive_val,
            "polarity": "positive",
        }
    else:
        return {
            "score": -max_negative_val,
            "emotion": max_negative_emotion,
            "intensity": max_negative_val,
            "polarity": "negative",
        }


def identify_peaks(
    emotion_data: pd.DataFrame,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Identify top 5 positive and negative sentiment peaks with dominant emotions.

    Calculates compound scores and finds highest (positive) and lowest
    (negative) sentiment moments in the film. Also identifies which specific
    emotions are driving each peak.

    Args:
        emotion_data: DataFrame with minute_offset and emotion_* columns

    Returns:
        Dictionary with 'positive' and 'negative' keys, each containing list of
        peak dictionaries with keys:
        - minute_offset: Minute in film
        - score: Compound sentiment score
        - top_emotions: List of (emotion_name, intensity) tuples (top 3)
        - dominant_emotion: Single strongest emotion name

    Example:
        >>> df = pd.DataFrame({'minute_offset': [0, 1, 2], 'emotion_joy': [0.8, 0.2, 0.5]})
        >>> peaks = identify_peaks(df)
        >>> peaks['positive'][0]['dominant_emotion']
        'joy'
    """
    # Calculate compound scores
    emotion_data = emotion_data.copy()
    emotion_data["compound"] = emotion_data.apply(calculate_compound_score, axis=1)

    # Find top 5 positive peaks
    positive_peak_rows = emotion_data.nlargest(5, "compound")
    positive_peaks = []
    
    for _, row in positive_peak_rows.iterrows():
        # Get top 3 positive emotions for this peak
        positive_emotions = {
            e: row.get(f"emotion_{e}", 0.0) for e in POSITIVE_EMOTIONS
        }
        top_3 = sorted(positive_emotions.items(), key=lambda x: x[1], reverse=True)[:3]
        
        positive_peaks.append({
            "minute_offset": int(row["minute_offset"]),
            "score": float(row["compound"]),
            "top_emotions": top_3,
            "dominant_emotion": top_3[0][0] if top_3 else "unknown",
        })

    # Find top 5 negative peaks
    negative_peak_rows = emotion_data.nsmallest(5, "compound")
    negative_peaks = []
    
    for _, row in negative_peak_rows.iterrows():
        # Get top 3 negative emotions for this peak
        negative_emotions = {
            e: row.get(f"emotion_{e}", 0.0) for e in NEGATIVE_EMOTIONS
        }
        top_3 = sorted(negative_emotions.items(), key=lambda x: x[1], reverse=True)[:3]
        
        negative_peaks.append({
            "minute_offset": int(row["minute_offset"]),
            "score": float(row["compound"]),
            "top_emotions": top_3,
            "dominant_emotion": top_3[0][0] if top_3 else "unknown",
        })

    return {"positive": positive_peaks, "negative": negative_peaks}


def load_dialogue_excerpts(
    film_slug: str, language_code: str, minute_offsets: List[int]
) -> Dict[int, List[str]]:
    """
    Load top 3 dialogue excerpts for specific minute offsets from parsed subtitle JSON.

    Reads parsed subtitle file and extracts the longest/most substantial dialogue
    lines for requested minutes. Returns top 3 dialogues per minute (by length,
    as a proxy for importance).

    Args:
        film_slug: URL-safe film identifier (e.g., "spirited_away")
        language_code: ISO 639-1 language code (e.g., "en", "fr")
        minute_offsets: List of minute offsets to extract dialogues for

    Returns:
        Dictionary mapping minute_offset to list of top 3 dialogue strings.
        Each dialogue is truncated to 80 chars max.
        Returns empty dict if file not found or malformed.

    Raises:
        FileNotFoundError: If parsed subtitle JSON not found (logged, not raised)
        JSONDecodeError: If JSON malformed (logged, not raised)

    Example:
        >>> excerpts = load_dialogue_excerpts("spirited_away", "en", [10, 20])
        >>> excerpts[10]
        ["Don't be such a scaredy-cat, Chihiro.", "They're just stone statues.", "Come on!"]
    """
    subtitle_path = (
        Path("data/processed/subtitles")
        / f"{film_slug}_{language_code}_parsed.json"
    )

    # Check if file exists
    if not subtitle_path.exists():
        logger.warning(
            f"Parsed subtitle file not found: {subtitle_path}. Skipping dialogue excerpts."
        )
        return {}

    try:
        # Load parsed subtitle JSON
        with open(subtitle_path, "r", encoding="utf-8") as f:
            subtitle_data = json.load(f)

        subtitles = subtitle_data.get("subtitles", [])

        # Extract dialogues for each minute offset
        excerpts = {}
        for minute in minute_offsets:
            # Find all subtitles within this minute bucket
            minute_start = minute * 60
            minute_end = (minute + 1) * 60

            minute_dialogues = [
                sub["dialogue_text"]
                for sub in subtitles
                if minute_start <= sub["start_time"] < minute_end
                and sub["dialogue_text"].strip()  # Skip empty dialogues
            ]

            if minute_dialogues:
                # Sort by length (longer = more substantial) and take top 3
                sorted_dialogues = sorted(
                    minute_dialogues, key=lambda x: len(x), reverse=True
                )[:3]
                
                # Truncate each to 80 chars for tooltip readability
                excerpts[minute] = [
                    (d[:77] + "..." if len(d) > 80 else d)
                    for d in sorted_dialogues
                ]
            else:
                excerpts[minute] = ["[No dialogue]"]

        return excerpts

    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse subtitle JSON {subtitle_path}: {e}")
        return {}
    except Exception as e:
        logger.error(f"Unexpected error loading dialogue excerpts: {e}")
        return {}


def plot_sentiment_timeline(
    conn: duckdb.DuckDBPyConnection,
    film_slug: str,
    film_title: str,
    language_code: str = "en",
) -> Optional[go.Figure]:
    """
    Create animated sentiment timeline visualization for a film.

    Queries emotion data from DuckDB, calculates compound sentiment scores,
    and generates interactive Plotly line chart with animation controls,
    peak annotations, shaded sentiment zones, and dialogue excerpts.

    Args:
        conn: Active DuckDB connection
        film_slug: URL-safe film identifier (e.g., "spirited_away")
        film_title: Human-readable film title for chart title
        language_code: ISO 639-1 language code (default: "en")

    Returns:
        Plotly Figure object with animated sentiment timeline, or None if
        no data available for the film+language combination.

    Raises:
        duckdb.Error: If database query fails (logged, returns None)

    Example:
        >>> conn = get_duckdb_connection()
        >>> fig = plot_sentiment_timeline(conn, "spirited_away", "Spirited Away", "en")
        >>> st.plotly_chart(fig, use_container_width=True)
    """
    logger.info(
        f"Generating sentiment timeline for {film_slug} ({language_code})..."
    )

    # Query emotion data
    # Note: film_slug in raw.film_emotions includes language suffix (e.g., "spirited_away_en")
    query = """
        SELECT 
            minute_offset,
            emotion_admiration, emotion_amusement, emotion_approval, emotion_caring,
            emotion_excitement, emotion_gratitude, emotion_joy, emotion_love,
            emotion_optimism, emotion_pride, emotion_relief,
            emotion_anger, emotion_annoyance, emotion_disappointment, emotion_disapproval,
            emotion_disgust, emotion_embarrassment, emotion_fear, emotion_grief,
            emotion_nervousness, emotion_remorse, emotion_sadness
        FROM raw.film_emotions
        WHERE film_slug = ? || '_' || ? AND language_code = ?
        ORDER BY minute_offset
    """

    try:
        df = conn.execute(query, [film_slug, language_code, language_code]).fetch_df()

        # Handle empty results
        if df.empty:
            logger.warning(
                f"No emotion data found for {film_slug} in {language_code}"
            )
            return None

        logger.info(f"Loaded {len(df)} minute buckets of emotion data")

        # Calculate compound sentiment scores
        df["compound"] = df.apply(calculate_compound_score, axis=1)

        # Create base figure
        fig = go.Figure()

        # Calculate dynamic y-axis range first for zone shading
        compound_min = df['compound'].min()
        compound_max = df['compound'].max()
        range_padding = (compound_max - compound_min) * 0.2
        y_min = max(-1, compound_min - range_padding)
        y_max = min(1, compound_max + range_padding)
        
        # Ensure minimum range of 0.2 for readability
        if (y_max - y_min) < 0.2:
            center = (y_max + y_min) / 2
            y_min = max(-1, center - 0.1)
            y_max = min(1, center + 0.1)

        # Add sentiment zone shading (behind data line) - use dynamic range
        # Positive zone (green tint above zero)
        if y_max > 0:
            fig.add_hrect(
                y0=0,
                y1=y_max,
                fillcolor="green",
                opacity=0.1,
                layer="below",
                line_width=0,
            )

        # Negative zone (red tint below zero)
        if y_min < 0:
            fig.add_hrect(
                y0=y_min,
                y1=0,
                fillcolor="red",
                opacity=0.1,
                layer="below",
                line_width=0,
            )

        # Add zero baseline (only if zero is in range)
        if y_min < 0 < y_max:
            fig.add_hline(
                y=0,
                line_dash="dash",
                line_color="gray",
                line_width=1,
            )

        # Add main sentiment line
        fig.add_trace(
            go.Scatter(
                x=df["minute_offset"],
                y=df["compound"],
                mode="lines",
                name="Sentiment",
                line=dict(color="blue", width=2),
                hovertemplate="Minute: %{x}<br>Score: %{y:.2f}<extra></extra>",
            )
        )

        # Identify and annotate peaks
        peaks = identify_peaks(df)

        # Load dialogue excerpts for peaks
        all_peak_minutes = [p["minute_offset"] for p in peaks["positive"]] + [
            p["minute_offset"] for p in peaks["negative"]
        ]
        dialogue_excerpts = load_dialogue_excerpts(
            film_slug, language_code, all_peak_minutes
        )

        # Add positive peak markers with dominant emotions
        if peaks["positive"]:
            positive_minutes = [p["minute_offset"] for p in peaks["positive"]]
            positive_scores = [p["score"] for p in peaks["positive"]]
            positive_texts = []
            
            for p in peaks["positive"]:
                m = p["minute_offset"]
                dominant = p.get("dominant_emotion", "unknown")
                top_emotions_str = " • ".join([
                    f"{e}: {v:.2f}" for e, v in p.get("top_emotions", [])[:3]
                ])
                dialogues = dialogue_excerpts.get(m, ['[Not available]'])
                
                # Format dialogues as clean numbered list with better spacing
                dialogue_lines = "<br>".join([
                    f"<span style='color:#e0e0e0; font-size:12px'>  {i+1}. {d}</span>"
                    for i, d in enumerate(dialogues)
                ])
                
                text = (
                    f"<span style='font-size:13px'>"
                    f"<b style='color:#90EE90'>Dominant:</b> {dominant.capitalize()}<br>"
                    f"<b style='color:#90EE90'>Top emotions:</b> {top_emotions_str}<br>"
                    f"<b style='color:#90EE90'>Key dialogue:</b></span><br>{dialogue_lines}"
                )
                positive_texts.append(text)

            fig.add_trace(
                go.Scatter(
                    x=positive_minutes,
                    y=positive_scores,
                    mode="markers",
                    name="Positive Peaks",
                    marker=dict(color="green", size=12, symbol="triangle-up"),
                    hovertemplate="<b style='font-size:14px'>✨ Positive Peak</b><br>"
                    + "<span style='font-size:13px'>"
                    + "Minute: %{x} | Score: %{y:.2f}"
                    + "</span><br>"
                    + "%{text}<extra></extra>",
                    text=positive_texts,
                    hoverlabel=dict(
                        bgcolor="#2d5016",  # Dark green background
                        font=dict(
                            size=13,
                            family="Arial, sans-serif",
                            color="white"
                        ),
                        align="left",
                        namelength=-1
                    ),
                )
            )

        # Add negative peak markers with dominant emotions
        if peaks["negative"]:
            negative_minutes = [p["minute_offset"] for p in peaks["negative"]]
            negative_scores = [p["score"] for p in peaks["negative"]]
            negative_texts = []
            
            for p in peaks["negative"]:
                m = p["minute_offset"]
                dominant = p.get("dominant_emotion", "unknown")
                top_emotions_str = " • ".join([
                    f"{e}: {v:.2f}" for e, v in p.get("top_emotions", [])[:3]
                ])
                dialogues = dialogue_excerpts.get(m, ['[Not available]'])
                
                # Format dialogues as clean numbered list with better spacing
                dialogue_lines = "<br>".join([
                    f"<span style='color:#e0e0e0; font-size:12px'>  {i+1}. {d}</span>"
                    for i, d in enumerate(dialogues)
                ])
                
                text = (
                    f"<span style='font-size:13px'>"
                    f"<b style='color:#FFB6C1'>Dominant:</b> {dominant.capitalize()}<br>"
                    f"<b style='color:#FFB6C1'>Top emotions:</b> {top_emotions_str}<br>"
                    f"<b style='color:#FFB6C1'>Key dialogue:</b></span><br>{dialogue_lines}"
                )
                negative_texts.append(text)

            fig.add_trace(
                go.Scatter(
                    x=negative_minutes,
                    y=negative_scores,
                    mode="markers",
                    name="Negative Peaks",
                    marker=dict(color="red", size=12, symbol="triangle-down"),
                    hovertemplate="<b style='font-size:14px'>⚡ Negative Peak</b><br>"
                    + "<span style='font-size:13px'>"
                    + "Minute: %{x} | Score: %{y:.2f}"
                    + "</span><br>"
                    + "%{text}<extra></extra>",
                    text=negative_texts,
                    hoverlabel=dict(
                        bgcolor="#5c1a1a",  # Dark red/maroon background
                        font=dict(
                            size=13,
                            family="Arial, sans-serif",
                            color="white"
                        ),
                        align="left",
                        namelength=-1
                    ),
                )
            )

        # Option A: Add top 5 emotions overlay (dotted lines)
        # Identify top 5 most variable emotions across the film
        emotion_cols = [col for col in df.columns if col.startswith('emotion_')]
        emotion_variances = {col: df[col].std() for col in emotion_cols}
        top_5_emotions = sorted(emotion_variances.items(), key=lambda x: x[1], reverse=True)[:5]
        
        # Define negative emotions (will be displayed with negative intensity)
        negative_emotions = {
            'anger', 'annoyance', 'disappointment', 'disapproval', 'disgust',
            'embarrassment', 'fear', 'grief', 'nervousness', 'remorse', 'sadness'
        }
        
        emotion_colors = {
            'joy': 'gold', 'sadness': 'purple', 'fear': 'orange', 
            'anger': 'darkred', 'surprise': 'pink', 'love': 'hotpink',
            'excitement': 'lime', 'caring': 'lightblue', 'gratitude': 'cyan',
            'admiration': 'lightgreen', 'amusement': 'yellow', 'disgust': 'brown',
            'disappointment': 'gray', 'disapproval': 'darkgray', 'embarrassment': 'salmon',
            'nervousness': 'orchid', 'grief': 'indigo', 'remorse': 'mediumpurple',
            'confusion': 'darkgoldenrod', 'curiosity': 'coral', 'pride': 'khaki'
        }
        
        for emotion_col, variance in top_5_emotions:
            emotion_name = emotion_col.replace('emotion_', '')
            color = emotion_colors.get(emotion_name, 'gray')
            is_negative = emotion_name in negative_emotions
            
            # Invert negative emotions to show as negative values
            emotion_values = df[emotion_col] * (-1 if is_negative else 1)
            
            fig.add_trace(
                go.Scatter(
                    x=df["minute_offset"],
                    y=emotion_values,
                    name=emotion_name.capitalize(),
                    mode='lines',
                    line=dict(color=color, width=1, dash='dot'),
                    opacity=0.6,
                    hovertemplate=f"<b>{emotion_name.capitalize()}</b><br>"
                    + "Minute: %{x}<br>"
                    + "Intensity: %{y:.2f}<extra></extra>",
                )
            )

        # Add animation frames
        frames = [
            go.Frame(
                data=[
                    go.Scatter(
                        x=df["minute_offset"][:k],
                        y=df["compound"][:k],
                        mode="lines",
                        line=dict(color="blue", width=2),
                    )
                ],
                name=str(k),
            )
            for k in range(1, len(df) + 1)
        ]
        fig.frames = frames

        # Configure layout (y_min and y_max already calculated earlier for zone shading)
        fig.update_layout(
            title=f"Emotional Arc: {film_title} ({language_code.upper()})",
            xaxis_title="Timeline (minutes)",
            yaxis_title="Sentiment Score",
            yaxis_range=[y_min, y_max],
            height=400,
            autosize=True,
            hovermode="closest",
            showlegend=True,
            # Add play/pause buttons
            updatemenus=[
                {
                    "type": "buttons",
                    "showactive": False,
                    "x": 0.1,
                    "y": 1.15,
                    "buttons": [
                        {
                            "label": "▶ Play",
                            "method": "animate",
                            "args": [
                                None,
                                {
                                    "frame": {"duration": 200, "redraw": True},
                                    "fromcurrent": True,
                                    "mode": "immediate",
                                    "transition": {"duration": 0},
                                },
                            ],
                        },
                        {
                            "label": "⏸ Pause",
                            "method": "animate",
                            "args": [
                                [None],
                                {
                                    "frame": {"duration": 0, "redraw": False},
                                    "mode": "immediate",
                                    "transition": {"duration": 0},
                                },
                            ],
                        },
                    ],
                }
            ],
            # Add slider for timeline scrubbing
            sliders=[
                {
                    "active": len(df) - 1,
                    "steps": [
                        {
                            "args": [
                                [str(k)],
                                {
                                    "frame": {"duration": 0, "redraw": True},
                                    "mode": "immediate",
                                    "transition": {"duration": 0},
                                },
                            ],
                            "label": str(df.iloc[k - 1]["minute_offset"]),
                            "method": "animate",
                        }
                        for k in range(1, len(df) + 1)
                    ],
                    "x": 0.1,
                    "y": 0,
                    "len": 0.9,
                    "xanchor": "left",
                    "yanchor": "top",
                    "pad": {"b": 10, "t": 50},
                    "currentvalue": {
                        "visible": True,
                        "prefix": "Minute: ",
                        "xanchor": "right",
                    },
                }
            ],
        )

        logger.info("Sentiment timeline chart generated successfully")
        return fig

    except duckdb.Error as e:
        logger.error(f"Database query failed: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error generating sentiment timeline: {e}")
        return None


def plot_emotion_composition(
    conn: duckdb.DuckDBPyConnection,
    film_slug: str,
    film_title: str,
    language_code: str = "en",
) -> Optional[go.Figure]:
    """
    Create stacked area chart showing emotion composition over time (Option B).

    Shows how different emotions contribute to the emotional landscape throughout
    the film. Uses top 5-7 most prevalent emotions as stacked areas.

    Args:
        conn: Active DuckDB connection
        film_slug: URL-safe film identifier (e.g., "spirited_away")
        film_title: Human-readable film title for chart title
        language_code: ISO 639-1 language code (default: "en")

    Returns:
        Plotly Figure object with stacked area chart, or None if no data available.

    Example:
        >>> conn = get_duckdb_connection()
        >>> fig = plot_emotion_composition(conn, "spirited_away", "Spirited Away", "en")
        >>> st.plotly_chart(fig, use_container_width=True)
    """
    logger.info(
        f"Generating emotion composition chart for {film_slug} ({language_code})..."
    )

    # Query emotion data
    query = """
        SELECT 
            minute_offset,
            emotion_admiration, emotion_amusement, emotion_approval, emotion_caring,
            emotion_excitement, emotion_gratitude, emotion_joy, emotion_love,
            emotion_optimism, emotion_pride, emotion_relief,
            emotion_anger, emotion_annoyance, emotion_disappointment, emotion_disapproval,
            emotion_disgust, emotion_embarrassment, emotion_fear, emotion_grief,
            emotion_nervousness, emotion_remorse, emotion_sadness,
            emotion_surprise, emotion_curiosity, emotion_confusion
        FROM raw.film_emotions
        WHERE film_slug = ? || '_' || ? AND language_code = ?
        ORDER BY minute_offset
    """

    try:
        df = conn.execute(query, [film_slug, language_code, language_code]).fetch_df()

        if df.empty:
            logger.warning(
                f"No emotion data found for {film_slug} in {language_code}"
            )
            return None

        logger.info(f"Loaded {len(df)} minute buckets of emotion data")

        # Select top 7 most prevalent emotions (highest average intensity)
        emotion_cols = [col for col in df.columns if col.startswith('emotion_')]
        emotion_means = {col: df[col].mean() for col in emotion_cols}
        top_7_emotions = sorted(emotion_means.items(), key=lambda x: x[1], reverse=True)[:7]

        # Create figure with stacked areas
        fig = go.Figure()

        # Emotion colors
        emotion_colors = {
            'joy': '#FFD700', 'sadness': '#9370DB', 'fear': '#FF8C00', 
            'anger': '#DC143C', 'surprise': '#FF69B4', 'love': '#FF1493',
            'excitement': '#32CD32', 'caring': '#87CEEB', 'gratitude': '#00CED1',
            'admiration': '#90EE90', 'amusement': '#FFFF00', 'disgust': '#8B4513',
            'disappointment': '#A9A9A9', 'disapproval': '#696969', 'embarrassment': '#FA8072',
            'nervousness': '#DDA0DD', 'grief': '#4B0082', 'remorse': '#800080',
            'confusion': '#B8860B', 'curiosity': '#FFA500', 'pride': '#FFD700',
            'relief': '#98FB98', 'approval': '#B0E0E6', 'optimism': '#FFFFE0',
            'realization': '#F0E68C', 'desire': '#FF6347'
        }

        # Add stacked area traces for each emotion
        for emotion_col, mean_val in top_7_emotions:
            emotion_name = emotion_col.replace('emotion_', '')
            color = emotion_colors.get(emotion_name, '#CCCCCC')

            fig.add_trace(
                go.Scatter(
                    x=df['minute_offset'],
                    y=df[emotion_col],
                    name=emotion_name.capitalize(),
                    mode='lines',
                    stackgroup='one',  # Creates stacked effect
                    fillcolor=color,
                    line=dict(width=0.5, color=color),
                    hovertemplate=f"<b>{emotion_name.capitalize()}</b><br>"
                    + "Minute: %{x}<br>"
                    + "Intensity: %{y:.2f}<extra></extra>",
                )
            )

        # Configure layout
        fig.update_layout(
            title=f"Emotion Composition: {film_title} ({language_code.upper()})",
            xaxis_title="Timeline (minutes)",
            yaxis_title="Cumulative Emotion Intensity",
            height=400,
            autosize=True,
            hovermode="x unified",
            showlegend=True,
            legend=dict(
                orientation="v",
                yanchor="top",
                y=1,
                xanchor="left",
                x=1.02
            )
        )

        logger.info("Emotion composition chart generated successfully")
        return fig

    except duckdb.Error as e:
        logger.error(f"Database query failed: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error generating emotion composition: {e}")
        return None


def get_character_metadata(
    conn: duckdb.DuckDBPyConnection, character_node_ids: List[str], G: nx.MultiDiGraph
) -> Dict[str, Dict[str, Any]]:
    """
    Get film appearances and degree counts for characters.

    Queries DuckDB for film appearances count and uses NetworkX graph
    to get total degree (connections) for each character.

    Args:
        conn: Active DuckDB connection
        character_node_ids: List of character node IDs to query
        G: NetworkX graph for degree calculation

    Returns:
        Dictionary mapping node_id to metadata dict with keys:
        - film_count: Number of films character appears in
        - degree: Total number of connections (in-degree + out-degree)
    """
    metadata = {}

    if not character_node_ids:
        return metadata

    # Batch query for film appearances using parameterized queries
    # DuckDB supports parameter binding with ? placeholders
    placeholders = ",".join(["?" for _ in character_node_ids])
    query = f"""
        SELECT 
            source_node_id,
            COUNT(DISTINCT target_node_id) as film_count
        FROM main_marts.mart_graph_edges
        WHERE source_node_id IN ({placeholders})
          AND edge_type = 'appears_in'
        GROUP BY source_node_id
    """

    try:
        results = conn.execute(query, character_node_ids).fetchall()

        # Build metadata dict with film counts
        for node_id, film_count in results:
            metadata[node_id] = {"film_count": film_count}

        # Add degree counts from NetworkX graph
        for node_id in character_node_ids:
            if node_id not in metadata:
                metadata[node_id] = {"film_count": 0}

            # Get total degree (in-degree + out-degree)
            degree = G.degree(node_id)
            metadata[node_id]["degree"] = degree

    except duckdb.Error as e:
        logger.warning(f"Failed to query character metadata: {e}")
        # Fallback: use graph degree only
        for node_id in character_node_ids:
            degree = G.degree(node_id)
            metadata[node_id] = {"film_count": 0, "degree": degree}

    return metadata


def load_or_build_graph(conn: duckdb.DuckDBPyConnection) -> nx.MultiDiGraph:
    """
    Load NetworkX graph from pickle file or build from DuckDB.

    Tries to load from pickle file first (faster), falls back to loading
    from DuckDB marts and building graph if pickle not available.

    Args:
        conn: Active DuckDB connection

    Returns:
        NetworkX MultiDiGraph object

    Raises:
        FileNotFoundError: If graph pickle file not found and DuckDB query fails
        nx.NetworkXError: If graph building fails
    """
    pickle_path = Path("data/processed/ghibli_graph.pkl")

    # Try loading from pickle first (faster)
    if pickle_path.exists():
        try:
            logger.info("Loading graph from pickle file...")
            with open(pickle_path, "rb") as f:
                G = pickle.load(f)
            logger.info(f"Loaded graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
            return G
        except Exception as e:
            logger.warning(f"Failed to load pickle file: {e}. Falling back to DuckDB...")

    # Fallback to loading from DuckDB
    try:
        logger.info("Loading graph from DuckDB marts...")
        nodes = load_nodes_from_duckdb(conn)
        edges = load_edges_from_duckdb(conn)
        
        # Validate we have data before building
        if not nodes:
            raise ValueError("No graph nodes found in database. Please run graph construction script first.")
        if not edges:
            raise ValueError(
                "No graph edges found in database. Graph construction may be incomplete. "
                "Please run: python src/graph/build_graph.py"
            )
        
        G = build_networkx_graph(nodes, edges)
        logger.info(f"Built graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
        return G
    except ValueError as e:
        # Re-raise ValueError with better context
        logger.error(f"Failed to build graph: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to load graph from DuckDB: {e}")
        raise


def calculate_film_similarity(
    film1_id: str, film2_id: str, G: nx.MultiDiGraph
) -> Dict[str, Any]:
    """
    Calculate similarity between two films based on shared graph attributes.
    
    Similarity is calculated based on:
    - Shared director (+5 points)
    - Shared locations (+1 point per shared location)
    - Shared species (+1 point per shared species)
    
    Args:
        film1_id: First film node ID (e.g., "film_uuid")
        film2_id: Second film node ID
        G: NetworkX graph containing film relationships
        
    Returns:
        Dictionary with similarity score and details about shared attributes
    """
    score = 0
    shared_attrs = {
        "director": False,
        "locations": [],
        "species": [],
    }
    
    # Check shared director
    film1_directors = [
        s for s, t, d in G.edges(data=True)
        if t == film1_id and d.get("edge_type") == "directed"
    ]
    film2_directors = [
        s for s, t, d in G.edges(data=True)
        if t == film2_id and d.get("edge_type") == "directed"
    ]
    
    if film1_directors and film2_directors:
        if set(film1_directors) & set(film2_directors):
            score += 5
            shared_attrs["director"] = True
    
    # Check shared locations
    film1_locations = [
        s for s, t, d in G.edges(data=True)
        if t == film1_id and d.get("edge_type") == "filmed_at"
    ]
    film2_locations = [
        s for s, t, d in G.edges(data=True)
        if t == film2_id and d.get("edge_type") == "filmed_at"
    ]
    
    shared_locations = set(film1_locations) & set(film2_locations)
    if shared_locations:
        score += len(shared_locations)
        shared_attrs["locations"] = [G.nodes[loc]["name"] for loc in shared_locations]
    
    # Check shared species (via characters)
    # Get characters for each film
    film1_characters = [
        s for s, t, d in G.edges(data=True)
        if t == film1_id and d.get("edge_type") == "appears_in"
    ]
    film2_characters = [
        s for s, t, d in G.edges(data=True)
        if t == film2_id and d.get("edge_type") == "appears_in"
    ]
    
    # Get species for each film's characters
    film1_species = set()
    for char in film1_characters:
        char_species = [
            t for s, t, d in G.edges(char, data=True)
            if d.get("edge_type") == "is_species"
        ]
        film1_species.update(char_species)
    
    film2_species = set()
    for char in film2_characters:
        char_species = [
            t for s, t, d in G.edges(char, data=True)
            if d.get("edge_type") == "is_species"
        ]
        film2_species.update(char_species)
    
    shared_species = film1_species & film2_species
    if shared_species:
        score += len(shared_species)
        shared_attrs["species"] = [G.nodes[sp]["name"] for sp in shared_species]
    
    return {
        "score": score,
        "shared_director": shared_attrs["director"],
        "shared_locations": shared_attrs["locations"],
        "shared_species": shared_attrs["species"],
    }


def build_film_similarity_network(
    conn: duckdb.DuckDBPyConnection, min_similarity: int = 1
) -> Optional[nx.Graph]:
    """
    Build a film-to-film similarity network based on shared attributes.
    
    Creates an undirected graph where:
    - Nodes are films
    - Edges connect films with similarity >= min_similarity
    - Edge weights represent similarity scores
    
    Args:
        conn: Active DuckDB connection
        min_similarity: Minimum similarity score to create an edge (default: 1)
        
    Returns:
        NetworkX Graph with film nodes and similarity edges, or None on error
    """
    try:
        # Load the main graph
        G = load_or_build_graph(conn)
        
        # Get all film nodes
        film_nodes = [
            n for n in G.nodes()
            if G.nodes[n].get("node_type") == "film"
        ]
        
        if len(film_nodes) < 2:
            logger.warning("Need at least 2 films to build similarity network")
            return None
        
        # Create new undirected graph for film similarities
        sim_graph = nx.Graph()
        
        # Add film nodes with attributes
        for film_id in film_nodes:
            film_data = G.nodes[film_id]
            sim_graph.add_node(
                film_id,
                name=film_data.get("name", "Unknown"),
                properties=film_data.get("properties", {}),
            )
        
        # Calculate pairwise similarities
        logger.info(f"Calculating similarities for {len(film_nodes)} films...")
        edge_count = 0
        
        for i, film1 in enumerate(film_nodes):
            for film2 in film_nodes[i + 1:]:
                similarity = calculate_film_similarity(film1, film2, G)
                
                if similarity["score"] >= min_similarity:
                    sim_graph.add_edge(
                        film1,
                        film2,
                        weight=similarity["score"],
                        shared_director=similarity["shared_director"],
                        shared_locations=similarity["shared_locations"],
                        shared_species=similarity["shared_species"],
                    )
                    edge_count += 1
        
        logger.info(
            f"Built similarity network: {len(film_nodes)} films, "
            f"{edge_count} connections (min_similarity={min_similarity})"
        )
        
        return sim_graph
        
    except Exception as e:
        logger.error(f"Failed to build film similarity network: {e}")
        return None


def calculate_emotion_vectors(
    conn: duckdb.DuckDBPyConnection,
    exclude_neutral: bool = True,
    normalize: bool = True,
) -> Dict[str, Dict[str, float]]:
    """
    Calculate average emotion vectors for all films.
    
    By default, excludes 'neutral' emotion and normalizes to highlight differences.
    
    Args:
        conn: Active DuckDB connection
        exclude_neutral: If True, removes neutral emotion (default True)
        normalize: If True, normalizes vectors to sum to 1.0 (default True)
        
    Returns:
        Dict of {film_id: {emotion_name: score}}
    """
    try:
        # Get all films with emotion data
        query = """
        SELECT 
            film_id,
            AVG(emotion_admiration) as admiration,
            AVG(emotion_amusement) as amusement,
            AVG(emotion_anger) as anger,
            AVG(emotion_annoyance) as annoyance,
            AVG(emotion_approval) as approval,
            AVG(emotion_caring) as caring,
            AVG(emotion_confusion) as confusion,
            AVG(emotion_curiosity) as curiosity,
            AVG(emotion_desire) as desire,
            AVG(emotion_disappointment) as disappointment,
            AVG(emotion_disapproval) as disapproval,
            AVG(emotion_disgust) as disgust,
            AVG(emotion_embarrassment) as embarrassment,
            AVG(emotion_excitement) as excitement,
            AVG(emotion_fear) as fear,
            AVG(emotion_gratitude) as gratitude,
            AVG(emotion_grief) as grief,
            AVG(emotion_joy) as joy,
            AVG(emotion_love) as love,
            AVG(emotion_nervousness) as nervousness,
            AVG(emotion_optimism) as optimism,
            AVG(emotion_pride) as pride,
            AVG(emotion_realization) as realization,
            AVG(emotion_relief) as relief,
            AVG(emotion_remorse) as remorse,
            AVG(emotion_sadness) as sadness,
            AVG(emotion_surprise) as surprise,
            AVG(emotion_neutral) as neutral
        FROM raw.film_emotions
        WHERE film_id IS NOT NULL
        GROUP BY film_id
        """
        
        results = conn.execute(query).fetchall()
        
        emotion_names = [
            'admiration', 'amusement', 'anger', 'annoyance', 'approval', 'caring',
            'confusion', 'curiosity', 'desire', 'disappointment', 'disapproval',
            'disgust', 'embarrassment', 'excitement', 'fear', 'gratitude', 'grief',
            'joy', 'love', 'nervousness', 'optimism', 'pride', 'realization',
            'relief', 'remorse', 'sadness', 'surprise', 'neutral'
        ]
        
        emotion_vectors = {}
        for row in results:
            film_id = row[0]
            emotions = {name: score for name, score in zip(emotion_names, row[1:])}
            
            # Exclude neutral if requested
            if exclude_neutral and 'neutral' in emotions:
                del emotions['neutral']
            
            # Normalize if requested (so emotions sum to 1.0)
            if normalize:
                total = sum(emotions.values())
                if total > 0:
                    emotions = {k: v / total for k, v in emotions.items()}
            
            emotion_vectors[film_id] = emotions
        
        logger.info(f"Calculated emotion vectors for {len(emotion_vectors)} films (exclude_neutral={exclude_neutral}, normalize={normalize})")
        return emotion_vectors
        
    except Exception as e:
        logger.error(f"Failed to calculate emotion vectors: {e}")
        return {}


def cosine_similarity(vec1: Dict[str, float], vec2: Dict[str, float]) -> float:
    """Calculate cosine similarity between two emotion vectors."""
    import math
    
    # Get common keys
    keys = set(vec1.keys()) & set(vec2.keys())
    if not keys:
        return 0.0
    
    # Calculate dot product and magnitudes
    dot_product = sum(vec1[k] * vec2[k] for k in keys)
    magnitude1 = math.sqrt(sum(vec1[k] ** 2 for k in keys))
    magnitude2 = math.sqrt(sum(vec2[k] ** 2 for k in keys))
    
    if magnitude1 == 0 or magnitude2 == 0:
        return 0.0
    
    return dot_product / (magnitude1 * magnitude2)


def euclidean_distance(vec1: Dict[str, float], vec2: Dict[str, float]) -> float:
    """
    Calculate Euclidean distance between two emotion vectors.
    Returns a value where 0 = identical, higher = more different.
    """
    import math
    
    keys = set(vec1.keys()) & set(vec2.keys())
    if not keys:
        return 1.0
    
    squared_diff = sum((vec1[k] - vec2[k]) ** 2 for k in keys)
    return math.sqrt(squared_diff)


def distance_to_similarity(distance: float, max_distance: float = 1.0) -> float:
    """
    Convert distance to similarity score (0-1 scale).
    Distance of 0 = 100% similarity, max_distance = 0% similarity.
    """
    if max_distance == 0:
        return 1.0
    return max(0.0, 1.0 - (distance / max_distance))


def plot_emotion_similarity_heatmap(
    conn: duckdb.DuckDBPyConnection,
    selected_film_id: Optional[str] = None,
) -> Optional[go.Figure]:
    """
    Create interactive heatmap showing emotional similarity between films.
    
    Uses Euclidean distance of normalized emotion vectors (excluding neutral)
    to show which films have similar emotional profiles.
    
    Args:
        conn: Active DuckDB connection
        selected_film_id: Optional film ID to highlight
        
    Returns:
        Plotly Figure with heatmap, or None on error
    """
    try:
        # Get emotion vectors (exclude neutral, normalized)
        emotion_vectors = calculate_emotion_vectors(conn, exclude_neutral=True, normalize=True)
        
        if not emotion_vectors:
            logger.warning("No emotion data available")
            return None
        
        # Get film names
        film_ids = list(emotion_vectors.keys())
        film_names = {}
        for film_id in film_ids:
            result = conn.execute(
                "SELECT title FROM raw.films WHERE id = ?", [film_id]
            ).fetchone()
            if result:
                film_names[film_id] = result[0]
        
        # Calculate distance matrix first
        n = len(film_ids)
        distance_matrix = [[0.0] * n for _ in range(n)]
        max_distance = 0.0
        
        for i, film1 in enumerate(film_ids):
            for j, film2 in enumerate(film_ids):
                if i != j:
                    dist = euclidean_distance(emotion_vectors[film1], emotion_vectors[film2])
                    distance_matrix[i][j] = dist
                    max_distance = max(max_distance, dist)
        
        # Convert distances to similarities
        similarity_matrix = [[0.0] * n for _ in range(n)]
        for i in range(n):
            for j in range(n):
                if i == j:
                    similarity_matrix[i][j] = 1.0
                else:
                    similarity_matrix[i][j] = distance_to_similarity(distance_matrix[i][j], max_distance)
        
        # Create labels
        labels = [film_names.get(fid, fid) for fid in film_ids]
        
        # Create heatmap
        fig = go.Figure(data=go.Heatmap(
            z=similarity_matrix,
            x=labels,
            y=labels,
            colorscale='RdYlBu_r',  # Red (low) to Blue (high)
            zmid=0.5,
            zmin=0,
            zmax=1,
            text=[[f"{val*100:.0f}%" for val in row] for row in similarity_matrix],
            texttemplate="%{text}",
            textfont={"size": 10},
            hovertemplate="<b>%{y}</b> vs <b>%{x}</b><br>Similarity: %{z:.1%}<extra></extra>",
            colorbar=dict(
                title="Emotional<br>Similarity",
                tickformat=".0%",
                thickness=20,
                len=0.7,
            ),
        ))
        
        # Update layout
        fig.update_layout(
            title=dict(
                text="Film Emotional Similarity Matrix<br><sub>(Neutral emotion excluded for clarity)</sub>",
                x=0.5,
                xanchor="center",
                font=dict(size=20, color="#1a1a1a"),
            ),
            xaxis=dict(
                title="",
                tickangle=-45,
                side="bottom",
                tickfont=dict(size=10),
            ),
            yaxis=dict(
                title="",
                tickfont=dict(size=10),
            ),
            height=700,
            width=900,
            margin=dict(l=150, r=100, t=120, b=150),
            plot_bgcolor="white",
        )
        
        logger.info(f"Created heatmap with similarity range: {min(min(row) for row in similarity_matrix):.2f} - 1.00")
        return fig
        
    except Exception as e:
        logger.error(f"Failed to create emotion similarity heatmap: {e}")
        return None


def plot_emotion_fingerprint_radar(
    conn: duckdb.DuckDBPyConnection,
    film_ids: List[str],
    top_n_emotions: int = 8,
) -> Optional[go.Figure]:
    """
    Create radar chart comparing emotional fingerprints of selected films.
    
    Shows the top N emotions for each film as a radar/spider chart.
    Excludes 'neutral' and normalizes to show relative emotion distribution.
    
    Args:
        conn: Active DuckDB connection
        film_ids: List of film IDs to compare (max 3 recommended)
        top_n_emotions: Number of emotions to show (default 8)
        
    Returns:
        Plotly Figure with radar chart, or None on error
    """
    try:
        # Get emotion vectors (exclude neutral, normalized)
        emotion_vectors = calculate_emotion_vectors(conn, exclude_neutral=True, normalize=True)
        
        if not emotion_vectors:
            logger.warning("No emotion data available")
            return None
        
        # Filter to requested films
        film_ids = [fid for fid in film_ids if fid in emotion_vectors]
        if not film_ids:
            logger.warning("No emotion data for selected films")
            return None
        
        # Get film names
        film_names = {}
        for film_id in film_ids:
            result = conn.execute(
                "SELECT title FROM raw.films WHERE id = ?", [film_id]
            ).fetchone()
            if result:
                film_names[film_id] = result[0]
        
        # Determine top emotions across all selected films
        all_emotions = {}
        for film_id in film_ids:
            for emotion, score in emotion_vectors[film_id].items():
                if emotion != 'neutral':  # Extra safety
                    all_emotions[emotion] = all_emotions.get(emotion, 0) + score
        
        top_emotions = sorted(all_emotions.keys(), key=lambda k: all_emotions[k], reverse=True)[:top_n_emotions]
        
        # Color palette for films
        colors = ['#FF6B6B', '#4ECDC4', '#95E1D3', '#FFE66D', '#A8E6CF']
        
        # Create traces for each film
        fig = go.Figure()
        
        for idx, film_id in enumerate(film_ids):
            values = [emotion_vectors[film_id].get(emotion, 0) * 100 for emotion in top_emotions]
            
            # Close the radar by appending first value
            values_closed = values + [values[0]]
            emotions_closed = [e.title() for e in top_emotions] + [top_emotions[0].title()]
            
            fig.add_trace(go.Scatterpolar(
                r=values_closed,
                theta=emotions_closed,
                fill='toself',
                name=film_names.get(film_id, film_id),
                line=dict(color=colors[idx % len(colors)], width=3),
                fillcolor=colors[idx % len(colors)],
                opacity=0.4,
                hovertemplate="<b>%{fullData.name}</b><br>%{theta}: %{r:.1f}%<extra></extra>",
            ))
        
        # Update layout with better scaling
        max_value = max(max(emotion_vectors[fid].get(e, 0) for e in top_emotions) for fid in film_ids) * 100
        
        fig.update_layout(
            polar=dict(
                radialaxis=dict(
                    visible=True,
                    range=[0, max_value * 1.1],  # 10% padding
                    ticksuffix="%",
                    gridcolor="#e0e0e0",
                ),
                angularaxis=dict(
                    gridcolor="#e0e0e0",
                ),
            ),
            showlegend=True,
            legend=dict(
                x=1.1,
                y=0.5,
                font=dict(size=12),
            ),
            title=dict(
                text="Emotional Fingerprint Comparison<br><sub>(Neutral excluded, normalized to 100%)</sub>",
                x=0.5,
                xanchor="center",
                font=dict(size=18, color="#1a1a1a"),
            ),
            height=600,
            width=800,
            margin=dict(l=80, r=200, t=120, b=80),
            plot_bgcolor="white",
            paper_bgcolor="white",
        )
        
        logger.info(f"Created radar chart for {len(film_ids)} films, max value: {max_value:.1f}%")
        return fig
        
    except Exception as e:
        logger.error(f"Failed to create emotion fingerprint radar: {e}")
        return None


def plot_film_similarity_network(
    conn: duckdb.DuckDBPyConnection,
    selected_film_id: Optional[str] = None,
    min_similarity: int = 1,
    layout: str = "spring",
) -> Optional[go.Figure]:
    """
    Create interactive network graph showing film similarities.
    
    Films are connected by edges representing shared attributes (director, locations, species).
    If a film is selected, highlights that film and its connections.
    
    Args:
        conn: Active DuckDB connection
        selected_film_id: Optional film ID to highlight (shows connections to this film)
        min_similarity: Minimum similarity score to show connection (default: 1)
        layout: Layout algorithm ('spring', 'circular', 'kamada_kawai')
        
    Returns:
        Plotly Figure object with network visualization, or None on error
    """
    try:
        # Build film similarity network
        sim_graph = build_film_similarity_network(conn, min_similarity)
        
        if sim_graph is None or sim_graph.number_of_nodes() == 0:
            logger.warning("No film similarity network to visualize")
            return None
        
        # Calculate layout positions with better parameters for visual clarity
        if layout == "circular":
            # Circular layout: arrange films in a circle by director
            pos = nx.circular_layout(sim_graph)
        elif layout == "shell":
            # Shell layout: organize by connectivity (hub films in center)
            # Group by degree for concentric shells
            degree_dict = dict(sim_graph.degree())
            shells = []
            # Center: most connected (degree >= 8)
            shell_center = [n for n, d in degree_dict.items() if d >= 8]
            if shell_center:
                shells.append(shell_center)
            # Middle ring: moderately connected (4-7)
            shell_mid = [n for n, d in degree_dict.items() if 4 <= d < 8]
            if shell_mid:
                shells.append(shell_mid)
            # Outer ring: less connected (< 4)
            shell_outer = [n for n, d in degree_dict.items() if d < 4]
            if shell_outer:
                shells.append(shell_outer)
            
            pos = nx.shell_layout(sim_graph, nlist=shells) if shells else nx.circular_layout(sim_graph)
        elif layout == "kamada_kawai":
            # Kamada-Kawai: physics-based, good for clusters
            pos = nx.kamada_kawai_layout(sim_graph)
        else:  # default: spring (improved)
            # Spring layout with better parameters for stability and spacing
            pos = nx.spring_layout(
                sim_graph, 
                k=2.5 / (sim_graph.number_of_nodes() ** 0.5),  # More spacing
                iterations=150,  # More iterations for stability
                seed=42,
                weight='weight',  # Use edge weights
                scale=2.0  # Larger scale for more spread
            )
        
        # Prepare edge traces
        edge_traces = []
        
        for edge in sim_graph.edges(data=True):
            film1, film2, edge_data = edge
            x0, y0 = pos[film1]
            x1, y1 = pos[film2]
            weight = edge_data.get("weight", 1)
            
            # Build hover text showing shared attributes
            hover_parts = []
            if edge_data.get("shared_director"):
                hover_parts.append("✓ <b>Same Director</b>")
            if edge_data.get("shared_locations"):
                locs = edge_data["shared_locations"]
                hover_parts.append(f"✓ <b>{len(locs)} Shared Location(s):</b> {', '.join(locs[:3])}")
            if edge_data.get("shared_species"):
                species = edge_data["shared_species"]
                hover_parts.append(f"✓ <b>{len(species)} Shared Species:</b> {', '.join(species[:3])}")
            
            hover_text = f"<b style='font-size:14px'>Similarity Score: {weight}</b><br>" + "<br>".join(hover_parts)
            
            # Create edge trace with better visibility
            edge_trace = go.Scatter(
                x=[x0, x1, None],
                y=[y0, y1, None],
                mode="lines",
                line=dict(
                    width=max(weight * 0.6, 1),  # Minimum width of 1
                    color="rgba(150, 150, 150, 0.3)",  # Lighter, more subtle
                ),
                hoverinfo="text",
                hovertext=hover_text,
                showlegend=False,
            )
            edge_traces.append(edge_trace)
        
        # Get director information for color coding
        G_full = load_or_build_graph(conn)
        film_directors = {}
        director_colors = {
            "Hayao Miyazaki": "#FF6B6B",      # Red
            "Isao Takahata": "#4ECDC4",       # Teal
            "Gorō Miyazaki": "#95E1D3",       # Light teal
            "Hiromasa Yonebayashi": "#FFE66D", # Yellow
            "Hiroyuki Morita": "#A8E6CF",     # Light green
            "Yoshifumi Kondō": "#FF8B94",     # Pink
            "Michaël Dudok de Wit": "#C7CEEA" # Lavender
        }
        
        for node in sim_graph.nodes():
            # Find director for this film
            directors = [
                G_full.nodes[s]["name"] for s, t, d in G_full.edges(data=True)
                if t == node and d.get("edge_type") == "directed"
            ]
            film_directors[node] = directors[0] if directors else "Unknown"
        
        # Prepare node traces
        node_x = []
        node_y = []
        node_text = []
        node_hover = []
        node_sizes = []
        node_colors = []
        
        for node in sim_graph.nodes():
            x, y = pos[node]
            node_x.append(x)
            node_y.append(y)
            
            film_name = sim_graph.nodes[node]["name"]
            director = film_directors[node]
            
            # Show abbreviated name on node
            abbreviated = film_name if len(film_name) <= 15 else film_name[:12] + "..."
            node_text.append(abbreviated)
            
            # Calculate node size based on degree (connections)
            degree = sim_graph.degree(node)
            node_sizes.append(30 + degree * 5)  # Even larger for better visibility
            
            # Calculate total similarity score for this film
            total_similarity = sum(
                [sim_graph[node][neighbor]["weight"] for neighbor in sim_graph.neighbors(node)]
            )
            
            # Build hover text with more details
            hover_info = f"<b style='font-size:16px'>{film_name}</b><br>"
            hover_info += f"<b>Director:</b> {director}<br>"
            hover_info += f"<b>Connections:</b> {degree}<br>"
            hover_info += f"<b>Total Similarity:</b> {total_similarity}<br>"
            hover_info += "<br><i>Click to select this film</i>"
            node_hover.append(hover_info)
            
            # Color based on director or selection
            if selected_film_id and node == f"film_{selected_film_id}":
                node_colors.append("#FFD700")  # Gold for selected
            elif selected_film_id and node in sim_graph.neighbors(f"film_{selected_film_id}"):
                node_colors.append("#87CEEB")  # Sky blue for connected
            else:
                # Color by director
                node_colors.append(director_colors.get(director, "#CCCCCC"))
        
        node_trace = go.Scatter(
            x=node_x,
            y=node_y,
            mode="markers",  # Remove text labels from nodes - show only on hover
            hoverinfo="text",
            hovertext=node_hover,
            marker=dict(
                size=node_sizes,
                color=node_colors,
                line=dict(width=4, color="white"),  # Even thicker border
                opacity=0.95,
            ),
            showlegend=False,
        )
        
        # Create figure
        fig = go.Figure(data=edge_traces + [node_trace])
        
        # Update layout
        title_text = "Studio Ghibli Film Similarity Network"
        if selected_film_id:
            selected_name = sim_graph.nodes[f"film_{selected_film_id}"]["name"]
            title_text = f"Films Similar to {selected_name}"
        
        fig.update_layout(
            title=dict(
                text=title_text,
                x=0.5,
                xanchor="center",
                font=dict(size=24, color="#1a1a1a", family="Arial, sans-serif"),
            ),
            showlegend=False,
            hovermode="closest",
            margin=dict(b=80, l=40, r=40, t=100),  # More margin
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            plot_bgcolor="white",  # Clean white background
            paper_bgcolor="white",
            height=800,  # Taller for better visibility
        )
        
        # Add legend for director colors (if not in selection mode)
        if not selected_film_id:
            legend_text = "<b>Hover over nodes to see film details</b><br>Color = Director | Node size = Connections | Edge thickness = Similarity"
        else:
            legend_text = "<b>Hover over nodes to see film details</b><br>🟡 = Selected | 🔵 = Connected | Other colors = By director"
        
        annotation_text = legend_text
        
        fig.add_annotation(
            text=annotation_text,
            xref="paper",
            yref="paper",
            x=0.5,
            y=-0.03,
            xanchor="center",
            yanchor="top",
            showarrow=False,
            font=dict(size=12, color="#555555"),
        )
        
        return fig
        
    except Exception as e:
        logger.error(f"Failed to create film similarity network visualization: {e}")
        return None


def plot_centrality_ranking(
    conn: duckdb.DuckDBPyConnection,
    metric: str,
    film_id: str,
    top_n: int = 10,
) -> Optional[go.Figure]:
    """
    DEPRECATED: This function is being replaced by plot_film_similarity_network.
    
    Create horizontal bar chart showing character centrality rankings for a specific film.

    Loads NetworkX graph from DuckDB marts (or pickle file), filters to film-specific
    subgraph, calculates selected centrality metric for character nodes within that film,
    and generates interactive Plotly bar chart with top N characters ranked by centrality score.

    Args:
        conn: Active DuckDB connection
        metric: Centrality metric name ("degree", "betweenness", or "closeness")
        film_id: Film ID (UUID format) to filter characters by
        top_n: Number of top characters to display (default: 10, range: 5-20)

    Returns:
        Plotly Figure object with horizontal bar chart, or None if error

    Raises:
        ValueError: If metric name is invalid or film not found
        FileNotFoundError: If graph pickle file not found and DuckDB query fails
        nx.NetworkXError: If NetworkX calculation fails
    """
    try:
        # Load or build NetworkX graph
        G = load_or_build_graph(conn)

        # Find film node_id from film_id
        film_node_id = f"film_{film_id}"
        
        # Verify film node exists, or try to find by searching nodes
        if film_node_id not in G.nodes():
            # Try to find film node by searching all film nodes
            logger.debug(f"Film node {film_node_id} not found, searching by film_id...")
            
            # Query DuckDB to get film title and verify film_id format
            try:
                result = conn.execute(
                    "SELECT id, title FROM main_staging.stg_films WHERE id = ?",
                    [film_id],
                ).fetchone()
                
                if result:
                    db_film_id, db_film_title = result
                    # Try with the exact ID from database
                    film_node_id = f"film_{db_film_id}"
                    if film_node_id not in G.nodes():
                        # Search by name as last resort
                        logger.debug(f"Searching for film node by title: {db_film_title}")
                        for node_id, node_data in G.nodes(data=True):
                            if (
                                node_data.get("node_type") == "film"
                                and node_data.get("name", "").lower() == db_film_title.lower()
                            ):
                                film_node_id = node_id
                                logger.info(f"Found film node by title: {film_node_id}")
                                break
            except Exception as e:
                logger.warning(f"Could not query film from database: {e}")
        
        # Verify film node exists after search
        if film_node_id not in G.nodes():
            logger.warning(f"Film node {film_node_id} not found in graph after search")
            return None

        # Get film title for display
        film_title = G.nodes[film_node_id].get("name", "Unknown Film")
        logger.info(f"Analyzing centrality for film: {film_title} (node: {film_node_id})")

        # Build film-specific subgraph
        # Include: film node, characters in film, locations in film, species of characters, director
        film_subgraph_nodes = {film_node_id}
        
        # Find all characters connected to this film (via appears_in edges)
        character_count = 0
        for edge in G.edges(data=True):
            source, target, edge_data = edge
            if (
                edge_data.get("edge_type") == "appears_in"
                and target == film_node_id
            ):
                film_subgraph_nodes.add(source)
                character_count += 1
        
        logger.debug(f"Found {character_count} characters connected to {film_title}")
        
        # Find all locations connected to this film (via filmed_at edges)
        for edge in G.edges(data=True):
            source, target, edge_data = edge
            if (
                edge_data.get("edge_type") == "filmed_at"
                and target == film_node_id
            ):
                film_subgraph_nodes.add(source)
        
        # Find all species connected to characters in this film (via is_species edges)
        character_nodes_in_film = {
            n for n in film_subgraph_nodes
            if G.nodes[n].get("node_type") == "character"
        }
        for edge in G.edges(data=True):
            source, target, edge_data = edge
            if (
                edge_data.get("edge_type") == "is_species"
                and source in character_nodes_in_film
            ):
                film_subgraph_nodes.add(target)
        
        # Find director connected to this film (via directed edges)
        for edge in G.edges(data=True):
            source, target, edge_data = edge
            if (
                edge_data.get("edge_type") == "directed"
                and target == film_node_id
            ):
                film_subgraph_nodes.add(source)

        # Create subgraph with only nodes and edges relevant to this film
        film_subgraph = G.subgraph(film_subgraph_nodes).copy()
        
        logger.debug(
            f"Created subgraph for {film_title}: "
            f"{film_subgraph.number_of_nodes()} nodes, "
            f"{film_subgraph.number_of_edges()} edges"
        )

        # Filter to character nodes only in this film
        character_nodes = [
            n
            for n in film_subgraph.nodes()
            if film_subgraph.nodes[n].get("node_type") == "character"
        ]
        
        # Also check in original graph in case subgraph filtering had issues
        if not character_nodes:
            logger.debug(
                f"No character nodes in subgraph. Checking original graph nodes: "
                f"{[n for n in film_subgraph_nodes if G.nodes[n].get('node_type') == 'character']}"
            )
            # Fallback: check original graph
            character_nodes = [
                n
                for n in film_subgraph_nodes
                if G.nodes[n].get("node_type") == "character"
            ]

        if not character_nodes:
            logger.warning(
                f"No character nodes found in graph for film {film_title} (node: {film_node_id}). "
                f"Subgraph has {len(film_subgraph_nodes)} nodes total: "
                f"{[G.nodes[n].get('node_type', 'unknown') for n in film_subgraph_nodes]}. "
                f"Found {character_count} appears_in edges. "
                f"Film may not have character data in graph."
            )
            return None

        # Map metric names to calculation functions
        metric_mapping = {
            "degree": "degree",
            "betweenness": "betweenness",
            "closeness": "closeness",
            "Degree Centrality": "degree",
            "Betweenness Centrality": "betweenness",
            "Closeness Centrality": "closeness",
        }

        # Normalize metric name
        metric_key = metric_mapping.get(metric, metric.lower())

        # Dictionary mapping metric names to calculation functions
        centrality_functions = {
            "degree": nx.degree_centrality,
            "betweenness": nx.betweenness_centrality,
            "closeness": nx.closeness_centrality,
        }

        # Select calculation function based on metric parameter
        if metric_key not in centrality_functions:
            raise ValueError(
                f"Invalid metric: {metric}. Must be one of: {list(centrality_functions.keys())}"
            )

        # Cap top_n to available characters
        if top_n > len(character_nodes):
            logger.info(
                f"top_n ({top_n}) exceeds available characters ({len(character_nodes)}). "
                f"Capping to {len(character_nodes)}"
            )
            top_n = len(character_nodes)

        calculate_centrality = centrality_functions[metric_key]

        # Calculate centrality for film-specific subgraph
        logger.info(
            f"Calculating {metric_key} centrality for {film_title} "
            f"({len(character_nodes)} characters)..."
        )
        try:
            centrality_scores = calculate_centrality(film_subgraph)
        except nx.NetworkXError as e:
            logger.error(f"NetworkX calculation failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during centrality calculation: {e}")
            raise

        # Extract centrality scores for character nodes in this film
        character_centrality = {
            node_id: centrality_scores[node_id]
            for node_id in character_nodes
            if node_id in centrality_scores
        }

        if not character_centrality:
            logger.warning("No centrality scores calculated for character nodes")
            return None

        # Sort by score (descending) and select top N
        sorted_characters = sorted(
            character_centrality.items(), key=lambda x: x[1], reverse=True
        )[:top_n]

        # Extract character names and scores
        char_names = [
            film_subgraph.nodes[node_id]["name"] for node_id, _ in sorted_characters
        ]
        scores = [score for _, score in sorted_characters]
        char_node_ids = [node_id for node_id, _ in sorted_characters]

        # Get character metadata (degree within film subgraph)
        # Note: For per-film analysis, we only care about connections within the film
        metadata = {}
        for node_id in char_node_ids:
            degree = film_subgraph.degree(node_id)
            metadata[node_id] = {"film_count": 1, "degree": degree}

        # Prepare hover data
        hover_texts = []
        for node_id, score in zip(char_node_ids, scores):
            char_name = film_subgraph.nodes[node_id]["name"]
            degree = metadata.get(node_id, {}).get("degree", 0)
            hover_texts.append(
                f"Character: {char_name}<br>"
                f"Centrality: {score:.2f}<br>"
                f"Connections in Film: {degree}"
            )

        # Create horizontal bar chart
        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=scores,
                y=char_names,
                orientation="h",
                marker=dict(
                    color=scores,
                    colorscale="Blues",
                    showscale=True,
                    colorbar=dict(title=f"{metric_key.title()} Centrality"),
                ),
                text=[f"{s:.2f}" for s in scores],
                textposition="outside",
                hovertemplate="%{hovertext}<extra></extra>",
                hovertext=hover_texts,
            )
        )

        # Configure layout
        metric_display_name = {
            "degree": "Degree",
            "betweenness": "Betweenness",
            "closeness": "Closeness",
        }.get(metric_key, metric_key.title())

        # Methodology text based on selected metric (per-film context)
        methodology_texts = {
            "degree": (
                "Measures number of direct connections within this film's character network. "
                "Higher values indicate characters with more relationships to other characters, "
                "locations, or species in this film."
            ),
            "betweenness": (
                "Measures how often a character lies on shortest paths between other "
                "characters within this film. Higher values indicate bridge characters "
                "connecting different parts of the film's character network."
            ),
            "closeness": (
                "Measures average distance to all other characters within this film. "
                "Higher values indicate characters reachable from most others in few steps "
                "within the film's character network."
            ),
        }

        methodology_text = methodology_texts.get(
            metric_key,
            "Centrality metric indicating character importance in the graph.",
        )

        fig.update_layout(
            title=f"Top {len(sorted_characters)} Characters in {film_title} by {metric_display_name} Centrality",
            xaxis_title=f"{metric_display_name} Centrality Score",
            yaxis_title="Character",
            height=max(400, len(sorted_characters) * 30),
            autosize=True,
            yaxis=dict(autorange="reversed"),  # Highest score at top
        )

        # Add methodology annotation
        fig.add_annotation(
            text=f"<b>Methodology:</b> {methodology_text}",
            xref="paper",
            yref="paper",
            x=0.5,
            y=-0.15,
            xanchor="center",
            yanchor="top",
            showarrow=False,
            font=dict(size=10),
            bgcolor="lightgray",
            bordercolor="gray",
            borderwidth=1,
            borderpad=4,
        )

        logger.info(f"Generated centrality chart: {len(sorted_characters)} characters")
        return fig

    except ValueError as e:
        error_msg = str(e)
        if "Invalid metric" in error_msg or "metric" in error_msg.lower():
            logger.error(f"Invalid metric parameter: {e}")
        else:
            logger.error(f"Graph construction failed: {e}")
        return None
    except FileNotFoundError as e:
        logger.error(f"Graph file not found: {e}")
        return None
    except nx.NetworkXError as e:
        logger.error(f"NetworkX calculation failed: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error in plot_centrality_ranking: {e}")
        return None

