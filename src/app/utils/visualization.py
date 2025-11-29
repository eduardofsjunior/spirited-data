"""
Plotly visualization helpers for SpiritedData Streamlit app.

Provides chart generation functions following "Spirit World" dark theme.
Follows Epic 3.5 emotion analysis guidelines:
- Exclude neutral emotion from calculations
- Show top 5 emotions (not top 3)
- Display negative emotions with negative intensity (below zero)

[Source: Story 5.2 - AC4, AC3; Story 3.5.2 - Emotion Guidelines]
"""

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from typing import List, Tuple, Optional

from .config import THEME

# Emotion categorization per Epic 3.5 guidelines
POSITIVE_EMOTIONS = [
    'admiration', 'amusement', 'approval', 'caring', 'excitement',
    'gratitude', 'joy', 'love', 'optimism', 'pride', 'relief'
]

NEGATIVE_EMOTIONS = [
    'anger', 'annoyance', 'disappointment', 'disapproval', 'disgust',
    'embarrassment', 'fear', 'grief', 'nervousness', 'remorse', 'sadness'
]

# Neutral/ambiguous emotions EXCLUDED from analysis per Epic 3.5
EXCLUDED_EMOTIONS = ['confusion', 'curiosity', 'desire', 'realization', 'surprise', 'neutral']


def get_top_n_emotions(df: pd.DataFrame, n: int = 5) -> List[str]:
    """
    Calculate top N dominant emotions across entire timeline.

    Excludes neutral/ambiguous emotions per Epic 3.5 guidelines.

    Args:
        df: DataFrame with emotion_* columns
        n: Number of top emotions to return (default: 5 per Epic 3.5)

    Returns:
        List of emotion names (e.g., ['joy', 'fear', 'admiration', 'sadness', 'caring'])

    [Source: Story 5.2 - AC4.5; Story 3.5.2 - Emotion Guidelines]
    """
    # Get all emotion columns EXCLUDING neutral/ambiguous per Epic 3.5
    all_cols = [col for col in df.columns if col.startswith("emotion_")]
    emotion_cols = [
        col for col in all_cols
        if col.replace("emotion_", "") not in EXCLUDED_EMOTIONS
    ]

    # Calculate average score for each emotion across timeline
    emotion_averages = df[emotion_cols].mean().sort_values(ascending=False)

    # Get top N emotion names (strip "emotion_" prefix)
    top_emotions = [col.replace("emotion_", "") for col in emotion_averages.head(n).index]

    return top_emotions


def plot_emotion_preview(
    df: pd.DataFrame,
    film_title: str,
    language_code: str,
    top_emotions: Optional[List[str]] = None
) -> go.Figure:
    """
    Create emotion timeline preview chart for film selector.

    Follows Epic 3.5 guidelines:
    - Shows top 5 emotions (excludes neutral/ambiguous)
    - Negative emotions displayed with negative intensity (inverted below zero)

    Args:
        df: DataFrame with minute_offset and emotion_* columns
        film_title: Film title for chart title
        language_code: Language code for chart subtitle
        top_emotions: List of emotions to plot (default: auto-detect top 5)

    Returns:
        Plotly Figure object

    [Source: Story 5.2 - AC4; Story 3.5.2 - Emotion Guidelines]
    """
    if top_emotions is None:
        top_emotions = get_top_n_emotions(df, n=5)

    # Define emotion colors (neon/pastel for dark background)
    emotion_colors = {
        # Positive emotions - warm colors
        "joy": "#FFD700",           # Gold
        "admiration": "#EC4899",    # Pink
        "amusement": "#F59E0B",     # Amber
        "love": "#F472B6",          # Light Pink
        "excitement": "#FBBF24",    # Yellow
        "gratitude": "#34D399",     # Green
        "optimism": "#60A5FA",      # Light Blue
        "caring": "#A78BFA",        # Light Purple
        "approval": "#4ADE80",      # Light Green
        "pride": "#FB923C",         # Orange
        "relief": "#22D3EE",        # Cyan

        # Negative emotions - cool/dark colors
        "fear": "#9333EA",          # Purple
        "sadness": "#3B82F6",       # Blue
        "anger": "#EF4444",         # Red
        "disappointment": "#6366F1", # Indigo
        "disgust": "#DC2626",       # Dark Red
        "grief": "#1E3A8A",         # Dark Blue
        "embarrassment": "#DB2777", # Pink-Red
        "nervousness": "#7C3AED",   # Violet
        "annoyance": "#F97316",     # Orange-Red
        "disapproval": "#7C2D12",   # Brown
        "remorse": "#831843",       # Dark Pink
    }

    fig = go.Figure()

    # Track all y-values for dynamic scaling
    all_y_values = []

    # Add line trace for each top emotion
    for emotion in top_emotions:
        col_name = f"emotion_{emotion}"
        if col_name not in df.columns:
            continue

        color = emotion_colors.get(emotion, "#94A3B8")  # Default to slate

        # Invert negative emotions per Epic 3.5 guidelines
        if emotion in NEGATIVE_EMOTIONS:
            y_values = -df[col_name]  # Negative values display below zero
            hover_template = f"<b>{emotion.capitalize()} (negative)</b><br>Minute: %{{x}}<br>Intensity: %{{y:.3f}}<extra></extra>"
        else:
            y_values = df[col_name]
            hover_template = f"<b>{emotion.capitalize()}</b><br>Minute: %{{x}}<br>Intensity: %{{y:.3f}}<extra></extra>"

        all_y_values.extend(y_values.tolist())

        fig.add_trace(go.Scatter(
            x=df["minute_offset"],
            y=y_values,
            mode="lines",
            name=emotion.capitalize(),
            line=dict(color=color, width=2),
            hovertemplate=hover_template
        ))

    # Calculate dynamic y-axis range with 20% padding
    if all_y_values:
        y_min = min(all_y_values)
        y_max = max(all_y_values)
        y_range = y_max - y_min

        # Add 20% padding for visual breathing room
        padding = y_range * 0.2
        y_axis_min = y_min - padding
        y_axis_max = y_max + padding

        # Ensure minimum range of 0.02 for very flat data
        if y_range < 0.01:
            y_axis_min = -0.01
            y_axis_max = 0.01
    else:
        # Fallback if no data
        y_axis_min = -0.05
        y_axis_max = 0.05

    # Add zero baseline per Epic 3.5 (separates positive/negative emotions)
    fig.add_hline(y=0, line_dash="dash", line_color="gray", line_width=1, opacity=0.5)

    # Add positive zone shading (above zero) - use dynamic range
    if y_axis_max > 0:
        fig.add_hrect(
            y0=0, y1=y_axis_max,
            fillcolor="green",
            opacity=0.05,
            layer="below",
            line_width=0
        )

    # Add negative zone shading (below zero) - use dynamic range
    if y_axis_min < 0:
        fig.add_hrect(
            y0=y_axis_min, y1=0,
            fillcolor="red",
            opacity=0.05,
            layer="below",
            line_width=0
        )

    # Style chart with "Spirit World" theme
    fig.update_layout(
        title=dict(
            text=f"{film_title} - Emotional Journey",
            font=dict(size=18, color=THEME["text_color"]),
            x=0.5,
            xanchor="center"
        ),
        xaxis_title="Runtime (minutes)",
        yaxis_title="Emotion Intensity (+ positive / - negative)",
        xaxis=dict(
            gridcolor="#1E293B",
            color=THEME["text_color"]
        ),
        yaxis=dict(
            gridcolor="#1E293B",
            color=THEME["text_color"],
            range=[y_axis_min, y_axis_max],  # Dynamic range based on actual data
            zeroline=True,
            zerolinecolor="gray",
            zerolinewidth=1,
            tickformat=".3f"  # Show 3 decimal places for small values
        ),
        plot_bgcolor=THEME["background_color"],
        paper_bgcolor=THEME["background_color"],
        font=dict(color=THEME["text_color"], family=THEME["font_body"]),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            bgcolor="rgba(30, 41, 59, 0.8)",  # Semi-transparent slate
            bordercolor=THEME["primary_color"],
            borderwidth=1
        ),
        hovermode="x unified",
        height=400
    )

    return fig


def plot_emotion_bar(emotion_score: float, max_score: float = 1.0) -> go.Figure:
    """
    Create small horizontal bar chart for insight cards.

    Args:
        emotion_score: Emotion score value (0-1)
        max_score: Maximum score for scale (default: 1.0)

    Returns:
        Plotly Figure object

    [Source: Story 5.2 - AC3]
    """
    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=[emotion_score],
        y=[""],
        orientation="h",
        marker=dict(
            color=THEME["primary_color"],
            line=dict(color=THEME["accent_color"], width=1)
        ),
        hoverinfo="skip"
    ))

    fig.update_layout(
        xaxis=dict(
            range=[0, max_score],
            showgrid=False,
            showticklabels=False,
            zeroline=False
        ),
        yaxis=dict(
            showticklabels=False
        ),
        plot_bgcolor=THEME["background_color"],
        paper_bgcolor=THEME["background_color"],
        margin=dict(l=0, r=0, t=0, b=0),
        height=30,
        showlegend=False
    )

    return fig


# ============================================================================
# Epic 5.3: Film Explorer (The Spirit Archives) Visualizations
# ============================================================================

def plot_emotion_timeline(
    df: pd.DataFrame,
    film_title: str,
    language_code: str,
    is_smoothed: bool = True,
    peaks_df: Optional[pd.DataFrame] = None
) -> go.Figure:
    """
    Create emotion timeline with zone shading and top 5 dominant emotions.

    Follows Epic 3.5 guidelines:
    - Shows top 5 emotions (excludes neutral/ambiguous)
    - Negative emotions displayed with negative intensity (inverted below zero)
    - Zero baseline with green/red zone shading

    Args:
        df: DataFrame with minute_offset and emotion_* columns
        film_title: Film title for chart title
        language_code: Language code for display
        is_smoothed: Whether data is smoothed (10-min rolling avg) or raw

    Returns:
        Plotly Figure object

    [Source: Story 5.3 - Task 2.1, AC2]
    """
    if df.empty:
        # Return empty figure with message
        fig = go.Figure()
        fig.add_annotation(
            text="No emotion data available for this film/language combination",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=16, color=THEME["text_color"])
        )
        fig.update_layout(
            plot_bgcolor=THEME["background_color"],
            paper_bgcolor=THEME["background_color"],
            height=500
        )
        return fig

    # Get top 5 emotions (excluding neutral/ambiguous per Epic 3.5)
    top_emotions = get_top_n_emotions(df, n=5)

    # Define emotion colors
    emotion_colors = {
        "joy": "#FFD700", "admiration": "#EC4899", "amusement": "#F59E0B",
        "love": "#F472B6", "excitement": "#FBBF24", "gratitude": "#34D399",
        "optimism": "#60A5FA", "caring": "#A78BFA", "approval": "#4ADE80",
        "pride": "#FB923C", "relief": "#22D3EE", "fear": "#9333EA",
        "sadness": "#3B82F6", "anger": "#EF4444", "disappointment": "#6366F1",
        "disgust": "#DC2626", "grief": "#1E3A8A", "embarrassment": "#DB2777",
        "nervousness": "#7C3AED", "annoyance": "#F97316", "disapproval": "#7C2D12",
        "remorse": "#831843"
    }

    fig = go.Figure()

    # Track all y-values for dynamic scaling
    all_y_values = []

    # Add line trace for each top emotion
    for emotion in top_emotions:
        col_name = f"emotion_{emotion}"
        if col_name not in df.columns:
            continue

        color = emotion_colors.get(emotion, "#94A3B8")

        # Invert negative emotions per Epic 3.5 guidelines
        if emotion in NEGATIVE_EMOTIONS:
            y_values = -df[col_name]
            hover_template = f"<b>{emotion.capitalize()} (negative)</b><br>Minute: %{{x}}<br>Intensity: %{{y:.3f}}<extra></extra>"
        else:
            y_values = df[col_name]
            hover_template = f"<b>{emotion.capitalize()}</b><br>Minute: %{{x}}<br>Intensity: %{{y:.3f}}<extra></extra>"

        all_y_values.extend(y_values.tolist())

        fig.add_trace(go.Scatter(
            x=df["minute_offset"],
            y=y_values,
            mode="lines",
            name=emotion.capitalize(),
            line=dict(color=color, width=2),
            hovertemplate=hover_template
        ))

    # Calculate dynamic y-axis range with 20% padding
    if all_y_values:
        y_min = min(all_y_values)
        y_max = max(all_y_values)
        y_range = y_max - y_min
        padding = y_range * 0.2
        y_axis_min = y_min - padding
        y_axis_max = y_max + padding

        if y_range < 0.01:
            y_axis_min = -0.01
            y_axis_max = 0.01
    else:
        y_axis_min = -0.05
        y_axis_max = 0.05

    # Add positive zone shading (green, above zero)
    if y_axis_max > 0:
        fig.add_hrect(
            y0=0, y1=y_axis_max,
            fillcolor="green",
            opacity=0.05,
            layer="below",
            line_width=0
        )

    # Add negative zone shading (red, below zero)
    if y_axis_min < 0:
        fig.add_hrect(
            y0=y_axis_min, y1=0,
            fillcolor="red",
            opacity=0.05,
            layer="below",
            line_width=0
        )

    # Add zero baseline
    fig.add_hline(y=0, line_dash="dash", line_color="gray", line_width=1, opacity=0.5)

    # Add peak markers with emotion-matched colors if provided
    if peaks_df is not None and not peaks_df.empty:
        for _, peak in peaks_df.iterrows():
            emotion_label = peak["emotion_type"]
            minute = peak["peak_minute_offset"]
            peak_rank = peak.get("peak_rank", 1)

            # Only annotate if this emotion is in top 5
            if emotion_label in top_emotions:
                # Determine if negative emotion for y-position
                y_val = df[df["minute_offset"] == minute][f"emotion_{emotion_label}"]
                if not y_val.empty:
                    y_val = y_val.iloc[0]
                    if emotion_label in NEGATIVE_EMOTIONS:
                        y_val = -y_val

                    # Use same color as emotion line for star marker
                    star_color = emotion_colors.get(emotion_label, "#94A3B8")

                    # Add star marker (no confusing tooltips - just visual markers)
                    fig.add_trace(go.Scatter(
                        x=[minute],
                        y=[y_val],
                        mode='markers',
                        marker=dict(
                            size=12,
                            color=star_color,
                            symbol='star',
                            line=dict(color='white', width=1)
                        ),
                        showlegend=False,
                        hoverinfo='skip'  # Disable hover to avoid confusion with line tooltips
                    ))

    # Style chart
    data_type = "Smoothed (10-min avg)" if is_smoothed else "Raw (dialogue-level)"
    language_display = {"en": "English", "fr": "French", "es": "Spanish", "nl": "Dutch", "ar": "Arabic"}.get(language_code, language_code.upper())

    fig.update_layout(
        title=dict(
            text=f"{film_title} - Emotion Timeline ({language_display}, {data_type})",
            font=dict(size=18, color=THEME["text_color"]),
            x=0.5,
            xanchor="center"
        ),
        xaxis_title="Runtime (minutes)",
        yaxis_title="Emotion Intensity (+ positive / - negative)",
        xaxis=dict(
            gridcolor="#1E293B",
            color=THEME["text_color"]
        ),
        yaxis=dict(
            gridcolor="#1E293B",
            color=THEME["text_color"],
            range=[y_axis_min, y_axis_max],
            zeroline=True,
            zerolinecolor="gray",
            zerolinewidth=1,
            tickformat=".3f"
        ),
        plot_bgcolor=THEME["background_color"],
        paper_bgcolor=THEME["background_color"],
        font=dict(color=THEME["text_color"], family=THEME["font_body"]),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            bgcolor="rgba(30, 41, 59, 0.8)",
            bordercolor=THEME["primary_color"],
            borderwidth=1
        ),
        hovermode="x unified",
        height=500
    )

    return fig


def plot_emotion_composition(df: pd.DataFrame, film_title: str) -> go.Figure:
    """
    Create stacked area chart showing emotion intensity over time.
    Negative emotions displayed below zero axis, positive above.
    Uses actual intensity values (not percentages) like Story 3.5.

    Args:
        df: DataFrame with minute_offset and emotion_* columns
        film_title: Film title for chart title

    Returns:
        Plotly Figure object

    [Source: Story 5.3 - Task 2.2, AC3 + Story 3.5 styling]
    """
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No emotion data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=16, color=THEME["text_color"])
        )
        fig.update_layout(
            plot_bgcolor=THEME["background_color"],
            paper_bgcolor=THEME["background_color"],
            height=400
        )
        return fig

    # Get top 7 emotions for composition view
    top_emotions = get_top_n_emotions(df, n=7)

    # Separate positive and negative emotions
    positive_emotions = [e for e in top_emotions if e not in NEGATIVE_EMOTIONS]
    negative_emotions = [e for e in top_emotions if e in NEGATIVE_EMOTIONS]

    # Use same emotion colors as timeline for consistency
    emotion_colors = {
        "joy": "#FFD700", "admiration": "#EC4899", "amusement": "#F59E0B",
        "love": "#F472B6", "excitement": "#FBBF24", "gratitude": "#34D399",
        "optimism": "#60A5FA", "caring": "#A78BFA", "approval": "#4ADE80",
        "pride": "#FB923C", "relief": "#22D3EE", "fear": "#9333EA",
        "sadness": "#3B82F6", "anger": "#EF4444", "disappointment": "#6366F1",
        "disgust": "#DC2626", "grief": "#1E3A8A", "embarrassment": "#DB2777",
        "nervousness": "#7C3AED", "annoyance": "#F97316", "disapproval": "#7C2D12",
        "remorse": "#831843"
    }

    # Create stacked area chart
    fig = go.Figure()

    # Add positive emotions (stacked above zero) - use actual values
    for i, emotion in enumerate(positive_emotions):
        col_name = f"emotion_{emotion}"
        color = emotion_colors.get(emotion, "#94A3B8")

        fig.add_trace(go.Scatter(
            x=df["minute_offset"],
            y=df[col_name],
            mode="lines",
            name=emotion.capitalize(),
            fill='tonexty' if i > 0 else 'tozeroy',
            line=dict(width=0.5, color=color),
            fillcolor=color,
            hovertemplate=f"<b>{emotion.capitalize()}</b><br>Minute: %{{x}}<br>Intensity: %{{y:.3f}}<extra></extra>"
        ))

    # Add negative emotions (stacked below zero) - invert values
    for i, emotion in enumerate(negative_emotions):
        col_name = f"emotion_{emotion}"
        color = emotion_colors.get(emotion, "#94A3B8")

        # Invert for negative display
        y_values = -df[col_name]

        fig.add_trace(go.Scatter(
            x=df["minute_offset"],
            y=y_values,
            mode="lines",
            name=f"{emotion.capitalize()} (negative)",
            fill='tonexty' if i > 0 else 'tozeroy',
            line=dict(width=0.5, color=color),
            fillcolor=color,
            hovertemplate=f"<b>{emotion.capitalize()} (negative)</b><br>Minute: %{{x}}<br>Intensity: %{{y:.3f}}<extra></extra>"
        ))

    # Add zero baseline
    fig.add_hline(y=0, line_dash="dash", line_color="gray", line_width=1, opacity=0.5)

    # Calculate dynamic y-axis range from data
    all_positive = [df[f"emotion_{e}"].max() for e in positive_emotions if f"emotion_{e}" in df.columns]
    all_negative = [df[f"emotion_{e}"].max() for e in negative_emotions if f"emotion_{e}" in df.columns]

    # Sum of stacked values
    if positive_emotions:
        max_positive = sum(all_positive) if all_positive else 0.1
    else:
        max_positive = 0.1

    if negative_emotions:
        max_negative = sum(all_negative) if all_negative else 0.1
    else:
        max_negative = 0.1

    # Add 20% padding
    y_max = max_positive * 1.2
    y_min = -max_negative * 1.2

    # Add positive zone shading (green, above zero)
    fig.add_hrect(
        y0=0, y1=y_max,
        fillcolor="green",
        opacity=0.05,
        layer="below",
        line_width=0
    )

    # Add negative zone shading (red, below zero)
    fig.add_hrect(
        y0=y_min, y1=0,
        fillcolor="red",
        opacity=0.05,
        layer="below",
        line_width=0
    )

    fig.update_layout(
        title=dict(
            text=f"{film_title} - Emotion Composition",
            font=dict(size=18, color=THEME["text_color"]),
            x=0.5,
            xanchor="center"
        ),
        xaxis_title="Runtime (minutes)",
        yaxis_title="Emotion Intensity (+ positive / - negative)",
        xaxis=dict(
            gridcolor="#1E293B",
            color=THEME["text_color"]
        ),
        yaxis=dict(
            gridcolor="#1E293B",
            color=THEME["text_color"],
            range=[y_min, y_max],  # Dynamic range based on data
            zeroline=True,
            zerolinecolor="gray",
            zerolinewidth=1
        ),
        plot_bgcolor=THEME["background_color"],
        paper_bgcolor=THEME["background_color"],
        font=dict(color=THEME["text_color"], family=THEME["font_body"]),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            bgcolor="rgba(30, 41, 59, 0.8)",
            bordercolor=THEME["primary_color"],
            borderwidth=1
        ),
        hovermode="x unified",
        height=400
    )

    return fig


def plot_emotional_fingerprint(
    emotion_summaries: List[Tuple[str, dict]],
    comparison_mode: bool = False
) -> go.Figure:
    """
    Create radar chart showing overall emotion profile with optional multi-film comparison.

    Args:
        emotion_summaries: List of tuples (film_title, emotion_summary_dict)
        comparison_mode: If True, overlay multiple films for comparison

    Returns:
        Plotly Figure object

    [Source: Story 5.3 - Task 2.3, AC4 + Enhancement]
    """
    if not emotion_summaries:
        fig = go.Figure()
        fig.add_annotation(
            text="No emotion summary available",
            xref="paper", yref="paper",
            x=0.5, y=0.5,
            showarrow=False,
            font=dict(size=16, color=THEME["text_color"])
        )
        fig.update_layout(
            plot_bgcolor=THEME["background_color"],
            paper_bgcolor=THEME["background_color"],
            height=600
        )
        return fig

    # Color palette for multiple films
    colors = ["#38BDF8", "#F59E0B", "#EC4899", "#34D399", "#A78BFA"]

    fig = go.Figure()
    all_values = []

    for idx, (film_title, emotion_summary) in enumerate(emotion_summaries):
        if not emotion_summary:
            continue

        # Get top 8 emotions (excluding neutral/ambiguous)
        excluded = set(EXCLUDED_EMOTIONS)
        eligible_emotions = {k: v for k, v in emotion_summary.items() if k not in excluded}

        # Sort by score and get top 8
        top_emotions = sorted(eligible_emotions.items(), key=lambda x: x[1], reverse=True)[:8]

        # Extract labels and values
        labels = [e[0].capitalize() for e in top_emotions]
        values = [e[1] for e in top_emotions]
        all_values.extend(values)

        color = colors[idx % len(colors)]

        fig.add_trace(go.Scatterpolar(
            r=values,
            theta=labels,
            fill='toself',
            fillcolor=color,
            opacity=0.2 if comparison_mode else 0.3,
            line=dict(color=color, width=2),
            name=film_title
        ))

    # Title based on mode
    if comparison_mode and len(emotion_summaries) > 1:
        title_text = "Emotional Fingerprint Comparison"
    else:
        title_text = f"{emotion_summaries[0][0]} - Emotional Fingerprint"

    fig.update_layout(
        title=dict(
            text=title_text,
            font=dict(size=20, color=THEME["text_color"]),
            x=0.5,
            xanchor="center"
        ),
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, max(all_values) * 1.2] if all_values else [0, 1],
                gridcolor="#1E293B",
                color=THEME["text_color"]
            ),
            angularaxis=dict(
                gridcolor="#1E293B",
                color=THEME["text_color"],
                rotation=90
            ),
            bgcolor=THEME["background_color"]
        ),
        plot_bgcolor=THEME["background_color"],
        paper_bgcolor=THEME["background_color"],
        font=dict(color=THEME["text_color"], family=THEME["font_body"]),
        height=600,  # Increased from 400
        showlegend=comparison_mode and len(emotion_summaries) > 1,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.15,
            xanchor="center",
            x=0.5,
            bgcolor="rgba(30, 41, 59, 0.8)",
            bordercolor=THEME["primary_color"],
            borderwidth=1
        )
    )

    return fig
