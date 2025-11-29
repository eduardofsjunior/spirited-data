"""
The Spirit Archives - Film Explorer Page

Interactive emotion analysis for Studio Ghibli films across languages.
Features: Film selector, language selector, emotion timeline, composition chart,
emotional fingerprint radar chart, smoothed vs raw toggle, CSV export.

[Source: Story 5.3 - Epic 5: Production-Grade Streamlit Emotion Analysis App]
"""

import streamlit as st
import sys
import logging
from pathlib import Path

# Configure logging (QA Fix CODE-002)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add parent directory to path for utils imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.theme import apply_custom_css, render_header
from utils.data_loader import (
    get_film_list_with_metadata,
    get_film_emotion_timeseries_by_id,
    get_raw_emotion_peaks,
    get_film_emotion_summary_by_id,
    get_emotion_peaks_with_scenes,
    get_peak_dialogues,
    get_film_slug_from_id,
    get_validation_status
)
from utils.visualization import (
    plot_emotion_timeline,
    plot_emotion_composition,
    plot_emotional_fingerprint,
    get_top_n_emotions
)
from utils.data_quality import render_data_quality_warning

# Page configuration
st.set_page_config(
    page_title="The Spirit Archives - Film Explorer",
    page_icon="🎬",
    layout="wide"
)

apply_custom_css()

# ============================================================================
# Page Header
# ============================================================================

render_header(
    "🎬 The Spirit Archives",
    "Explore the emotional heartbeat of Studio Ghibli films across languages"
)

st.markdown(
    """
    Select a film and language to visualize emotion analysis powered by multilingual
    transformers and signal processing.
    """,
    unsafe_allow_html=True
)

st.markdown("---")

# ============================================================================
# Film and Language Selectors
# ============================================================================

# Language mapping
LANGUAGE_MAP = {
    "English": "en",
    "French": "fr",
    "Spanish": "es",
    "Dutch": "nl",
    "Arabic": "ar"
}

# Load film list
try:
    films = get_film_list_with_metadata()
except Exception as e:
    logger.error(f"Failed to load film list: {e}", exc_info=True)
    st.error(f"Failed to load film list: {e}")
    st.stop()

# Create selectors in horizontal row
col1, col2 = st.columns([1, 1])

with col1:
    # Film selector
    film_options = {film["display_name"]: film for film in films}

    # Default to Princess Kaguya if available
    default_film = None
    for display_name, film in film_options.items():
        if "Princess Kaguya" in display_name or "Kaguya" in display_name:
            default_film = display_name
            break

    if default_film is None and film_options:
        default_film = list(film_options.keys())[0]

    selected_film_display = st.selectbox(
        "Select Film",
        options=list(film_options.keys()),
        index=list(film_options.keys()).index(default_film) if default_film else 0
    )

    selected_film = film_options[selected_film_display]

with col2:
    # Language selector
    selected_language = st.selectbox(
        "Select Language",
        options=list(LANGUAGE_MAP.keys()),
        index=0  # Default to English
    )

    selected_language_code = LANGUAGE_MAP[selected_language]

# ============================================================================
# Data Quality Validation Warning (Epic 3.6.5)
# ============================================================================

# Fetch validation status for selected film-language combination
validation_data = None
try:
    validation_data = get_validation_status(
        selected_film["film_id"],
        selected_language_code
    )
except Exception as e:
    logger.warning(f"Failed to fetch validation status: {e}")

# Display warning if data quality issues detected
render_data_quality_warning(validation_data)

# ============================================================================
# Smoothed vs Raw Toggle
# ============================================================================

st.markdown("### Data Resolution")

data_mode = st.radio(
    "Choose data resolution for timeline visualization:",
    options=["Smoothed (10-min rolling avg)", "Raw (dialogue-level)"],
    index=0,
    horizontal=True
)

is_smoothed = data_mode.startswith("Smoothed")

# Display info about data mode
if is_smoothed:
    st.info(
        "**Smoothed:** 10-minute rolling average for clearer emotional trends and patterns."
    )
else:
    st.info(
        "**Raw:** Original dialogue-level emotion scores showing granular emotional moments (noisier)."
    )

st.markdown("---")

# ============================================================================
# Load Data
# ============================================================================

with st.spinner("Loading emotion data..."):
    try:
        # Load appropriate data based on toggle
        if is_smoothed:
            timeseries_df = get_film_emotion_timeseries_by_id(
                selected_film["film_id"],
                selected_language_code
            )
        else:
            timeseries_df = get_raw_emotion_peaks(
                selected_film["film_id"],
                selected_language_code
            )

        # Load emotion summary for fingerprint
        emotion_summary = get_film_emotion_summary_by_id(
            selected_film["film_id"],
            selected_language_code
        )

        # Load emotion peaks with scene descriptions for timeline tooltips
        peaks_df = get_emotion_peaks_with_scenes(
            selected_film["film_id"],
            selected_language_code
        )

    except Exception as e:
        logger.error(f"Failed to load emotion data for {selected_film['title']}: {e}", exc_info=True)
        st.error(f"Failed to load emotion data: {e}")
        st.stop()

# Check if data is available
if timeseries_df.empty:
    st.warning(
        f"Emotion data not available for **{selected_film['title']}** in **{selected_language}**. "
        "Please select another language or film."
    )
    st.stop()

# ============================================================================
# Visualization 1: Emotion Timeline + Peak Dialogue Sidebar
# ============================================================================

st.markdown("### Emotion Timeline")
st.markdown("Evolution of dominant emotions over the film's runtime. ⭐ Colored stars mark peak emotional moments.")

# Create two columns: timeline (left) + peak dialogues (right)
col_timeline, col_peaks = st.columns([2, 1])

with col_timeline:
    try:
        timeline_fig = plot_emotion_timeline(
            timeseries_df,
            selected_film["title"],
            selected_language_code,
            is_smoothed,
            peaks_df=peaks_df
        )
        st.plotly_chart(timeline_fig, use_container_width=True)
    except Exception as e:
        logger.error(f"Failed to render emotion timeline for {selected_film['title']}: {e}", exc_info=True)
        st.error(f"Failed to render emotion timeline: {e}")

with col_peaks:
    st.markdown("#### 🎭 Peak Emotional Moments")
    st.markdown("*Actual dialogue from the most intense scenes*")

    # Load peak dialogues from parsed subtitle files
    try:
        # Get top 5 emotions shown on timeline (only show peaks for these)
        top_5_emotions = get_top_n_emotions(timeseries_df, n=5)

        # Get film slug using dedicated function (QA Fix CODE-001)
        film_slug_base = get_film_slug_from_id(selected_film["film_id"], selected_language_code)

        if film_slug_base:
            all_peak_dialogues = get_peak_dialogues(film_slug_base, selected_language_code, peaks_df)

            # Filter peaks: must be in top 5 emotions AND above intensity threshold
            # Threshold 0.05 is ~75th percentile of non-neutral emotion peaks (data-driven)
            INTENSITY_THRESHOLD = 0.05
            peak_dialogues = [
                peak for peak in all_peak_dialogues
                if peak["emotion_type"] in top_5_emotions and peak["intensity"] >= INTENSITY_THRESHOLD
            ]

            # Show maximum 4 high-intensity peaks
            if peak_dialogues:
                for peak in peak_dialogues[:4]:
                    emotion_emoji = {
                        "joy": "😊", "fear": "😨", "sadness": "😢",
                        "anger": "😠", "surprise": "😲", "love": "💗",
                        "excitement": "🤩", "disgust": "🤢", "admiration": "🤩",
                        "amusement": "😄", "caring": "🤗", "curiosity": "🤔",
                        "gratitude": "🙏", "optimism": "☀️", "pride": "💪",
                        "relief": "😌", "confusion": "😕", "disappointment": "😞",
                        "nervousness": "😰", "annoyance": "😤", "grief": "😭"
                    }.get(peak["emotion_type"], "✨")

                    with st.expander(
                        f"{emotion_emoji} **{peak['emotion_type'].capitalize()}** ({peak['minute_range']})",
                        expanded=False
                    ):
                        st.caption(f"Intensity: {peak['intensity']:.3f} • Rank #{peak['peak_rank']}")
                        for line in peak["dialogue_lines"]:
                            st.markdown(f"> {line}")
            else:
                st.info("No high-intensity peak moments found (intensity threshold: 0.05).")
        else:
            st.info("Film slug not found in database.")

    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Failed to load peak dialogues for {selected_film['title']}: {e}", exc_info=True)
        st.warning(f"Could not load peak dialogues: {e}")

st.markdown("---")

# ============================================================================
# Visualization 2: Emotion Composition
# ============================================================================

st.markdown("### Emotion Composition")
st.markdown("Stacked intensity of emotions over time. Positive emotions stack above zero, negative emotions stack below.")

try:
    # Use smoothed data for composition (more stable for percentage view)
    composition_df = get_film_emotion_timeseries_by_id(
        selected_film["film_id"],
        selected_language_code
    )

    composition_fig = plot_emotion_composition(
        composition_df,
        selected_film["title"]
    )
    st.plotly_chart(composition_fig, use_container_width=True)
except Exception as e:
    logger.error(f"Failed to render emotion composition for {selected_film['title']}: {e}", exc_info=True)
    st.error(f"Failed to render emotion composition: {e}")

st.markdown("---")

# ============================================================================
# Visualization 3: Emotional Fingerprint
# ============================================================================

st.markdown("### Emotional Fingerprint")
st.markdown("Overall emotion profile - the film's unique emotional signature")

# Comparison mode toggle
enable_comparison = st.checkbox("📊 Compare with other films", value=False, key="fingerprint_comparison")

if enable_comparison:
    # Multi-select for comparison films
    comparison_films = st.multiselect(
        "Select films to compare (up to 4 additional films):",
        options=[f["display_name"] for f in films if f["film_id"] != selected_film["film_id"]],
        max_selections=4,
        key="comparison_selector"
    )

    # Load emotion summaries for all selected films
    emotion_summaries = [(selected_film["title"], emotion_summary)]

    if comparison_films:
        for comp_film_display in comparison_films:
            comp_film = film_options[comp_film_display]
            comp_summary = get_film_emotion_summary_by_id(
                comp_film["film_id"],
                selected_language_code
            )
            emotion_summaries.append((comp_film["title"], comp_summary))
else:
    emotion_summaries = [(selected_film["title"], emotion_summary)]

try:
    fingerprint_fig = plot_emotional_fingerprint(
        emotion_summaries,
        comparison_mode=enable_comparison
    )
    st.plotly_chart(fingerprint_fig, use_container_width=True)
except Exception as e:
    logger.error(f"Failed to render emotional fingerprint: {e}", exc_info=True)
    st.error(f"Failed to render emotional fingerprint: {e}")

st.markdown("---")

# ============================================================================
# CSV Export
# ============================================================================

st.markdown("### Export Data")

try:
    # Generate CSV from timeseries data
    csv_data = timeseries_df.to_csv(index=False).encode('utf-8')

    # Create dynamic filename
    film_slug = selected_film["title"].lower().replace(' ', '_').replace("'", '').replace(':', '')
    filename = f"{film_slug}_{selected_language_code}_emotions.csv"

    st.download_button(
        label="📥 Export Emotion Data (CSV)",
        data=csv_data,
        file_name=filename,
        mime='text/csv',
        help=f"Download emotion timeline data for {selected_film['title']} in {selected_language}"
    )
except Exception as e:
    logger.error(f"Failed to generate CSV export for {selected_film['title']}: {e}", exc_info=True)
    st.error(f"Failed to generate CSV export: {e}")

# ============================================================================
# Footer
# ============================================================================

st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #94A3B8; font-size: 0.9em; padding: 20px 0;'>
        <p>Emotion analysis powered by <strong>multilingual-go-emotions</strong> transformer model</p>
        <p>Signal processing: 10-minute rolling average for smoothed visualization</p>
    </div>
    """,
    unsafe_allow_html=True
)
