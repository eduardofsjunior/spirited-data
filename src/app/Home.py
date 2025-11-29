# src/app/Home.py

"""
SpiritedData: Emotional Landscape of Studio Ghibli

Main entry point for Streamlit multi-page application.
Emotion Analysis Showcase Landing Page.

[Source: prd/epic-details.md - Epic 5: Story 5.2]
"""

import streamlit as st
from utils.config import APP_TITLE, THEME
from utils.theme import apply_custom_css, render_header, render_glass_card, render_footer
from utils.data_loader import (
    get_hero_stats,
    get_top_joyful_film,
    get_top_fearful_film,
    get_director_comparison,
    get_film_list,
    get_film_emotion_timeseries
)
from utils.visualization import plot_emotion_preview

# Page config (must be first Streamlit command)
st.set_page_config(
    page_title=APP_TITLE,
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply "Spirit World" Design System
apply_custom_css()

# ============================================================================
# AC1: Hero Section with New Title and Tagline
# ============================================================================

render_header(
    "SpiritedData: Emotional Landscape of Studio Ghibli",
    "Exploring the emotional heartbeat of Studio Ghibli films through multilingual NLP analysis"
)

st.markdown(
    "<p style='text-align: center; color: #94A3B8; font-size: 16px; margin-top: -10px;'>"
    "Analyzing 22 films across 5 languages with 28-dimension emotion classification - "
    "powered by transformers and signal processing"
    "</p>",
    unsafe_allow_html=True
)

st.markdown("<br>", unsafe_allow_html=True)

# ============================================================================
# AC2: Data Stats Dashboard
# ============================================================================

st.markdown("### 📊 The Ghibli Archive")

# Load actual stats from database
with st.spinner("Loading statistics..."):
    stats = get_hero_stats()

col1, col2, col3, col4 = st.columns(4)

with col1:
    render_glass_card(
        "Films Analyzed",
        str(stats["film_count"]),
        "Studio Ghibli films",
        icon="🎬"
    )

with col2:
    render_glass_card(
        "Emotion Data Points",
        f"{stats['emotion_data_points']:,}",
        "Minute-by-minute emotion tracking",
        icon="📈"
    )

with col3:
    render_glass_card(
        "Languages",
        str(stats["languages_count"]),
        "EN • FR • ES • NL • AR",
        icon="🌍"
    )

with col4:
    render_glass_card(
        "Dialogue Entries",
        f"{stats['dialogue_entries']:,}",
        "Parsed subtitle analysis",
        icon="💬"
    )

st.markdown("---")

# ============================================================================
# AC3: Quick Insights Section
# ============================================================================

st.markdown("### ✨ Quick Insights")

# Load insights data
with st.spinner("Computing insights..."):
    joyful = get_top_joyful_film()
    fearful = get_top_fearful_film()
    directors = get_director_comparison()

col_i1, col_i2, col_i3 = st.columns(3)

with col_i1:
    st.markdown(f"""
    <div class="glass-card">
        <h4 style="color: {THEME['accent_color']};">😊 Most Joyful Film</h4>
        <p style="font-size: 18px; font-weight: bold; margin: 10px 0;">{joyful['film_title']}</p>
        <p style="color: #94A3B8; font-size: 14px;">Joy Score: {joyful['joy_score']:.3f}</p>
    </div>
    """, unsafe_allow_html=True)

with col_i2:
    st.markdown(f"""
    <div class="glass-card">
        <h4 style="color: {THEME['primary_color']};">😨 Most Fearful Film</h4>
        <p style="font-size: 18px; font-weight: bold; margin: 10px 0;">{fearful['film_title']}</p>
        <p style="color: #94A3B8; font-size: 14px;">Fear Score: {fearful['fear_score']:.3f}</p>
    </div>
    """, unsafe_allow_html=True)

with col_i3:
    miyazaki = directors.get("miyazaki", {})
    takahata = directors.get("takahata", {})

    # Calculate percentage differences for display
    joy_diff = ((takahata.get("joy", 0) / miyazaki.get("joy", 1)) - 1) * 100
    sadness_diff = ((takahata.get("sadness", 0) / miyazaki.get("sadness", 1)) - 1) * 100

    st.markdown(f"""
    <div class="glass-card">
        <h4 style="color: {THEME['primary_color']}; margin-bottom: 15px;">🎭 Director Styles</h4>
        <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 10px;">
            <div style="border-right: 1px solid rgba(255, 255, 255, 0.1); padding-right: 8px;">
                <p style="font-size: 13px; font-weight: bold; margin: 0 0 8px 0;">Miyazaki</p>
                <p style="font-size: 11px; color: {THEME['accent_color']}; margin: 0 0 8px 0;">{miyazaki.get('film_count', 0)} films</p>
                <p style="font-size: 10px; color: #94A3B8; margin: 0 0 4px 0; line-height: 1.5;">{miyazaki.get('style_label', 'Unknown')}</p>
                <p style="font-size: 10px; color: #64748B; margin: 0;">Diversity: {miyazaki.get('diversity', 0):.3f}</p>
            </div>
            <div style="padding-left: 8px;">
                <p style="font-size: 13px; font-weight: bold; margin: 0 0 8px 0;">Takahata</p>
                <p style="font-size: 11px; color: {THEME['accent_color']}; margin: 0 0 8px 0;">{takahata.get('film_count', 0)} films</p>
                <p style="font-size: 10px; color: #94A3B8; margin: 0 0 4px 0; line-height: 1.5;">{takahata.get('style_label', 'Unknown')}</p>
                <p style="font-size: 10px; color: #64748B; margin: 0;">+{joy_diff:.0f}% joy<br/>+{sadness_diff:.0f}% sad</p>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("---")

# ============================================================================
# AC4: Interactive "Emotion Journey" Selector
# ============================================================================

st.markdown("### 🎞️ Emotion Journey Explorer")
st.markdown(
    "<p style='color: #94A3B8;'>Select a film to preview its emotional arc across time</p>",
    unsafe_allow_html=True
)

# Load film list
films_df = get_film_list()
film_titles = films_df["title"].tolist()

col_sel1, col_sel2 = st.columns([3, 1])

with col_sel1:
    selected_film = st.selectbox(
        "Choose a Film",
        film_titles,
        index=film_titles.index("Spirited Away") if "Spirited Away" in film_titles else 0,
        key="film_selector"
    )

with col_sel2:
    selected_language = st.selectbox(
        "Language",
        ["en", "fr", "es", "nl", "ar"],
        format_func=lambda x: {"en": "English", "fr": "French", "es": "Spanish", "nl": "Dutch", "ar": "Arabic"}[x],
        key="language_selector"
    )

# Load and plot emotion timeline
if selected_film:
    with st.spinner(f"Loading emotion timeline for {selected_film}..."):
        emotion_df = get_film_emotion_timeseries(selected_film, selected_language)

    if not emotion_df.empty:
        fig = plot_emotion_preview(emotion_df, selected_film, selected_language)
        st.plotly_chart(fig, use_container_width=True)

        # Call-to-action button
        col_cta1, col_cta2, col_cta3 = st.columns([1, 2, 1])
        with col_cta2:
            if st.button("🎬 Explore Full Analysis →", use_container_width=True, type="primary"):
                st.switch_page("pages/1_🎬_The_Spirit_Archives.py")
    else:
        st.warning(f"No emotion data found for {selected_film} in {selected_language.upper()}")

st.markdown("---")

# ============================================================================
# AC6: Navigation to Other Pages
# ============================================================================

st.markdown("### 🌟 Journey into the Spirit World")

col_nav1, col_nav2 = st.columns(2)

with col_nav1:
    st.markdown(f"""
    <div class="glass-card">
        <h4>🎬 The Spirit Archives</h4>
        <p style="color: #94A3B8;">Visualize the emotional heartbeat of the films</p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown(f"""
    <div class="glass-card">
        <h4>🎭 Architects of Emotion</h4>
        <p style="color: #94A3B8;">Miyazaki vs. Takahata: A study in contrast</p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown(f"""
    <div class="glass-card">
        <h4>🧠 Memories of Sora</h4>
        <p style="color: #94A3B8;">The rise and fall of our AI Archivist</p>
    </div>
    """, unsafe_allow_html=True)

with col_nav2:
    st.markdown(f"""
    <div class="glass-card">
        <h4>🌍 Echoes Across Languages</h4>
        <p style="color: #94A3B8;">How meaning shifts in translation</p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown(f"""
    <div class="glass-card">
        <h4>📊 The Alchemy of Data</h4>
        <p style="color: #94A3B8;">Decoding the signal processing behind the magic</p>
    </div>
    """, unsafe_allow_html=True)

# Footer
render_footer()

# Sidebar info
with st.sidebar:
    st.markdown("<br><br>", unsafe_allow_html=True)  # Add spacing from nav

    st.markdown("### About")

    st.markdown("""
    **SpiritedData** is an emotion analysis engine developed as a personal showcase of Data Engineering best practices. It was **speedbuilt** using smart context engineering within Agentic IDEs.

    While this demo analyzes Studio Ghibli films, the underlying engine is flexible. You can download the project and adapt it to your own datasets via the [GitHub Repo](https://github.com/edjunior/ghibli_pipeline).

    *(If you enjoy the analysis, a star on the repo is greatly appreciated!)*

    ---

    **Note:** Analysis results vary by subtitle version. See "The Alchemy of Data" for source details.
    """)
