"""
Epic 3.5: Analytical Validation Dashboard

Single-page Streamlit dashboard for visual validation of emotion analysis
and knowledge graph data. Queries DuckDB for staging/mart layer data and
provides interactive visualizations for data quality assessment.

This is a developer-facing validation tool (Epic 3.5), not the production
portfolio app (Epic 5).

Usage:
    streamlit run src/validation/dashboard.py
"""

import logging
import sys
from pathlib import Path

# Add project root to Python path for imports
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import duckdb
import pandas as pd
import streamlit as st

from src.shared.config import DUCKDB_PATH
from src.validation.chart_utils import (
    plot_centrality_ranking,
    plot_film_similarity_network,
    plot_emotion_composition,
    plot_sentiment_timeline,
    plot_emotion_similarity_heatmap,
    plot_emotion_fingerprint_radar,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def generate_film_slug(title: str) -> str:
    """
    Generate URL-safe film slug from title.

    Converts film title to lowercase slug format by replacing spaces with
    underscores and removing special characters.

    Args:
        title: Film title string

    Returns:
        URL-safe film slug (e.g., "Spirited Away" -> "spirited_away")

    Example:
        >>> generate_film_slug("Spirited Away")
        'spirited_away'
        >>> generate_film_slug("Howl's Moving Castle")
        'howls_moving_castle'
    """
    import re

    # Convert to lowercase
    slug = title.lower()

    # Replace spaces with underscores
    slug = slug.replace(" ", "_")

    # Remove apostrophes and other special characters
    slug = re.sub(r"['\"]", "", slug)

    # Remove any other non-alphanumeric characters (except underscores)
    slug = re.sub(r"[^a-z0-9_]", "", slug)

    # Remove multiple consecutive underscores
    slug = re.sub(r"_+", "_", slug)

    # Remove leading/trailing underscores
    slug = slug.strip("_")

    return slug


@st.cache_resource
def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    """
    Get DuckDB connection with error handling.

    Connects to DuckDB database at DUCKDB_PATH with comprehensive error
    handling for missing files and connection failures. Connection is
    cached for performance using Streamlit's @st.cache_resource decorator.

    Returns:
        duckdb.DuckDBPyConnection: Active DuckDB connection

    Raises:
        FileNotFoundError: If database file doesn't exist
        duckdb.Error: If connection fails

    Example:
        >>> conn = get_duckdb_connection()
        >>> conn.execute("SELECT * FROM staging.stg_films LIMIT 1")
    """
    db_path = Path(DUCKDB_PATH)

    try:
        # Check if database file exists
        if not db_path.exists():
            error_msg = f"Database not found at {DUCKDB_PATH}"
            logger.error(error_msg)
            st.error(f"❌ {error_msg}")
            st.info("💡 Please run the data ingestion pipeline first:")
            st.code("python src/ingestion/load_to_duckdb.py")
            st.stop()

        # Attempt connection
        conn = duckdb.connect(str(db_path), read_only=True)
        logger.info(f"Connected to DuckDB at {DUCKDB_PATH}")
        return conn

    except duckdb.Error as e:
        error_msg = f"Database connection failed: {e}"
        logger.error(error_msg)
        st.error(f"❌ {error_msg}")
        st.info("💡 The database file may be corrupted. Try regenerating it:")
        st.code("python src/ingestion/load_to_duckdb.py")
        st.stop()


@st.cache_data
def load_films(_conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Load film list from DuckDB staging schema.

    Queries staging.stg_films table for all films with id, title,
    release_year, and director. Results are cached for performance
    using Streamlit's @st.cache_data decorator.

    Args:
        _conn: DuckDB connection (underscore prefix prevents hashing by Streamlit)

    Returns:
        DataFrame with columns: id, title, release_year, director

    Raises:
        duckdb.Error: If database query fails

    Example:
        >>> conn = get_duckdb_connection()
        >>> films_df = load_films(conn)
        >>> films_df["title"].tolist()
        ['Castle in the Sky', 'Grave of the Fireflies', ...]
    """
    logger.info("Loading films from staging.stg_films...")

    query = """
        SELECT id, title, release_year, director
        FROM main_staging.stg_films
        ORDER BY title
    """

    try:
        result_df = _conn.execute(query).fetch_df()

        # Handle empty results
        if result_df.empty:
            logger.warning("No films found in staging.stg_films table")
            st.warning("⚠️ No films found in database. Please run data ingestion first.")
            return pd.DataFrame(columns=["id", "title", "release_year", "director"])

        logger.info(f"Loaded {len(result_df)} films from database")
        return result_df

    except duckdb.Error as e:
        error_msg = f"Failed to load films: {e}"
        logger.error(error_msg)
        st.error(f"❌ {error_msg}")
        st.info("💡 Ensure the staging schema exists:")
        st.code("cd src/transformation && dbt run && cd ../..")
        return pd.DataFrame(columns=["id", "title", "release_year", "director"])


def main() -> None:
    """
    Main dashboard entry point.

    Configures Streamlit page, establishes database connection,
    loads film data, and renders dashboard layout with header,
    sidebar film selector, and main content area with 3 chart containers.
    """
    # Page configuration
    st.set_page_config(
        page_title="Validation Dashboard",
        layout="wide",
        page_icon="📊"
    )

    # Header section
    st.title("Epic 3.5: Analytical Validation Dashboard")
    st.caption("Visual validation of emotion analysis and graph data")

    st.divider()

    # Connect to database with loading spinner
    with st.spinner("Loading dashboard..."):
        conn = get_duckdb_connection()
        films_df = load_films(conn)

    # Check if we have films to display
    if films_df.empty:
        st.stop()

    # Sidebar - Film selector
    st.sidebar.header("🎬 Film Selection")

    # Create film display names (title + year)
    films_df["display_name"] = films_df.apply(
        lambda row: f"{row['title']} ({row['release_year']})",
        axis=1
    )

    # Film selector dropdown
    selected_film_display = st.sidebar.selectbox(
        "Select Film",
        options=films_df["display_name"].tolist(),
        index=0
    )

    # Language selector dropdown
    st.sidebar.divider()
    st.sidebar.header("🌍 Language Selection")
    selected_language = st.sidebar.selectbox(
        "Select Language",
        options=["en", "fr", "es", "nl", "ar"],
        format_func=lambda x: {
            "en": "🇬🇧 English",
            "fr": "🇫🇷 French",
            "es": "🇪🇸 Spanish",
            "nl": "🇳🇱 Dutch",
            "ar": "🇸🇦 Arabic"
        }[x],
        index=0
    )

    # Get selected film data
    selected_film_row = films_df[films_df["display_name"] == selected_film_display].iloc[0]

    # Store selected film in session state
    if "selected_film" not in st.session_state:
        st.session_state["selected_film"] = {}

    st.session_state["selected_film"] = {
        "id": selected_film_row["id"],
        "title": selected_film_row["title"],
        "release_year": selected_film_row["release_year"],
        "director": selected_film_row["director"]
    }

    # Display selected film info in sidebar
    st.sidebar.divider()
    st.sidebar.markdown("**Selected Configuration:**")
    st.sidebar.write(f"**Title:** {selected_film_row['title']}")
    st.sidebar.write(f"**Year:** {selected_film_row['release_year']}")
    st.sidebar.write(f"**Director:** {selected_film_row['director']}")
    st.sidebar.write(f"**Language:** {selected_language.upper()}")
    st.sidebar.write(f"**Film ID:** `{selected_film_row['id']}`")

    # Main content area - 3 chart containers
    st.subheader(f"Data Validation: {selected_film_row['title']}")

    # Create 3 columns for chart containers
    col1, col2, col3 = st.columns([1, 1, 1])

    with col1:
        chart1_container = st.container()
        with chart1_container:
            st.markdown("### 📈 Sentiment Evolution Timeline")
            
            # Generate film slug from title
            film_slug = generate_film_slug(selected_film_row["title"])
            
            # Generate sentiment timeline chart
            with st.spinner("Loading sentiment data..."):
                fig = plot_sentiment_timeline(
                    conn=conn,
                    film_slug=film_slug,
                    film_title=selected_film_row["title"],
                    language_code=selected_language
                )
            
            # Display chart or warning
            if fig is not None:
                st.plotly_chart(fig, use_container_width=True, config={"responsive": True})
                st.caption(
                    f"💡 Use play/pause to animate timeline. "
                    f"Hover over peaks to see dialogue excerpts."
                )
            else:
                st.warning(
                    f"⚠️ No sentiment data available for "
                    f"{selected_film_row['title']} in {selected_language.upper()}"
                )
                st.info(
                    "This film may not have been processed for emotion analysis yet, "
                    "or subtitles may not be available in this language."
                )

    with col2:
        chart2_container = st.container()
        with chart2_container:
            st.markdown("### 📊 Emotion Composition (Stacked Area)")
            
            # Generate emotion composition chart
            with st.spinner("Loading emotion composition..."):
                fig2 = plot_emotion_composition(
                    conn=conn,
                    film_slug=film_slug,
                    film_title=selected_film_row["title"],
                    language_code=selected_language
                )
            
            # Display chart or warning
            if fig2 is not None:
                st.plotly_chart(fig2, use_container_width=True, config={"responsive": True})
                st.caption(
                    f"💡 Shows top 7 emotions over time. "
                    f"Stacked areas represent cumulative emotional intensity."
                )
            else:
                st.warning(
                    f"⚠️ No emotion data available for "
                    f"{selected_film_row['title']} in {selected_language.upper()}"
                )

    with col3:
        chart3_container = st.container()
        with chart3_container:
            st.markdown("### 🕸️ Chart 3")
            st.info("**Placeholder:** Knowledge graph connections")
            st.caption("Future: Graph visualization of film-character-location relationships")

    # Emotion Analysis Section
    st.divider()
    st.subheader("🎭 Emotion Analysis: Film Emotional Similarity")
    st.caption(
        "Discover which Studio Ghibli films share similar emotional profiles. "
        "Based on 28 emotions analyzed minute-by-minute throughout each film."
    )

    # Show metrics before charts
    with st.expander("📊 Quick Insights", expanded=False):
        metrics_col1, metrics_col2 = st.columns(2)
        
        with metrics_col1:
            st.markdown(f"**😊 Emotions: {selected_film_row['title']}**")
            # Query emotion data for selected film
            try:
                emotion_query = """
                SELECT 
                    AVG(emotion_joy) as joy,
                    AVG(emotion_sadness) as sadness,
                    AVG(emotion_fear) as fear,
                    AVG(emotion_anger) as anger,
                    AVG(emotion_surprise) as surprise,
                    AVG(emotion_love) as love,
                    AVG(emotion_admiration) as admiration,
                    AVG(emotion_excitement) as excitement
                FROM raw.film_emotions
                WHERE film_id = ?
                """
                emotion_data = conn.execute(emotion_query, [selected_film_row["id"]]).fetchone()
                
                if emotion_data and any(emotion_data):
                    emotions = [
                        ("Joy", emotion_data[0]),
                        ("Sadness", emotion_data[1]),
                        ("Fear", emotion_data[2]),
                        ("Anger", emotion_data[3]),
                        ("Surprise", emotion_data[4]),
                        ("Love", emotion_data[5]),
                        ("Admiration", emotion_data[6]),
                        ("Excitement", emotion_data[7]),
                    ]
                    emotions = [(name, score) for name, score in emotions if score is not None]
                    emotions.sort(key=lambda x: x[1], reverse=True)
                    
                    for emotion, score in emotions[:3]:
                        percentage = score * 100
                        st.write(f"• {emotion}: {percentage:.1f}%")
                else:
                    st.write("_No emotion data available_")
            except Exception as e:
                st.write("_No emotion data yet_")
        
        with metrics_col2:
            st.markdown("**ℹ️ About This Analysis**")
            st.write("• 27 emotions (neutral excluded)")
            st.write("• Minute-by-minute data")
            st.write("• Normalized & distance-based")

    # Chart 1: Emotion Similarity Heatmap
    st.markdown("### 📊 Chart 1: Emotion Similarity Heatmap")
    st.caption("See which films share similar emotional profiles. Darker colors = higher similarity.")
    
    with st.spinner("Calculating emotional similarities..."):
        heatmap_fig = plot_emotion_similarity_heatmap(conn, selected_film_row["id"])
    
    if heatmap_fig:
        st.plotly_chart(heatmap_fig, use_container_width=True, config={"responsive": True})
        st.info("💡 **How to read**: Each cell shows emotional similarity. Higher % = more similar emotional profiles (excluding neutral emotion which dominated at 56%). Look for surprising connections!")
    else:
        st.warning("⚠️ No emotion data available yet. Run emotion analysis first.")
    
    # Chart 2: Emotion Fingerprint Radar
    st.markdown("### 🎯 Chart 2: Emotional Fingerprint Comparison")
    st.caption("Compare the emotional 'shape' of different films. Larger area = more of that emotion.")
    
    # Let user select films to compare
    col1, col2 = st.columns([1, 3])
    
    with col1:
        # Get all films with emotion data
        films_with_emotions = conn.execute("""
            SELECT DISTINCT f.id, f.title
            FROM raw.films f
            INNER JOIN raw.film_emotions e ON f.id = e.film_id
            ORDER BY f.title
        """).fetchall()
        
        if films_with_emotions:
            film_options = {title: film_id for film_id, title in films_with_emotions}
            
            # Always include selected film
            default_films = [selected_film_row["title"]]
            
            # Add up to 2 more films
            other_films = [title for title in film_options.keys() if title != selected_film_row["title"]]
            if len(other_films) >= 2:
                default_films.extend(other_films[:2])
            elif len(other_films) == 1:
                default_films.append(other_films[0])
            
            selected_titles = st.multiselect(
                "Select films to compare (max 3)",
                options=list(film_options.keys()),
                default=default_films[:3],
                max_selections=3,
            )
            
            selected_film_ids = [film_options[title] for title in selected_titles]
    
    with col2:
        if selected_film_ids:
            with st.spinner("Creating emotional fingerprints..."):
                radar_fig = plot_emotion_fingerprint_radar(conn, selected_film_ids, top_n_emotions=8)
            
            if radar_fig:
                st.plotly_chart(radar_fig, use_container_width=True, config={"responsive": True})
                st.info("💡 **How to read**: Each film has a unique 'shape' based on its top emotions (normalized to 100%, neutral excluded). Larger areas = that emotion dominates. Overlaps = shared characteristics.")
            else:
                st.warning("⚠️ Could not create radar chart. Try selecting different films.")
        else:
            st.info("👆 Select films above to compare their emotional profiles.")

    # Footer
    st.divider()
    st.caption("Data source: DuckDB staging schema (`data/ghibli.duckdb`)")
    st.caption("Dashboard Purpose: Epic 3.5 analytical validation (developer tool)")


if __name__ == "__main__":
    main()

