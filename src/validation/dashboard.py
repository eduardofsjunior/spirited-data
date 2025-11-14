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
    get_film_duration,
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


def initialize_filter_state() -> None:
    """
    Initialize Streamlit session state with default filter values.

    Sets up session state keys for:
    - selected_film_id: Currently selected film (None = use dropdown default)
    - time_range_min: Sentiment chart time window start (minutes)
    - time_range_max: Sentiment chart time window end (minutes, None = max available)
    - intensity_threshold: Peak filtering threshold (0.0 to 1.0)
    - centrality_top_n: Number of characters to show in centrality chart

    Called at dashboard startup before any widgets are rendered.
    Only initializes keys that don't already exist (preserves session state).
    """
    if 'selected_film_id' not in st.session_state:
        st.session_state['selected_film_id'] = None
    
    if 'time_range_min' not in st.session_state:
        st.session_state['time_range_min'] = 0
    
    if 'time_range_max' not in st.session_state:
        st.session_state['time_range_max'] = None
    
    if 'intensity_threshold' not in st.session_state:
        st.session_state['intensity_threshold'] = 0.5
    
    if 'centrality_top_n' not in st.session_state:
        st.session_state['centrality_top_n'] = 10


def on_film_change() -> None:
    """
    Callback when film selector changes.
    
    Resets time range filters to defaults when a different film is selected,
    since each film has different duration. Other filters (intensity threshold,
    centrality top N) are preserved across film changes.
    """
    # Reset time range to full film duration (will be set by slider later)
    st.session_state['time_range_min'] = 0
    st.session_state['time_range_max'] = None
    
    logger.info(f"Film changed to: {st.session_state.get('selected_film_id')}")


def export_sentiment_data(
    conn: duckdb.DuckDBPyConnection,
    film_slug: str,
    language_code: str,
    time_range_min: int = 0,
    time_range_max: int = None
) -> pd.DataFrame:
    """
    Export filtered sentiment data as pandas DataFrame for CSV download.
    
    Queries emotion data from DuckDB for the specified film and language,
    applies time range filter, and calculates compound sentiment scores.
    Returns DataFrame ready for CSV export.
    
    Args:
        conn: Active DuckDB connection
        film_slug: URL-safe film identifier (e.g., "spirited_away")
        language_code: ISO 639-1 language code (e.g., "en")
        time_range_min: Start minute for time range filter (default: 0)
        time_range_max: End minute for time range filter (default: None = no limit)
    
    Returns:
        DataFrame with columns: minute_offset, compound_score, top_emotion,
        and individual emotion scores (emotion_joy, emotion_sadness, etc.)
    
    Example:
        >>> conn = get_duckdb_connection()
        >>> df = export_sentiment_data(conn, "spirited_away", "en", 0, 60)
        >>> df.to_csv("spirited_away_sentiment.csv", index=False)
    """
    from src.validation.chart_utils import calculate_compound_score, calculate_dominant_emotion
    
    try:
        # Build query with time range filter
        query = """
            SELECT 
                minute_offset,
                emotion_admiration, emotion_amusement, emotion_anger, emotion_annoyance,
                emotion_approval, emotion_caring, emotion_confusion, emotion_curiosity,
                emotion_desire, emotion_disappointment, emotion_disapproval, emotion_disgust,
                emotion_embarrassment, emotion_excitement, emotion_fear, emotion_gratitude,
                emotion_grief, emotion_joy, emotion_love, emotion_nervousness,
                emotion_optimism, emotion_pride, emotion_realization, emotion_relief,
                emotion_remorse, emotion_sadness, emotion_surprise, emotion_neutral
            FROM raw.film_emotions
            WHERE film_slug = ? AND language_code = ?
        """
        
        params = [f"{film_slug}_{language_code}", language_code]
        
        if time_range_max is not None:
            query += " AND minute_offset BETWEEN ? AND ?"
            params.extend([time_range_min, time_range_max])
        elif time_range_min > 0:
            query += " AND minute_offset >= ?"
            params.append(time_range_min)
        
        query += " ORDER BY minute_offset"
        
        # Execute query
        df = conn.execute(query, params).fetch_df()
        
        if df.empty:
            logger.warning(f"No sentiment data found for export: {film_slug} ({language_code})")
            return pd.DataFrame()
        
        # Calculate compound score
        df['compound_score'] = df.apply(calculate_compound_score, axis=1)
        
        # Identify dominant emotion for each minute
        df['dominant_emotion'] = df.apply(
            lambda row: calculate_dominant_emotion(row)['emotion'], 
            axis=1
        )
        
        # Reorder columns for better readability
        cols = ['minute_offset', 'compound_score', 'dominant_emotion'] + [
            col for col in df.columns if col.startswith('emotion_')
        ]
        df = df[cols]
        
        logger.info(f"Exported {len(df)} rows of sentiment data for {film_slug} ({language_code})")
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to export sentiment data: {e}")
        return pd.DataFrame()


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
    
    # Initialize session state for filters
    initialize_filter_state()

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

    # Create film display names (title + year) and ID mapping
    films_df["display_name"] = films_df.apply(
        lambda row: f"{row['title']} ({row['release_year']})",
        axis=1
    )
    
    # Create mapping from film_id to index for default selection
    film_id_to_index = {row["id"]: idx for idx, row in films_df.iterrows()}
    
    # Determine default index based on session state
    default_index = 0
    if st.session_state.get('selected_film_id') and st.session_state['selected_film_id'] in film_id_to_index:
        default_index = film_id_to_index[st.session_state['selected_film_id']]

    # Film selector dropdown with session state
    selected_film_index = st.sidebar.selectbox(
        "Select Film",
        options=range(len(films_df)),
        format_func=lambda i: films_df.iloc[i]["display_name"],
        index=default_index,
        key="film_selector"
    )
    
    # Get selected film data
    selected_film_row = films_df.iloc[selected_film_index]
    
    # Update session state if film changed
    if st.session_state['selected_film_id'] != selected_film_row["id"]:
        st.session_state['selected_film_id'] = selected_film_row["id"]
        on_film_change()

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
    
    # Filters section
    st.sidebar.divider()
    st.sidebar.header("🎛️ Filters")
    
    # Sentiment Chart Filters
    st.sidebar.subheader("📈 Sentiment Chart")
    
    # Time range slider for sentiment chart
    film_slug = generate_film_slug(selected_film_row["title"])
    min_minute, max_minute = get_film_duration(conn, film_slug, selected_language)
    
    if max_minute > 0:
        # Create time range slider
        time_range = st.sidebar.slider(
            "Time Range (minutes)",
            min_value=int(min_minute),
            max_value=int(max_minute),
            value=(int(min_minute), int(max_minute)),
            help="Filter sentiment chart to show specific time range"
        )
        
        # Update session state
        st.session_state['time_range_min'] = time_range[0]
        st.session_state['time_range_max'] = time_range[1]
    else:
        st.sidebar.info("⚠️ No emotion data available for time range filter")
        st.session_state['time_range_min'] = 0
        st.session_state['time_range_max'] = None
    
    # Intensity threshold slider for peaks
    intensity_threshold = st.sidebar.slider(
        "Peak Intensity Threshold",
        min_value=0.0,
        max_value=1.0,
        value=st.session_state['intensity_threshold'],
        step=0.1,
        help="Filter peaks to show only high-intensity moments. Higher values = fewer, more intense peaks."
    )
    st.session_state['intensity_threshold'] = intensity_threshold
    
    # Reset Filters button
    st.sidebar.divider()
    if st.sidebar.button("🔄 Reset Filters", use_container_width=True):
        # Reset all filter values to defaults
        st.session_state['time_range_min'] = 0
        st.session_state['time_range_max'] = None
        st.session_state['intensity_threshold'] = 0.5
        st.session_state['centrality_top_n'] = 10
        
        # Log the reset
        logger.info("Filters reset to default values")
        
        # Show success message and rerun
        st.success("✅ Filters reset to defaults")
        st.rerun()

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
            
            # Generate sentiment timeline chart with time range and intensity filters
            with st.spinner("Loading sentiment data..."):
                fig = plot_sentiment_timeline(
                    conn=conn,
                    film_slug=film_slug,
                    film_title=selected_film_row["title"],
                    language_code=selected_language,
                    time_range_min=st.session_state['time_range_min'],
                    time_range_max=st.session_state['time_range_max'],
                    intensity_threshold=st.session_state['intensity_threshold']
                )
            
            # Display chart or warning
            if fig is not None:
                st.plotly_chart(fig, use_container_width=True, config={"responsive": True})
                st.caption(
                    f"💡 Use play/pause to animate timeline. "
                    f"Hover over peaks to see dialogue excerpts."
                )
                
                # Export sentiment data button
                export_df = export_sentiment_data(
                    conn=conn,
                    film_slug=film_slug,
                    language_code=selected_language,
                    time_range_min=st.session_state['time_range_min'],
                    time_range_max=st.session_state['time_range_max']
                )
                
                if not export_df.empty:
                    csv = export_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Export Sentiment Data",
                        data=csv,
                        file_name=f"{film_slug}_{selected_language}_sentiment.csv",
                        mime="text/csv",
                        help="Download filtered sentiment data as CSV"
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
    st.caption(f"See which films share similar emotional profiles in {selected_language.upper()}. Darker colors = higher similarity.")
    
    with st.spinner("Calculating emotional similarities..."):
        heatmap_fig = plot_emotion_similarity_heatmap(
            conn, selected_film_row["id"], language_code=selected_language
        )
    
    if heatmap_fig:
        st.plotly_chart(heatmap_fig, use_container_width=True, config={"responsive": True})
        st.info(f"💡 **How to read**: Each cell shows emotional similarity in **{selected_language.upper()}** subtitles. Higher % = more similar emotional profiles (excluding neutral emotion which dominated at 56%). Look for surprising connections!")
    else:
        st.warning(f"⚠️ No emotion data available for {selected_language.upper()}. Try a different language.")
    
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
                radar_fig = plot_emotion_fingerprint_radar(
                    conn, selected_film_ids, top_n_emotions=8, language_code=selected_language
                )
            
            if radar_fig:
                st.plotly_chart(radar_fig, use_container_width=True, config={"responsive": True})
                st.info(f"💡 **How to read**: Each film has a unique 'shape' based on its top emotions in **{selected_language.upper()}** (normalized to 100%, neutral excluded). Larger areas = that emotion dominates. Overlaps = shared characteristics.")
            else:
                st.warning(f"⚠️ Could not create radar chart for {selected_language.upper()}. Try selecting different films or language.")
        else:
            st.info("👆 Select films above to compare their emotional profiles.")

    # Footer
    st.divider()
    st.caption("Data source: DuckDB staging schema (`data/ghibli.duckdb`)")
    st.caption("Dashboard Purpose: Epic 3.5 analytical validation (developer tool)")


if __name__ == "__main__":
    main()

