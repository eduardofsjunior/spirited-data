# src/app/utils/config.py

"""
Shared configuration for SpiritedData Streamlit application.

IMPORTANT: This app focuses on EMOTION ANALYSIS, NOT RAG.
Do NOT add OpenAI API keys, ChromaDB paths, or RAG configuration.
"""

from pathlib import Path
from typing import Dict, List

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DUCKDB_PATH = DATA_DIR / "ghibli.duckdb"

# App metadata
APP_TITLE = "SpiritedData"
APP_SUBTITLE = "Emotional Landscape of Studio Ghibli"
APP_ICON = "🎬"

# "Spirit World" Theme Palette
# Inspired by Spirited Away night scenes: Deep Indigo, Bioluminescent Cyan, Lantern Gold
THEME = {
    "primary_color": "#38BDF8",        # Spirit Blue/Cyan (Glowing)
    "background_color": "#0F172A",     # Deep Indigo/Midnight (Not black)
    "secondary_bg_color": "#1E293B",   # Slate for cards/sidebar
    "accent_color": "#F59E0B",         # Gold/Lantern
    "text_color": "#F1F5F9",           # Stardust/Off-white
    "font_headers": "'Cinzel', serif", # Cinematic Ghibli feel
    "font_body": "'Inter', sans-serif" # Clean readability
}

# Data statistics (from Epic 2.5 + Epic 3 + Epic 4.X)
DATA_STATS = {
    "film_count": 22,
    "emotion_data_points": 9_873,     # minute-level buckets
    "languages": ["English", "French", "Spanish", "Dutch", "Arabic"],
    "language_codes": ["en", "fr", "es", "nl", "ar"],
    "dialogue_entries": 98_963,
    "emotion_dimensions": 28,
    "subtitle_validation_pass_rate": 0.724,  # 72.4% after Epic 4.X improvements
}

# Emotion mart tables (Epic 2.5 + Epic 5)
EMOTION_MARTS = {
    "director_profile": "main_marts.mart_director_emotion_profile",
    "emotion_peaks_smoothed": "main_marts.mart_emotion_peaks_smoothed",
    "emotion_peaks_raw": "main_marts.mart_emotion_peaks_raw",
    "film_similarity": "main_marts.mart_film_similarity_matrix",
    "cross_language": "main_marts.mart_cross_language_emotion_comparison",
    "kaggle_correlation": "main_marts.mart_kaggle_emotion_correlation",
    "methodology_metrics": "main_marts.mart_emotion_methodology_metrics",
    "film_emotion_summary": "main_marts.mart_film_emotion_summary",
    "film_emotion_timeseries": "main_marts.mart_film_emotion_timeseries",  # Epic 5.2
}

# GoEmotions 28 dimensions
EMOTION_LABELS = [
    "admiration", "amusement", "anger", "annoyance", "approval",
    "caring", "confusion", "curiosity", "desire", "disappointment",
    "disapproval", "disgust", "embarrassment", "excitement", "fear",
    "gratitude", "grief", "joy", "love", "nervousness",
    "optimism", "pride", "realization", "relief", "remorse",
    "sadness", "surprise", "neutral"
]
