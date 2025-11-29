# src/app/utils/theme.py

"""
Custom CSS styling and theme utilities for "Spirit World" design.
Inspired by Spirited Away night scenes: Deep Indigo, Bioluminescent Cyan, Lantern Gold.

[Source: architecture/tech-stack.md - Custom CSS via st.markdown()]
"""

import streamlit as st
from .config import THEME


def apply_custom_css() -> None:
    """
    Apply 'Spirit World' design system to Streamlit app.
    
    Includes:
    - Google Fonts imports (Cinzel, Inter)
    - Glassmorphism card styles
    - Cinematic header typography
    - Immersive background gradients
    """

    custom_css = f"""
    <style>
    /* Import Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Cinzel:wght@400;700&family=Inter:wght@300;400;600&display=swap');

    /* Global Typography */
    html, body, [class*="css"] {{
        font-family: {THEME['font_body']};
        color: {THEME['text_color']};
    }}

    h1, h2, h3, h4, h5, h6 {{
        font-family: {THEME['font_headers']};
        font-weight: 700;
        letter-spacing: 0.05em;
        color: {THEME['text_color']};
    }}

    /* Immersive Background */
    .stApp {{
        background: radial-gradient(circle at 50% 0%, #1E293B 0%, #0F172A 100%);
        background-attachment: fixed;
    }}

    /* Main Header Styling */
    .main-header {{
        font-family: {THEME['font_headers']};
        font-size: 3.5rem;
        font-weight: 700;
        background: linear-gradient(180deg, #FFFFFF 0%, {THEME['primary_color']} 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        padding: 2rem 0 1rem 0;
        margin-bottom: 0.5rem;
        text-shadow: 0 0 30px rgba(56, 189, 248, 0.3);
    }}

    /* Subtitle Styling */
    .subtitle {{
        font-family: {THEME['font_body']};
        font-size: 1.1rem;
        color: {THEME['primary_color']};
        text-align: center;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        margin-bottom: 3rem;
        opacity: 0.8;
    }}

    /* Glassmorphism Card */
    .glass-card {{
        background: rgba(30, 41, 59, 0.7);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border: 1px solid rgba(56, 189, 248, 0.1);
        border-radius: 16px;
        padding: 1.5rem;
        margin: 1rem 0;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.2);
        transition: transform 0.2s ease, border-color 0.2s ease;
    }}

    .glass-card:hover {{
        transform: translateY(-2px);
        border-color: rgba(56, 189, 248, 0.3);
        box-shadow: 0 8px 30px rgba(0, 0, 0, 0.3);
    }}

    /* Stat Value Styling */
    .stat-value {{
        font-family: {THEME['font_headers']};
        font-size: 2.5rem;
        font-weight: 700;
        color: {THEME['accent_color']}; /* Lantern Gold */
        text-shadow: 0 0 15px rgba(245, 158, 11, 0.4);
        margin: 0.5rem 0;
    }}

    .stat-label {{
        font-size: 0.9rem;
        color: #94A3B8;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }}

    /* Sidebar Styling */
    section[data-testid="stSidebar"] {{
        background-color: rgba(15, 23, 42, 0.95);
        border-right: 1px solid rgba(56, 189, 248, 0.1);
    }}

    /* Streamlit UI Overrides */
    div[data-testid="stMetricValue"] {{
        color: {THEME['accent_color']} !important;
    }}
    
    /* Button Styling */
    .stButton > button {{
        background: linear-gradient(135deg, {THEME['primary_color']} 0%, #0284C7 100%);
        color: #FFFFFF;
        border: none;
        border-radius: 8px;
        padding: 0.6rem 1.5rem;
        font-family: {THEME['font_body']};
        font-weight: 600;
        letter-spacing: 0.05em;
        transition: all 0.3s ease;
    }}

    .stButton > button:hover {{
        box-shadow: 0 0 20px rgba(56, 189, 248, 0.5);
        transform: translateY(-1px);
    }}

    /* Divider */
    hr {{
        border-color: rgba(56, 189, 248, 0.2);
    }}
    
    /* Footer */
    .footer {{
        text-align: center;
        padding: 3rem 0;
        color: #64748B;
        font-size: 0.85rem;
        border-top: 1px solid rgba(56, 189, 248, 0.1);
        margin-top: 4rem;
    }}
    </style>
    """

    st.markdown(custom_css, unsafe_allow_html=True)


def render_header(title: str, subtitle: str = "") -> None:
    """
    Render cinematic header with 'Spirit World' typography.

    Args:
        title: Main header text
        subtitle: Optional subtitle text
    """
    st.markdown(f'<div class="main-header">{title}</div>', unsafe_allow_html=True)

    if subtitle:
        st.markdown(f'<div class="subtitle">{subtitle}</div>', unsafe_allow_html=True)


def render_glass_card(title: str, value: str, description: str = "", icon: str = "") -> None:
    """
    Render a glassmorphism stat card.

    Args:
        title: Card title (Label)
        value: Main stat value
        description: Optional description text
        icon: Optional emoji icon
    """
    icon_html = f'<span style="font-size: 1.5rem; margin-right: 0.5rem;">{icon}</span>' if icon else ""
    
    html = f"""
    <div class="glass-card">
        <div class="stat-label">{icon_html}{title}</div>
        <div class="stat-value">{value}</div>
        {f'<div style="color: #94A3B8; font-size: 0.85rem; margin-top: 0.5rem;">{description}</div>' if description else ''}
    </div>
    """

    st.markdown(html, unsafe_allow_html=True)


def render_footer() -> None:
    """Render minimalist footer."""

    footer_html = """
    <div class="footer">
        <p>SpiritedData • Built with Streamlit & dbt</p>
        <p style="margin-top: 0.5rem; opacity: 0.6;">
            Emotion Analysis powered by HuggingFace GoEmotions
        </p>
    </div>
    """

    st.markdown(footer_html, unsafe_allow_html=True)
