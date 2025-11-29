"""
Methodology Page

Understand signal processing and data quality.

[Source: prd/epic-details.md - Epic 5: Story 5.6]
"""

import streamlit as st

# Add parent directory to path for utils imports
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.theme import apply_custom_css, render_header

st.set_page_config(page_title="Methodology", page_icon="📊", layout="wide")
apply_custom_css()

render_header("📊 Methodology", "Understand signal processing and data quality")

st.info("**Story 5.6**: Methodology Page (Coming soon)")
st.markdown(
    "This page will display methodology transparency and data quality metrics."
)
