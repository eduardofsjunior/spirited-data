"""
Director Profiles Page

Compare signature emotion styles (Miyazaki vs Takahata).

[Source: prd/epic-details.md - Epic 5: Story 5.4]
"""

import streamlit as st

# Add parent directory to path for utils imports
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.theme import apply_custom_css, render_header

st.set_page_config(page_title="Director Profiles", page_icon="🎭", layout="wide")
apply_custom_css()

render_header(
    "🎭 Director Profiles", "Compare signature emotion styles (Miyazaki vs Takahata)"
)

st.info("**Story 5.4**: Director Profiles Page (Coming soon)")
st.markdown(
    "This page will display director emotion profiles and cross-film comparisons."
)
