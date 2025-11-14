"""
Unit tests for validation dashboard (Story 3.5.4: Filtering & Interaction Features).

Tests session state management, filter initialization, and helper functions
for the Streamlit validation dashboard.
"""

import pytest
from unittest.mock import MagicMock, patch
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


@pytest.fixture
def mock_dependencies():
    """Mock all external dependencies for dashboard testing."""
    mock_st = MagicMock()
    mock_st.session_state = {}
    
    mock_duckdb = MagicMock()
    mock_pd = MagicMock()
    
    with patch.dict('sys.modules', {
        'streamlit': mock_st,
        'duckdb': mock_duckdb,
        'pandas': mock_pd,
        'src.shared.config': MagicMock(),
        'src.validation.chart_utils': MagicMock(),
    }):
        yield mock_st


def test_initialize_filter_state(mock_dependencies):
    """Test that filter state initializes with default values."""
    import streamlit as st
    from src.validation.dashboard import initialize_filter_state
    
    # Clear any existing state
    st.session_state.clear()
    
    # Call initialization function
    initialize_filter_state()
    
    # Assert all filter keys exist with correct default values
    assert 'selected_film_id' in st.session_state
    assert st.session_state['selected_film_id'] is None
    
    assert 'time_range_min' in st.session_state
    assert st.session_state['time_range_min'] == 0
    
    assert 'time_range_max' in st.session_state
    assert st.session_state['time_range_max'] is None
    
    assert 'intensity_threshold' in st.session_state
    assert st.session_state['intensity_threshold'] == 0.5
    
    assert 'centrality_top_n' in st.session_state
    assert st.session_state['centrality_top_n'] == 10


def test_initialize_filter_state_preserves_existing_values(mock_dependencies):
    """Test that initialization doesn't override existing session state values."""
    import streamlit as st
    from src.validation.dashboard import initialize_filter_state
    
    # Set some existing values
    st.session_state['selected_film_id'] = 'test-film-id-123'
    st.session_state['time_range_min'] = 10
    st.session_state['intensity_threshold'] = 0.8
    
    # Call initialization
    initialize_filter_state()
    
    # Assert existing values are preserved
    assert st.session_state['selected_film_id'] == 'test-film-id-123'
    assert st.session_state['time_range_min'] == 10
    assert st.session_state['intensity_threshold'] == 0.8
    
    # Assert missing keys are initialized
    assert st.session_state['time_range_max'] is None
    assert st.session_state['centrality_top_n'] == 10


def test_initialize_filter_state_idempotent(mock_dependencies):
    """Test that calling initialize multiple times has no side effects."""
    import streamlit as st
    from src.validation.dashboard import initialize_filter_state
    
    # Initialize once
    initialize_filter_state()
    
    # Store initial values
    initial_values = {
        'selected_film_id': st.session_state['selected_film_id'],
        'time_range_min': st.session_state['time_range_min'],
        'intensity_threshold': st.session_state['intensity_threshold'],
    }
    
    # Initialize again
    initialize_filter_state()
    
    # Assert values unchanged
    assert st.session_state['selected_film_id'] == initial_values['selected_film_id']
    assert st.session_state['time_range_min'] == initial_values['time_range_min']
    assert st.session_state['intensity_threshold'] == initial_values['intensity_threshold']


def test_on_film_change_resets_time_range(mock_dependencies):
    """Test that on_film_change callback resets time range filters."""
    import streamlit as st
    from src.validation.dashboard import on_film_change, initialize_filter_state
    
    # Initialize state
    initialize_filter_state()
    
    # Set some custom time range values
    st.session_state['time_range_min'] = 10
    st.session_state['time_range_max'] = 50
    st.session_state['intensity_threshold'] = 0.8
    st.session_state['selected_film_id'] = 'film-123'
    
    # Call film change callback
    on_film_change()
    
    # Assert time range was reset to defaults
    assert st.session_state['time_range_min'] == 0
    assert st.session_state['time_range_max'] is None
    
    # Assert other filters preserved
    assert st.session_state['intensity_threshold'] == 0.8


def test_on_film_change_preserves_other_filters(mock_dependencies):
    """Test that on_film_change preserves intensity and centrality filters."""
    import streamlit as st
    from src.validation.dashboard import on_film_change, initialize_filter_state
    
    # Initialize state
    initialize_filter_state()
    
    # Set custom filter values
    st.session_state['intensity_threshold'] = 0.7
    st.session_state['centrality_top_n'] = 15
    st.session_state['time_range_min'] = 20
    st.session_state['time_range_max'] = 80
    
    # Store original values
    original_intensity = st.session_state['intensity_threshold']
    original_top_n = st.session_state['centrality_top_n']
    
    # Call film change
    on_film_change()
    
    # Assert intensity and centrality preserved
    assert st.session_state['intensity_threshold'] == original_intensity
    assert st.session_state['centrality_top_n'] == original_top_n
    
    # Assert only time range reset
    assert st.session_state['time_range_min'] == 0
    assert st.session_state['time_range_max'] is None
