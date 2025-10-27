"""Shared pytest fixtures for all tests."""
import pytest


@pytest.fixture
def sample_film_data():
    """Sample film data for testing."""
    return {
        "id": "test-123",
        "title": "Test Film",
        "director": "Test Director",
        "release_date": "2000"
    }
