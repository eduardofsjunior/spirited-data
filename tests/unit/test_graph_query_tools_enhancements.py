"""
Unit tests for graph query tools enhancements (Story 4.4.1).

Tests cover correlation tool and cross-language sentiment arc comparison.
"""
from unittest.mock import MagicMock, patch

import duckdb
import numpy as np
import pytest
from scipy.stats import pearsonr

from src.ai.graph_query_tools import (
    compare_sentiment_arcs_across_languages,
    correlate_metrics,
    find_character_connections,
)


class TestCorrelateMetrics:
    """Tests for correlate_metrics tool."""

    @patch("src.ai.graph_query_tools._get_duckdb_connection")
    def test_correlate_metrics_sentiment_boxoffice(self, mock_get_conn, mock_duckdb_connection):
        """Test correlation between sentiment and box office."""
        mock_get_conn.return_value = mock_duckdb_connection
        # Mock query result: (film_id, title, x_value, y_value)
        mock_duckdb_connection.execute.return_value.fetchall.return_value = [
            ("film_1", "Spirited Away", 0.15, 395000000.0),
            ("film_2", "Howl's Moving Castle", 0.12, 236000000.0),
            ("film_3", "Princess Mononoke", 0.08, 169000000.0),
        ]

        result = correlate_metrics("sentiment", "box_office")

        assert "answer" in result
        assert "data_sources" in result
        assert "visualization_data" in result
        assert "correlation" in result["answer"].lower()
        assert result["visualization_data"]["chart_type"] == "scatter"

    @patch("src.ai.graph_query_tools._get_duckdb_connection")
    def test_correlate_metrics_rt_score_tmdb_rating(self, mock_get_conn, mock_duckdb_connection):
        """Test correlation between RT score and TMDB rating."""
        mock_get_conn.return_value = mock_duckdb_connection
        mock_duckdb_connection.execute.return_value.fetchall.return_value = [
            ("film_1", "Spirited Away", 97, 8.6),
            ("film_2", "Howl's Moving Castle", 87, 8.2),
            ("film_3", "Princess Mononoke", 93, 8.4),
        ]

        result = correlate_metrics("rt_score", "tmdb_rating")

        assert "answer" in result
        assert "correlation" in result["answer"].lower()

    def test_correlate_metrics_invalid_metric(self):
        """Test invalid metric name."""
        result = correlate_metrics("invalid_metric", "box_office")

        assert "error" in result["answer"].lower()
        assert "supported" in result["answer"].lower()

    @patch("src.ai.graph_query_tools._get_duckdb_connection")
    def test_correlate_metrics_insufficient_data(self, mock_get_conn, mock_duckdb_connection):
        """Test with insufficient data (< 2 films)."""
        mock_get_conn.return_value = mock_duckdb_connection
        mock_duckdb_connection.execute.return_value.fetchall.return_value = [
            ("film_1", "Spirited Away", 0.15, 395000000.0),
        ]

        result = correlate_metrics("sentiment", "box_office")

        assert "insufficient" in result["answer"].lower() or "need at least" in result["answer"].lower()

    @patch("src.ai.graph_query_tools._get_duckdb_connection")
    def test_correlate_metrics_no_data(self, mock_get_conn, mock_duckdb_connection):
        """Test when no data is found."""
        mock_get_conn.return_value = mock_duckdb_connection
        mock_duckdb_connection.execute.return_value.fetchall.return_value = []

        result = correlate_metrics("sentiment", "box_office")

        assert "no data" in result["answer"].lower()

    def test_correlate_metrics_same_metric(self):
        """Test error when both metrics are the same."""
        result = correlate_metrics("sentiment", "sentiment")

        assert "error" in result["answer"].lower() or "different" in result["answer"].lower()


class TestCompareSentimentArcsAcrossLanguages:
    """Tests for compare_sentiment_arcs_across_languages tool."""

    @patch("src.ai.graph_query_tools._get_duckdb_connection")
    @patch("src.ai.graph_query_tools._fuzzy_match_film_title")
    def test_compare_arcs_happy_path(self, mock_fuzzy_match, mock_get_conn, mock_duckdb_connection):
        """Test comparing arcs across multiple languages."""
        mock_get_conn.return_value = mock_duckdb_connection
        mock_fuzzy_match.return_value = "Spirited Away"

        # Mock emotion data: (language_code, minute_offset, emotion_score)
        mock_duckdb_connection.execute.return_value.fetchall.return_value = [
            ("en", 0, 0.5),
            ("en", 1, 0.6),
            ("en", 2, 0.7),
            ("fr", 0, 0.4),
            ("fr", 1, 0.5),
            ("fr", 2, 0.6),
        ]

        result = compare_sentiment_arcs_across_languages("Spirited Away", ["en", "fr"])

        assert "answer" in result
        assert "data_sources" in result
        assert "visualization_data" in result
        assert result["visualization_data"]["chart_type"] == "line"
        assert "comparing" in result["answer"].lower() or "spirited away" in result["answer"].lower()

    @patch("src.ai.graph_query_tools._get_duckdb_connection")
    @patch("src.ai.graph_query_tools._fuzzy_match_film_title")
    def test_compare_arcs_film_not_found(self, mock_fuzzy_match, mock_get_conn, mock_duckdb_connection):
        """Test when film is not found."""
        mock_get_conn.return_value = mock_duckdb_connection
        mock_fuzzy_match.return_value = None

        result = compare_sentiment_arcs_across_languages("NonExistent Film", ["en", "fr"])

        assert "not found" in result["answer"].lower()

    def test_compare_arcs_invalid_language(self):
        """Test invalid language code."""
        result = compare_sentiment_arcs_across_languages("Spirited Away", ["en", "invalid"])

        assert "error" in result["answer"].lower()
        assert "invalid" in result["answer"].lower()

    def test_compare_arcs_invalid_emotion_dimension(self):
        """Test invalid emotion dimension."""
        result = compare_sentiment_arcs_across_languages("Spirited Away", ["en", "fr"], emotion_dimension="invalid")

        assert "error" in result["answer"].lower()
        assert "emotion_dimension" in result["answer"].lower() or "supported" in result["answer"].lower()

    def test_compare_arcs_insufficient_languages(self):
        """Test with less than 2 languages."""
        result = compare_sentiment_arcs_across_languages("Spirited Away", ["en"])

        assert "error" in result["answer"].lower()
        assert "at least 2" in result["answer"].lower()

    @patch("src.ai.graph_query_tools._get_duckdb_connection")
    @patch("src.ai.graph_query_tools._fuzzy_match_film_title")
    def test_compare_arcs_insufficient_data(self, mock_fuzzy_match, mock_get_conn, mock_duckdb_connection):
        """Test with insufficient language data."""
        mock_get_conn.return_value = mock_duckdb_connection
        mock_fuzzy_match.return_value = "Spirited Away"
        mock_duckdb_connection.execute.return_value.fetchall.return_value = [
            ("en", 0, 0.5),
        ]

        result = compare_sentiment_arcs_across_languages("Spirited Away", ["en", "fr"])

        assert "insufficient" in result["answer"].lower() or "at least 2" in result["answer"].lower()

    @patch("src.ai.graph_query_tools._get_duckdb_connection")
    @patch("src.ai.graph_query_tools._fuzzy_match_film_title")
    def test_compare_arcs_specific_emotion(self, mock_fuzzy_match, mock_get_conn, mock_duckdb_connection):
        """Test comparing specific emotion dimension."""
        mock_get_conn.return_value = mock_duckdb_connection
        mock_fuzzy_match.return_value = "Spirited Away"
        mock_duckdb_connection.execute.return_value.fetchall.return_value = [
            ("en", 0, 0.8),
            ("fr", 0, 0.7),
        ]

        result = compare_sentiment_arcs_across_languages("Spirited Away", ["en", "fr"], emotion_dimension="joy")

        assert "answer" in result
        assert "joy" in result["visualization_data"]["ylabel"].lower() or "joy" in result["answer"].lower()


class TestToolResponseFormat:
    """Tests for ToolResponse format standardization."""

    @patch("src.ai.graph_query_tools._get_duckdb_connection")
    def test_tool_response_has_required_fields(self, mock_get_conn, mock_duckdb_connection):
        """Test that all tools return standardized ToolResponse format."""
        mock_get_conn.return_value = mock_duckdb_connection
        mock_duckdb_connection.execute.return_value.fetchone.return_value = None

        result = find_character_connections("Test")

        # Check required fields
        assert "answer" in result
        assert "data_sources" in result
        assert isinstance(result["data_sources"], dict)
        assert "tables" in result["data_sources"]
        assert "functions" in result["data_sources"]
        assert "timestamp" in result["data_sources"]

    @patch("src.ai.graph_query_tools._get_duckdb_connection")
    def test_data_source_citation_structure(self, mock_get_conn, mock_duckdb_connection):
        """Test DataSourceCitation structure."""
        mock_get_conn.return_value = mock_duckdb_connection
        mock_duckdb_connection.execute.return_value.fetchone.return_value = None

        result = find_character_connections("Test")

        data_sources = result["data_sources"]
        assert isinstance(data_sources["tables"], list)
        assert isinstance(data_sources["functions"], list)
        assert isinstance(data_sources["timestamp"], str)
        # Check ISO 8601 format (contains T or has date format)
        assert "T" in data_sources["timestamp"] or len(data_sources["timestamp"]) >= 10

