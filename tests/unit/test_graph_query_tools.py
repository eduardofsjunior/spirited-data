"""
Unit tests for graph query tools module.

Tests cover all LangChain tools: character connections, film sentiment,
database queries, centrality calculations, and film filtering.
"""
import json
from unittest.mock import MagicMock, patch

import duckdb
import networkx as nx
import pytest

from src.ai.graph_query_tools import (
    calculate_character_centrality,
    find_character_connections,
    find_films_by_criteria,
    get_film_sentiment,
    query_graph_database,
)


@pytest.fixture
def mock_duckdb_connection():
    """Mock DuckDB connection for testing."""
    conn = MagicMock(spec=duckdb.DuckDBPyConnection)
    conn.description = None
    return conn


@pytest.fixture
def mock_networkx_graph():
    """Mock NetworkX graph for testing."""
    G = nx.MultiDiGraph()
    G.add_node("char_1", node_type="character", name="Chihiro")
    G.add_node("char_2", node_type="character", name="Haku")
    G.add_node("film_1", node_type="film", name="Spirited Away")
    G.add_edge("char_1", "film_1", edge_type="appears_in")
    G.add_edge("char_2", "film_1", edge_type="appears_in")
    return G


class TestFindCharacterConnections:
    """Tests for find_character_connections tool."""

    @patch("src.ai.graph_query_tools._get_duckdb_connection")
    def test_find_character_connections_happy_path(self, mock_get_conn, mock_duckdb_connection):
        """Test finding character connections with valid character."""
        # Mock character node query
        mock_get_conn.return_value = mock_duckdb_connection
        mock_duckdb_connection.execute.return_value.fetchone.return_value = ("char_1", "Chihiro")

        # Mock films query
        mock_duckdb_connection.execute.return_value.fetchall.side_effect = [
            [("Spirited Away", "film_1")],  # Films
            [("Haku", "char_2")],  # Co-characters
            [],  # Species
        ]

        result = find_character_connections("Chihiro")

        assert "answer" in result
        assert "data_sources" in result
        assert "Chihiro" in result["answer"]
        assert "Spirited Away" in result["answer"]
        assert result["data_sources"]["row_count"] == 1

    @patch("src.ai.graph_query_tools._get_duckdb_connection")
    def test_find_character_connections_not_found(self, mock_get_conn, mock_duckdb_connection):
        """Test character not found case."""
        mock_get_conn.return_value = mock_duckdb_connection
        mock_duckdb_connection.execute.return_value.fetchone.return_value = None

        result = find_character_connections("NonExistent")

        assert "answer" in result
        assert "not found" in result["answer"].lower()
        assert result["data_sources"]["row_count"] == 0

    def test_find_character_connections_validation_error(self):
        """Test input validation."""
        result = find_character_connections("")

        assert "error" in result["answer"].lower() or "must be" in result["answer"].lower()


class TestGetFilmSentiment:
    """Tests for get_film_sentiment tool."""

    @patch("src.ai.graph_query_tools._get_duckdb_connection")
    @patch("src.ai.graph_query_tools._fuzzy_match_film_title")
    def test_get_film_sentiment_happy_path(self, mock_fuzzy_match, mock_get_conn, mock_duckdb_connection):
        """Test getting film sentiment with valid data."""
        mock_get_conn.return_value = mock_duckdb_connection
        mock_fuzzy_match.return_value = "Spirited Away"

        # Mock emotion data (minute_offset, dialogue_count, 28 emotion columns)
        emotion_row = (
            0,  # minute_offset
            2,  # dialogue_count
            # 28 emotion columns (simplified - just a few)
            0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.8, 0.7, 0.1, 0.1, 0.1,  # positive emotions
            0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,  # negative emotions
            0.0, 0.0, 0.0, 0.0, 0.0, 0.3,  # neutral emotions
        )
        mock_duckdb_connection.execute.return_value.fetchall.return_value = [emotion_row]

        result = get_film_sentiment("Spirited Away")

        assert "answer" in result
        assert "data_sources" in result
        assert "visualization_data" in result
        assert result["visualization_data"]["chart_type"] == "line"
        assert "Spirited Away" in result["answer"]

    @patch("src.ai.graph_query_tools._get_duckdb_connection")
    @patch("src.ai.graph_query_tools._fuzzy_match_film_title")
    def test_get_film_sentiment_not_found(self, mock_fuzzy_match, mock_get_conn, mock_duckdb_connection):
        """Test film not found case."""
        mock_get_conn.return_value = mock_duckdb_connection
        mock_fuzzy_match.return_value = None

        result = get_film_sentiment("NonExistent Film")

        assert "not found" in result["answer"].lower()


class TestQueryGraphDatabase:
    """Tests for query_graph_database tool."""

    @patch("src.ai.graph_query_tools._get_duckdb_connection")
    def test_query_graph_database_valid_select(self, mock_get_conn, mock_duckdb_connection):
        """Test valid SELECT query execution."""
        mock_get_conn.return_value = mock_duckdb_connection
        mock_duckdb_connection.description = [("name",), ("node_type",)]
        mock_duckdb_connection.execute.return_value.fetchall.return_value = [
            ("Chihiro", "character"),
            ("Haku", "character"),
        ]

        result = query_graph_database("SELECT name, node_type FROM main_marts.mart_graph_nodes LIMIT 2")

        assert "answer" in result
        assert "data_sources" in result
        assert "2 row" in result["answer"].lower()

    @patch("src.ai.graph_query_tools._get_duckdb_connection")
    def test_query_graph_database_rejects_drop(self, mock_get_conn):
        """Test that DROP queries are rejected."""
        result = query_graph_database("DROP TABLE test")

        assert "error" in result["answer"].lower() or "dangerous" in result["answer"].lower()

    @patch("src.ai.graph_query_tools._get_duckdb_connection")
    def test_query_graph_database_rejects_update(self, mock_get_conn):
        """Test that UPDATE queries are rejected."""
        result = query_graph_database("UPDATE films SET title = 'test'")

        assert "error" in result["answer"].lower() or "dangerous" in result["answer"].lower()

    def test_query_graph_database_validation_error(self):
        """Test input validation."""
        result = query_graph_database("")

        assert "error" in result["answer"].lower()


class TestCalculateCharacterCentrality:
    """Tests for calculate_character_centrality tool."""

    @patch("src.ai.graph_query_tools._get_duckdb_connection")
    @patch("src.ai.graph_query_tools._load_networkx_graph")
    def test_calculate_character_centrality_happy_path(
        self, mock_load_graph, mock_get_conn, mock_duckdb_connection, mock_networkx_graph
    ):
        """Test calculating centrality with valid graph."""
        mock_get_conn.return_value = mock_duckdb_connection
        mock_load_graph.return_value = mock_networkx_graph

        result = calculate_character_centrality(5)

        assert "answer" in result
        assert "data_sources" in result
        assert "visualization_data" in result
        assert result["visualization_data"]["chart_type"] == "bar"
        assert "centrality" in result["answer"].lower()

    @patch("src.ai.graph_query_tools._get_duckdb_connection")
    @patch("src.ai.graph_query_tools._load_networkx_graph")
    def test_calculate_character_centrality_empty_graph(self, mock_load_graph, mock_get_conn, mock_duckdb_connection):
        """Test with empty graph."""
        mock_get_conn.return_value = mock_duckdb_connection
        mock_load_graph.return_value = nx.MultiDiGraph()

        result = calculate_character_centrality(5)

        assert "empty" in result["answer"].lower() or "no" in result["answer"].lower()

    def test_calculate_character_centrality_validation_error(self):
        """Test input validation."""
        result = calculate_character_centrality(0)

        assert "error" in result["answer"].lower()

        result = calculate_character_centrality(25)

        assert "error" in result["answer"].lower()


class TestFindFilmsByCriteria:
    """Tests for find_films_by_criteria tool."""

    @patch("src.ai.graph_query_tools._get_duckdb_connection")
    def test_find_films_by_director(self, mock_get_conn, mock_duckdb_connection):
        """Test filtering films by director."""
        mock_get_conn.return_value = mock_duckdb_connection
        mock_duckdb_connection.execute.return_value.fetchall.return_value = [
            ("film_1", "Spirited Away", 2001, "Hayao Miyazaki", 97),
        ]

        result = find_films_by_criteria(director="Hayao Miyazaki")

        assert "answer" in result
        assert "data_sources" in result
        assert "Spirited Away" in result["answer"]

    @patch("src.ai.graph_query_tools._get_duckdb_connection")
    def test_find_films_by_min_year(self, mock_get_conn, mock_duckdb_connection):
        """Test filtering films by minimum year."""
        mock_get_conn.return_value = mock_duckdb_connection
        mock_duckdb_connection.execute.return_value.fetchall.return_value = [
            ("film_1", "Spirited Away", 2001, "Hayao Miyazaki", 97),
        ]

        result = find_films_by_criteria(min_year=2000)

        assert "answer" in result
        assert "Spirited Away" in result["answer"]

    @patch("src.ai.graph_query_tools._get_duckdb_connection")
    def test_find_films_no_results(self, mock_get_conn, mock_duckdb_connection):
        """Test when no films match criteria."""
        mock_get_conn.return_value = mock_duckdb_connection
        mock_duckdb_connection.execute.return_value.fetchall.return_value = []

        result = find_films_by_criteria(director="NonExistent Director")

        assert "no films found" in result["answer"].lower() or "not found" in result["answer"].lower()

    def test_find_films_validation_error(self):
        """Test input validation."""
        result = find_films_by_criteria()

        assert "error" in result["answer"].lower() or "at least one" in result["answer"].lower()

        result = find_films_by_criteria(min_rating=150)

        assert "error" in result["answer"].lower()








