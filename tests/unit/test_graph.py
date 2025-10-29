"""
Unit tests for graph construction module.

Tests cover node/edge loading from DuckDB, NetworkX graph construction,
metrics calculation, and graph validation.
"""
import json
import pickle
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import networkx as nx
import pytest

from src.graph.build_graph import (
    build_networkx_graph,
    calculate_graph_metrics,
    generate_graph_summary_report,
    load_edges_from_duckdb,
    load_nodes_from_duckdb,
    save_graph,
    validate_graph_connectivity,
)


@pytest.fixture
def mock_duckdb_connection():
    """Mock DuckDB connection with sample query results."""
    conn = MagicMock()

    # Mock nodes query result
    nodes_result = [
        ("film_123", "film", "Test Film", '{"release_year": 2000}'),
        ("character_456", "character", "Test Character", '{"gender": "Male"}'),
        ("location_789", "location", "Test Location", '{"climate": "Temperate"}'),
    ]
    conn.execute.return_value.fetchall.return_value = nodes_result

    return conn


@pytest.fixture
def mock_duckdb_connection_with_edges():
    """Mock DuckDB connection with nodes and edges."""
    conn = MagicMock()

    # Mock nodes query result
    nodes_result = [
        ("film_123", "film", "Test Film", '{"release_year": 2000}'),
        ("character_456", "character", "Test Character", '{"gender": "Male"}'),
    ]
    # Mock edges query result
    edges_result = [
        (
            "edge_1",
            "character_456",
            "film_123",
            "appears_in",
            '{"relationship_strength": 1.0}',
        ),
    ]

    def execute_side_effect(query):
        mock_result = MagicMock()
        if "mart_graph_nodes" in query:
            mock_result.fetchall.return_value = nodes_result
        elif "mart_graph_edges" in query:
            mock_result.fetchall.return_value = edges_result
        return mock_result

    conn.execute.side_effect = execute_side_effect
    return conn


@pytest.fixture
def sample_nodes():
    """Sample nodes dictionary for testing."""
    return {
        "film_123": {
            "node_type": "film",
            "name": "Test Film",
            "release_year": 2000,
        },
        "character_456": {
            "node_type": "character",
            "name": "Test Character",
            "gender": "Male",
        },
        "location_789": {
            "node_type": "location",
            "name": "Test Location",
            "climate": "Temperate",
        },
    }


@pytest.fixture
def sample_edges():
    """Sample edges list for testing."""
    return [
        {
            "edge_id": "edge_1",
            "source": "character_456",
            "target": "film_123",
            "edge_type": "appears_in",
            "relationship_strength": 1.0,
        },
        {
            "edge_id": "edge_2",
            "source": "film_123",
            "target": "location_789",
            "edge_type": "filmed_at",
            "relationship_strength": 1.0,
        },
    ]


@pytest.fixture
def sample_graph(sample_nodes, sample_edges):
    """Sample NetworkX graph for testing."""
    G = nx.MultiDiGraph()
    for node_id, attrs in sample_nodes.items():
        G.add_node(node_id, **attrs)
    for edge in sample_edges:
        G.add_edge(
            edge["source"],
            edge["target"],
            edge_type=edge["edge_type"],
            relationship_strength=edge.get("relationship_strength", 1.0),
        )
    return G


class TestLoadNodesFromDuckDB:
    """Test node loading from DuckDB."""

    def test_load_nodes_success(self, mock_duckdb_connection):
        """Test successful node loading."""
        nodes = load_nodes_from_duckdb(mock_duckdb_connection)

        assert len(nodes) == 3
        assert "film_123" in nodes
        assert nodes["film_123"]["node_type"] == "film"
        assert nodes["film_123"]["name"] == "Test Film"
        assert nodes["film_123"]["release_year"] == 2000

    def test_load_nodes_with_invalid_json(self, mock_duckdb_connection):
        """Test node loading with invalid JSON in properties."""
        # Override mock to return invalid JSON
        invalid_json_result = [
            ("film_123", "film", "Test Film", "invalid json {"),
        ]
        mock_duckdb_connection.execute.return_value.fetchall.return_value = (
            invalid_json_result
        )

        nodes = load_nodes_from_duckdb(mock_duckdb_connection)

        assert len(nodes) == 1
        assert nodes["film_123"]["node_type"] == "film"
        # Properties should be empty dict if JSON parsing fails
        assert "release_year" not in nodes["film_123"]

    def test_load_nodes_empty_properties(self, mock_duckdb_connection):
        """Test node loading with empty/null properties."""
        empty_props_result = [
            ("film_123", "film", "Test Film", None),
        ]
        mock_duckdb_connection.execute.return_value.fetchall.return_value = (
            empty_props_result
        )

        nodes = load_nodes_from_duckdb(mock_duckdb_connection)

        assert len(nodes) == 1
        assert nodes["film_123"]["node_type"] == "film"
        assert nodes["film_123"]["name"] == "Test Film"

    def test_load_nodes_database_error(self, mock_duckdb_connection):
        """Test node loading with database error."""
        import duckdb

        mock_duckdb_connection.execute.side_effect = duckdb.Error("Database error")

        with pytest.raises(duckdb.Error):
            load_nodes_from_duckdb(mock_duckdb_connection)


class TestLoadEdgesFromDuckDB:
    """Test edge loading from DuckDB."""

    def test_load_edges_success(self, mock_duckdb_connection_with_edges):
        """Test successful edge loading."""
        edges = load_edges_from_duckdb(mock_duckdb_connection_with_edges)

        assert len(edges) == 1
        assert edges[0]["edge_id"] == "edge_1"
        assert edges[0]["source"] == "character_456"
        assert edges[0]["target"] == "film_123"
        assert edges[0]["edge_type"] == "appears_in"
        assert edges[0]["relationship_strength"] == 1.0

    def test_load_edges_with_invalid_json(self):
        """Test edge loading with invalid JSON in properties."""
        conn = MagicMock()
        invalid_json_result = [
            (
                "edge_1",
                "character_456",
                "film_123",
                "appears_in",
                "invalid json {",
            ),
        ]
        conn.execute.return_value.fetchall.return_value = invalid_json_result

        edges = load_edges_from_duckdb(conn)

        assert len(edges) == 1
        assert edges[0]["edge_type"] == "appears_in"
        # Properties should not include relationship_strength if JSON parsing fails
        assert "relationship_strength" not in edges[0]

    def test_load_edges_database_error(self):
        """Test edge loading with database error."""
        import duckdb

        conn = MagicMock()
        conn.execute.side_effect = duckdb.Error("Database error")

        with pytest.raises(duckdb.Error):
            load_edges_from_duckdb(conn)


class TestBuildNetworkXGraph:
    """Test NetworkX graph construction."""

    def test_build_graph_success(self, sample_nodes, sample_edges):
        """Test successful graph construction."""
        G = build_networkx_graph(sample_nodes, sample_edges)

        assert isinstance(G, nx.MultiDiGraph)
        assert G.number_of_nodes() == 3
        assert G.number_of_edges() == 2

        # Check node attributes
        assert G.nodes["film_123"]["node_type"] == "film"
        assert G.nodes["film_123"]["name"] == "Test Film"

        # Check edge attributes
        edges_data = list(G.edges(data=True, keys=True))
        assert len(edges_data) == 2

    def test_build_graph_empty_nodes(self, sample_edges):
        """Test graph construction with empty nodes."""
        with pytest.raises(ValueError, match="no nodes"):
            build_networkx_graph({}, sample_edges)

    def test_build_graph_empty_edges(self, sample_nodes):
        """Test graph construction with empty edges."""
        with pytest.raises(ValueError, match="no edges"):
            build_networkx_graph(sample_nodes, [])

    def test_build_graph_node_attributes(self, sample_nodes, sample_edges):
        """Test that node attributes are correctly added."""
        G = build_networkx_graph(sample_nodes, sample_edges)

        assert G.nodes["film_123"]["release_year"] == 2000
        assert G.nodes["character_456"]["gender"] == "Male"
        assert G.nodes["location_789"]["climate"] == "Temperate"

    def test_build_graph_edge_attributes(self, sample_nodes, sample_edges):
        """Test that edge attributes are correctly added."""
        G = build_networkx_graph(sample_nodes, sample_edges)

        # Get edges (format: (u, v, key, data))
        edges = list(G.edges(data=True, keys=True))
        appears_in_edge = [
            e for e in edges if e[3].get("edge_type") == "appears_in"
        ][0]

        assert appears_in_edge[3]["edge_type"] == "appears_in"
        assert appears_in_edge[3]["relationship_strength"] == 1.0


class TestCalculateGraphMetrics:
    """Test graph metrics calculation."""

    def test_calculate_metrics_success(self, sample_graph):
        """Test successful metrics calculation."""
        metrics = calculate_graph_metrics(sample_graph)

        assert metrics["node_count"] == 3
        assert metrics["edge_count"] == 2
        assert metrics["avg_degree"] > 0
        assert metrics["connected_components"] == 1

    def test_calculate_metrics_empty_graph(self):
        """Test metrics calculation with empty graph."""
        G = nx.MultiDiGraph()
        metrics = calculate_graph_metrics(G)

        assert metrics["node_count"] == 0
        assert metrics["edge_count"] == 0
        assert metrics["avg_degree"] == 0.0
        assert metrics["connected_components"] == 0

    def test_calculate_metrics_single_node(self):
        """Test metrics calculation with single node."""
        G = nx.MultiDiGraph()
        G.add_node("node_1", node_type="test", name="Test")
        metrics = calculate_graph_metrics(G)

        assert metrics["node_count"] == 1
        assert metrics["edge_count"] == 0
        assert metrics["avg_degree"] == 0.0


class TestGenerateGraphSummaryReport:
    """Test graph summary report generation."""

    def test_generate_report_success(self, sample_graph):
        """Test successful report generation."""
        report = generate_graph_summary_report(sample_graph)

        assert "Graph Summary Report" in report
        assert "Total Nodes: 3" in report
        assert "Total Edges: 2" in report
        assert "Top 10 Nodes by Degree Centrality" in report
        assert "Edge Type Distribution" in report

    def test_generate_report_includes_node_names(self, sample_graph):
        """Test that report includes node names."""
        report = generate_graph_summary_report(sample_graph)

        assert "Test Film" in report or "Test Character" in report

    def test_generate_report_edge_types(self, sample_graph):
        """Test that report includes edge type distribution."""
        report = generate_graph_summary_report(sample_graph)

        assert "appears_in" in report or "filmed_at" in report


class TestValidateGraphConnectivity:
    """Test graph connectivity validation."""

    def test_validate_connectivity_success(self):
        """Test successful connectivity validation."""
        G = nx.MultiDiGraph()

        # Add film and character nodes
        G.add_node("film_1", node_type="film", name="Film 1")
        G.add_node("character_1", node_type="character", name="Character 1")

        # Add edge connecting character to film
        G.add_edge("character_1", "film_1", edge_type="appears_in")

        is_valid, stats = validate_graph_connectivity(G)

        assert is_valid
        assert stats["total_films"] == 1
        assert stats["validated_films"] == 1
        assert stats["total_characters"] == 1
        assert stats["validated_characters"] == 1

    def test_validate_connectivity_failure(self):
        """Test connectivity validation failure."""
        G = nx.MultiDiGraph()

        # Add film and character nodes but no edges
        G.add_node("film_1", node_type="film", name="Film 1")
        G.add_node("character_1", node_type="character", name="Character 1")

        is_valid, stats = validate_graph_connectivity(G)

        assert not is_valid
        assert stats["validated_films"] == 0
        assert stats["validated_characters"] == 0

    def test_validate_connectivity_no_films(self):
        """Test validation with no film nodes."""
        G = nx.MultiDiGraph()
        G.add_node("character_1", node_type="character", name="Character 1")

        is_valid, stats = validate_graph_connectivity(G)

        assert stats["total_films"] == 0
        assert stats["validated_films"] == 0

    def test_validate_connectivity_complex_graph(self):
        """Test validation with complex graph structure."""
        G = nx.MultiDiGraph()

        # Add multiple films and characters
        G.add_node("film_1", node_type="film", name="Film 1")
        G.add_node("film_2", node_type="film", name="Film 2")
        G.add_node("character_1", node_type="character", name="Character 1")
        G.add_node("character_2", node_type="character", name="Character 2")

        # Connect characters to films
        G.add_edge("character_1", "film_1", edge_type="appears_in")
        G.add_edge("character_2", "film_1", edge_type="appears_in")
        G.add_edge("character_2", "film_2", edge_type="appears_in")

        is_valid, stats = validate_graph_connectivity(G)

        assert is_valid
        assert stats["total_films"] == 2
        assert stats["validated_films"] == 2
        assert stats["total_characters"] == 2
        assert stats["validated_characters"] == 2


class TestSaveGraph:
    """Test graph saving functionality."""

    def test_save_graph_success(self, sample_graph, tmp_path):
        """Test successful graph saving."""
        output_path = tmp_path / "test_graph.pkl"

        save_graph(sample_graph, str(output_path))

        assert output_path.exists()

        # Verify graph can be loaded
        with open(output_path, "rb") as f:
            loaded_graph = pickle.load(f)

        assert isinstance(loaded_graph, nx.MultiDiGraph)
        assert loaded_graph.number_of_nodes() == sample_graph.number_of_nodes()
        assert loaded_graph.number_of_edges() == sample_graph.number_of_edges()

    def test_save_graph_creates_directory(self, sample_graph, tmp_path):
        """Test that save_graph creates output directory if needed."""
        output_dir = tmp_path / "subdir"
        output_path = output_dir / "test_graph.pkl"

        save_graph(sample_graph, str(output_path))

        assert output_dir.exists()
        assert output_path.exists()

    def test_save_graph_io_error(self, sample_graph):
        """Test save_graph with invalid path (IO error)."""
        invalid_path = "/nonexistent/directory/graph.pkl"

        with pytest.raises(IOError):
            save_graph(sample_graph, invalid_path)

