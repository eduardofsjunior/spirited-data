"""
Unit tests for centrality chart utilities.

Tests centrality ranking chart generation, metric selection, filtering,
and error handling for validation dashboard.
"""

from unittest.mock import MagicMock, patch

import networkx as nx
import plotly.graph_objects as go
import pytest

from src.validation.chart_utils import (
    get_character_metadata,
    load_or_build_graph,
    plot_centrality_ranking,
)


class TestPlotCentralityRanking:
    """Test plot_centrality_ranking function."""

    @pytest.fixture
    def mock_graph(self) -> nx.MultiDiGraph:
        """Create mock NetworkX graph with character nodes."""
        G = nx.MultiDiGraph()
        
        # Add character nodes
        G.add_node("char1", node_type="character", name="Character 1")
        G.add_node("char2", node_type="character", name="Character 2")
        G.add_node("char3", node_type="character", name="Character 3")
        
        # Add film nodes
        G.add_node("film_test-film-id", node_type="film", name="Film 1")
        G.add_node("film2", node_type="film", name="Film 2")
        
        # Add edges (character appears in film)
        G.add_edge("char1", "film_test-film-id", edge_type="appears_in")
        G.add_edge("char2", "film_test-film-id", edge_type="appears_in")
        G.add_edge("char3", "film_test-film-id", edge_type="appears_in")
        G.add_edge("char1", "film2", edge_type="appears_in")
        
        return G

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create mock DuckDB connection."""
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [
            ("char1", 2),  # char1 appears in 2 films
            ("char2", 1),  # char2 appears in 1 film
            ("char3", 1),  # char3 appears in 1 film
        ]
        return conn

    def test_plot_centrality_ranking_degree(
        self, mock_graph: nx.MultiDiGraph, mock_conn: MagicMock
    ) -> None:
        """Test degree centrality calculation and chart generation."""
        with patch(
            "src.validation.chart_utils.load_or_build_graph", return_value=mock_graph
        ):
            fig = plot_centrality_ranking(
                mock_conn, "degree", film_id="test-film-id", top_n=10
            )

        assert fig is not None
        assert isinstance(fig, go.Figure)
        assert len(fig.data) > 0  # Has bar trace

    def test_plot_centrality_ranking_betweenness(
        self, mock_graph: nx.MultiDiGraph, mock_conn: MagicMock
    ) -> None:
        """Test betweenness centrality calculation."""
        with patch(
            "src.validation.chart_utils.load_or_build_graph", return_value=mock_graph
        ):
            fig = plot_centrality_ranking(
                mock_conn, "betweenness", film_id="test-film-id", top_n=10
            )

        assert fig is not None
        assert isinstance(fig, go.Figure)

    def test_plot_centrality_ranking_closeness(
        self, mock_graph: nx.MultiDiGraph, mock_conn: MagicMock
    ) -> None:
        """Test closeness centrality calculation."""
        with patch(
            "src.validation.chart_utils.load_or_build_graph", return_value=mock_graph
        ):
            fig = plot_centrality_ranking(
                mock_conn, "closeness", film_id="test-film-id", top_n=10
            )

        assert fig is not None
        assert isinstance(fig, go.Figure)

    def test_plot_centrality_ranking_metric_name_mapping(
        self, mock_graph: nx.MultiDiGraph, mock_conn: MagicMock
    ) -> None:
        """Test metric name mapping (full names to keys)."""
        with patch(
            "src.validation.chart_utils.load_or_build_graph", return_value=mock_graph
        ):
            # Test full metric names
            fig1 = plot_centrality_ranking(
                mock_conn, "Degree Centrality", film_id="test-film-id", top_n=10
            )
            fig2 = plot_centrality_ranking(
                mock_conn, "Betweenness Centrality", film_id="test-film-id", top_n=10
            )
            fig3 = plot_centrality_ranking(
                mock_conn, "Closeness Centrality", film_id="test-film-id", top_n=10
            )

        assert fig1 is not None
        assert fig2 is not None
        assert fig3 is not None

    def test_plot_centrality_ranking_top_n_filtering(
        self, mock_graph: nx.MultiDiGraph, mock_conn: MagicMock
    ) -> None:
        """Test top_n filtering (top 5, top 10, top 20)."""
        with patch(
            "src.validation.chart_utils.load_or_build_graph", return_value=mock_graph
        ):
            fig_top5 = plot_centrality_ranking(
                mock_conn, "degree", film_id="test-film-id", top_n=5
            )
            fig_top10 = plot_centrality_ranking(
                mock_conn, "degree", film_id="test-film-id", top_n=10
            )
            fig_top20 = plot_centrality_ranking(
                mock_conn, "degree", film_id="test-film-id", top_n=20
            )

        assert fig_top5 is not None
        assert fig_top10 is not None
        assert fig_top20 is not None

    def test_plot_centrality_ranking_invalid_metric(
        self, mock_graph: nx.MultiDiGraph, mock_conn: MagicMock
    ) -> None:
        """Test error handling for invalid metric name."""
        with patch(
            "src.validation.chart_utils.load_or_build_graph", return_value=mock_graph
        ):
            fig = plot_centrality_ranking(
                mock_conn, "invalid_metric", film_id="test-film-id", top_n=10
            )

        assert fig is None  # Should return None on error

    def test_plot_centrality_ranking_empty_graph(
        self, mock_conn: MagicMock
    ) -> None:
        """Test error handling for empty graph."""
        empty_graph = nx.MultiDiGraph()
        
        with patch(
            "src.validation.chart_utils.load_or_build_graph", return_value=empty_graph
        ):
            fig = plot_centrality_ranking(
                mock_conn, "degree", film_id="test-film-id", top_n=10
            )

        assert fig is None  # Should return None when film not found

    def test_plot_centrality_ranking_no_character_nodes(
        self, mock_conn: MagicMock
    ) -> None:
        """Test handling when film has no character nodes."""
        G = nx.MultiDiGraph()
        G.add_node("film_test-film-id", node_type="film", name="Film 1")
        
        with patch(
            "src.validation.chart_utils.load_or_build_graph", return_value=G
        ):
            fig = plot_centrality_ranking(
                mock_conn, "degree", film_id="test-film-id", top_n=10
            )

        assert fig is None  # Should return None when no character nodes in film

    def test_plot_centrality_ranking_chart_properties(
        self, mock_graph: nx.MultiDiGraph, mock_conn: MagicMock
    ) -> None:
        """Test bar chart properties (title, axis labels, colorscale)."""
        with patch(
            "src.validation.chart_utils.load_or_build_graph", return_value=mock_graph
        ):
            fig = plot_centrality_ranking(
                mock_conn, "degree", film_id="test-film-id", top_n=10
            )

        assert fig is not None
        assert fig.layout.title is not None
        assert "Degree" in fig.layout.title.text
        assert "Film 1" in fig.layout.title.text  # Film title should be in chart title
        assert fig.layout.xaxis.title.text is not None
        assert fig.layout.yaxis.title.text is not None

    def test_plot_centrality_ranking_methodology_note(
        self, mock_graph: nx.MultiDiGraph, mock_conn: MagicMock
    ) -> None:
        """Test methodology note text for each metric."""
        with patch(
            "src.validation.chart_utils.load_or_build_graph", return_value=mock_graph
        ):
            fig_degree = plot_centrality_ranking(
                mock_conn, "degree", film_id="test-film-id", top_n=10
            )
            fig_betweenness = plot_centrality_ranking(
                mock_conn, "betweenness", film_id="test-film-id", top_n=10
            )
            fig_closeness = plot_centrality_ranking(
                mock_conn, "closeness", film_id="test-film-id", top_n=10
            )

        assert fig_degree is not None
        assert fig_betweenness is not None
        assert fig_closeness is not None
        
        # Check that annotations exist (methodology notes)
        assert len(fig_degree.layout.annotations) > 0


class TestGetCharacterMetadata:
    """Test get_character_metadata function."""

    @pytest.fixture
    def mock_graph(self) -> nx.MultiDiGraph:
        """Create mock NetworkX graph."""
        G = nx.MultiDiGraph()
        G.add_node("char1", node_type="character", name="Character 1")
        G.add_node("char2", node_type="character", name="Character 2")
        G.add_edge("char1", "film1", edge_type="appears_in")
        G.add_edge("char1", "film2", edge_type="appears_in")
        return G

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create mock DuckDB connection."""
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [
            ("char1", 2),  # char1 appears in 2 films
            ("char2", 1),  # char2 appears in 1 film
        ]
        return conn

    def test_get_character_metadata_success(
        self, mock_conn: MagicMock, mock_graph: nx.MultiDiGraph
    ) -> None:
        """Test successful metadata retrieval."""
        metadata = get_character_metadata(mock_conn, ["char1", "char2"], mock_graph)

        assert "char1" in metadata
        assert "char2" in metadata
        assert metadata["char1"]["film_count"] == 2
        assert metadata["char2"]["film_count"] == 1

    def test_get_character_metadata_empty_list(
        self, mock_conn: MagicMock, mock_graph: nx.MultiDiGraph
    ) -> None:
        """Test handling of empty character list."""
        metadata = get_character_metadata(mock_conn, [], mock_graph)
        assert metadata == {}

    def test_get_character_metadata_fallback_on_error(
        self, mock_conn: MagicMock, mock_graph: nx.MultiDiGraph
    ) -> None:
        """Test fallback to graph degree when DuckDB query fails."""
        # Make DuckDB query raise an error
        import duckdb
        mock_conn.execute.side_effect = duckdb.Error("Query failed")

        metadata = get_character_metadata(mock_conn, ["char1"], mock_graph)

        # Should fallback to graph degree only
        assert "char1" in metadata
        assert metadata["char1"]["film_count"] == 0  # Default when query fails
        assert "degree" in metadata["char1"]


class TestLoadOrBuildGraph:
    """Test load_or_build_graph function."""

    @pytest.fixture
    def mock_graph(self) -> nx.MultiDiGraph:
        """Create mock NetworkX graph."""
        G = nx.MultiDiGraph()
        G.add_node("char1", node_type="character", name="Character 1")
        return G

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create mock DuckDB connection."""
        return MagicMock()

    def test_load_or_build_graph_from_pickle(
        self, mock_conn: MagicMock, mock_graph: nx.MultiDiGraph
    ) -> None:
        """Test loading graph from pickle file."""
        import pickle
        from pathlib import Path

        # Create temporary pickle file
        pickle_path = Path("tests/fixtures/test_graph.pkl")
        pickle_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(pickle_path, "wb") as f:
            pickle.dump(mock_graph, f)

        try:
            with patch("src.validation.chart_utils.Path", return_value=pickle_path):
                G = load_or_build_graph(mock_conn)
            
            assert G is not None
            assert isinstance(G, nx.MultiDiGraph)
        finally:
            # Cleanup
            if pickle_path.exists():
                pickle_path.unlink()

    def test_load_or_build_graph_from_duckdb_fallback(
        self, mock_conn: MagicMock, mock_graph: nx.MultiDiGraph
    ) -> None:
        """Test fallback to DuckDB when pickle not available."""
        from pathlib import Path

        # Mock pickle path to not exist
        mock_pickle_path = Path("nonexistent.pkl")
        
        # Mock graph building functions with valid edges
        with patch(
            "src.validation.chart_utils.Path", return_value=mock_pickle_path
        ), patch(
            "src.validation.chart_utils.load_nodes_from_duckdb",
            return_value={
                "char1": {"node_type": "character", "name": "Character 1"},
                "film1": {"node_type": "film", "name": "Film 1"},
            },
        ), patch(
            "src.validation.chart_utils.load_edges_from_duckdb",
            return_value=[
                {
                    "edge_id": "edge1",
                    "source": "char1",
                    "target": "film1",
                    "edge_type": "appears_in",
                }
            ],
        ), patch(
            "src.validation.chart_utils.build_networkx_graph",
            return_value=mock_graph,
        ):
            G = load_or_build_graph(mock_conn)

        assert G is not None
        assert isinstance(G, nx.MultiDiGraph)

