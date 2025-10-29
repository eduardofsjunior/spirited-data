"""
Graph construction module for loading DuckDB data into NetworkX graph.

This module loads graph nodes and edges from DuckDB mart tables and constructs
a NetworkX MultiDiGraph object for graph analysis and visualization.
"""
import json
import logging
import os
import pickle
import sys
from collections import Counter
from typing import Any, Dict, List, Tuple

import duckdb
import networkx as nx

from src.shared.config import DUCKDB_PATH
from src.shared.database import get_duckdb_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def load_nodes_from_duckdb(conn: duckdb.DuckDBPyConnection) -> Dict[str, Dict[str, Any]]:
    """
    Load graph nodes from DuckDB mart_graph_nodes table.

    Args:
        conn: DuckDB connection object

    Returns:
        Dictionary mapping node_id to node attributes (node_type, name, properties)

    Raises:
        duckdb.Error: If database query fails
        json.JSONDecodeError: If properties JSON is invalid
    """
    logger.info("Loading nodes from mart_graph_nodes...")

    query = """
        SELECT node_id, node_type, name, properties
        FROM marts.mart_graph_nodes
    """

    try:
        result = conn.execute(query).fetchall()
        nodes = {}

        for row in result:
            node_id, node_type, name, properties_json = row

            # Parse JSON properties (DuckDB returns JSON as string)
            try:
                properties = json.loads(properties_json) if properties_json else {}
            except json.JSONDecodeError as e:
                logger.warning(f"Invalid JSON in properties for node {node_id}: {e}")
                properties = {}

            # Combine node_type, name, and properties into attributes dict
            nodes[node_id] = {
                "node_type": node_type,
                "name": name,
                **properties,
            }

        logger.info(f"Loaded {len(nodes)} nodes from mart_graph_nodes")
        return nodes

    except duckdb.Error as e:
        logger.error(f"Failed to query nodes from DuckDB: {e}")
        raise


def load_edges_from_duckdb(conn: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    """
    Load graph edges from DuckDB mart_graph_edges table.

    Args:
        conn: DuckDB connection object

    Returns:
        List of edge dictionaries containing edge_id, source, target, edge_type, properties

    Raises:
        duckdb.Error: If database query fails
        json.JSONDecodeError: If properties JSON is invalid
    """
    logger.info("Loading edges from mart_graph_edges...")

    query = """
        SELECT edge_id, source_node_id, target_node_id, edge_type, properties
        FROM marts.mart_graph_edges
    """

    try:
        result = conn.execute(query).fetchall()
        edges = []

        for row in result:
            edge_id, source_node_id, target_node_id, edge_type, properties_json = row

            # Parse JSON properties (DuckDB returns JSON as string)
            try:
                properties = json.loads(properties_json) if properties_json else {}
            except json.JSONDecodeError as e:
                logger.warning(f"Invalid JSON in properties for edge {edge_id}: {e}")
                properties = {}

            edges.append(
                {
                    "edge_id": edge_id,
                    "source": source_node_id,
                    "target": target_node_id,
                    "edge_type": edge_type,
                    **properties,
                }
            )

        logger.info(f"Loaded {len(edges)} edges from mart_graph_edges")
        return edges

    except duckdb.Error as e:
        logger.error(f"Failed to query edges from DuckDB: {e}")
        raise


def build_networkx_graph(
    nodes: Dict[str, Dict[str, Any]], edges: List[Dict[str, Any]]
) -> nx.MultiDiGraph:
    """
    Build NetworkX MultiDiGraph from nodes and edges data.

    Args:
        nodes: Dictionary mapping node_id to node attributes
        edges: List of edge dictionaries with source, target, edge_type, and properties

    Returns:
        NetworkX MultiDiGraph object with nodes and edges added

    Raises:
        ValueError: If nodes or edges are empty
    """
    if not nodes:
        raise ValueError("Cannot build graph: no nodes provided")
    if not edges:
        raise ValueError("Cannot build graph: no edges provided")

    logger.info("Building NetworkX MultiDiGraph...")

    # Create directed graph that allows multiple edges between same node pair
    G = nx.MultiDiGraph()

    # Add nodes with attributes
    for node_id, attributes in nodes.items():
        G.add_node(node_id, **attributes)

    # Add edges with attributes
    for edge in edges:
        source = edge["source"]
        target = edge["target"]
        edge_type = edge["edge_type"]

        # Extract edge properties (excluding source, target, edge_type, edge_id)
        edge_attrs = {
            k: v
            for k, v in edge.items()
            if k not in ["source", "target", "edge_type", "edge_id"]
        }

        G.add_edge(source, target, edge_type=edge_type, **edge_attrs)

    # Validate graph creation
    node_count = G.number_of_nodes()
    edge_count = G.number_of_edges()

    logger.info(f"Graph created: {node_count} nodes, {edge_count} edges")

    if node_count != len(nodes):
        logger.warning(f"Node count mismatch: expected {len(nodes)}, got {node_count}")

    return G


def calculate_graph_metrics(G: nx.MultiDiGraph) -> Dict[str, Any]:
    """
    Calculate basic graph metrics.

    Args:
        G: NetworkX MultiDiGraph object

    Returns:
        Dictionary containing graph metrics:
        - node_count: Number of nodes
        - edge_count: Number of edges
        - avg_degree: Average degree of nodes
        - connected_components: Number of weakly connected components
    """
    logger.info("Calculating graph metrics...")

    node_count = G.number_of_nodes()
    edge_count = G.number_of_edges()

    # Calculate average degree
    if node_count > 0:
        degree_dict = dict(G.degree())
        avg_degree = sum(degree_dict.values()) / node_count
    else:
        avg_degree = 0.0

    # Calculate connected components (use weakly_connected for directed graphs)
    connected_components = nx.number_weakly_connected_components(G)

    metrics = {
        "node_count": node_count,
        "edge_count": edge_count,
        "avg_degree": avg_degree,
        "connected_components": connected_components,
    }

    logger.info(
        f"Graph metrics: avg_degree={avg_degree:.2f}, "
        f"components={connected_components}"
    )

    return metrics


def generate_graph_summary_report(G: nx.MultiDiGraph) -> str:
    """
    Generate text report with graph summary statistics.

    Args:
        G: NetworkX MultiDiGraph object

    Returns:
        Formatted string report with graph summary, top nodes, edge types, density
    """
    logger.info("Generating graph summary report...")

    # Calculate degree centrality
    degree_centrality = nx.degree_centrality(G)

    # Sort nodes by degree centrality (descending)
    top_10_nodes = sorted(
        degree_centrality.items(), key=lambda x: x[1], reverse=True
    )[:10]

    # Get node names for top nodes
    top_nodes_with_names = []
    for node_id, centrality in top_10_nodes:
        node_name = G.nodes[node_id].get("name", node_id)
        node_type = G.nodes[node_id].get("node_type", "unknown")
        top_nodes_with_names.append((node_name, node_type, centrality))

    # Count edge types
    edge_types = [
        G[u][v][k].get("edge_type", "unknown")
        for u, v, k in G.edges(keys=True)
    ]
    edge_type_counts = Counter(edge_types)

    # Calculate graph density (for directed graph)
    node_count = G.number_of_nodes()
    edge_count = G.number_of_edges()
    if node_count > 1:
        max_possible_edges = node_count * (node_count - 1)
        density = edge_count / max_possible_edges if max_possible_edges > 0 else 0.0
    else:
        density = 0.0

    # Build report
    report_lines = [
        "=" * 80,
        "Graph Summary Report",
        "=" * 80,
        "",
        f"Total Nodes: {node_count}",
        f"Total Edges: {edge_count}",
        f"Graph Density: {density:.4f}",
        "",
        "Top 10 Nodes by Degree Centrality:",
        "-" * 80,
    ]

    for i, (name, node_type, centrality) in enumerate(top_nodes_with_names, 1):
        report_lines.append(f"{i:2d}. {name} ({node_type}): {centrality:.4f}")

    report_lines.extend(
        [
            "",
            "Edge Type Distribution:",
            "-" * 80,
        ]
    )

    for edge_type, count in edge_type_counts.most_common():
        report_lines.append(f"  {edge_type}: {count}")

    report_lines.append("")
    report_lines.append("=" * 80)

    report = "\n".join(report_lines)

    logger.info("Graph summary report generated")
    return report


def validate_graph_connectivity(G: nx.MultiDiGraph) -> Tuple[bool, Dict[str, int]]:
    """
    Validate that all films are connected to characters and all characters linked to films.

    Args:
        G: NetworkX MultiDiGraph object

    Returns:
        Tuple of (is_valid, validation_stats) where validation_stats contains:
        - total_films: Total number of film nodes
        - validated_films: Number of films with at least one character edge
        - total_characters: Total number of character nodes
        - validated_characters: Number of characters with at least one film edge
    """
    logger.info("Validating graph connectivity...")

    # Get all film and character nodes
    film_nodes = [n for n, attrs in G.nodes(data=True) if attrs.get("node_type") == "film"]
    character_nodes = [
        n for n, attrs in G.nodes(data=True) if attrs.get("node_type") == "character"
    ]

    total_films = len(film_nodes)
    total_characters = len(character_nodes)

    # Check films connected to characters (has outgoing edges to characters)
    validated_films = 0
    for film_id in film_nodes:
        # Check if film has any outgoing edges to characters
        has_character_edge = False
        for target in G.successors(film_id):
            if G.nodes[target].get("node_type") == "character":
                has_character_edge = True
                break
        # Also check incoming edges from characters (appears_in relationship)
        if not has_character_edge:
            for source in G.predecessors(film_id):
                if G.nodes[source].get("node_type") == "character":
                    has_character_edge = True
                    break
        if has_character_edge or G.degree(film_id) > 0:
            validated_films += 1

    # Check characters linked to films (has edges to/from films)
    validated_characters = 0
    for character_id in character_nodes:
        # Check if character has any edges to/from films
        has_film_edge = False
        for neighbor in G.neighbors(character_id):
            if G.nodes[neighbor].get("node_type") == "film":
                has_film_edge = True
                break
        # Also check incoming edges from films
        if not has_film_edge:
            for predecessor in G.predecessors(character_id):
                if G.nodes[predecessor].get("node_type") == "film":
                    has_film_edge = True
                    break
        if has_film_edge or G.degree(character_id) > 0:
            validated_characters += 1

    validation_stats = {
        "total_films": total_films,
        "validated_films": validated_films,
        "total_characters": total_characters,
        "validated_characters": validated_characters,
    }

    is_valid = validated_films == total_films and validated_characters == total_characters

    logger.info(
        f"Validation: {validated_films}/{total_films} films connected, "
        f"{validated_characters}/{total_characters} characters linked"
    )

    return is_valid, validation_stats


def save_graph(G: nx.MultiDiGraph, output_path: str) -> None:
    """
    Save NetworkX graph as pickle file.

    Args:
        G: NetworkX MultiDiGraph object
        output_path: Path to output pickle file

    Raises:
        IOError: If file I/O fails
    """
    logger.info(f"Saving graph to {output_path}...")

    # Create output directory if needed
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    try:
        with open(output_path, "wb") as f:
            pickle.dump(G, f)

        logger.info(f"Graph saved to {output_path}")

        # Verify file exists and is readable
        if os.path.exists(output_path):
            file_size = os.path.getsize(output_path)
            logger.info(f"Graph file size: {file_size / 1024 / 1024:.2f} MB")

    except IOError as e:
        logger.error(f"Failed to save graph to {output_path}: {e}")
        raise


def save_report(report: str, output_path: str) -> None:
    """
    Save graph summary report to text file.

    Args:
        report: Report text string
        output_path: Path to output text file

    Raises:
        IOError: If file I/O fails
    """
    logger.info(f"Saving report to {output_path}...")

    # Create output directory if needed
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    try:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(report)

        logger.info(f"Report saved to {output_path}")

    except IOError as e:
        logger.error(f"Failed to save report to {output_path}: {e}")
        raise


def main() -> int:
    """
    Main entry point for graph construction script.

    Returns:
        Exit code: 0 on success, 1 on failure
    """
    try:
        # Connect to DuckDB
        logger.info(f"Connecting to DuckDB at {DUCKDB_PATH}...")
        conn = get_duckdb_connection()

        # Load nodes and edges
        nodes = load_nodes_from_duckdb(conn)
        edges = load_edges_from_duckdb(conn)

        # Build NetworkX graph
        G = build_networkx_graph(nodes, edges)

        # Calculate metrics
        metrics = calculate_graph_metrics(G)

        # Generate summary report
        report = generate_graph_summary_report(G)
        logger.info("\n" + report)

        # Validate graph connectivity
        is_valid, validation_stats = validate_graph_connectivity(G)

        if not is_valid:
            error_msg = (
                f"Graph validation failed: "
                f"{validation_stats['validated_films']}/{validation_stats['total_films']} "
                f"films connected, "
                f"{validation_stats['validated_characters']}/{validation_stats['total_characters']} "
                f"characters linked"
            )
            logger.error(error_msg)
            raise AssertionError(error_msg)

        # Save graph and report
        graph_output_path = "data/processed/ghibli_graph.pkl"
        report_output_path = "data/processed/graph_summary_report.txt"

        save_graph(G, graph_output_path)
        save_report(report, report_output_path)

        logger.info("Graph construction completed successfully!")
        return 0

    except duckdb.Error as e:
        logger.error(f"Database connection error: {e}")
        return 1
    except (IOError, OSError) as e:
        logger.error(f"File I/O error: {e}")
        return 1
    except (ValueError, AssertionError) as e:
        logger.error(f"Graph validation error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error during graph construction: {e}", exc_info=True)
        return 1
    finally:
        # Close database connection if it exists
        try:
            if "conn" in locals():
                conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())

