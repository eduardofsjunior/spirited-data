"""
Integration tests for RAG CLI with real RAG pipeline.

These tests require:
- OpenAI API key in environment
- DuckDB database at data/ghibli.duckdb
- ChromaDB vector store at data/vectors

WARNING: These tests make real API calls and may incur costs (~$0.05 per run).
"""

import json
import os
import tempfile
from typing import Any, Dict

import pytest

from src.ai.rag_cli import ConversationSession, process_query, save_conversation_history
from src.ai.rag_pipeline import initialize_rag_pipeline


# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def rag_pipeline() -> Dict[str, Any]:
    """
    Initialize real RAG pipeline for integration testing.

    Skips tests if required environment or data not available.
    """
    # Check for required environment variables
    if not os.getenv("OPENAI_API_KEY"):
        pytest.skip("OPENAI_API_KEY not set - skipping integration tests")

    # Check for required data files
    duckdb_path = os.getenv("DUCKDB_PATH", "data/ghibli.duckdb")
    chromadb_path = os.getenv("CHROMADB_PATH", "data/vectors")

    if not os.path.exists(duckdb_path):
        pytest.skip(f"DuckDB not found at {duckdb_path} - skipping integration tests")

    if not os.path.exists(chromadb_path):
        pytest.skip(f"ChromaDB not found at {chromadb_path} - skipping integration tests")

    # Initialize pipeline
    try:
        pipeline = initialize_rag_pipeline()
        return pipeline
    except Exception as e:
        pytest.skip(f"Failed to initialize RAG pipeline: {e}")


def test_end_to_end_cli_query(rag_pipeline: Dict[str, Any], capsys: Any) -> None:
    """
    Test end-to-end CLI query with real RAG pipeline.

    Verifies:
    - Query processing completes successfully
    - Response contains expected content
    - Session history is updated
    - Tokens and cost are tracked
    """
    session = ConversationSession()

    # Process a simple query
    result = process_query(
        query="Who are the most central characters?",
        session=session,
        debug=False,
        streaming=False,
    )

    # Verify response structure
    assert "answer" in result
    assert isinstance(result["answer"], str)
    assert len(result["answer"]) > 0

    # Verify answer contains character-related content
    # (Flexible check - exact response depends on RAG system)
    answer_lower = result["answer"].lower()
    assert any(
        keyword in answer_lower
        for keyword in ["character", "central", "chihiro", "pazu", "analysis"]
    )

    # Verify session updated
    assert session.total_queries == 1
    assert len(session.history) == 2
    assert session.history[0]["role"] == "user"
    assert session.history[1]["role"] == "assistant"

    # Verify tokens tracked
    assert session.total_tokens > 0

    # Verify cost tracked
    assert session.total_cost > 0

    # Verify response time tracked
    assert len(session.response_times) == 1
    assert session.response_times[0] > 0


def test_debug_mode_output(rag_pipeline: Dict[str, Any], capsys: Any) -> None:
    """
    Test CLI query with debug mode enabled.

    Verifies debug output includes:
    - Retrieved documents
    - Function calls
    - Token usage
    - Response time
    """
    session = ConversationSession()

    # Process query with debug mode
    process_query(
        query="Calculate character centrality for top 5 characters",
        session=session,
        debug=True,
        streaming=False,
    )

    # Capture output
    captured = capsys.readouterr()

    # Verify debug output present
    assert "[DEBUG]" in captured.out
    assert "Retrieved Documents:" in captured.out or "Function Calls:" in captured.out
    assert "Token Usage:" in captured.out
    assert "Response Time:" in captured.out

    # Verify function calls shown (this query should trigger centrality calculation)
    # Note: Exact function calls depend on agent behavior
    if "Function Calls:" in captured.out:
        # At least one function call should be shown
        assert (
            "calculate_character_centrality" in captured.out
            or "query_graph_database" in captured.out
        )


def test_multiple_queries_session_stats(rag_pipeline: Dict[str, Any]) -> None:
    """
    Test session statistics after multiple queries.

    Verifies:
    - Query counter increments
    - Token counts accumulate
    - Costs accumulate
    - Average response time calculated
    """
    session = ConversationSession()

    # Process multiple queries
    queries = [
        "Who are the most central characters?",
        "What films did Miyazaki direct?",
        "Show sentiment for Spirited Away",
    ]

    for query in queries:
        process_query(query, session, debug=False, streaming=False)

    # Verify stats
    stats = session.get_stats()

    assert stats["total_queries"] == 3
    assert stats["total_tokens"] > 0
    assert stats["total_cost"] > 0
    assert stats["average_response_time"] > 0

    # Verify session tracking
    assert session.total_queries == 3
    assert len(session.history) == 6  # 3 user + 3 assistant messages
    assert len(session.response_times) == 3


def test_conversation_history_saving(rag_pipeline: Dict[str, Any]) -> None:
    """
    Test saving conversation history to file.

    Verifies:
    - File created with correct format
    - JSON structure is valid
    - Metadata is accurate
    - History is preserved
    """
    session = ConversationSession()

    # Process some queries
    process_query("Who directed Spirited Away?", session, streaming=False)
    process_query("What's the plot?", session, streaming=False)

    # Save conversation
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = save_conversation_history(session, save_dir=tmpdir)

        # Verify file exists
        assert os.path.exists(filepath)

        # Load and verify JSON
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Verify structure
        assert "metadata" in data
        assert "history" in data
        assert "statistics" in data

        # Verify metadata
        assert data["metadata"]["total_queries"] == 2
        assert data["metadata"]["total_tokens"] > 0
        assert data["metadata"]["total_cost"] > 0

        # Verify history
        assert len(data["history"]) == 4  # 2 user + 2 assistant
        assert data["history"][0]["role"] == "user"
        assert data["history"][0]["content"] == "Who directed Spirited Away?"


def test_chat_history_context(rag_pipeline: Dict[str, Any]) -> None:
    """
    Test that conversation context is maintained across queries.

    Verifies:
    - Follow-up questions use previous context
    - Session history passed to RAG pipeline
    """
    session = ConversationSession()

    # First query establishes context
    result1 = process_query("Tell me about Spirited Away", session, streaming=False)

    # Follow-up query should use context
    result2 = process_query("Who directed it?", session, streaming=False)

    # Verify both queries succeeded
    assert result1["answer"]
    assert result2["answer"]

    # Verify history maintained
    assert len(session.history) == 4  # 2 user + 2 assistant

    # Second response should reference Miyazaki or director
    # (This is a flexible check as exact response depends on RAG system)
    answer2_lower = result2["answer"].lower()
    assert any(
        keyword in answer2_lower for keyword in ["miyazaki", "director", "hayao"]
    )


def test_error_handling_invalid_query(rag_pipeline: Dict[str, Any]) -> None:
    """
    Test error handling for invalid queries.

    Verifies:
    - Empty queries raise ValueError
    - Too long queries raise ValueError
    """
    session = ConversationSession()

    # Test empty query
    with pytest.raises(ValueError, match="Query cannot be empty"):
        process_query("", session)

    # Test too long query
    long_query = "a" * 1001
    with pytest.raises(ValueError, match="Query too long"):
        process_query(long_query, session)


@pytest.mark.slow
def test_streaming_output(rag_pipeline: Dict[str, Any], capsys: Any) -> None:
    """
    Test streaming output effect.

    Note: This test is marked as slow because streaming adds delay.
    """
    session = ConversationSession()

    # Process query with streaming enabled
    process_query(
        query="What is the film count?",
        session=session,
        debug=False,
        streaming=True,  # Enable streaming
    )

    # Verify output exists (streaming just changes display, not content)
    captured = capsys.readouterr()
    assert len(captured.out) > 0


def test_special_characters_in_query(rag_pipeline: Dict[str, Any]) -> None:
    """
    Test queries with special characters and unicode.

    Verifies:
    - Queries with Japanese characters work
    - Queries with punctuation work
    - Results are returned successfully
    """
    session = ConversationSession()

    # Query with Japanese and punctuation
    result = process_query(
        query="What is もののけ姫 (Princess Mononoke) about?",
        session=session,
        streaming=False,
    )

    # Verify query processed successfully
    assert result["answer"]
    assert len(result["answer"]) > 0


# Performance benchmarks (optional, slow tests)


@pytest.mark.slow
@pytest.mark.benchmark
def test_query_performance_benchmark(rag_pipeline: Dict[str, Any]) -> None:
    """
    Benchmark query performance.

    Verifies queries complete in reasonable time (< 10 seconds).
    """
    session = ConversationSession()

    result = process_query(
        query="Who are the main characters?",
        session=session,
        streaming=False,
    )

    # Verify response time is reasonable
    assert result["response_time"] < 10.0  # Should complete in < 10 seconds


@pytest.mark.slow
@pytest.mark.benchmark
def test_cost_estimation(rag_pipeline: Dict[str, Any]) -> None:
    """
    Estimate cost per query for budgeting.

    Verifies cost tracking works and logs estimated costs.
    """
    session = ConversationSession()

    # Process a typical query
    result = process_query(
        query="What films did Miyazaki direct?",
        session=session,
        streaming=False,
    )

    # Log cost for reference
    print(f"\nEstimated cost per query: ${result.get('cost', 0.0):.4f}")
    print(f"Tokens used: {result.get('tokens_used', {}).get('total_tokens', 0)}")

    # Verify cost is tracked and reasonable
    assert result.get("cost", 0) > 0
    assert result.get("cost", 0) < 0.10  # Should be < $0.10 per query
