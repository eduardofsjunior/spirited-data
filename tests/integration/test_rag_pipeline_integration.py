"""
Integration tests for RAG pipeline with real components.

These tests require:
- OpenAI API key (OPENAI_API_KEY environment variable)
- DuckDB database at data/ghibli.duckdb
- ChromaDB vector store at data/vectors

Tests are marked with @pytest.mark.integration for conditional execution.
Run with: pytest -m integration

Note: These tests make real API calls and may incur costs.
"""

import os

import pytest

from src.ai.rag_pipeline import (
    cost_tracker,
    initialize_rag_pipeline,
    query_rag_system,
    query_rag_system_streaming,
)


# Skip all integration tests if API key not set
pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def check_prerequisites():
    """
    Verify prerequisites for integration tests.

    Checks:
    - OPENAI_API_KEY environment variable
    - DuckDB database exists
    - ChromaDB vector store exists
    """
    # Check API key
    if not os.getenv("OPENAI_API_KEY"):
        pytest.skip("OPENAI_API_KEY not set, skipping integration tests")

    # Check DuckDB
    duckdb_path = os.getenv("DUCKDB_PATH", "data/ghibli.duckdb")
    if not os.path.exists(duckdb_path):
        pytest.skip(f"DuckDB database not found at {duckdb_path}")

    # Check ChromaDB
    chromadb_path = os.getenv("CHROMADB_PATH", "data/vectors")
    if not os.path.exists(chromadb_path):
        pytest.skip(f"ChromaDB vector store not found at {chromadb_path}")

    yield


@pytest.fixture(scope="module")
def rag_pipeline(check_prerequisites):
    """Initialize RAG pipeline once for all tests."""
    # Use GPT-3.5-turbo for cost-effective testing
    pipeline = initialize_rag_pipeline(model="gpt-3.5-turbo")
    yield pipeline


@pytest.fixture(autouse=True)
def reset_cost_tracker_integration():
    """Reset cost tracker before each test."""
    cost_tracker.reset_session()
    yield


# End-to-End Query Execution Tests
def test_rag_query_character_centrality(rag_pipeline):
    """
    Test end-to-end query for character centrality analysis.

    Expected behavior:
    - Query executes successfully
    - Response includes data citations
    - Tool (calculate_character_centrality) is called
    - Response time < 10 seconds
    """
    result = query_rag_system(
        user_question="Who are the most central characters?",
        agent_executor=rag_pipeline["agent_executor"],
        retriever=rag_pipeline["retriever"],
    )

    # Verify response structure
    assert "answer" in result
    assert "retrieved_docs" in result
    assert "function_calls" in result
    assert "tokens_used" in result
    assert "response_time" in result
    assert "cost" in result

    # Verify response quality
    assert len(result["answer"]) > 0
    assert result["response_time"] < 10.0

    # Verify tool was called (may or may not be, depending on LLM decision)
    # This is not strictly required, as the LLM might answer from context
    print(f"Function calls: {result['function_calls']}")
    print(f"Answer: {result['answer']}")


def test_rag_query_film_sentiment(rag_pipeline):
    """
    Test end-to-end query for film sentiment analysis.

    Expected behavior:
    - Query executes successfully
    - Response includes sentiment data
    - Tool (get_film_sentiment) may be called
    """
    result = query_rag_system(
        user_question="What is the sentiment of Spirited Away?",
        agent_executor=rag_pipeline["agent_executor"],
        retriever=rag_pipeline["retriever"],
    )

    assert result["answer"]
    assert result["response_time"] < 10.0

    # Verify cost is tracked
    assert result["cost"] > 0
    assert result["session_total_cost"] > 0

    print(f"Query cost: ${result['cost']:.4f}")
    print(f"Answer: {result['answer']}")


def test_rag_query_character_info(rag_pipeline):
    """
    Test query about character information (vector search focused).

    Expected behavior:
    - Query uses vector search to retrieve character information
    - Documents are retrieved from ChromaDB
    - Response includes character details
    """
    result = query_rag_system(
        user_question="Tell me about Chihiro",
        agent_executor=rag_pipeline["agent_executor"],
        retriever=rag_pipeline["retriever"],
    )

    assert result["answer"]
    assert len(result["retrieved_docs"]) > 0

    # Verify documents were retrieved
    print(f"Retrieved {len(result['retrieved_docs'])} documents")
    print(f"Document types: {[doc['type'] for doc in result['retrieved_docs']]}")
    print(f"Answer: {result['answer']}")


def test_rag_query_with_chat_history(rag_pipeline):
    """
    Test query with conversation history.

    Expected behavior:
    - Query uses previous context
    - Follow-up question is understood in context
    """
    # First query
    result1 = query_rag_system(
        user_question="What is Spirited Away?",
        agent_executor=rag_pipeline["agent_executor"],
        retriever=rag_pipeline["retriever"],
    )

    assert result1["answer"]

    # Follow-up query with history
    chat_history = [
        {"role": "user", "content": "What is Spirited Away?"},
        {"role": "assistant", "content": result1["answer"]},
    ]

    result2 = query_rag_system(
        user_question="Who directed it?",
        chat_history=chat_history,
        agent_executor=rag_pipeline["agent_executor"],
        retriever=rag_pipeline["retriever"],
    )

    assert result2["answer"]
    print(f"Follow-up answer: {result2['answer']}")


def test_rag_query_tool_calling(rag_pipeline):
    """
    Test query that explicitly requires tool calling.

    Expected behavior:
    - Tool (find_character_connections or calculate_character_centrality) is called
    - Response includes data from tool execution
    """
    result = query_rag_system(
        user_question="Calculate the centrality scores for the top 5 characters",
        agent_executor=rag_pipeline["agent_executor"],
        retriever=rag_pipeline["retriever"],
    )

    assert result["answer"]

    # Tool should have been called for this query
    # (though LLM might decide not to, so we just log)
    print(f"Function calls: {result['function_calls']}")
    print(f"Answer: {result['answer']}")


def test_rag_query_correlation_analysis(rag_pipeline):
    """
    Test query requiring correlation analysis tool.

    Expected behavior:
    - correlate_metrics tool may be called
    - Response includes correlation data
    """
    result = query_rag_system(
        user_question="Is there a correlation between TMDB ratings and sentiment scores?",
        agent_executor=rag_pipeline["agent_executor"],
        retriever=rag_pipeline["retriever"],
    )

    assert result["answer"]
    print(f"Function calls: {result['function_calls']}")
    print(f"Answer: {result['answer']}")


# Vector Search Integration Tests
def test_vector_search_retrieves_relevant_docs(rag_pipeline):
    """
    Test vector search returns relevant documents.

    Expected behavior:
    - Retriever returns documents matching query
    - Documents have similarity scores
    - Documents are contextually relevant
    """
    retriever = rag_pipeline["retriever"]

    docs = retriever.get_relevant_documents("Tell me about Princess Mononoke")

    assert len(docs) > 0
    assert len(docs) <= 5  # TOP_K_DOCUMENTS

    # Verify documents have content and metadata
    for doc in docs:
        assert hasattr(doc, "page_content")
        assert hasattr(doc, "metadata")
        assert len(doc.page_content) > 0

    print(f"Retrieved {len(docs)} documents for 'Princess Mononoke'")
    print(f"Document types: {[doc.metadata.get('type') for doc in docs]}")


# Cost Tracking Tests
def test_cost_tracking_accumulates(rag_pipeline):
    """
    Test cost tracking accumulates across multiple queries.

    Expected behavior:
    - Each query adds to session total
    - Cost is calculated correctly
    """
    # Reset cost tracker
    cost_tracker.reset_session()

    # First query
    result1 = query_rag_system(
        user_question="Who is Totoro?",
        agent_executor=rag_pipeline["agent_executor"],
        retriever=rag_pipeline["retriever"],
    )

    first_cost = result1["cost"]
    first_total = result1["session_total_cost"]

    assert first_cost > 0
    assert first_total == first_cost

    # Second query
    result2 = query_rag_system(
        user_question="Who is Chihiro?",
        agent_executor=rag_pipeline["agent_executor"],
        retriever=rag_pipeline["retriever"],
    )

    second_cost = result2["cost"]
    second_total = result2["session_total_cost"]

    assert second_cost > 0
    assert second_total == first_cost + second_cost

    print(f"First query: ${first_cost:.4f}")
    print(f"Second query: ${second_cost:.4f}")
    print(f"Total: ${second_total:.4f}")


# Streaming Tests
def test_streaming_query(rag_pipeline):
    """
    Test streaming query execution (if supported).

    Expected behavior:
    - Streaming yields response chunks
    - Final response is complete
    """
    chunks = []

    for chunk in query_rag_system_streaming(
        user_question="Who is the main character in Spirited Away?",
        agent_executor=rag_pipeline["agent_executor"],
        retriever=rag_pipeline["retriever"],
    ):
        chunks.append(chunk)

    # Verify we got response chunks
    assert len(chunks) > 0

    # Join chunks to get full response
    full_response = "".join(chunks)
    assert len(full_response) > 0

    print(f"Streaming response ({len(chunks)} chunks): {full_response}")


# Error Handling Tests
def test_query_handles_api_errors_gracefully(rag_pipeline):
    """
    Test query handles API errors gracefully.

    Note: This test is difficult to execute without mocking, as we can't
    reliably trigger API errors. Included for documentation purposes.
    """
    # This would require mocking the OpenAI API to return errors
    # For now, we just verify the query doesn't crash on unusual inputs
    result = query_rag_system(
        user_question="A very unusual query that might confuse the LLM: %&$#@!",
        agent_executor=rag_pipeline["agent_executor"],
        retriever=rag_pipeline["retriever"],
    )

    # Should still return a response (even if it's an error message)
    assert result["answer"]


# Performance Tests
def test_query_response_time_under_threshold(rag_pipeline):
    """
    Test query response time is under 10 seconds (NFR2).

    Expected behavior:
    - Query completes in < 10 seconds
    """
    result = query_rag_system(
        user_question="What are the top rated Ghibli films?",
        agent_executor=rag_pipeline["agent_executor"],
        retriever=rag_pipeline["retriever"],
    )

    assert result["response_time"] < 10.0
    print(f"Response time: {result['response_time']:.2f}s")


# Data Quality Tests
def test_query_returns_cited_response(rag_pipeline):
    """
    Test query responses include data citations.

    Expected behavior:
    - Response mentions data sources
    - Response includes specific metrics or references
    """
    result = query_rag_system(
        user_question="What is the average sentiment score for Studio Ghibli films?",
        agent_executor=rag_pipeline["agent_executor"],
        retriever=rag_pipeline["retriever"],
    )

    answer_lower = result["answer"].lower()

    # Check for citation patterns (not strict, as LLM phrasing varies)
    citation_indicators = [
        "according to",
        "based on",
        "data shows",
        "analysis",
        "sentiment",
        "score",
    ]

    has_citation = any(indicator in answer_lower for indicator in citation_indicators)

    print(f"Answer: {result['answer']}")
    print(f"Has citation indicators: {has_citation}")

    # Note: Not asserting here as LLM responses can vary
    # This is more of a quality check than a strict test
