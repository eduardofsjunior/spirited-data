"""
Unit tests for RAG pipeline module.

Tests cover:
- LLM initialization
- Retriever initialization
- System prompt creation
- Agent creation
- Query execution
- Cost tracking
- Logging
- Error handling
"""

import logging
import os
from typing import Any, Dict, List
from unittest.mock import MagicMock, Mock, patch

import pytest

from src.ai.rag_pipeline import (
    _CostTracker,
    _create_agent,
    _create_system_prompt,
    _initialize_llm,
    _initialize_retriever,
    _validate_query,
    cost_tracker,
    initialize_rag_pipeline,
    normalize_tool_response,
    query_rag_system,
)


# Test fixtures
@pytest.fixture
def mock_openai_key(monkeypatch):
    """Set OPENAI_API_KEY environment variable for tests."""
    monkeypatch.setenv("OPENAI_API_KEY", "test-api-key-123")


@pytest.fixture
def mock_chromadb_collection():
    """Mock ChromaDB collection."""
    collection = Mock()
    collection.count.return_value = 100
    return collection


@pytest.fixture
def mock_retriever():
    """Mock ChromaDB retriever."""
    retriever = Mock()

    # Create mock documents
    doc1 = Mock()
    doc1.page_content = "Spirited Away is a film about..."
    doc1.metadata = {"id": "doc1", "type": "film", "score": 0.95}

    doc2 = Mock()
    doc2.page_content = "Chihiro is the main character..."
    doc2.metadata = {"id": "doc2", "type": "character", "score": 0.88}

    retriever.get_relevant_documents.return_value = [doc1, doc2]
    return retriever


@pytest.fixture
def mock_llm():
    """Mock ChatOpenAI LLM."""
    llm = Mock()
    llm.model_name = "gpt-3.5-turbo"
    return llm


@pytest.fixture
def mock_agent_executor():
    """Mock AgentExecutor (LangGraph format)."""
    executor = Mock()
    mock_message = Mock()
    mock_message.content = "Based on the data, Chihiro is the most central character with a centrality score of 0.85."
    # Configure tool_calls to not exist (hasattr will return False)
    del mock_message.tool_calls
    executor.invoke.return_value = {
        "messages": [mock_message],
        "input_tokens": 150,
        "output_tokens": 50,
        "total_tokens": 200,
    }
    return executor


@pytest.fixture
def reset_cost_tracker():
    """Reset cost tracker before each test."""
    cost_tracker.reset_session()
    yield
    cost_tracker.reset_session()


# LLM Initialization Tests
def test_initialize_llm_success(mock_openai_key):
    """Test LLM initialization with valid API key."""
    with patch("src.ai.rag_pipeline.ChatOpenAI") as mock_chat_openai:
        mock_instance = Mock()
        mock_chat_openai.return_value = mock_instance

        llm = _initialize_llm(model="gpt-3.5-turbo")

        assert llm is not None
        mock_chat_openai.assert_called_once()
        call_kwargs = mock_chat_openai.call_args[1]
        assert call_kwargs["model"] == "gpt-3.5-turbo"
        assert call_kwargs["temperature"] == 0.3
        assert call_kwargs["max_tokens"] == 500


def test_initialize_llm_missing_api_key():
    """Test LLM initialization fails without API key."""
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError, match="OPENAI_API_KEY"):
            _initialize_llm()


def test_initialize_llm_default_model(mock_openai_key):
    """Test LLM uses default model if not specified."""
    with patch("src.ai.rag_pipeline.ChatOpenAI") as mock_chat_openai:
        mock_instance = Mock()
        mock_chat_openai.return_value = mock_instance

        _initialize_llm()

        call_kwargs = mock_chat_openai.call_args[1]
        # Should use DEFAULT_MODEL (gpt-3.5-turbo)
        assert call_kwargs["model"] in ["gpt-3.5-turbo", "gpt-4"]


# Retriever Initialization Tests
def test_initialize_retriever_success(mock_openai_key, mock_chromadb_collection):
    """Test retriever initialization with valid ChromaDB collection."""
    with patch("src.ai.rag_pipeline.Chroma") as mock_chroma, \
         patch("src.ai.rag_pipeline.OpenAIEmbeddings") as mock_embeddings:

        mock_vectorstore = Mock()
        mock_vectorstore._collection = mock_chromadb_collection
        mock_retriever_instance = Mock()
        mock_vectorstore.as_retriever.return_value = mock_retriever_instance
        mock_chroma.return_value = mock_vectorstore

        retriever = _initialize_retriever()

        assert retriever is not None
        mock_chroma.assert_called_once()
        mock_vectorstore.as_retriever.assert_called_once()


def test_initialize_retriever_empty_collection(mock_openai_key):
    """Test retriever fails with empty ChromaDB collection."""
    with patch("src.ai.rag_pipeline.Chroma") as mock_chroma, \
         patch("src.ai.rag_pipeline.OpenAIEmbeddings"):

        mock_collection = Mock()
        mock_collection.count.return_value = 0
        mock_vectorstore = Mock()
        mock_vectorstore._collection = mock_collection
        mock_chroma.return_value = mock_vectorstore

        with pytest.raises(ValueError, match="empty"):
            _initialize_retriever()


def test_initialize_retriever_missing_api_key():
    """Test retriever fails without OpenAI API key."""
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError, match="OPENAI_API_KEY"):
            _initialize_retriever()


# System Prompt Tests
def test_create_system_prompt():
    """Test system prompt creation."""
    prompt = _create_system_prompt()

    assert prompt is not None
    # Verify prompt is ChatPromptTemplate
    assert hasattr(prompt, "format_messages") or hasattr(prompt, "format")


# Agent Creation Tests
def test_create_agent_success(mock_llm):
    """Test agent creation with valid LLM and prompt using LangGraph."""
    prompt = _create_system_prompt()

    with patch("src.ai.rag_pipeline.create_react_agent") as mock_create_agent:
        mock_agent = Mock()
        mock_create_agent.return_value = mock_agent

        agent = _create_agent(mock_llm, prompt)

        assert agent is not None
        mock_create_agent.assert_called_once()

        # Verify tools are registered (LangGraph uses 'tools' parameter)
        create_agent_kwargs = mock_create_agent.call_args[1]
        assert "tools" in create_agent_kwargs
        assert len(create_agent_kwargs["tools"]) == 7  # All 7 custom tools


# Query Validation Tests
def test_validate_query_success():
    """Test query validation accepts valid queries."""
    _validate_query("Who is the main character in Spirited Away?")
    _validate_query("Calculate character centrality")
    _validate_query("What is the sentiment of Totoro?")


def test_validate_query_empty():
    """Test query validation rejects empty queries."""
    with pytest.raises(ValueError, match="empty"):
        _validate_query("")

    with pytest.raises(ValueError, match="empty"):
        _validate_query("   ")


def test_validate_query_too_long():
    """Test query validation rejects excessively long queries."""
    long_query = "x" * 1001  # MAX_QUERY_LENGTH is 1000
    with pytest.raises(ValueError, match="too long"):
        _validate_query(long_query)


def test_validate_query_suspicious_patterns():
    """Test query validation rejects suspicious patterns."""
    suspicious_queries = [
        "<script>alert('xss')</script>",
        "javascript:alert(1)",
        "<img onerror='alert(1)' src='x'>",
        "<div onclick='alert(1)'>click</div>",
    ]

    for query in suspicious_queries:
        with pytest.raises(ValueError, match="suspicious"):
            _validate_query(query)


# Cost Tracking Tests
def test_cost_tracker_calculate_cost_gpt4(reset_cost_tracker):
    """Test cost calculation for GPT-4."""
    tracker = _CostTracker()
    cost = tracker.calculate_cost(input_tokens=1000, output_tokens=500, model="gpt-4")

    # GPT-4: $0.03/1K input, $0.06/1K output
    expected_cost = (1000 / 1000) * 0.03 + (500 / 1000) * 0.06
    assert abs(cost - expected_cost) < 0.0001


def test_cost_tracker_calculate_cost_gpt35(reset_cost_tracker):
    """Test cost calculation for GPT-3.5-turbo."""
    tracker = _CostTracker()
    cost = tracker.calculate_cost(input_tokens=1000, output_tokens=500, model="gpt-3.5-turbo")

    # GPT-3.5-turbo: $0.0015/1K input, $0.002/1K output
    expected_cost = (1000 / 1000) * 0.0015 + (500 / 1000) * 0.002
    assert abs(cost - expected_cost) < 0.0001


def test_cost_tracker_threshold_warnings(reset_cost_tracker, caplog):
    """Test cost tracker emits warnings at thresholds."""
    tracker = _CostTracker()

    with caplog.at_level(logging.WARNING):
        # Add costs to reach $5 threshold
        tracker.add_query_cost(2.5)
        tracker.add_query_cost(2.5)
        assert "$5" in caplog.text

        # Add more to reach $10
        tracker.add_query_cost(5.0)
        assert "$10" in caplog.text

        # Add more to reach $15
        tracker.add_query_cost(5.0)
        assert "$15" in caplog.text


def test_cost_tracker_no_duplicate_warnings(reset_cost_tracker, caplog):
    """Test cost tracker doesn't duplicate threshold warnings."""
    tracker = _CostTracker()

    with caplog.at_level(logging.WARNING):
        tracker.add_query_cost(6.0)
        first_count = caplog.text.count("$5")

        # Add more cost, should not trigger $5 warning again
        tracker.add_query_cost(1.0)
        second_count = caplog.text.count("$5")

        assert first_count == second_count


def test_cost_tracker_session_total(reset_cost_tracker):
    """Test session total accumulation."""
    tracker = _CostTracker()

    tracker.add_query_cost(1.5)
    assert tracker.get_session_total() == 1.5

    tracker.add_query_cost(2.3)
    assert abs(tracker.get_session_total() - 3.8) < 0.0001


def test_cost_tracker_reset(reset_cost_tracker):
    """Test session reset."""
    tracker = _CostTracker()

    tracker.add_query_cost(5.0)
    assert tracker.get_session_total() == 5.0

    tracker.reset_session()
    assert tracker.get_session_total() == 0.0
    assert len(tracker.warnings_shown) == 0


# Tool Response Normalization Tests
def test_normalize_tool_response_already_correct():
    """Test normalization preserves correctly formatted responses."""
    correct_response = {
        "answer": "Test answer",
        "data_sources": {
            "tables": ["mart_graph_nodes"],
            "functions": ["calculate_centrality"],
            "computation_method": "NetworkX",
            "row_count": 10,
            "timestamp": "2025-01-08T12:00:00",
        },
        "visualization_data": {"chart_type": "bar"},
        "suggested_followups": ["Next question?"],
    }

    normalized = normalize_tool_response(correct_response)
    assert normalized == correct_response


def test_normalize_tool_response_string():
    """Test normalization converts string to ToolResponse."""
    response = "This is a simple string response"
    normalized = normalize_tool_response(response)

    assert normalized["answer"] == response
    assert "data_sources" in normalized
    assert "timestamp" in normalized["data_sources"]


def test_normalize_tool_response_dict_without_format():
    """Test normalization converts non-standard dict."""
    response = {
        "answer": "Test answer",
        "extra_field": "extra_value",
    }

    normalized = normalize_tool_response(response)
    assert normalized["answer"] == "Test answer"
    assert "data_sources" in normalized
    assert "timestamp" in normalized["data_sources"]


# Query Execution Tests
def test_query_rag_system_success(
    mock_openai_key,
    mock_retriever,
    mock_agent_executor,
    reset_cost_tracker,
):
    """Test successful RAG query execution."""
    result = query_rag_system(
        user_question="Who is the main character?",
        agent_executor=mock_agent_executor,
        retriever=mock_retriever,
    )

    assert "answer" in result
    assert "retrieved_docs" in result
    assert "tokens_used" in result
    assert "response_time" in result
    assert "cost" in result
    assert result["response_time"] > 0


def test_query_rag_system_with_chat_history(
    mock_openai_key,
    mock_retriever,
    mock_agent_executor,
    reset_cost_tracker,
):
    """Test query execution with chat history."""
    chat_history = [
        {"role": "user", "content": "What is Spirited Away?"},
        {"role": "assistant", "content": "It's a Studio Ghibli film..."},
    ]

    result = query_rag_system(
        user_question="Who is the director?",
        chat_history=chat_history,
        agent_executor=mock_agent_executor,
        retriever=mock_retriever,
    )

    assert result is not None
    mock_agent_executor.invoke.assert_called_once()

    # Verify messages were passed (LangGraph uses messages list, not chat_history dict)
    invoke_kwargs = mock_agent_executor.invoke.call_args[0][0]
    assert "messages" in invoke_kwargs
    # Should have system message, history messages, and current query
    assert len(invoke_kwargs["messages"]) >= 3


def test_query_rag_system_invalid_query(
    mock_openai_key,
    mock_retriever,
    mock_agent_executor,
):
    """Test query execution fails with invalid query."""
    with pytest.raises(ValueError, match="empty"):
        query_rag_system(
            user_question="",
            agent_executor=mock_agent_executor,
            retriever=mock_retriever,
        )


def test_query_rag_system_logs_cost(
    mock_openai_key,
    mock_retriever,
    mock_agent_executor,
    reset_cost_tracker,
    caplog,
):
    """Test query execution logs cost information."""
    with caplog.at_level(logging.INFO):
        query_rag_system(
            user_question="Test query",
            agent_executor=mock_agent_executor,
            retriever=mock_retriever,
        )

        assert "Query cost" in caplog.text
        assert "session total" in caplog.text


# Pipeline Initialization Tests
def test_initialize_rag_pipeline_success(mock_openai_key, mock_chromadb_collection):
    """Test full pipeline initialization with LangGraph."""
    with patch("src.ai.rag_pipeline.ChatOpenAI") as mock_chat_openai, \
         patch("src.ai.rag_pipeline.Chroma") as mock_chroma, \
         patch("src.ai.rag_pipeline.OpenAIEmbeddings"), \
         patch("src.ai.rag_pipeline.create_react_agent") as mock_create_agent:

        mock_llm_instance = Mock()
        mock_llm_instance.model_name = "gpt-4"
        mock_chat_openai.return_value = mock_llm_instance

        mock_vectorstore = Mock()
        mock_vectorstore._collection = mock_chromadb_collection
        mock_retriever = Mock()
        mock_vectorstore.as_retriever.return_value = mock_retriever
        mock_chroma.return_value = mock_vectorstore

        mock_agent = Mock()
        mock_create_agent.return_value = mock_agent

        pipeline = initialize_rag_pipeline(model="gpt-4")

        assert "llm" in pipeline
        assert "retriever" in pipeline
        assert "agent_executor" in pipeline


def test_initialize_rag_pipeline_missing_api_key():
    """Test pipeline initialization fails without API key."""
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError, match="OPENAI_API_KEY"):
            initialize_rag_pipeline()


# Logging Tests
def test_query_logs_retrieved_documents(
    mock_openai_key,
    mock_retriever,
    mock_agent_executor,
    reset_cost_tracker,
    caplog,
):
    """Test query execution logs retrieved document information."""
    with caplog.at_level(logging.DEBUG):
        query_rag_system(
            user_question="Test query",
            agent_executor=mock_agent_executor,
            retriever=mock_retriever,
        )

        assert "Retrieved 2 documents" in caplog.text
        assert "doc1" in caplog.text or "Similarity scores" in caplog.text


def test_query_warns_on_slow_execution(
    mock_openai_key,
    mock_retriever,
    reset_cost_tracker,
    caplog,
):
    """Test query execution warns on slow queries (>10s)."""
    # Mock agent executor with slow execution (LangGraph format)
    slow_executor = Mock()
    mock_message = Mock()
    mock_message.content = "Slow response"
    del mock_message.tool_calls  # Remove mock tool_calls attribute
    slow_executor.invoke.return_value = {
        "messages": [mock_message],
    }

    # Mock time.perf_counter to simulate slow execution
    # Need multiple calls: start, retrieval_start, retrieval_end, agent_start, agent_end, end
    with patch("src.ai.rag_pipeline.time.perf_counter") as mock_time, \
         caplog.at_level(logging.WARNING):

        mock_time.side_effect = [0.0, 0.0, 0.1, 0.1, 0.2, 11.0]  # 11 second total execution

        query_rag_system(
            user_question="Test query",
            agent_executor=slow_executor,
            retriever=mock_retriever,
        )

        assert "Slow query detected" in caplog.text


def test_query_warns_on_high_token_usage(
    mock_openai_key,
    mock_retriever,
    reset_cost_tracker,
    caplog,
):
    """Test query execution warns on high token usage (>1000 tokens)."""
    # Mock agent executor with high token response (LangGraph format)
    high_token_executor = Mock()
    mock_message = Mock()
    mock_message.content = "Response"
    del mock_message.tool_calls  # Remove mock tool_calls attribute
    high_token_executor.invoke.return_value = {
        "messages": [mock_message],
        "input_tokens": 800,
        "output_tokens": 300,
        "total_tokens": 1100,
    }

    with caplog.at_level(logging.WARNING):
        query_rag_system(
            user_question="Test query",
            agent_executor=high_token_executor,
            retriever=mock_retriever,
        )

        assert "High token usage" in caplog.text


# Edge Cases
def test_query_handles_missing_token_counts(
    mock_openai_key,
    mock_retriever,
    reset_cost_tracker,
):
    """Test query execution estimates tokens if not provided by LangChain."""
    executor_no_tokens = Mock()
    mock_message = Mock()
    mock_message.content = "Response without token counts"
    del mock_message.tool_calls  # Remove mock tool_calls attribute
    executor_no_tokens.invoke.return_value = {
        "messages": [mock_message],
    }

    result = query_rag_system(
        user_question="Test query",
        agent_executor=executor_no_tokens,
        retriever=mock_retriever,
    )

    # Should have estimated token counts (via tiktoken or fallback)
    assert result["tokens_used"]["total_tokens"] > 0


def test_query_handles_empty_retrieved_docs(
    mock_openai_key,
    mock_agent_executor,
    reset_cost_tracker,
):
    """Test query execution handles empty retriever results."""
    empty_retriever = Mock()
    empty_retriever.get_relevant_documents.return_value = []

    result = query_rag_system(
        user_question="Test query",
        agent_executor=mock_agent_executor,
        retriever=empty_retriever,
    )

    assert result is not None
    assert result["retrieved_docs"] == []
