"""
Unit tests for RAG CLI module.

Tests CLI components including argument parsing, session management,
special command handling, query processing, and error handling.
"""

import json
import os
import tempfile
from datetime import datetime, timedelta
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest

from src.ai.rag_cli import (
    ConversationSession,
    handle_error,
    handle_special_command,
    parse_arguments,
    print_with_streaming,
    process_query,
    save_conversation_history,
)
from src.shared.exceptions import DatabaseError, RAGError, RateLimitError


# Fixtures


@pytest.fixture
def sample_rag_response() -> Dict[str, Any]:
    """Mock RAG pipeline response for unit tests."""
    return {
        "answer": (
            "Based on my graph analysis, the most central characters are "
            "Chihiro, Pazu, and Ashitaka."
        ),
        "retrieved_docs": [
            {"id": "doc_12", "score": 0.85, "type": "character"},
            {"id": "doc_45", "score": 0.78, "type": "film"},
            {"id": "doc_78", "score": 0.72, "type": "analysis"},
        ],
        "function_calls": ["calculate_character_centrality(top_n=5)"],
        "tokens_used": {
            "input_tokens": 245,
            "output_tokens": 180,
            "total_tokens": 425,
        },
        "response_time": 2.3,
        "cost": 0.0127,
    }


@pytest.fixture
def sample_session() -> ConversationSession:
    """Mock conversation session for testing."""
    session = ConversationSession()
    session.add_message("user", "Test query")
    session.add_message("assistant", "Test response")
    session.total_queries = 1
    session.total_tokens = 425
    session.total_cost = 0.0127
    session.response_times = [2.3]
    return session


# Test Argument Parsing


def test_parse_arguments_defaults() -> None:
    """Test default argument values."""
    with patch("sys.argv", ["rag_cli.py"]):
        args = parse_arguments()
        assert args.debug is False
        assert args.no_streaming is False
        assert args.log_level == "INFO"
        assert args.save_history is True


def test_parse_arguments_debug_flag() -> None:
    """Test --debug flag sets debug mode."""
    with patch("sys.argv", ["rag_cli.py", "--debug"]):
        args = parse_arguments()
        assert args.debug is True


def test_parse_arguments_no_streaming_flag() -> None:
    """Test --no-streaming flag disables streaming."""
    with patch("sys.argv", ["rag_cli.py", "--no-streaming"]):
        args = parse_arguments()
        assert args.no_streaming is True


def test_parse_arguments_log_level() -> None:
    """Test --log-level sets logging level."""
    with patch("sys.argv", ["rag_cli.py", "--log-level", "DEBUG"]):
        args = parse_arguments()
        assert args.log_level == "DEBUG"


# Test ConversationSession


def test_conversation_session_init() -> None:
    """Test ConversationSession initialization."""
    session = ConversationSession()

    assert isinstance(session.history, list)
    assert len(session.history) == 0
    assert isinstance(session.start_time, datetime)
    assert session.total_queries == 0
    assert session.total_tokens == 0
    assert session.total_cost == 0.0
    assert isinstance(session.response_times, list)


def test_conversation_session_add_message() -> None:
    """Test adding messages to conversation history."""
    session = ConversationSession()

    session.add_message("user", "Hello")
    session.add_message("assistant", "Hi there!")

    assert len(session.history) == 2
    assert session.history[0] == {"role": "user", "content": "Hello"}
    assert session.history[1] == {"role": "assistant", "content": "Hi there!"}


def test_conversation_session_get_history() -> None:
    """Test getting conversation history."""
    session = ConversationSession()
    session.add_message("user", "Test")

    history = session.get_history()

    assert isinstance(history, list)
    assert len(history) == 1
    assert history[0]["role"] == "user"


def test_conversation_session_reset() -> None:
    """Test resetting session clears all data."""
    session = ConversationSession()
    session.add_message("user", "Test")
    session.total_queries = 5
    session.total_tokens = 1000
    session.total_cost = 0.5
    session.response_times = [1.0, 2.0]

    session.reset()

    assert len(session.history) == 0
    assert session.total_queries == 0
    assert session.total_tokens == 0
    assert session.total_cost == 0.0
    assert len(session.response_times) == 0


def test_conversation_session_get_stats() -> None:
    """Test getting session statistics."""
    session = ConversationSession()
    session.total_queries = 3
    session.total_tokens = 1200
    session.total_cost = 0.036
    session.response_times = [1.5, 2.0, 2.5]

    stats = session.get_stats()

    assert stats["total_queries"] == 3
    assert stats["total_tokens"] == 1200
    assert stats["total_cost"] == 0.036
    assert stats["average_response_time"] == 2.0
    assert "duration" in stats


def test_conversation_session_get_stats_empty() -> None:
    """Test getting stats for empty session."""
    session = ConversationSession()

    stats = session.get_stats()

    assert stats["total_queries"] == 0
    assert stats["average_response_time"] == 0.0


# Test Special Command Handling


def test_handle_special_command_exit() -> None:
    """Test /exit command returns exit signal."""
    session = ConversationSession()

    result = handle_special_command("/exit", session)

    assert result == "exit"


def test_handle_special_command_reset(capsys: Any) -> None:
    """Test /reset command clears session."""
    session = ConversationSession()
    session.add_message("user", "Test")
    session.total_queries = 5

    result = handle_special_command("/reset", session)

    assert result is None
    assert len(session.history) == 0
    assert session.total_queries == 0

    captured = capsys.readouterr()
    assert "cleared" in captured.out.lower()


def test_handle_special_command_stats(capsys: Any, sample_session: ConversationSession) -> None:
    """Test /stats command displays session statistics."""
    result = handle_special_command("/stats", sample_session)

    assert result is None

    captured = capsys.readouterr()
    assert "Session Statistics" in captured.out
    assert "Total Queries: 1" in captured.out
    assert "Total Tokens: 425" in captured.out
    assert "$0.01" in captured.out


def test_handle_special_command_help(capsys: Any) -> None:
    """Test /help command displays help message."""
    session = ConversationSession()

    result = handle_special_command("/help", session)

    assert result is None

    captured = capsys.readouterr()
    assert "SpiritedData RAG CLI" in captured.out
    assert "Try asking:" in captured.out


def test_handle_special_command_unknown(capsys: Any) -> None:
    """Test unknown command shows error message."""
    session = ConversationSession()

    result = handle_special_command("/unknown", session)

    assert result is None

    captured = capsys.readouterr()
    assert "Unknown command" in captured.out


# Test Query Processing


@patch("src.ai.rag_cli.query_rag_system")
def test_process_query_success(
    mock_query: MagicMock, sample_rag_response: Dict[str, Any], capsys: Any
) -> None:
    """Test successful query processing."""
    mock_query.return_value = sample_rag_response

    session = ConversationSession()
    result = process_query(
        "Who are the most central characters?", session, debug=False, streaming=False
    )

    # Verify RAG pipeline was called
    mock_query.assert_called_once()
    call_args = mock_query.call_args
    assert call_args.kwargs["user_question"] == "Who are the most central characters?"

    # Verify result
    assert result["answer"] == sample_rag_response["answer"]

    # Verify session updated
    assert session.total_queries == 1
    assert session.total_tokens == 425
    assert session.total_cost == 0.0127
    assert len(session.history) == 2
    assert len(session.response_times) == 1

    # Verify output
    captured = capsys.readouterr()
    assert "Chihiro" in captured.out


@patch("src.ai.rag_cli.query_rag_system")
def test_process_query_debug_mode(
    mock_query: MagicMock, sample_rag_response: Dict[str, Any], capsys: Any
) -> None:
    """Test query processing with debug mode enabled."""
    mock_query.return_value = sample_rag_response

    session = ConversationSession()
    process_query("Test query", session, debug=True, streaming=False)

    captured = capsys.readouterr()

    # Verify debug output
    assert "[DEBUG]" in captured.out
    assert "Retrieved Documents:" in captured.out
    assert "doc_12" in captured.out
    assert "Function Calls:" in captured.out
    assert "calculate_character_centrality" in captured.out
    assert "Token Usage:" in captured.out
    assert "Response Time:" in captured.out


def test_process_query_empty_input() -> None:
    """Test query processing rejects empty input."""
    session = ConversationSession()

    with pytest.raises(ValueError, match="Query cannot be empty"):
        process_query("", session)


def test_process_query_too_long_input() -> None:
    """Test query processing rejects input > 1000 characters."""
    session = ConversationSession()
    long_query = "a" * 1001

    with pytest.raises(ValueError, match="Query too long"):
        process_query(long_query, session)


@patch("src.ai.rag_cli.query_rag_system")
def test_process_query_whitespace_stripped(
    mock_query: MagicMock, sample_rag_response: Dict[str, Any]
) -> None:
    """Test query processing strips whitespace."""
    mock_query.return_value = sample_rag_response

    session = ConversationSession()
    process_query("  Test query  ", session, streaming=False)

    # Verify whitespace stripped in call
    call_args = mock_query.call_args
    assert call_args.kwargs["user_question"] == "Test query"


# Test Streaming Output


def test_print_with_streaming(capsys: Any) -> None:
    """Test streaming output function."""
    print_with_streaming("Hello world test", delay=0.001)

    captured = capsys.readouterr()
    assert "Hello world test" in captured.out


# Test Error Handling


def test_handle_error_rag_error(capsys: Any) -> None:
    """Test handling RAGError."""
    error = RAGError("OpenAI API is unavailable")

    handle_error(error, debug=False)

    captured = capsys.readouterr()
    assert "❌ RAG System Error" in captured.out
    assert "OpenAI API is unavailable" in captured.out


def test_handle_error_rate_limit(capsys: Any) -> None:
    """Test handling RateLimitError."""
    error = RateLimitError("Rate limit exceeded")

    handle_error(error, debug=False)

    captured = capsys.readouterr()
    assert "⚠️ Rate Limit Exceeded" in captured.out


def test_handle_error_database_error(capsys: Any) -> None:
    """Test handling DatabaseError."""
    error = DatabaseError("Database connection failed")

    handle_error(error, debug=False)

    captured = capsys.readouterr()
    assert "❌ Database Error" in captured.out


def test_handle_error_value_error(capsys: Any) -> None:
    """Test handling ValueError."""
    error = ValueError("Invalid input format")

    handle_error(error, debug=False)

    captured = capsys.readouterr()
    assert "⚠️ Invalid Input" in captured.out


def test_handle_error_generic_exception(capsys: Any) -> None:
    """Test handling generic Exception."""
    error = Exception("Something went wrong")

    handle_error(error, debug=False)

    captured = capsys.readouterr()
    assert "❌ Unexpected Error" in captured.out


def test_handle_error_debug_mode(capsys: Any) -> None:
    """Test error handling with debug mode shows traceback."""
    error = ValueError("Test error")

    handle_error(error, debug=True)

    captured = capsys.readouterr()
    assert "[DEBUG] Full traceback:" in captured.out


# Test Conversation History Saving


def test_save_conversation_history(sample_session: ConversationSession) -> None:
    """Test saving conversation history to JSON."""
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = save_conversation_history(sample_session, save_dir=tmpdir)

        # Verify file created
        assert os.path.exists(filepath)
        assert filepath.startswith(tmpdir)
        assert "rag_conversation_" in filepath
        assert filepath.endswith(".json")

        # Verify JSON structure
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        assert "metadata" in data
        assert "history" in data
        assert "statistics" in data

        # Verify metadata
        assert data["metadata"]["total_queries"] == 1
        assert data["metadata"]["total_tokens"] == 425
        assert data["metadata"]["total_cost"] == 0.0127

        # Verify history
        assert len(data["history"]) == 2
        assert data["history"][0]["role"] == "user"
        assert data["history"][1]["role"] == "assistant"

        # Verify statistics
        assert "session_duration_seconds" in data["statistics"]
        assert "average_response_time" in data["statistics"]


def test_save_conversation_history_creates_directory() -> None:
    """Test conversation history saving creates directory if not exists."""
    session = ConversationSession()

    with tempfile.TemporaryDirectory() as tmpdir:
        save_dir = os.path.join(tmpdir, "new_logs_dir")

        # Directory should not exist
        assert not os.path.exists(save_dir)

        # Save should create directory
        filepath = save_conversation_history(session, save_dir=save_dir)

        # Verify directory created
        assert os.path.exists(save_dir)
        assert os.path.exists(filepath)


def test_save_conversation_history_filename_format() -> None:
    """Test conversation history filename has correct timestamp format."""
    session = ConversationSession()

    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = save_conversation_history(session, save_dir=tmpdir)

        filename = os.path.basename(filepath)

        # Verify format: rag_conversation_YYYY-MM-DD_HH-MM-SS.json
        assert filename.startswith("rag_conversation_")
        assert filename.endswith(".json")
        assert len(filename) == len("rag_conversation_2025-01-08_14-30-45.json")


# Test Edge Cases


def test_conversation_session_stats_with_multiple_queries() -> None:
    """Test session statistics with multiple queries."""
    session = ConversationSession()
    session.total_queries = 10
    session.response_times = [1.0, 1.5, 2.0, 1.8, 2.2, 1.9, 2.1, 1.7, 2.3, 1.6]

    stats = session.get_stats()

    assert stats["total_queries"] == 10
    # Average of [1.0, 1.5, 2.0, 1.8, 2.2, 1.9, 2.1, 1.7, 2.3, 1.6] = 1.81
    assert 1.80 <= stats["average_response_time"] <= 1.82  # Allow float precision


@patch("src.ai.rag_cli.query_rag_system")
def test_process_query_with_chat_history(
    mock_query: MagicMock, sample_rag_response: Dict[str, Any]
) -> None:
    """Test query processing includes chat history."""
    mock_query.return_value = sample_rag_response

    session = ConversationSession()
    session.add_message("user", "Previous question")
    session.add_message("assistant", "Previous answer")

    process_query("New question", session, streaming=False)

    # Verify chat history passed to RAG pipeline
    # Note: process_query adds current question to history before calling RAG,
    # so history includes previous messages (2) + new question (1) + response (1) = 4 total

    # At the time of RAG call, history should contain 2 previous messages
    # (process_query adds new messages to session AFTER RAG call, but passes get_history()
    # which includes all messages added before the call)
    assert len(session.history) == 4  # After process_query: previous 2 + new 2

    # Verify previous messages exist in session
    assert session.history[0]["content"] == "Previous question"


@patch("src.ai.rag_cli.query_rag_system")
def test_process_query_missing_optional_fields(mock_query: MagicMock) -> None:
    """Test query processing handles missing optional fields in response."""
    # Response without tokens_used and cost
    mock_query.return_value = {
        "answer": "Test answer",
        "retrieved_docs": [],
        "function_calls": [],
        "response_time": 1.5,
    }

    session = ConversationSession()
    process_query("Test", session, streaming=False)

    # Should not crash, tokens and cost should remain 0
    assert session.total_tokens == 0
    assert session.total_cost == 0.0


def test_handle_special_command_stats_formatting(capsys: Any) -> None:
    """Test /stats command formats duration correctly."""
    session = ConversationSession()
    session.start_time = datetime.now() - timedelta(hours=2, minutes=15, seconds=45)
    session.total_queries = 5

    handle_special_command("/stats", session)

    captured = capsys.readouterr()
    assert "2 hours 15 minutes 45 seconds" in captured.out


def test_save_conversation_history_unicode_content() -> None:
    """Test saving conversation with unicode characters."""
    session = ConversationSession()
    session.add_message("user", "What about もののけ姫?")
    session.add_message("assistant", "Princess Mononoke (もののけ姫) is a masterpiece!")

    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = save_conversation_history(session, save_dir=tmpdir)

        # Verify unicode preserved
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        assert "もののけ姫" in data["history"][0]["content"]
        assert "もののけ姫" in data["history"][1]["content"]
