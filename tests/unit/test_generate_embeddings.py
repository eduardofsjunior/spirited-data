"""Unit tests for generate_embeddings.py module."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from src.ai.generate_embeddings import (
    create_batches,
    estimate_embedding_cost,
    estimate_tokens,
    generate_embeddings_batch,
    load_embedding_corpus,
    log_processing_summary,
    validate_embeddings,
)


def test_load_embedding_corpus(tmp_path: Path) -> None:
    """Test corpus loading from JSON file."""
    # Create sample corpus
    corpus_data = [
        {
            "doc_id": "film_1",
            "type": "film",
            "name": "Spirited Away",
            "text": "Spirited Away is a 2001 animated film directed by Hayao Miyazaki.",
            "metadata": {"director": "Hayao Miyazaki", "release_year": 2001},
        },
        {
            "doc_id": "character_1",
            "type": "character",
            "name": "Chihiro",
            "text": "Chihiro is the main character in Spirited Away.",
            "metadata": {"gender": "Female", "age": 10},
        },
    ]

    corpus_file = tmp_path / "test_corpus.json"
    with open(corpus_file, "w", encoding="utf-8") as f:
        json.dump(corpus_data, f)

    # Test loading
    documents = load_embedding_corpus(str(corpus_file))

    assert len(documents) == 2
    assert all("doc_id" in doc for doc in documents)
    assert all("text" in doc for doc in documents)
    assert all("type" in doc for doc in documents)
    assert documents[0]["name"] == "Spirited Away"
    assert documents[1]["name"] == "Chihiro"


def test_load_embedding_corpus_file_not_found() -> None:
    """Test corpus loading with non-existent file."""
    with pytest.raises(FileNotFoundError):
        load_embedding_corpus("nonexistent_file.json")


def test_load_embedding_corpus_missing_fields(tmp_path: Path) -> None:
    """Test corpus loading with missing required fields."""
    corpus_data = [
        {
            "doc_id": "film_1",
            "type": "film",
            # Missing: name, text, metadata
        }
    ]

    corpus_file = tmp_path / "invalid_corpus.json"
    with open(corpus_file, "w", encoding="utf-8") as f:
        json.dump(corpus_data, f)

    with pytest.raises(ValueError, match="missing required fields"):
        load_embedding_corpus(str(corpus_file))


def test_estimate_tokens() -> None:
    """Test token counting with tiktoken."""
    text = "Spirited Away is a 2001 film directed by Hayao Miyazaki."
    token_count = estimate_tokens(text)

    assert isinstance(token_count, int)
    assert token_count > 0
    assert token_count < 30  # Short sentence


def test_estimate_tokens_empty_string() -> None:
    """Test token counting with empty string."""
    token_count = estimate_tokens("")
    assert token_count == 0


def test_estimate_embedding_cost() -> None:
    """Test cost estimation calculation."""
    documents = [
        {"text": "This is a test document with some text."},
        {"text": "Another test document with different content."},
        {"text": "A third document for testing purposes."},
    ]

    cost_info = estimate_embedding_cost(documents)

    assert "total_documents" in cost_info
    assert "total_tokens" in cost_info
    assert "estimated_cost_usd" in cost_info
    assert "budget_usd" in cost_info
    assert "under_budget" in cost_info

    assert cost_info["total_documents"] == 3
    assert cost_info["total_tokens"] > 0
    assert cost_info["estimated_cost_usd"] > 0
    assert cost_info["budget_usd"] == 2.0
    assert cost_info["under_budget"] is True


def test_create_batches() -> None:
    """Test batch splitting logic."""
    documents = [{"doc_id": f"doc_{i}", "text": f"Document {i}"} for i in range(250)]

    batches = create_batches(documents, batch_size=100)

    assert len(batches) == 3
    assert len(batches[0]) == 100
    assert len(batches[1]) == 100
    assert len(batches[2]) == 50


def test_create_batches_single_batch() -> None:
    """Test batch creation with documents less than batch size."""
    documents = [{"doc_id": f"doc_{i}", "text": f"Document {i}"} for i in range(50)]

    batches = create_batches(documents, batch_size=100)

    assert len(batches) == 1
    assert len(batches[0]) == 50


def test_create_batches_empty_list() -> None:
    """Test batch creation with empty document list."""
    batches = create_batches([], batch_size=100)
    assert len(batches) == 0


@patch("src.ai.generate_embeddings.OpenAI")
def test_generate_embeddings_batch(mock_openai_class: MagicMock) -> None:
    """Test embedding generation with mocked OpenAI API."""
    # Create mock client
    mock_client = MagicMock()

    # Mock OpenAI response
    mock_response = MagicMock()
    mock_response.data = [
        MagicMock(embedding=[0.1] * 1536),
        MagicMock(embedding=[0.2] * 1536),
    ]
    mock_response.usage.total_tokens = 20

    mock_client.embeddings.create.return_value = mock_response

    # Test documents
    documents = [
        {"doc_id": "doc1", "text": "Test document 1"},
        {"doc_id": "doc2", "text": "Test document 2"},
    ]

    # Generate embeddings
    embeddings, tokens = generate_embeddings_batch(mock_client, documents)

    # Assertions
    assert len(embeddings) == 2
    assert all(isinstance(emb, np.ndarray) for emb in embeddings)
    assert all(emb.shape == (1536,) for emb in embeddings)
    assert tokens == 20
    mock_client.embeddings.create.assert_called_once()


@patch("src.ai.generate_embeddings.OpenAI")
def test_generate_embeddings_batch_rate_limit_retry(
    mock_openai_class: MagicMock,
) -> None:
    """Test retry logic with rate limit errors."""
    from openai import RateLimitError

    # Create mock client
    mock_client = MagicMock()

    # Mock rate limit error on first call, success on second
    mock_response = MagicMock()
    mock_response.data = [MagicMock(embedding=[0.1] * 1536)]
    mock_response.usage.total_tokens = 10

    mock_client.embeddings.create.side_effect = [
        RateLimitError(
            message="Rate limit exceeded",
            response=MagicMock(status_code=429),
            body=None,
        ),
        mock_response,
    ]

    documents = [{"doc_id": "doc1", "text": "Test document"}]

    # Should succeed after retry
    embeddings, tokens = generate_embeddings_batch(mock_client, documents)

    assert len(embeddings) == 1
    assert tokens == 10
    assert mock_client.embeddings.create.call_count == 2


@patch("src.ai.generate_embeddings.OpenAI")
def test_generate_embeddings_batch_max_retries_exceeded(
    mock_openai_class: MagicMock,
) -> None:
    """Test max retries exceeded raises error."""
    from openai import RateLimitError

    # Create mock client
    mock_client = MagicMock()

    # Always fail with rate limit error
    mock_client.embeddings.create.side_effect = RateLimitError(
        message="Rate limit exceeded", response=MagicMock(status_code=429), body=None
    )

    documents = [{"doc_id": "doc1", "text": "Test document"}]

    # Should raise after max retries
    with pytest.raises(RateLimitError):
        generate_embeddings_batch(mock_client, documents)


def test_validate_embeddings() -> None:
    """Test embedding validation logic."""
    # Create sample embeddings
    embeddings = np.random.rand(100, 1536).astype(np.float32)
    doc_ids = [f"doc_{i}" for i in range(100)]
    documents = [
        {"doc_id": doc_ids[i], "type": "film", "name": f"Film {i}"} for i in range(100)
    ]

    # Validate
    report = validate_embeddings(embeddings, doc_ids, documents)

    assert report["shape_valid"] is True
    assert report["count_valid"] is True
    assert report["nan_check_passed"] is True
    assert report["document_count"] == 100
    assert report["embedding_dimensions"] == 1536


def test_validate_embeddings_wrong_shape() -> None:
    """Test validation fails with wrong embedding shape."""
    embeddings = np.random.rand(100, 512).astype(np.float32)  # Wrong dimensions
    doc_ids = [f"doc_{i}" for i in range(100)]
    documents = [{"doc_id": doc_ids[i], "type": "film"} for i in range(100)]

    with pytest.raises(AssertionError, match="Expected shape"):
        validate_embeddings(embeddings, doc_ids, documents)


def test_validate_embeddings_count_mismatch() -> None:
    """Test validation fails with count mismatch."""
    embeddings = np.random.rand(100, 1536).astype(np.float32)
    doc_ids = [f"doc_{i}" for i in range(50)]  # Mismatch
    documents = [{"doc_id": doc_ids[i], "type": "film"} for i in range(50)]

    with pytest.raises(AssertionError, match="Expected shape"):
        validate_embeddings(embeddings, doc_ids, documents)


def test_validate_embeddings_nan_values() -> None:
    """Test validation fails with NaN values."""
    embeddings = np.random.rand(100, 1536).astype(np.float32)
    embeddings[0, 0] = np.nan  # Introduce NaN
    doc_ids = [f"doc_{i}" for i in range(100)]
    documents = [{"doc_id": doc_ids[i], "type": "film"} for i in range(100)]

    with pytest.raises(AssertionError, match="NaN values"):
        validate_embeddings(embeddings, doc_ids, documents)


def test_log_processing_summary(caplog: pytest.LogCaptureFixture) -> None:
    """Test processing summary logging."""
    import logging

    caplog.set_level(logging.INFO)

    stats = {
        "total_documents": 5411,
        "total_tokens": 541000,
        "actual_cost_usd": 0.01,
        "processing_time_seconds": 120.5,
        "budget_usd": 2.0,
    }

    log_processing_summary(stats)

    # Check log messages
    assert "EMBEDDING GENERATION SUMMARY" in caplog.text
    assert "Documents processed: 5411" in caplog.text
    assert "Total tokens used: 541,000" in caplog.text
    assert "Actual cost: $0.0100" in caplog.text
    assert "Cost under budget" in caplog.text


def test_log_processing_summary_over_budget(caplog: pytest.LogCaptureFixture) -> None:
    """Test processing summary with budget exceeded."""
    import logging

    caplog.set_level(logging.INFO)

    stats = {
        "total_documents": 1000000,
        "total_tokens": 100000000,
        "actual_cost_usd": 3.5,
        "processing_time_seconds": 600.0,
        "budget_usd": 2.0,
    }

    log_processing_summary(stats)

    # Check warning logged
    assert "COST EXCEEDED BUDGET" in caplog.text
