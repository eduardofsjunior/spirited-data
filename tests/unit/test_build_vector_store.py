"""Unit tests for src/ai/build_vector_store.py"""
import json
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch, mock_open

import numpy as np
import pytest

from src.ai.build_vector_store import (
    BATCH_SIZE,
    COLLECTION_NAME,
    EMBEDDING_DIMENSION,
    add_documents_to_collection,
    create_ghibli_collection,
    generate_collection_stats,
    get_chromadb_client,
    load_embedding_corpus,
    load_embeddings,
    validate_similarity_search,
    validate_documents,
)


@pytest.fixture
def mock_chromadb_client():
    """Mock ChromaDB persistent client."""
    client = MagicMock()
    client.delete_collection = MagicMock()
    client.create_collection = MagicMock()
    return client


@pytest.fixture
def mock_collection():
    """Mock ChromaDB collection."""
    collection = MagicMock()
    collection.add = MagicMock()
    collection.query = MagicMock()
    collection.get = MagicMock()
    return collection


@pytest.fixture
def sample_embeddings():
    """Sample embeddings array."""
    return np.random.rand(10, EMBEDDING_DIMENSION).astype(np.float32)


@pytest.fixture
def sample_corpus():
    """Sample embedding corpus."""
    return [
        {
            "doc_id": f"test_doc_{i}",
            "type": "film" if i < 5 else "character",
            "name": f"Test Entity {i}",
            "text": f"This is test document {i} with some content.",
            "metadata": {
                "film_id": "test_film_123" if i < 5 else "",
                "source": "test_source",
            },
        }
        for i in range(10)
    ]


class TestGetChromaDBClient:
    """Tests for get_chromadb_client function."""

    @patch("src.ai.build_vector_store.chromadb.PersistentClient")
    @patch("src.ai.build_vector_store.Path")
    def test_client_initialization_success(self, mock_path, mock_persistent_client):
        """Test successful ChromaDB client initialization."""
        mock_path_instance = MagicMock()
        mock_path.return_value = mock_path_instance
        mock_client = MagicMock()
        mock_persistent_client.return_value = mock_client

        client = get_chromadb_client()

        assert client is mock_client
        mock_path_instance.mkdir.assert_called_once_with(parents=True, exist_ok=True)
        mock_persistent_client.assert_called_once()

    @patch("src.ai.build_vector_store.chromadb.PersistentClient")
    @patch("src.ai.build_vector_store.Path")
    def test_client_initialization_failure(self, mock_path, mock_persistent_client):
        """Test ChromaDB client initialization failure."""
        mock_path_instance = MagicMock()
        mock_path.return_value = mock_path_instance
        mock_persistent_client.side_effect = Exception("Connection failed")

        with pytest.raises(Exception, match="Connection failed"):
            get_chromadb_client()


class TestCreateGhibliCollection:
    """Tests for create_ghibli_collection function."""

    def test_collection_creation_new(self, mock_chromadb_client, mock_collection):
        """Test creating new collection when none exists."""
        mock_chromadb_client.delete_collection.side_effect = ValueError(
            "Collection not found"
        )
        mock_chromadb_client.create_collection.return_value = mock_collection

        collection = create_ghibli_collection(mock_chromadb_client)

        assert collection is mock_collection
        mock_chromadb_client.delete_collection.assert_called_once_with(
            name=COLLECTION_NAME
        )
        mock_chromadb_client.create_collection.assert_called_once()

        # Verify metadata structure
        call_args = mock_chromadb_client.create_collection.call_args
        assert call_args[1]["name"] == COLLECTION_NAME
        metadata = call_args[1]["metadata"]
        assert "created_at" in metadata
        assert metadata["embedding_model"] == "text-embedding-3-small"
        assert metadata["embedding_dimension"] == str(EMBEDDING_DIMENSION)

    def test_collection_creation_idempotent(self, mock_chromadb_client, mock_collection):
        """Test idempotent collection creation (delete existing)."""
        mock_chromadb_client.create_collection.return_value = mock_collection

        collection = create_ghibli_collection(mock_chromadb_client)

        assert collection is mock_collection
        mock_chromadb_client.delete_collection.assert_called_once_with(
            name=COLLECTION_NAME
        )
        mock_chromadb_client.create_collection.assert_called_once()

    def test_collection_creation_failure(self, mock_chromadb_client):
        """Test collection creation failure."""
        mock_chromadb_client.create_collection.side_effect = Exception("Creation failed")

        with pytest.raises(Exception, match="Creation failed"):
            create_ghibli_collection(mock_chromadb_client)


class TestLoadEmbeddings:
    """Tests for load_embeddings function."""

    @patch("src.ai.build_vector_store.EMBEDDINGS_NPY_PATH")
    @patch("src.ai.build_vector_store.np.load")
    def test_load_embeddings_success(self, mock_np_load, mock_path):
        """Test successful embeddings loading."""
        mock_path.exists.return_value = True
        embeddings = np.random.rand(100, EMBEDDING_DIMENSION).astype(np.float32)
        mock_np_load.return_value = embeddings

        result = load_embeddings()

        assert result.shape == (100, EMBEDDING_DIMENSION)
        mock_np_load.assert_called_once()

    @patch("src.ai.build_vector_store.EMBEDDINGS_NPY_PATH")
    def test_load_embeddings_file_not_found(self, mock_path):
        """Test embeddings file not found."""
        mock_path.exists.return_value = False

        with pytest.raises(FileNotFoundError, match="Embeddings file not found"):
            load_embeddings()

    @patch("src.ai.build_vector_store.EMBEDDINGS_NPY_PATH")
    @patch("src.ai.build_vector_store.np.load")
    def test_load_embeddings_wrong_shape(self, mock_np_load, mock_path):
        """Test embeddings with wrong shape."""
        mock_path.exists.return_value = True
        mock_np_load.return_value = np.random.rand(100)  # 1D array

        with pytest.raises(ValueError, match="Expected 2D array"):
            load_embeddings()

    @patch("src.ai.build_vector_store.EMBEDDINGS_NPY_PATH")
    @patch("src.ai.build_vector_store.np.load")
    def test_load_embeddings_wrong_dimension(self, mock_np_load, mock_path):
        """Test embeddings with wrong dimension."""
        mock_path.exists.return_value = True
        mock_np_load.return_value = np.random.rand(100, 512)  # Wrong dimension

        with pytest.raises(ValueError, match="Expected embedding dimension"):
            load_embeddings()

    @patch("src.ai.build_vector_store.EMBEDDINGS_NPY_PATH")
    @patch("src.ai.build_vector_store.np.load")
    def test_load_embeddings_contains_nan(self, mock_np_load, mock_path):
        """Test embeddings containing NaN values."""
        mock_path.exists.return_value = True
        embeddings = np.random.rand(100, EMBEDDING_DIMENSION)
        embeddings[0, 0] = np.nan
        mock_np_load.return_value = embeddings

        with pytest.raises(ValueError, match="contain NaN values"):
            load_embeddings()

    @patch("src.ai.build_vector_store.EMBEDDINGS_NPY_PATH")
    @patch("src.ai.build_vector_store.np.load")
    def test_load_embeddings_contains_inf(self, mock_np_load, mock_path):
        """Test embeddings containing infinite values."""
        mock_path.exists.return_value = True
        embeddings = np.random.rand(100, EMBEDDING_DIMENSION)
        embeddings[0, 0] = np.inf
        mock_np_load.return_value = embeddings

        with pytest.raises(ValueError, match="contain infinite values"):
            load_embeddings()


class TestLoadEmbeddingCorpus:
    """Tests for load_embedding_corpus function."""

    @patch("src.ai.build_vector_store.EMBEDDING_CORPUS_PATH")
    @patch("builtins.open", new_callable=mock_open)
    def test_load_corpus_success(self, mock_file, mock_path):
        """Test successful corpus loading."""
        mock_path.exists.return_value = True
        corpus_data = [
            {
                "doc_id": "test_1",
                "type": "film",
                "name": "Test Film",
                "text": "Test content",
                "metadata": {"film_id": "123"},
            }
        ]
        mock_file.return_value.read.return_value = json.dumps(corpus_data)

        with patch("src.ai.build_vector_store.json.load", return_value=corpus_data):
            result = load_embedding_corpus()

        assert len(result) == 1
        assert result[0]["doc_id"] == "test_1"

    @patch("src.ai.build_vector_store.EMBEDDING_CORPUS_PATH")
    def test_load_corpus_file_not_found(self, mock_path):
        """Test corpus file not found."""
        mock_path.exists.return_value = False

        with pytest.raises(FileNotFoundError, match="Embedding corpus not found"):
            load_embedding_corpus()

    @patch("src.ai.build_vector_store.EMBEDDING_CORPUS_PATH")
    @patch("builtins.open", new_callable=mock_open)
    def test_load_corpus_empty(self, mock_file, mock_path):
        """Test empty corpus."""
        mock_path.exists.return_value = True
        mock_file.return_value.read.return_value = json.dumps([])

        with patch("src.ai.build_vector_store.json.load", return_value=[]):
            with pytest.raises(ValueError, match="Corpus is empty"):
                load_embedding_corpus()

    @patch("src.ai.build_vector_store.EMBEDDING_CORPUS_PATH")
    @patch("builtins.open", new_callable=mock_open)
    def test_load_corpus_missing_field(self, mock_file, mock_path):
        """Test corpus document missing required field."""
        mock_path.exists.return_value = True
        corpus_data = [{"doc_id": "test_1", "type": "film"}]  # Missing name, text, metadata
        mock_file.return_value.read.return_value = json.dumps(corpus_data)

        with patch("src.ai.build_vector_store.json.load", return_value=corpus_data):
            with pytest.raises(ValueError, match="missing required field"):
                load_embedding_corpus()

    @patch("src.ai.build_vector_store.EMBEDDING_CORPUS_PATH")
    @patch("builtins.open", new_callable=mock_open)
    def test_load_corpus_empty_text(self, mock_file, mock_path):
        """Test corpus document with empty text."""
        mock_path.exists.return_value = True
        corpus_data = [
            {
                "doc_id": "test_1",
                "type": "film",
                "name": "Test",
                "text": "",
                "metadata": {},
            }
        ]
        mock_file.return_value.read.return_value = json.dumps(corpus_data)

        with patch("src.ai.build_vector_store.json.load", return_value=corpus_data):
            with pytest.raises(ValueError, match="empty text field"):
                load_embedding_corpus()


class TestAddDocumentsToCollection:
    """Tests for add_documents_to_collection function."""

    def test_add_documents_success(
        self, mock_collection, sample_embeddings, sample_corpus
    ):
        """Test successful document addition."""
        add_documents_to_collection(mock_collection, sample_embeddings, sample_corpus)

        # Should be called once for batch of 10 documents (< BATCH_SIZE)
        assert mock_collection.add.call_count == 1

        # Verify call arguments
        call_args = mock_collection.add.call_args[1]
        assert len(call_args["ids"]) == 10
        assert len(call_args["documents"]) == 10
        assert len(call_args["metadatas"]) == 10
        assert len(call_args["embeddings"]) == 10

    def test_add_documents_mismatch(
        self, mock_collection, sample_embeddings, sample_corpus
    ):
        """Test document addition with count mismatch."""
        wrong_corpus = sample_corpus[:5]  # Only 5 docs vs 10 embeddings

        with pytest.raises(ValueError, match="does not match"):
            add_documents_to_collection(mock_collection, sample_embeddings, wrong_corpus)

    def test_add_documents_batching(self, mock_collection):
        """Test document batching logic."""
        # Create corpus larger than BATCH_SIZE
        num_docs = BATCH_SIZE + 50
        embeddings = np.random.rand(num_docs, EMBEDDING_DIMENSION).astype(np.float32)
        corpus = [
            {
                "doc_id": f"doc_{i}",
                "type": "test",
                "name": f"Test {i}",
                "text": f"Content {i}",
                "metadata": {"film_id": "", "source": "test"},
            }
            for i in range(num_docs)
        ]

        add_documents_to_collection(mock_collection, embeddings, corpus)

        # Should be called twice (100 + 50)
        assert mock_collection.add.call_count == 2

        # Verify first batch has BATCH_SIZE documents
        first_call_args = mock_collection.add.call_args_list[0][1]
        assert len(first_call_args["ids"]) == BATCH_SIZE

        # Verify second batch has remaining documents
        second_call_args = mock_collection.add.call_args_list[1][1]
        assert len(second_call_args["ids"]) == 50


class TestSimilaritySearch:
    """Tests for test_similarity_search function."""

    def test_similarity_search_mononoke_found(self, mock_collection):
        """Test similarity search with Princess Mononoke in results."""
        mock_collection.query.return_value = {
            "ids": [["doc_1", "doc_2", "doc_3", "doc_4", "doc_5"]],
            "documents": [["Text 1", "Text 2", "Text 3", "Text 4", "Text 5"]],
            "metadatas": [
                [
                    {"name": "San", "type": "character"},
                    {"name": "Other Character", "type": "character"},
                    {"name": "Another Film", "type": "film"},
                    {"name": "Location", "type": "location"},
                    {"name": "Quote", "type": "quote"},
                ]
            ],
            "distances": [[0.1, 0.2, 0.3, 0.4, 0.5]],
        }

        # Should not raise exception
        validate_similarity_search(mock_collection)

        mock_collection.query.assert_called_once()

    def test_similarity_search_mononoke_not_found(self, mock_collection):
        """Test similarity search without Princess Mononoke in top 3."""
        mock_collection.query.return_value = {
            "ids": [["doc_1", "doc_2", "doc_3"]],
            "documents": [["Text 1", "Text 2", "Text 3"]],
            "metadatas": [
                [
                    {"name": "Other Character", "type": "character"},
                    {"name": "Another Film", "type": "film"},
                    {"name": "Location", "type": "location"},
                ]
            ],
            "distances": [[0.1, 0.2, 0.3]],
        }

        # Should log warning but not raise exception
        validate_similarity_search(mock_collection)


class TestGenerateCollectionStats:
    """Tests for generate_collection_stats function."""

    @patch("src.ai.build_vector_store.Path")
    @patch("builtins.open", new_callable=mock_open)
    def test_generate_stats_success(self, mock_file, mock_path, mock_collection):
        """Test successful stats generation."""
        mock_collection.get.return_value = {
            "ids": ["doc_1", "doc_2", "doc_3"],
            "metadatas": [
                {"type": "film"},
                {"type": "character"},
                {"type": "character"},
            ],
        }

        mock_collection.query.return_value = {
            "ids": [["doc_1"]],
            "documents": [["test"]],
        }

        # Mock file size calculation
        mock_path_instance = MagicMock()
        mock_path.return_value = mock_path_instance
        mock_rglob = MagicMock()
        mock_file_obj = MagicMock()
        mock_file_obj.is_file.return_value = True
        mock_file_obj.stat.return_value.st_size = 1024 * 1024  # 1 MB
        mock_rglob.return_value = [mock_file_obj]
        mock_path_instance.rglob = mock_rglob

        stats = generate_collection_stats(mock_collection)

        assert stats["total_documents"] == 3
        assert stats["documents_by_type"]["film"] == 1
        assert stats["documents_by_type"]["character"] == 2
        assert "collection_size_mb" in stats
        assert "average_query_time_ms" in stats


class TestValidateDocuments:
    """Tests for validate_documents function."""

    def test_validate_documents_success(self, mock_collection, sample_corpus):
        """Test successful document validation."""
        # Mock collection.get to return matching text
        def mock_get(ids):
            doc_id = ids[0]
            doc = next(d for d in sample_corpus if d["doc_id"] == doc_id)
            return {"ids": [doc_id], "documents": [doc["text"]]}

        mock_collection.get = mock_get

        # Should not raise exception
        validate_documents(mock_collection, sample_corpus)

    def test_validate_documents_not_found(self, mock_collection, sample_corpus):
        """Test document validation when document not found."""
        mock_collection.get.return_value = {"ids": [], "documents": []}

        with pytest.raises(AssertionError, match="not found in collection"):
            validate_documents(mock_collection, sample_corpus)

    def test_validate_documents_text_mismatch(self, mock_collection, sample_corpus):
        """Test document validation with text mismatch."""

        def mock_get(ids):
            return {"ids": ids, "documents": ["Different text content"]}

        mock_collection.get = mock_get

        with pytest.raises(AssertionError, match="Text mismatch"):
            validate_documents(mock_collection, sample_corpus)
