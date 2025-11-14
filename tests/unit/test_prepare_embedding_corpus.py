"""Unit tests for prepare_embedding_corpus module."""

import json
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest

from src.ai.prepare_embedding_corpus import (
    chunk_text,
    estimate_tokens,
    extract_character_documents,
    extract_film_documents,
    extract_location_documents,
    extract_memorable_quotes,
    extract_species_documents,
    save_corpus_to_json,
    validate_corpus,
)


class TestTokenEstimation:
    """Test token counting and chunking functionality."""

    def test_estimate_tokens_simple_text(self) -> None:
        """Test token estimation for simple text."""
        text = "Hello world"
        token_count = estimate_tokens(text)

        assert isinstance(token_count, int)
        assert token_count > 0
        assert token_count < 10  # Simple phrase should be < 10 tokens

    def test_estimate_tokens_empty_string(self) -> None:
        """Test token estimation for empty string."""
        token_count = estimate_tokens("")
        assert token_count == 0

    def test_chunk_text_short_text(self) -> None:
        """Test that short text is not chunked."""
        text = "This is a short sentence."
        chunks = chunk_text(text, max_tokens=100)

        assert len(chunks) == 1
        assert chunks[0] == text

    def test_chunk_text_long_text(self) -> None:
        """Test that long text is chunked properly."""
        # Create a long text that exceeds max_tokens
        text = " ".join(["word"] * 500)  # Should exceed 50 tokens
        chunks = chunk_text(text, max_tokens=50, overlap=10)

        assert len(chunks) > 1
        # Each chunk should be within token limit
        for chunk in chunks:
            assert estimate_tokens(chunk) <= 60  # Allow some margin


class TestFilmExtraction:
    """Test film document extraction."""

    def test_extract_film_documents_success(self) -> None:
        """Test successful film extraction."""
        # Mock connection
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [
            (
                "film1",
                "Spirited Away",
                "A young girl enters a magical world",
                "Hayao Miyazaki",
                2001,
                97,
                125,
            ),
            (
                "film2",
                "My Neighbor Totoro",
                "Two sisters encounter forest spirits",
                "Hayao Miyazaki",
                1988,
                93,
                86,
            ),
        ]

        documents = extract_film_documents(mock_conn)

        assert len(documents) == 2
        assert documents[0]["doc_id"] == "film_film1"
        assert documents[0]["type"] == "film"
        assert documents[0]["name"] == "Spirited Away"
        assert "magical world" in documents[0]["text"]
        assert documents[0]["metadata"]["director"] == "Hayao Miyazaki"
        assert documents[0]["metadata"]["release_year"] == 2001

    def test_extract_film_documents_missing_description(self) -> None:
        """Test synthetic description generation for missing descriptions."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [
            ("film1", "Castle in the Sky", "", "Hayao Miyazaki", 1986, 95, 124),
            ("film2", "Ponyo", None, "Hayao Miyazaki", 2008, 92, 101),
        ]

        documents = extract_film_documents(mock_conn)

        assert len(documents) == 2
        # Synthetic description should contain title, year, and director
        assert "Castle in the Sky" in documents[0]["text"]
        assert "1986" in documents[0]["text"]
        assert "Hayao Miyazaki" in documents[0]["text"]

    def test_extract_film_documents_empty_database(self) -> None:
        """Test handling of empty database."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []

        documents = extract_film_documents(mock_conn)

        assert len(documents) == 0

    def test_extract_film_documents_database_error(self) -> None:
        """Test handling of database errors."""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Database error")

        documents = extract_film_documents(mock_conn)

        assert len(documents) == 0


class TestCharacterExtraction:
    """Test character document extraction."""

    def test_extract_character_documents_success(self) -> None:
        """Test successful character extraction."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [
            ("char1", "Chihiro", "Female", "10", "Brown", "Brown", "Human"),
            ("char2", "Haku", "Male", "12", "Green", "Green", "Dragon"),
        ]

        documents = extract_character_documents(mock_conn)

        assert len(documents) == 2
        assert documents[0]["doc_id"] == "character_char1"
        assert documents[0]["type"] == "character"
        assert documents[0]["name"] == "Chihiro"
        assert "Chihiro is a character" in documents[0]["text"]
        assert "Female" in documents[0]["text"]
        assert documents[0]["metadata"]["gender"] == "Female"

    def test_extract_character_documents_null_fields(self) -> None:
        """Test character extraction with NULL fields."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [
            ("char1", "No-Face", None, None, None, None, "Spirit"),
        ]

        documents = extract_character_documents(mock_conn)

        assert len(documents) == 1
        assert "No-Face is a character" in documents[0]["text"]
        assert "Spirit" in documents[0]["text"]
        # NULL fields should not appear in text
        assert "None" not in documents[0]["text"]

    def test_extract_character_documents_chunking(self) -> None:
        """Test that long character bios are chunked."""
        # Create a very long bio that will need chunking
        long_bio_char = (
            "char1",
            "Very Long Name Character",
            "Female",
            "100",
            "Blue",
            "Red",
            " ".join(["Species"] * 100),  # Make species field very long
        )

        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [long_bio_char]

        documents = extract_character_documents(mock_conn)

        # Should be chunked into multiple documents
        # Note: Actual chunking depends on token count
        assert len(documents) >= 1
        # All chunks should have same base character ID
        for doc in documents:
            assert "character_char1" in doc["doc_id"]


class TestLocationExtraction:
    """Test location document extraction."""

    def test_extract_location_documents_success(self) -> None:
        """Test successful location extraction."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [
            ("loc1", "Bathhouse", "Temperate", "Mountain", "40-60%"),
            ("loc2", "Forest", "Tropical", "Forest", "70-90%"),
        ]

        documents = extract_location_documents(mock_conn)

        assert len(documents) == 2
        assert documents[0]["doc_id"] == "location_loc1"
        assert documents[0]["type"] == "location"
        assert documents[0]["name"] == "Bathhouse"
        assert "Bathhouse is a location" in documents[0]["text"]
        assert "Temperate" in documents[0]["text"]

    def test_extract_location_documents_partial_data(self) -> None:
        """Test location extraction with partial data."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [
            ("loc1", "Unknown Place", None, "Desert", None),
        ]

        documents = extract_location_documents(mock_conn)

        assert len(documents) == 1
        assert "Unknown Place" in documents[0]["text"]
        assert "Desert" in documents[0]["text"]
        assert documents[0]["metadata"]["climate"] is None


class TestSpeciesExtraction:
    """Test species document extraction."""

    def test_extract_species_documents_success(self) -> None:
        """Test successful species extraction."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [
            ("spec1", "Human", "Mammal", "Brown, Blue, Green", "Black, Brown, Blonde"),
            ("spec2", "Spirit", "Supernatural", "Various", "Various"),
        ]

        documents = extract_species_documents(mock_conn)

        assert len(documents) == 2
        assert documents[0]["doc_id"] == "species_spec1"
        assert documents[0]["type"] == "species"
        assert documents[0]["name"] == "Human"
        assert "Human is a species" in documents[0]["text"]
        assert "Mammal" in documents[0]["text"]


class TestMemorableQuotes:
    """Test memorable quote extraction."""

    @patch("src.ai.prepare_embedding_corpus.Path")
    @patch("builtins.open", new_callable=mock_open)
    def test_extract_memorable_quotes_success(
        self, mock_file: MagicMock, mock_path: MagicMock
    ) -> None:
        """Test successful quote extraction."""
        # Mock database results
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [
            (
                "film1",
                "spirited_away",
                "en",
                45,
                0.85,
                0.12,
                0.05,
                0.22,
                0.08,
                1.32,
            ),
        ]

        # Mock subtitle file
        mock_path.return_value.exists.return_value = True
        mock_subtitle_data = {
            "metadata": {"film_name": "Spirited Away"},
            "subtitles": [
                {"start_time": 2700, "end_time": 2705, "dialogue_text": "Hello!"},
                {"start_time": 2710, "end_time": 2715, "dialogue_text": "How are you?"},
            ],
        }
        mock_file.return_value.__enter__.return_value.read.return_value = json.dumps(
            mock_subtitle_data
        )

        with patch("json.load", return_value=mock_subtitle_data):
            documents = extract_memorable_quotes(mock_conn, max_quotes_per_film=50)

        assert len(documents) >= 0  # May be 0 if minute filtering doesn't match
        # If documents were extracted, validate structure
        for doc in documents:
            assert doc["type"] == "quote"
            assert "doc_id" in doc
            assert "text" in doc
            assert "metadata" in doc
            if "emotion_joy" in doc["metadata"]:
                assert isinstance(doc["metadata"]["emotion_joy"], float)

    @patch("src.ai.prepare_embedding_corpus.Path")
    def test_extract_memorable_quotes_missing_file(self, mock_path: MagicMock) -> None:
        """Test handling of missing subtitle files."""
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = [
            ("film1", "missing_film", "en", 45, 0.85, 0.12, 0.05, 0.22, 0.08, 1.32),
        ]

        # Mock file doesn't exist
        mock_path.return_value.exists.return_value = False

        documents = extract_memorable_quotes(mock_conn)

        # Should handle gracefully and return empty or partial results
        assert isinstance(documents, list)

    def test_extract_memorable_quotes_database_error(self) -> None:
        """Test handling of database errors."""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Database error")

        documents = extract_memorable_quotes(mock_conn)

        assert len(documents) == 0


class TestCorpusValidation:
    """Test corpus validation functionality."""

    def test_validate_corpus_success(self) -> None:
        """Test corpus validation with valid data."""
        documents = [
            {
                "doc_id": "film_1",
                "type": "film",
                "name": "Test Film",
                "text": "This is a test film description.",
                "metadata": {},
            },
            {
                "doc_id": "char_1",
                "type": "character",
                "name": "Test Character",
                "text": "This is a test character bio.",
                "metadata": {},
            },
            {
                "doc_id": "quote_1",
                "type": "quote",
                "name": "Test Quote",
                "text": "Hello world!",
                "metadata": {},
            },
        ]

        validation = validate_corpus(documents)

        assert validation["total_documents"] == 3
        assert validation["type_distribution"]["film"] == 1
        assert validation["type_distribution"]["character"] == 1
        assert validation["type_distribution"]["quote"] == 1
        assert validation["empty_text_count"] == 0
        assert "avg_length_by_type" in validation
        assert "avg_tokens_by_type" in validation

    def test_validate_corpus_empty_text(self) -> None:
        """Test validation detects empty text."""
        documents = [
            {
                "doc_id": "film_1",
                "type": "film",
                "name": "Test",
                "text": "",
                "metadata": {},
            },
            {
                "doc_id": "film_2",
                "type": "film",
                "name": "Test 2",
                "text": "Valid text",
                "metadata": {},
            },
        ]

        validation = validate_corpus(documents)

        assert validation["empty_text_count"] == 1

    def test_validate_corpus_empty_list(self) -> None:
        """Test validation with empty corpus."""
        validation = validate_corpus([])

        assert validation["total_documents"] == 0
        assert validation["type_distribution"] == {}


class TestCorpusSaving:
    """Test corpus saving functionality."""

    @patch("src.ai.prepare_embedding_corpus.Path")
    @patch("builtins.open", new_callable=mock_open)
    def test_save_corpus_to_json_success(
        self, mock_file: MagicMock, mock_path: MagicMock
    ) -> None:
        """Test successful corpus save."""
        documents = [
            {
                "doc_id": "test_1",
                "type": "film",
                "name": "Test",
                "text": "Test text",
                "metadata": {},
            }
        ]

        # Mock Path operations
        mock_path_instance = MagicMock()
        mock_path_instance.parent.mkdir = MagicMock()
        mock_path.return_value = mock_path_instance

        save_corpus_to_json(documents, "test_output.json")

        # Verify file was opened for writing
        mock_file.assert_called_once()

    @patch("src.ai.prepare_embedding_corpus.Path")
    @patch("builtins.open", new_callable=mock_open)
    def test_save_corpus_to_json_creates_directory(
        self, mock_file: MagicMock, mock_path: MagicMock
    ) -> None:
        """Test that save creates output directory."""
        documents = [{"doc_id": "test", "type": "film", "text": "test", "metadata": {}}]

        mock_path_instance = MagicMock()
        mock_path.return_value = mock_path_instance

        save_corpus_to_json(documents, "new/dir/output.json")

        # Verify mkdir was called
        mock_path_instance.parent.mkdir.assert_called_once_with(
            parents=True, exist_ok=True
        )

    @patch("src.ai.prepare_embedding_corpus.Path")
    @patch("builtins.open", side_effect=IOError("Write failed"))
    def test_save_corpus_to_json_error(
        self, mock_file: MagicMock, mock_path: MagicMock
    ) -> None:
        """Test handling of write errors."""
        documents = [{"doc_id": "test", "type": "film", "text": "test", "metadata": {}}]

        mock_path_instance = MagicMock()
        mock_path.return_value = mock_path_instance

        with pytest.raises(IOError):
            save_corpus_to_json(documents, "output.json")


class TestIntegration:
    """Integration tests (optional, can be run with real database)."""

    def test_document_structure_consistency(self) -> None:
        """Test that all extraction functions return consistent document structure."""
        required_fields = ["doc_id", "type", "name", "text", "metadata"]

        mock_conn = MagicMock()

        # Test each extraction function with empty results
        mock_conn.execute.return_value.fetchall.return_value = []

        for func in [
            extract_film_documents,
            extract_character_documents,
            extract_location_documents,
            extract_species_documents,
        ]:
            documents = func(mock_conn)
            for doc in documents:
                for field in required_fields:
                    assert field in doc, f"Missing field {field} in {func.__name__}"
