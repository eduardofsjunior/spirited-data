"""
Unit tests for Ghibli API data ingestion module.

Tests cover API fetching, caching, validation, error handling, and retry logic.
"""

import json
import time
from pathlib import Path
from unittest.mock import Mock, patch, mock_open

import pytest
import requests

from src.ingestion.fetch_ghibli_api import (
    fetch_endpoint,
    should_fetch,
    load_cached_data,
    save_endpoint_data,
    validate_film_data,
    save_metadata,
    main,
)
from src.shared.exceptions import DataIngestionError, DataValidationError


# Test Data
SAMPLE_FILMS = [
    {
        "id": "2baf70d1-42bb-4437-b551-e5fed5a87abe",
        "title": "Spirited Away",
        "director": "Hayao Miyazaki",
        "release_date": "2001",
    },
    {
        "id": "12cfb892-aac0-4c5b-94af-521852e46d6a",
        "title": "My Neighbor Totoro",
        "director": "Hayao Miyazaki",
        "release_date": "1988",
    },
    {
        "id": "58611129-2dbc-4a81-a72f-77ddfc1b1b49",
        "title": "Princess Mononoke",
        "director": "Hayao Miyazaki",
        "release_date": "1997",
    },
]


class TestFetchEndpoint:
    """Tests for fetch_endpoint function."""

    @patch("src.ingestion.fetch_ghibli_api.requests.get")
    def test_fetch_endpoint_success(self, mock_get):
        """Test successful API fetch."""
        mock_response = Mock()
        mock_response.json.return_value = SAMPLE_FILMS
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = fetch_endpoint("films")

        assert len(result) == 3
        assert result[0]["title"] == "Spirited Away"
        mock_get.assert_called_once()

    @patch("src.ingestion.fetch_ghibli_api.requests.get")
    @patch("src.ingestion.fetch_ghibli_api.time.sleep")
    def test_fetch_endpoint_timeout_retry(self, mock_sleep, mock_get):
        """Test timeout with retry logic."""
        # Simulate 3 timeouts
        mock_get.side_effect = requests.Timeout("Connection timeout")

        with pytest.raises(DataIngestionError, match="timed out"):
            fetch_endpoint("films")

        # Should retry 3 times
        assert mock_get.call_count == 3
        # Should sleep 2 times (not after last attempt)
        assert mock_sleep.call_count == 2

    @patch("src.ingestion.fetch_ghibli_api.requests.get")
    def test_fetch_endpoint_404_no_retry(self, mock_get):
        """Test 404 returns empty list without retry."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.HTTPError(response=mock_response)
        mock_get.return_value = mock_response

        result = fetch_endpoint("films")

        assert result == []
        # Should not retry on 404
        assert mock_get.call_count == 1

    @patch("src.ingestion.fetch_ghibli_api.requests.get")
    @patch("src.ingestion.fetch_ghibli_api.time.sleep")
    def test_fetch_endpoint_500_retry(self, mock_sleep, mock_get):
        """Test 500 error retries with exponential backoff."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.HTTPError(response=mock_response)
        mock_get.return_value = mock_response

        with pytest.raises(DataIngestionError, match="temporarily unavailable"):
            fetch_endpoint("films")

        assert mock_get.call_count == 3
        # Verify exponential backoff: 1s, 2s (no sleep after last)
        assert mock_sleep.call_count == 2
        mock_sleep.assert_any_call(1)
        mock_sleep.assert_any_call(2)

    @patch("src.ingestion.fetch_ghibli_api.requests.get")
    def test_fetch_endpoint_invalid_json(self, mock_get):
        """Test handling of invalid JSON response."""
        mock_response = Mock()
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        with pytest.raises(DataIngestionError, match="invalid data"):
            fetch_endpoint("films")


class TestCaching:
    """Tests for caching functionality."""

    def test_should_fetch_missing_file(self, tmp_path):
        """Test should_fetch returns True for missing file."""
        cache_file = tmp_path / "films.json"

        assert should_fetch(cache_file) is True

    def test_should_fetch_recent_file(self, tmp_path):
        """Test should_fetch returns False for recent file."""
        cache_file = tmp_path / "films.json"
        cache_file.write_text("[]")

        assert should_fetch(cache_file) is False

    def test_should_fetch_expired_file(self, tmp_path):
        """Test should_fetch returns True for expired file."""
        cache_file = tmp_path / "films.json"
        cache_file.write_text("[]")

        # Modify file time to 25 hours ago
        old_time = time.time() - (25 * 3600)
        import os

        os.utime(cache_file, (old_time, old_time))

        assert should_fetch(cache_file) is True

    def test_load_cached_data_success(self, tmp_path):
        """Test loading valid cached data."""
        cache_file = tmp_path / "films.json"
        cache_file.write_text(json.dumps(SAMPLE_FILMS))

        result = load_cached_data(cache_file)

        assert len(result) == 3
        assert result[0]["title"] == "Spirited Away"

    def test_load_cached_data_corrupted(self, tmp_path):
        """Test loading corrupted cache file."""
        cache_file = tmp_path / "films.json"
        cache_file.write_text("invalid json{")

        with pytest.raises(DataIngestionError, match="Corrupted cache"):
            load_cached_data(cache_file)


class TestDataSaving:
    """Tests for data saving functionality."""

    def test_save_endpoint_data_success(self, tmp_path):
        """Test successful data save."""
        save_endpoint_data("films", SAMPLE_FILMS, tmp_path)

        output_file = tmp_path / "films.json"
        assert output_file.exists()

        with open(output_file, "r") as f:
            saved_data = json.load(f)

        assert len(saved_data) == 3
        assert saved_data[0]["title"] == "Spirited Away"

    def test_save_endpoint_data_atomic_write(self, tmp_path):
        """Test atomic write with temp file."""
        save_endpoint_data("films", SAMPLE_FILMS, tmp_path)

        # Temp file should not exist after successful save
        temp_file = tmp_path / ".films.json.tmp"
        assert not temp_file.exists()

    def test_save_metadata_success(self, tmp_path):
        """Test metadata save."""
        metadata = {
            "films": {
                "fetch_timestamp": "2025-10-27T10:00:00",
                "record_count": 22,
                "api_version": "1.0",
                "cache_status": "fresh",
            }
        }

        save_metadata(metadata, tmp_path)

        metadata_file = tmp_path / "metadata.json"
        assert metadata_file.exists()

        with open(metadata_file, "r") as f:
            saved_metadata = json.load(f)

        assert saved_metadata["films"]["record_count"] == 22


class TestValidation:
    """Tests for data validation."""

    def test_validate_film_data_valid(self):
        """Test validation with valid film data."""
        valid_count, errors = validate_film_data(SAMPLE_FILMS)

        assert valid_count == 3
        assert len(errors) == 0

    def test_validate_film_data_missing_title(self):
        """Test validation catches missing title."""
        invalid_films = [
            {"id": "2baf70d1-42bb-4437-b551-e5fed5a87abe"},  # Missing title
        ]

        valid_count, errors = validate_film_data(invalid_films)

        assert valid_count == 0
        assert len(errors) == 1
        assert "title" in errors[0]

    def test_validate_film_data_missing_id(self):
        """Test validation catches missing id."""
        invalid_films = [
            {"title": "Test Film"},  # Missing id
        ]

        valid_count, errors = validate_film_data(invalid_films)

        assert valid_count == 0
        assert len(errors) == 1
        assert "id" in errors[0]

    def test_validate_film_data_invalid_uuid(self):
        """Test validation catches invalid UUID."""
        invalid_films = [
            {"id": "not-a-uuid", "title": "Test Film"},
        ]

        valid_count, errors = validate_film_data(invalid_films)

        assert valid_count == 0
        assert len(errors) == 1
        assert "UUID" in errors[0]

    def test_validate_film_data_all_invalid_logs(self):
        """Test validation logs error when all films invalid."""
        invalid_films = [
            {"id": "not-a-uuid", "title": "Test Film"},
            {"id": "also-invalid"},
        ]

        valid_count, errors = validate_film_data(invalid_films)

        assert valid_count == 0
        assert len(errors) == 2


class TestMainWorkflow:
    """Tests for main execution workflow."""

    @patch("src.ingestion.fetch_ghibli_api.fetch_endpoint")
    @patch("src.ingestion.fetch_ghibli_api.save_endpoint_data")
    @patch("src.ingestion.fetch_ghibli_api.save_metadata")
    @patch("src.ingestion.fetch_ghibli_api.should_fetch")
    def test_main_force_fetch(
        self, mock_should_fetch, mock_save_metadata, mock_save_data, mock_fetch
    ):
        """Test main with force fetch."""
        mock_fetch.return_value = SAMPLE_FILMS

        main(force_fetch=True)

        # Should fetch all 5 endpoints
        assert mock_fetch.call_count == 5
        assert mock_save_data.call_count == 5
        assert mock_save_metadata.call_count == 1

        # should_fetch should not be called when force=True
        assert mock_should_fetch.call_count == 0

    @patch("src.ingestion.fetch_ghibli_api.fetch_endpoint")
    @patch("src.ingestion.fetch_ghibli_api.load_cached_data")
    @patch("src.ingestion.fetch_ghibli_api.save_metadata")
    @patch("src.ingestion.fetch_ghibli_api.should_fetch")
    def test_main_use_cache(
        self, mock_should_fetch, mock_save_metadata, mock_load_cache, mock_fetch
    ):
        """Test main uses cache when available."""
        mock_should_fetch.return_value = False  # Cache is fresh
        mock_load_cache.return_value = SAMPLE_FILMS

        main(force_fetch=False)

        # Should not fetch when cache is fresh
        assert mock_fetch.call_count == 0
        # Should load from cache 5 times
        assert mock_load_cache.call_count == 5
        assert mock_save_metadata.call_count == 1
