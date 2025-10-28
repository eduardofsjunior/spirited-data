"""
Unit tests for load_kaggle_data module.

Tests CSV loading, validation, data cleaning, type conversions,
and cross-reference matching logic.
"""

import json
import os
import tempfile
from pathlib import Path

import pandas as pd
import pytest

from src.ingestion.load_kaggle_data import (
    clean_currency,
    convert_data_types,
    cross_reference_with_ghibli_api,
    handle_missing_values,
    load_kaggle_csv,
    normalize_title,
    safe_int_convert,
    save_cleaned_data,
    validate_required_columns,
)
from src.shared.exceptions import DataValidationError


class TestLoadKaggleCSV:
    """Tests for load_kaggle_csv function."""

    def test_load_valid_csv(self):
        """Test loading a valid CSV file."""
        csv_path = "tests/fixtures/kaggle_sample.csv"
        df = load_kaggle_csv(csv_path)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 3
        assert "Name" in df.columns
        assert "Year" in df.columns
        assert "Director" in df.columns

    def test_load_csv_with_utf8_encoding(self):
        """Test CSV loads with UTF-8 encoding (default)."""
        csv_path = "tests/fixtures/kaggle_sample.csv"
        df = load_kaggle_csv(csv_path)

        # Should successfully load
        assert len(df) > 0
        assert df["Name"].iloc[0] == "Spirited Away"

    def test_load_nonexistent_file(self):
        """Test FileNotFoundError when CSV doesn't exist."""
        with pytest.raises(FileNotFoundError) as exc_info:
            load_kaggle_csv("nonexistent_file.csv")

        assert "not found" in str(exc_info.value).lower()

    def test_load_empty_csv(self):
        """Test pd.errors.EmptyDataError when CSV is empty."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            temp_path = f.name
            # Write nothing (empty file)

        try:
            with pytest.raises(pd.errors.EmptyDataError):
                load_kaggle_csv(temp_path)
        finally:
            os.unlink(temp_path)


class TestValidateRequiredColumns:
    """Tests for validate_required_columns function."""

    def test_all_columns_present(self):
        """Test validation passes when all required columns exist."""
        df = pd.DataFrame({
            "Name": ["Film A"],
            "Year": [2000],
            "Director": ["Director A"],
            "Budget": [10000000],
        })

        result = validate_required_columns(df)
        assert result is True

    def test_missing_column_raises_error(self):
        """Test DataValidationError when required column missing."""
        df = pd.DataFrame({
            "Name": ["Film A"],
            "Year": [2000],
            # Missing "Director"
        })

        with pytest.raises(DataValidationError) as exc_info:
            validate_required_columns(df)

        assert "Missing required columns" in str(exc_info.value)
        assert "Director" in str(exc_info.value)

    def test_multiple_missing_columns(self):
        """Test error message includes all missing columns."""
        df = pd.DataFrame({
            "Name": ["Film A"],
            # Missing "Year" and "Director"
        })

        with pytest.raises(DataValidationError) as exc_info:
            validate_required_columns(df)

        error_msg = str(exc_info.value)
        assert "Year" in error_msg
        assert "Director" in error_msg


class TestHandleMissingValues:
    """Tests for handle_missing_values function."""

    def test_fill_numeric_with_zero(self):
        """Test numeric fields filled with 0."""
        df = pd.DataFrame({
            "Name": ["Film A"],
            "Year": [None],
            "Budget": [None],
            "Revenue": [None],
        })

        cleaned = handle_missing_values(df)

        assert cleaned["Year"].iloc[0] == 0
        assert cleaned["Budget"].iloc[0] == 0
        assert cleaned["Revenue"].iloc[0] == 0

    def test_fill_string_with_unknown(self):
        """Test string fields filled with 'Unknown'."""
        df = pd.DataFrame({
            "Name": [None],
            "Director": [None],
            "Screenplay": [None],
            "Genre 1": [None],
        })

        cleaned = handle_missing_values(df)

        assert cleaned["Name"].iloc[0] == "Unknown"
        assert cleaned["Director"].iloc[0] == "Unknown"
        assert cleaned["Screenplay"].iloc[0] == "Unknown"
        assert cleaned["Genre 1"].iloc[0] == "Unknown"

    def test_mixed_missing_values(self):
        """Test handling DataFrame with mixed missing values."""
        csv_path = "tests/fixtures/kaggle_missing_values.csv"
        df = load_kaggle_csv(csv_path)

        cleaned = handle_missing_values(df)

        # Check no NaN values remain
        assert cleaned.isnull().sum().sum() == 0

        # Check correct fill strategies applied
        assert cleaned["Name"].iloc[1] == "Unknown"  # String filled
        assert cleaned["Year"].iloc[2] == 0  # Numeric filled


class TestConvertDataTypes:
    """Tests for convert_data_types function."""

    def test_convert_year_to_int(self):
        """Test Year column converted to integer."""
        df = pd.DataFrame({
            "Year": ["2001", "1988.0", 1997]
        })

        converted = convert_data_types(df)

        assert converted["Year"].dtype == "int64"
        assert converted["Year"].iloc[0] == 2001
        assert converted["Year"].iloc[1] == 1988

    def test_convert_budget_to_float(self):
        """Test Budget converted to float with currency cleaning."""
        df = pd.DataFrame({
            "Budget": ["$10000000", "$1,000,000", "5000000.00"]
        })

        converted = convert_data_types(df)

        assert converted["Budget"].dtype == "float64"
        assert converted["Budget"].iloc[0] == 10000000.0
        assert converted["Budget"].iloc[1] == 1000000.0

    def test_convert_revenue_to_float(self):
        """Test Revenue converted to float."""
        df = pd.DataFrame({
            "Revenue": ["$289900000", "30476000", "$235,200,000"]
        })

        converted = convert_data_types(df)

        assert converted["Revenue"].dtype == "float64"
        assert converted["Revenue"].iloc[0] == 289900000.0
        assert converted["Revenue"].iloc[2] == 235200000.0

    def test_handle_invalid_year_gracefully(self):
        """Test invalid year strings handled without crashing."""
        df = pd.DataFrame({
            "Year": ["invalid", "", None]
        })

        converted = convert_data_types(df)

        # Should default to 0 for invalid values
        assert converted["Year"].iloc[0] == 0
        assert converted["Year"].iloc[1] == 0
        assert converted["Year"].iloc[2] == 0


class TestCleanCurrency:
    """Tests for clean_currency utility function."""

    def test_clean_dollar_sign(self):
        """Test $ symbol removed."""
        assert clean_currency("$10000000") == 10000000.0

    def test_clean_commas(self):
        """Test commas removed."""
        assert clean_currency("1,000,000") == 1000000.0

    def test_clean_both_dollar_and_commas(self):
        """Test both $ and commas removed."""
        assert clean_currency("$289,900,000") == 289900000.0

    def test_clean_plain_number(self):
        """Test plain number string converted."""
        assert clean_currency("5000000") == 5000000.0

    def test_clean_empty_string(self):
        """Test empty string returns 0.0."""
        assert clean_currency("") == 0.0

    def test_clean_none(self):
        """Test None returns 0.0."""
        assert clean_currency(None) == 0.0

    def test_clean_invalid_value(self):
        """Test invalid value returns 0.0 with warning."""
        assert clean_currency("not a number") == 0.0


class TestSafeIntConvert:
    """Tests for safe_int_convert utility function."""

    def test_convert_int_string(self):
        """Test integer string converted."""
        assert safe_int_convert("2001") == 2001

    def test_convert_float_string(self):
        """Test float string converted to int."""
        assert safe_int_convert("1988.0") == 1988

    def test_convert_int(self):
        """Test int passed through."""
        assert safe_int_convert(1997) == 1997

    def test_convert_empty_string(self):
        """Test empty string returns default."""
        assert safe_int_convert("") == 0
        assert safe_int_convert("", default=99) == 99

    def test_convert_none(self):
        """Test None returns default."""
        assert safe_int_convert(None) == 0

    def test_convert_invalid_string(self):
        """Test invalid string returns default."""
        assert safe_int_convert("invalid") == 0


class TestNormalizeTitle:
    """Tests for normalize_title function."""

    def test_normalize_lowercase(self):
        """Test title converted to lowercase."""
        assert normalize_title("Spirited Away") == "spirited away"

    def test_normalize_remove_newlines(self):
        """Test newlines removed."""
        assert normalize_title("Spirited Away\n       (2001)") == "spirited away"

    def test_normalize_remove_year_suffix(self):
        """Test year in parentheses removed."""
        assert normalize_title("The Wind Rises (2013)") == "the wind rises"

    def test_normalize_extra_whitespace(self):
        """Test extra whitespace collapsed."""
        assert normalize_title("My  Neighbor   Totoro") == "my neighbor totoro"

    def test_normalize_none(self):
        """Test None returns empty string."""
        assert normalize_title(None) == ""

    def test_normalize_mixed_formatting(self):
        """Test complex title normalization."""
        messy_title = "Princess Mononoke\n       (1997)   "
        assert normalize_title(messy_title) == "princess mononoke"

    def test_normalize_year_without_parentheses(self):
        """Test year removal when not in parentheses."""
        assert normalize_title("Ocean Waves 1994") == "ocean waves"

    def test_normalize_special_characters(self):
        """Test special character transliteration."""
        assert normalize_title("Nausicaä of the Valley") == "nausicaa of the valley"
        assert normalize_title("Château d'été") == "chateau d'ete"

    def test_normalize_real_kaggle_format(self):
        """Test with actual malformed Kaggle CSV format."""
        # Real example from Kaggle dataset
        actual = "The Secret World of Arrietty\n       (2010)"
        assert normalize_title(actual) == "the secret world of arrietty"


class TestCrossReferenceWithGhibliAPI:
    """Tests for cross_reference_with_ghibli_api function."""

    def test_cross_reference_with_real_data(self):
        """Test cross-reference with actual API cache."""
        # Load actual Kaggle data
        kaggle_df = load_kaggle_csv("data/raw/kaggle/studio_ghibli_films.csv")

        report = cross_reference_with_ghibli_api(kaggle_df)

        # Verify report structure
        assert "matched_count" in report
        assert "total_kaggle" in report
        assert "total_api" in report
        assert "match_percentage" in report
        assert "kaggle_only" in report
        assert "api_only" in report

        # Verify types
        assert isinstance(report["matched_count"], int)
        assert isinstance(report["match_percentage"], float)
        assert isinstance(report["kaggle_only"], list)
        assert isinstance(report["api_only"], list)

        # Verify reasonable values
        assert report["matched_count"] > 0
        assert report["total_kaggle"] == len(kaggle_df)
        assert report["match_percentage"] <= 100.0

    def test_cross_reference_perfect_match(self):
        """Test cross-reference with synthetic perfect match."""
        # Create temp API films file
        temp_api_dir = tempfile.mkdtemp()
        temp_api_file = os.path.join(temp_api_dir, "films.json")

        api_films = [
            {"title": "Spirited Away"},
            {"title": "Princess Mononoke"},
        ]

        with open(temp_api_file, "w") as f:
            json.dump(api_films, f)

        # Create matching Kaggle data
        kaggle_df = pd.DataFrame({
            "Name": ["Spirited Away (2001)", "Princess Mononoke (1997)"]
        })

        # Temporarily override API_FILMS_PATH
        import src.ingestion.load_kaggle_data as module
        original_path = module.API_FILMS_PATH
        module.API_FILMS_PATH = temp_api_file

        try:
            report = cross_reference_with_ghibli_api(kaggle_df)

            assert report["matched_count"] == 2
            assert report["match_percentage"] == 100.0
            assert len(report["kaggle_only"]) == 0
            assert len(report["api_only"]) == 0
        finally:
            module.API_FILMS_PATH = original_path
            os.unlink(temp_api_file)
            os.rmdir(temp_api_dir)

    def test_cross_reference_api_file_not_found(self):
        """Test error when API films cache doesn't exist."""
        import src.ingestion.load_kaggle_data as module
        original_path = module.API_FILMS_PATH
        module.API_FILMS_PATH = "nonexistent_path.json"

        try:
            df = pd.DataFrame({"Name": ["Film A"]})
            with pytest.raises(FileNotFoundError) as exc_info:
                cross_reference_with_ghibli_api(df)

            assert "not found" in str(exc_info.value).lower()
        finally:
            module.API_FILMS_PATH = original_path


class TestSaveCleanedData:
    """Tests for save_cleaned_data function."""

    def test_save_csv_and_summary(self):
        """Test cleaned CSV and validation summary saved."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create test data
            df = pd.DataFrame({
                "Name": ["Film A", "Film B"],
                "Year": [2000, 2010],
                "Director": ["Director A", "Director B"],
            })

            cross_ref = {
                "matched_count": 2,
                "total_kaggle": 2,
                "total_api": 2,
                "match_percentage": 100.0,
                "kaggle_only": [],
                "api_only": [],
            }

            csv_path = os.path.join(temp_dir, "test_output.csv")
            json_path = os.path.join(temp_dir, "test_summary.json")

            # Override output paths
            import src.ingestion.load_kaggle_data as module
            original_summary_path = module.SUMMARY_OUTPUT_PATH
            module.SUMMARY_OUTPUT_PATH = json_path

            try:
                save_cleaned_data(df, csv_path, cross_ref)

                # Verify CSV saved
                assert os.path.exists(csv_path)
                saved_df = pd.read_csv(csv_path)
                assert len(saved_df) == 2

                # Verify JSON summary saved
                assert os.path.exists(json_path)
                with open(json_path) as f:
                    summary = json.load(f)

                assert summary["row_count"] == 2
                assert summary["column_count"] == 3
                assert "cross_reference" in summary
                assert summary["cross_reference"]["matched_count"] == 2
            finally:
                module.SUMMARY_OUTPUT_PATH = original_summary_path


class TestIntegration:
    """Integration tests for full pipeline."""

    def test_full_pipeline_with_fixture(self):
        """Test complete pipeline with fixture data."""
        # This would be better as an integration test
        # but including here for completeness
        csv_path = "tests/fixtures/kaggle_sample.csv"

        # Load
        df = load_kaggle_csv(csv_path)

        # Validate
        validate_required_columns(df)

        # Clean
        df = handle_missing_values(df)
        df = convert_data_types(df)

        # Verify pipeline output
        assert len(df) == 3
        assert df["Year"].dtype == "int64"
        assert df["Budget"].dtype == "float64"
        assert df.isnull().sum().sum() == 0  # No missing values
