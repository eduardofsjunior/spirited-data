"""
Unit tests for RAG system validation script.

Tests validation logic for sentiment-focused queries:
- Citation detection (table names, timestamps, IDs)
- Statistical term detection (correlation, p-value, z-score)
- Sentiment metric detection (compound_sentiment, variance, etc.)
- Expected element checking
- Validation scoring formula
"""

import pytest

from src.ai.validate_rag_system import (
    check_expected_elements,
    detect_citations,
    detect_interpretation,
    detect_sentiment_metrics,
    detect_statistics,
    validate_response,
)


class TestCitationDetection:
    """Test citation detection logic."""

    def test_detect_table_names(self):
        """Test detection of sentiment table names."""
        response = "Based on mart_sentiment_success_correlation and mart_film_sentiment_summary..."
        citations = detect_citations(response)

        assert citations["citation_count"] >= 2
        assert len(citations["tables_found"]) >= 2
        assert "mart_sentiment_success_correlation" in " ".join(citations["tables_found"]).lower()

    def test_detect_friendly_names(self):
        """Test detection of friendly archive names."""
        response = "This comes from my Emotion Archive, where I analyzed 50,000+ dialogue lines using Pattern Discovery Tools"
        citations = detect_citations(response)

        assert citations["citation_count"] >= 2
        assert len(citations["tables_found"]) >= 2

    def test_detect_timestamps(self):
        """Test detection of timestamp references."""
        response = "The peak occurs at minute 67 with sentiment 0.85"
        citations = detect_citations(response)

        assert citations["has_timestamps"] is True
        assert citations["citation_count"] > 0

    def test_detect_ids(self):
        """Test detection of film IDs."""
        response = "Film ID: 2baf70d1-42bb-4437-b551-e5fed5a87abe has high variance"
        citations = detect_citations(response)

        assert citations["has_ids"] is True
        assert citations["citation_count"] > 0

    def test_no_citations(self):
        """Test response with no citations."""
        response = "This is a general response without any data references"
        citations = detect_citations(response)

        assert citations["citation_count"] == 0
        assert len(citations["tables_found"]) == 0


class TestStatisticalDetection:
    """Test statistical term detection."""

    def test_detect_correlation(self):
        """Test detection of correlation coefficients."""
        response = "The correlation between sentiment and revenue is r=0.45"
        stats = detect_statistics(response)

        assert stats["has_correlation"] is True
        assert stats["stat_count"] > 0

    def test_detect_pvalue(self):
        """Test detection of p-values."""
        response = "Statistical significance: p-value = 0.03, which is p < 0.05"
        stats = detect_statistics(response)

        assert stats["has_pvalue"] is True
        assert stats["stat_count"] >= 2

    def test_detect_quartile(self):
        """Test detection of quartile references."""
        response = "Top-quartile films show higher sentiment"
        stats = detect_statistics(response)

        assert stats["has_quartile"] is True
        assert stats["stat_count"] > 0

    def test_detect_sample_size(self):
        """Test detection of sample size (n=)."""
        response = "Analysis of n=22 films shows correlation"
        stats = detect_statistics(response)

        assert stats["has_sample_size"] is True
        assert stats["stat_count"] > 0

    def test_no_statistics(self):
        """Test response with no statistical terms."""
        response = "This is a simple description without statistics"
        stats = detect_statistics(response)

        assert stats["stat_count"] == 0
        assert stats["has_correlation"] is False


class TestSentimentMetricDetection:
    """Test sentiment metric detection."""

    def test_detect_compound_sentiment(self):
        """Test detection of compound_sentiment."""
        response = "The avg_compound sentiment is 0.15"
        metrics = detect_sentiment_metrics(response)

        assert metrics["metric_count"] > 0
        assert len(metrics["metrics_found"]) > 0

    def test_detect_variance(self):
        """Test detection of sentiment_variance."""
        response = "Sentiment variance is highest in Spirited Away"
        metrics = detect_sentiment_metrics(response)

        assert metrics["metric_count"] > 0
        assert any("variance" in m.lower() for m in metrics["metrics_found"])

    def test_detect_trajectory_metrics(self):
        """Test detection of beginning/ending sentiment."""
        response = "Beginning_sentiment is -0.2, ending_sentiment is 0.3"
        metrics = detect_sentiment_metrics(response)

        assert metrics["metric_count"] >= 2

    def test_no_metrics(self):
        """Test response with no sentiment metrics."""
        response = "This is a general response about films without specific data"
        metrics = detect_sentiment_metrics(response)

        assert metrics["metric_count"] == 0


class TestInterpretationDetection:
    """Test interpretation detection logic."""

    def test_detect_interpretation_phrases(self):
        """Test detection of interpretation phrases."""
        response = "Based on this emotional pattern, I imagine this could be a moment of triumph"
        interpretation = detect_interpretation(response)

        assert interpretation["has_interpretation_phrases"] is True
        assert interpretation["interpretation_count"] > 0

    def test_detect_emotion_scores(self):
        """Test detection of emotion scores in dialogue quotes."""
        response = 'The dialogue "Where are you, baby!" (caring: 0.047, anger: 0.021) shows strong emotions'
        interpretation = detect_interpretation(response)

        assert interpretation["has_emotion_scores"] is True
        assert interpretation["emotion_score_count"] > 0

    def test_detect_dialogue_quotes(self):
        """Test detection of dialogue quotes."""
        response = 'The character says "You\'re lying! Give them back!" with high emotion'
        interpretation = detect_interpretation(response)

        assert interpretation["has_dialogue_quotes"] is True
        assert interpretation["dialogue_quote_count"] > 0

    def test_full_interpretation_response(self):
        """Test response with all interpretation elements."""
        response = """
        In minute 83, we see a sentiment valley (-0.65) driven by anger (0.78) and sadness (0.85).
        The key dialogue includes:
        - 'You're lying! Give them back!' (anger: 0.78, fear: 0.65)
        - 'I have nowhere else to go...' (sadness: 0.85, grief: 0.72)
        
        Based on this combination of anger + sadness + grief, I imagine this is a confrontation
        where a character feels betrayed and trapped.
        """
        interpretation = detect_interpretation(response)

        assert interpretation["has_interpretation_phrases"] is True
        assert interpretation["has_emotion_scores"] is True
        assert interpretation["has_dialogue_quotes"] is True
        assert interpretation["interpretation_count"] > 0
        assert interpretation["emotion_score_count"] > 0
        assert interpretation["dialogue_quote_count"] > 0

    def test_no_interpretation(self):
        """Test response with no interpretation elements."""
        response = "The correlation is r=0.45 with p-value 0.03"
        interpretation = detect_interpretation(response)

        assert interpretation["has_interpretation_phrases"] is False
        assert interpretation["has_emotion_scores"] is False
        assert interpretation["has_dialogue_quotes"] is False
        assert interpretation["interpretation_count"] == 0


class TestExpectedElements:
    """Test expected element checking."""

    def test_find_all_elements(self):
        """Test finding all expected elements."""
        response = "The correlation between sentiment and revenue shows statistical significance"
        expected = ["correlation", "sentiment", "statistical", "significance"]
        result = check_expected_elements(response, expected)

        assert result["found_count"] == result["total_count"]
        assert len(result["missing_elements"]) == 0

    def test_find_partial_elements(self):
        """Test finding some expected elements."""
        response = "The correlation shows significance"
        expected = ["correlation", "sentiment", "statistical", "significance"]
        result = check_expected_elements(response, expected)

        assert result["found_count"] < result["total_count"]
        assert len(result["missing_elements"]) > 0
        assert "sentiment" in result["missing_elements"]

    def test_case_insensitive(self):
        """Test case-insensitive matching."""
        response = "CORRELATION and Sentiment analysis"
        expected = ["correlation", "sentiment"]
        result = check_expected_elements(response, expected)

        assert result["found_count"] == 2


class TestValidationScoring:
    """Test validation scoring formula."""

    def test_high_score_response(self):
        """Test response that should score highly."""
        response = """
        Based on mart_sentiment_success_correlation, I found:
        - Correlation r=0.45, p-value=0.03 (n=22 films)
        - Compound_sentiment variance is 0.12
        - Peak occurs at minute 67 with timestamp 01:07:00
        - The dialogue "Finally!" (joy: 0.92, excitement: 0.85) shows triumph.
        Based on this emotional pattern, I imagine this is a moment of celebration.
        """
        expected = ["correlation", "sentiment", "statistical", "minute"]
        validation = validate_response(response, expected, "Q1")

        assert validation["validation_score"] >= 0.70
        assert validation["passed"] is True

    def test_low_score_response(self):
        """Test response that should score low."""
        response = "This is a general response without specific data"
        expected = ["correlation", "sentiment", "statistical"]
        validation = validate_response(response, expected, "Q1")

        assert validation["validation_score"] < 0.70
        assert validation["passed"] is False

    def test_scoring_breakdown(self):
        """Test that scoring breakdown is complete."""
        response = """
        From mart_film_sentiment_summary:
        - avg_compound: 0.15
        - sentiment_variance: 0.08
        Correlation r=0.32, p=0.05
        """
        expected = ["correlation", "sentiment"]
        validation = validate_response(response, expected, "Q1")

        assert "breakdown" in validation
        breakdown = validation["breakdown"]
        assert "citations" in breakdown
        assert "statistics" in breakdown
        assert "sentiment_metrics" in breakdown
        assert "interpretation" in breakdown
        assert "expected_elements" in breakdown

        # Check weights sum to 1.0
        total_weight = sum(component["weight"] for component in breakdown.values())
        assert abs(total_weight - 1.0) < 0.01

    def test_scoring_weights(self):
        """Test that scoring weights match specification."""
        response = "Test response"
        expected = []
        validation = validate_response(response, expected, "Q1")
        breakdown = validation["breakdown"]

        assert breakdown["citations"]["weight"] == 0.15
        assert breakdown["statistics"]["weight"] == 0.25
        assert breakdown["sentiment_metrics"]["weight"] == 0.25
        assert breakdown["interpretation"]["weight"] == 0.25
        assert breakdown["expected_elements"]["weight"] == 0.10


class TestQuerySpecificValidation:
    """Test validation for specific query types."""

    def test_q1_sentiment_curve(self):
        """Test Q1: Sentiment curve with intense moments."""
        response = """
        Spirited Away sentiment curve:
        - Peak at minute 67: compound_sentiment 0.85
        - Peak at minute 103: sentiment score 0.82
        - Valley at minute 80: sentiment value -0.18
        - Dialogue: "Finally!" (joy: 0.92, excitement: 0.85)
        Based on this emotional pattern, I imagine this is a moment of triumph.
        """
        expected = ["minute", "sentiment", "positive", "negative", "peak", "timestamp"]
        validation = validate_response(response, expected, "Q1")

        # Should have timestamps, sentiment metrics, and interpretation
        assert validation["breakdown"]["citations"]["details"]["has_timestamps"] is True
        assert validation["breakdown"]["sentiment_metrics"]["details"]["metric_count"] > 0
        assert validation["breakdown"]["interpretation"]["details"]["has_interpretation_phrases"] is True

    def test_q2_correlation(self):
        """Test Q2: Sentiment-revenue correlation."""
        response = """
        Correlation analysis:
        - r=0.34, p-value=0.08, n=18 films
        - Statistical significance: not significant (p > 0.05)
        """
        expected = ["correlation", "r=", "p=", "n=", "films", "statistical", "significance"]
        validation = validate_response(response, expected, "Q2")

        assert validation["breakdown"]["statistics"]["details"]["has_correlation"] is True
        assert validation["breakdown"]["statistics"]["details"]["has_pvalue"] is True

    def test_q6_multilingual(self):
        """Test Q6: Multilingual arc comparison."""
        response = """
        Comparing EN, FR, ES versions:
        - EN-FR correlation: 0.85
        - Divergence at minute 45
        - ES shows different emotional tone
        """
        expected = ["EN", "FR", "ES", "correlation", "minute", "divergence"]
        validation = validate_response(response, expected, "Q6")

        assert validation["breakdown"]["statistics"]["details"]["has_correlation"] is True
        assert validation["breakdown"]["citations"]["details"]["has_timestamps"] is True
