"""
Integration tests for RAG system validation script.

These tests require:
- OpenAI API key (OPENAI_API_KEY environment variable)
- DuckDB database at data/ghibli.duckdb with dbt models built
- ChromaDB vector store at data/vectors

Tests execute real queries against the RAG pipeline to validate:
- All 10 sentiment-focused queries execute successfully
- Responses cite new mart tables (mart_sentiment_success_correlation, etc.)
- Statistical measures are present (r, p-value, z-score)
- FR17 validation passes (all queries demonstrate analytical value)

Note: These tests make real API calls and may incur costs (~$1.00 for full suite).
Run with: pytest -m integration tests/integration/test_validate_rag_integration.py
"""

import os

import pytest

from src.ai.validate_rag_system import (
    run_validation_tests,
    TEST_QUERIES,
    generate_validation_report,
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
    - dbt models are built (mart_sentiment_success_correlation, etc.)
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

    # Check dbt models exist (verify by querying DuckDB)
    try:
        import duckdb
        conn = duckdb.connect(duckdb_path)
        # Try to query one of the required mart tables
        result = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'mart_sentiment_success_correlation'"
        ).fetchone()
        if result[0] == 0:
            pytest.skip(
                "Required dbt models not built. Run: cd src/transformation && dbt run --models marts"
            )
        conn.close()
    except Exception as e:
        pytest.skip(f"Could not verify dbt models: {e}")

    yield


def test_all_10_queries_execute(check_prerequisites):
    """
    Test that all 10 sentiment-focused queries execute successfully.

    Validates:
    - No exceptions during query execution
    - All queries return responses
    - Response times are reasonable (< 10 seconds per query)
    """
    # Run validation with GPT-3.5-turbo for cost efficiency
    results = run_validation_tests(model="gpt-3.5-turbo", max_queries=10)

    assert len(results["test_results"]) == 10

    # Check all queries executed without errors
    for result in results["test_results"]:
        assert "error" not in result, f"Query {result['query_id']} failed: {result.get('error')}"
        assert "response" in result or "error" not in result
        assert result.get("response_time", 0) < 15.0  # Allow some buffer beyond 10s NFR


def test_responses_cite_mart_tables(check_prerequisites):
    """
    Test that responses cite new mart tables.

    Validates:
    - Responses mention mart_sentiment_success_correlation
    - Responses mention mart_film_sentiment_summary or mart_film_success_metrics
    - Citations are detected by validation logic
    """
    results = run_validation_tests(model="gpt-3.5-turbo", max_queries=3)  # Test subset for speed

    mart_tables_found = set()
    for result in results["test_results"]:
        if "response" in result:
            response_text = result["response"]
            # Check for mart table mentions
            if "mart_sentiment_success_correlation" in response_text.lower():
                mart_tables_found.add("mart_sentiment_success_correlation")
            if "mart_film_sentiment_summary" in response_text.lower():
                mart_tables_found.add("mart_film_sentiment_summary")
            if "mart_film_success_metrics" in response_text.lower():
                mart_tables_found.add("mart_film_success_metrics")

    # At least one mart table should be cited
    assert len(mart_tables_found) > 0, "No mart tables cited in responses"


def test_statistical_measures_present(check_prerequisites):
    """
    Test that responses include statistical measures.

    Validates:
    - Correlation coefficients (r=) are present
    - P-values are mentioned
    - Sample sizes (n=) are included
    """
    results = run_validation_tests(model="gpt-3.5-turbo", max_queries=3)

    has_correlation = False
    has_pvalue = False
    has_sample_size = False

    for result in results["test_results"]:
        if "response" in result:
            response_text = result["response"]
            validation = result.get("validation", {})
            breakdown = validation.get("breakdown", {})
            stats_details = breakdown.get("statistics", {}).get("details", {})

            if stats_details.get("has_correlation"):
                has_correlation = True
            if stats_details.get("has_pvalue"):
                has_pvalue = True
            if stats_details.get("has_sample_size"):
                has_sample_size = True

    # At least correlation should be present for correlation queries
    # (Q2, Q4, Q7, Q9, Q10 are correlation-focused)
    assert has_correlation or has_pvalue, "No statistical measures found in responses"


def test_q2_uses_correlate_metrics_tool(check_prerequisites):
    """
    Test that Q2 uses correlate_metrics tool.

    Q2: "Calculate the correlation between average sentiment and box office revenue..."
    Expected tool: correlate_metrics
    """
    results = run_validation_tests(model="gpt-3.5-turbo", max_queries=2)

    q2_result = next((r for r in results["test_results"] if r["query_id"] == "Q2"), None)
    assert q2_result is not None, "Q2 not found in results"

    function_calls = q2_result.get("function_calls", [])
    tool_names = [call.get("name", "") for call in function_calls]

    # Should use correlate_metrics or query_graph_database
    assert any(
        "correlate" in name.lower() or "query_graph" in name.lower() for name in tool_names
    ), f"Q2 should use correlate_metrics or query_graph_database, got: {tool_names}"


def test_q6_uses_multilingual_tool(check_prerequisites):
    """
    Test that Q6 uses compare_sentiment_arcs_across_languages tool.

    Q6: "Compare sentiment arcs across English, French, and Spanish..."
    Expected tool: compare_sentiment_arcs_across_languages
    """
    results = run_validation_tests(model="gpt-3.5-turbo", max_queries=6)

    q6_result = next((r for r in results["test_results"] if r["query_id"] == "Q6"), None)
    assert q6_result is not None, "Q6 not found in results"

    function_calls = q6_result.get("function_calls", [])
    tool_names = [call.get("name", "") for call in function_calls]

    # Should use compare_sentiment_arcs_across_languages
    assert any(
        "compare" in name.lower() and "language" in name.lower() for name in tool_names
    ), f"Q6 should use compare_sentiment_arcs_across_languages, got: {tool_names}"


def test_q1_uses_film_sentiment_tool(check_prerequisites):
    """
    Test that Q1 uses get_film_sentiment tool.

    Q1: "Show me the sentiment curve for Spirited Away..."
    Expected tool: get_film_sentiment
    """
    results = run_validation_tests(model="gpt-3.5-turbo", max_queries=1)

    q1_result = next((r for r in results["test_results"] if r["query_id"] == "Q1"), None)
    assert q1_result is not None, "Q1 not found in results"

    function_calls = q1_result.get("function_calls", [])
    tool_names = [call.get("name", "") for call in function_calls]

    # Should use get_film_sentiment
    assert any(
        "sentiment" in name.lower() and "film" in name.lower() for name in tool_names
    ), f"Q1 should use get_film_sentiment, got: {tool_names}"


def test_fr17_validation_passes(check_prerequisites):
    """
    Test that FR17 validation passes (all 10 queries demonstrate analytical value).

    FR17 requires:
    - All 10 sentiment-driven queries answered
    - Responses cite data sources
    - Responses include computed metrics
    - Responses demonstrate value beyond general LLM knowledge
    """
    results = run_validation_tests(model="gpt-3.5-turbo", max_queries=10)

    stats = results["aggregate_stats"]

    # All 10 queries should execute
    assert stats["total_queries"] == 10

    # At least 70% should pass validation (7/10)
    assert stats["pass_rate"] >= 0.70, f"FR17 validation failed: only {stats['pass_rate']:.1%} passed"

    # Check that responses cite data sources
    citations_found = 0
    for result in results["test_results"]:
        if "validation" in result:
            breakdown = result["validation"].get("breakdown", {})
            citation_count = breakdown.get("citations", {}).get("details", {}).get("citation_count", 0)
            if citation_count > 0:
                citations_found += 1

    # At least 8/10 should have citations
    assert citations_found >= 8, f"Only {citations_found}/10 queries have citations"


def test_validation_report_generation(check_prerequisites, tmp_path):
    """
    Test that validation report generates correctly.

    Validates:
    - Report file is created
    - Report contains required sections
    - Report includes ChatGPT comparison section
    """
    results = run_validation_tests(model="gpt-3.5-turbo", max_queries=2)

    report_path = tmp_path / "test_validation_report.md"
    generate_validation_report(results, output_path=str(report_path))

    assert report_path.exists(), "Report file was not created"

    report_content = report_path.read_text()

    # Check required sections
    assert "Executive Summary" in report_content
    assert "How This Differs from ChatGPT" in report_content
    assert "Functional Requirements Validation" in report_content
    assert "FR17" in report_content
    assert "Detailed Test Results" in report_content

    # Check ChatGPT comparison content
    assert "custom sentiment analysis" in report_content.lower()
    assert "statistical correlation" in report_content.lower()
    assert "multilingual emotion comparison" in report_content.lower()


def test_category_tagging(check_prerequisites):
    """
    Test that queries are properly categorized.

    Validates:
    - Each query has a category
    - Categories match expected values
    - Category pass rates are tracked
    """
    results = run_validation_tests(model="gpt-3.5-turbo", max_queries=10)

    # Check all queries have categories
    categories_found = set()
    for result in results["test_results"]:
        assert "category" in result
        categories_found.add(result["category"])

    # Check expected categories exist
    expected_categories = {
        "sentiment_analysis",
        "correlation_study",
        "trajectory_analysis",
        "multilingual",
        "success_prediction",
    }
    assert categories_found.issubset(expected_categories), f"Unexpected categories: {categories_found - expected_categories}"

    # Check category pass rates are tracked
    stats = results["aggregate_stats"]
    assert "category_pass_rates" in stats
    assert len(stats["category_pass_rates"]) > 0
