"""
Data quality validation module for Studio Ghibli data pipeline.

This module provides comprehensive data quality checks for ingested raw data
stored in DuckDB. It validates record counts, completeness, duplicates,
referential integrity, and cross-reference consistency between data sources.

Usage:
    python src/ingestion/data_quality_check.py --output data/processed/data_quality_report.txt
    python src/ingestion/data_quality_check.py --verbose

Example:
    >>> from src.ingestion.data_quality_check import main
    >>> exit_code = main()
    >>> print(exit_code)
    0  # All checks passed
"""

# Standard library
import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

# Third-party
import duckdb

# Local
from src.shared.database import get_duckdb_connection

# Constants
EXPECTED_COUNTS = {
    "films": {"min": 22, "max": 22},
    "people": {"min": 87, "max": None},
    "locations": {"min": 13, "max": None},
    "species": {"min": 14, "max": None},
    "vehicles": {"min": 8, "max": None},
    "kaggle_films": {"min": 22, "max": None},
}

COMPLETENESS_THRESHOLD = 100.0  # Critical fields should be 100% complete


# ANSI color codes for terminal output
class Colors:
    """ANSI color codes for colored terminal output."""

    GREEN = "\033[92m"  # Green for success/pass
    YELLOW = "\033[93m"  # Yellow for warnings
    RED = "\033[91m"  # Red for errors/failures
    RESET = "\033[0m"  # Reset to default color
    BOLD = "\033[1m"  # Bold text


# Logger
logger = logging.getLogger(__name__)


def validate_record_counts(conn: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """
    Validate record counts against expected ranges.

    Args:
        conn: DuckDB database connection

    Returns:
        Validation result dictionary with structure:
        {
            "films": {"actual": 22, "expected": "22", "status": "PASS"},
            "people": {"actual": 57, "expected": "87+", "status": "WARN"},
            ...
        }

    Example:
        >>> conn = get_duckdb_connection()
        >>> result = validate_record_counts(conn)
        >>> result["films"]["status"]
        'PASS'
    """
    logger.info("Validating record counts...")
    results = {}

    for table_name, expected_range in EXPECTED_COUNTS.items():
        try:
            # Query row count for each table
            result = conn.execute(f"SELECT COUNT(*) FROM raw.{table_name}").fetchone()
            actual_count = result[0]

            # Determine expected string and validation status
            min_count = expected_range["min"]
            max_count = expected_range["max"]

            if max_count is None:
                expected_str = f"{min_count}+"
                status = "PASS" if actual_count >= min_count else "WARN"
            else:
                expected_str = str(min_count)
                status = "PASS" if actual_count == min_count else "WARN"

            results[table_name] = {
                "actual": actual_count,
                "expected": expected_str,
                "status": status,
            }

            # Log with colored output
            if status == "PASS":
                logger.info(
                    f"{Colors.GREEN}✓{Colors.RESET} {table_name}: {actual_count} records"
                )
            else:
                logger.warning(
                    f"{Colors.YELLOW}⚠{Colors.RESET} {table_name}: {actual_count} records"
                )

        except Exception as e:
            logger.error(f"Failed to query count for {table_name}: {e}")
            results[table_name] = {
                "actual": 0,
                "expected": expected_range["min"],
                "status": "ERROR",
                "error": str(e),
            }

    return results


def validate_completeness(conn: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """
    Validate completeness of critical fields in raw tables.

    Checks for NULL values in required fields (id, name/title) and calculates
    completeness percentage for each table. Critical fields should be 100% complete.

    Args:
        conn: DuckDB database connection

    Returns:
        Validation result dictionary with structure:
        {
            "films": {"id": 100.0, "title": 100.0, "status": "PASS"},
            "people": {"id": 100.0, "name": 98.5, "status": "WARN"},
            ...
        }

    Example:
        >>> conn = get_duckdb_connection()
        >>> result = validate_completeness(conn)
        >>> result["films"]["title"]
        100.0
    """
    logger.info("Validating field completeness...")
    results = {}

    # Define critical fields for each table
    table_fields = {
        "films": ["id", "title"],
        "people": ["id", "name"],
        "locations": ["id", "name"],
        "species": ["id", "name"],
        "vehicles": ["id", "name"],
        "kaggle_films": ["name"],  # Kaggle has "name" field, not "title"
    }

    for table_name, fields in table_fields.items():
        table_result = {}
        all_complete = True

        try:
            for field in fields:
                # Calculate completeness percentage
                result = conn.execute(
                    f"""
                    SELECT
                        COUNT(*) AS total,
                        SUM(CASE WHEN {field} IS NULL THEN 1 ELSE 0 END) AS null_count
                    FROM raw.{table_name}
                    """
                ).fetchone()

                total = result[0]
                null_count = result[1]

                if total > 0:
                    completeness_pct = ((total - null_count) / total) * 100
                else:
                    completeness_pct = 0.0

                table_result[field] = completeness_pct

                # Check if field meets threshold
                if completeness_pct < COMPLETENESS_THRESHOLD:
                    all_complete = False
                    logger.warning(
                        f"{Colors.YELLOW}⚠{Colors.RESET} {table_name}.{field}: "
                        f"{completeness_pct:.1f}% complete"
                    )
                else:
                    logger.info(
                        f"{Colors.GREEN}✓{Colors.RESET} {table_name}.{field}: "
                        f"{completeness_pct:.1f}% complete"
                    )

        except Exception as e:
            logger.error(f"Failed to check completeness for {table_name}: {e}")
            table_result["error"] = str(e)
            all_complete = False

        table_result["status"] = "PASS" if all_complete else "WARN"
        results[table_name] = table_result

    return results


def validate_no_duplicates(conn: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """
    Validate no duplicate IDs or titles exist in tables.

    Args:
        conn: DuckDB database connection

    Returns:
        Validation result dictionary with structure:
        {
            "films": {
                "duplicate_ids": [],
                "duplicate_titles": [],
                "status": "PASS"
            },
            ...
        }

    Example:
        >>> conn = get_duckdb_connection()
        >>> result = validate_no_duplicates(conn)
        >>> result["films"]["status"]
        'PASS'
    """
    logger.info("Checking for duplicate IDs and titles...")
    results = {}

    # Tables with ID fields
    id_tables = ["films", "people", "locations", "species", "vehicles"]

    # Tables with title fields to check
    title_tables = ["films", "kaggle_films"]

    for table_name in id_tables:
        table_result = {"duplicate_ids": [], "status": "PASS"}

        try:
            # Check for duplicate IDs
            duplicate_ids = conn.execute(
                f"""
                SELECT id, COUNT(*) as count
                FROM raw.{table_name}
                GROUP BY id
                HAVING COUNT(*) > 1
                """
            ).fetchall()

            if duplicate_ids:
                table_result["duplicate_ids"] = [
                    {"id": row[0], "count": row[1]} for row in duplicate_ids
                ]
                table_result["status"] = "FAIL"
                logger.error(
                    f"{Colors.RED}✗{Colors.RESET} {table_name}: Found {len(duplicate_ids)} "
                    f"duplicate IDs"
                )
            else:
                logger.info(
                    f"{Colors.GREEN}✓{Colors.RESET} {table_name}: No duplicate IDs"
                )

        except Exception as e:
            logger.error(f"Failed to check duplicates for {table_name}: {e}")
            table_result["error"] = str(e)
            table_result["status"] = "ERROR"

        results[table_name] = table_result

    # Check for duplicate titles in films and kaggle_films
    for table_name in title_tables:
        if table_name not in results:
            results[table_name] = {"status": "PASS"}

        try:
            # Use appropriate column name (title for films, name for kaggle_films)
            title_col = "name" if table_name == "kaggle_films" else "title"

            # Check for duplicate titles (case-insensitive)
            duplicate_titles = conn.execute(
                f"""
                SELECT LOWER({title_col}) as title_lower, COUNT(*) as count
                FROM raw.{table_name}
                GROUP BY LOWER({title_col})
                HAVING COUNT(*) > 1
                """
            ).fetchall()

            if duplicate_titles:
                results[table_name]["duplicate_titles"] = [
                    {"title": row[0], "count": row[1]} for row in duplicate_titles
                ]
                results[table_name]["status"] = "FAIL"
                logger.error(
                    f"{Colors.RED}✗{Colors.RESET} {table_name}: Found {len(duplicate_titles)} "
                    f"duplicate titles"
                )
            else:
                logger.info(
                    f"{Colors.GREEN}✓{Colors.RESET} {table_name}: No duplicate titles"
                )

        except Exception as e:
            logger.error(f"Failed to check title duplicates for {table_name}: {e}")
            results[table_name]["error"] = str(e)
            results[table_name]["status"] = "ERROR"

    # Add kaggle_films if not already in results
    if "kaggle_films" not in results:
        results["kaggle_films"] = {"status": "PASS"}

    return results


def validate_referential_integrity(conn: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """
    Validate referential integrity of film references.

    Checks that all film IDs extracted from URL arrays in people, locations,
    species, and vehicles tables exist in the raw.films table.

    Args:
        conn: DuckDB database connection

    Returns:
        Validation result dictionary with structure:
        {
            "orphaned_references": [
                {"table": "people", "record_id": "abc", "missing_film_id": "xyz"}
            ],
            "status": "PASS"|"FAIL"
        }

    Example:
        >>> conn = get_duckdb_connection()
        >>> result = validate_referential_integrity(conn)
        >>> len(result["orphaned_references"])
        0
    """
    logger.info("Validating referential integrity...")
    orphaned_references = []

    # Tables with film URL arrays
    tables_with_film_refs = ["people", "locations", "species", "vehicles"]

    for table_name in tables_with_film_refs:
        try:
            # Extract film IDs from URL arrays and check if they exist in films table
            # URL format: "https://ghibliapi.vercel.app/films/{film_id}"
            orphaned_refs = conn.execute(
                f"""
                WITH film_refs AS (
                    SELECT
                        t.id AS record_id,
                        UNNEST(t.films) AS film_url,
                        REGEXP_EXTRACT(UNNEST(t.films), '.*/(.*)$', 1) AS film_id
                    FROM raw.{table_name} t
                    WHERE COALESCE(array_length(t.films), 0) > 0
                )
                SELECT fr.record_id, fr.film_id
                FROM film_refs fr
                LEFT JOIN raw.films f ON fr.film_id = f.id
                WHERE f.id IS NULL
                """
            ).fetchall()

            for row in orphaned_refs:
                orphaned_references.append(
                    {
                        "table": table_name,
                        "record_id": row[0],
                        "missing_film_id": row[1],
                    }
                )
                logger.error(
                    f"{Colors.RED}✗{Colors.RESET} Orphaned reference in {table_name}: "
                    f"record {row[0]} references non-existent film {row[1]}"
                )

        except Exception as e:
            logger.error(f"Failed to check referential integrity for {table_name}: {e}")
            orphaned_references.append(
                {"table": table_name, "error": str(e), "record_id": None, "missing_film_id": None}
            )

    if orphaned_references:
        status = "FAIL"
        logger.error(
            f"{Colors.RED}✗{Colors.RESET} Found {len(orphaned_references)} orphaned references"
        )
    else:
        status = "PASS"
        logger.info(
            f"{Colors.GREEN}✓{Colors.RESET} All film references valid "
            f"(0 orphaned references)"
        )

    return {"orphaned_references": orphaned_references, "status": status}


def validate_kaggle_api_crossref(conn: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """
    Validate Kaggle-API cross-reference consistency.

    Compares film titles between Kaggle CSV and Ghibli API using multi-pass fuzzy matching:
    1. Exact match (after cleaning year suffixes and whitespace)
    2. Substring match (handles title variations like "Arrietty" vs "The Secret World of Arrietty")

    Note: It's normal for different data sources to have different coverage. This validation
    identifies matches where possible and reports genuine source differences.

    Args:
        conn: DuckDB database connection

    Returns:
        Validation result dictionary with structure:
        {
            "in_kaggle_not_api": ["Film Title"],
            "in_api_not_kaggle": ["Film Title"],
            "match_count": 22,
            "match_details": {"exact": 19, "fuzzy": 1}
        }

    Example:
        >>> conn = get_duckdb_connection()
        >>> result = validate_kaggle_api_crossref(conn)
        >>> result["match_count"]
        20
    """
    logger.info("Validating Kaggle-API cross-reference with multi-pass fuzzy matching...")

    try:
        # Step 1: Exact match (after cleaning year suffixes and whitespace)
        exact_matches = conn.execute(
            """
            SELECT
                f.title as api_title,
                k.name as kaggle_title,
                'exact' as match_type
            FROM raw.films f
            INNER JOIN raw.kaggle_films k ON
                LOWER(TRIM(f.title)) =
                LOWER(TRIM(REGEXP_REPLACE(k.name, '\\s+\\([0-9]{4}\\)\\s*$', '')))
            """
        ).fetchall()

        exact_match_count = len(exact_matches)
        matched_api_titles = {row[0] for row in exact_matches}
        matched_kaggle_titles = {row[1] for row in exact_matches}

        logger.info(f"  - Exact matches: {exact_match_count}")

        # Step 2: Fuzzy substring match for remaining unmatched titles
        # Get unmatched titles from both sources
        unmatched_api = conn.execute(
            f"""
            SELECT title FROM raw.films
            WHERE title NOT IN ({','.join('?' for _ in matched_api_titles)})
            """
            if matched_api_titles
            else "SELECT title FROM raw.films",
            list(matched_api_titles) if matched_api_titles else []
        ).fetchall()

        unmatched_kaggle = conn.execute(
            f"""
            SELECT name FROM raw.kaggle_films
            WHERE name NOT IN ({','.join('?' for _ in matched_kaggle_titles)})
            """
            if matched_kaggle_titles
            else "SELECT name FROM raw.kaggle_films",
            list(matched_kaggle_titles) if matched_kaggle_titles else []
        ).fetchall()

        # Fuzzy matching: Check if shorter title is contained in longer title
        # Example: "Arrietty" should match "The Secret World of Arrietty"
        fuzzy_matches = []
        remaining_api = [row[0] for row in unmatched_api]
        remaining_kaggle = [row[0] for row in unmatched_kaggle]

        for api_title in remaining_api[:]:  # Use slice to avoid modification during iteration
            api_clean = api_title.lower().strip()

            for kaggle_title in remaining_kaggle[:]:
                # Clean Kaggle title (remove year suffix and whitespace)
                kaggle_clean = kaggle_title
                if '(' in kaggle_clean and kaggle_clean.strip().endswith(')'):
                    kaggle_clean = kaggle_clean[:kaggle_clean.rfind('(')].strip()
                kaggle_clean = kaggle_clean.lower().strip()

                # Check if one title is substring of the other
                # Use the shorter title as the substring to search for
                if len(api_clean) <= len(kaggle_clean):
                    # Check if API title is in Kaggle title
                    if api_clean in kaggle_clean or kaggle_clean in api_clean:
                        fuzzy_matches.append((api_title, kaggle_title, 'fuzzy_substring'))
                        remaining_api.remove(api_title)
                        remaining_kaggle.remove(kaggle_title)
                        logger.info(
                            f"  - Fuzzy match: '{api_title}' ≈ '{kaggle_clean}'"
                        )
                        break
                else:
                    # Check if Kaggle title is in API title
                    if kaggle_clean in api_clean or api_clean in kaggle_clean:
                        fuzzy_matches.append((api_title, kaggle_title, 'fuzzy_substring'))
                        remaining_api.remove(api_title)
                        remaining_kaggle.remove(kaggle_title)
                        logger.info(
                            f"  - Fuzzy match: '{api_title}' ≈ '{kaggle_clean}'"
                        )
                        break

        fuzzy_match_count = len(fuzzy_matches)
        logger.info(f"  - Total fuzzy matches: {fuzzy_match_count}")

        # Total matches
        match_count = exact_match_count + fuzzy_match_count

        # Films that remain unmatched after both passes (genuine source differences)
        in_kaggle_not_api_list = remaining_kaggle
        in_api_not_kaggle_list = remaining_api

        # Log results with breakdown
        logger.info(
            f"{Colors.GREEN}✓{Colors.RESET} Total matching films: {match_count} "
            f"(exact: {exact_match_count}, fuzzy: {fuzzy_match_count})"
        )

        if in_kaggle_not_api_list:
            logger.info(
                f"{Colors.YELLOW}ℹ{Colors.RESET} Films in Kaggle but not API: "
                f"{len(in_kaggle_not_api_list)} (genuine source differences)"
            )
            for title in in_kaggle_not_api_list:
                logger.info(f"  - {title}")

        if in_api_not_kaggle_list:
            logger.info(
                f"{Colors.YELLOW}ℹ{Colors.RESET} Films in API but not Kaggle: "
                f"{len(in_api_not_kaggle_list)} (genuine source differences)"
            )
            for title in in_api_not_kaggle_list:
                logger.info(f"  - {title}")

        return {
            "in_kaggle_not_api": in_kaggle_not_api_list,
            "in_api_not_kaggle": in_api_not_kaggle_list,
            "match_count": match_count,
            "match_details": {
                "exact": exact_match_count,
                "fuzzy": fuzzy_match_count
            }
        }

    except Exception as e:
        logger.error(f"Failed to validate Kaggle-API cross-reference: {e}")
        return {
            "in_kaggle_not_api": [],
            "in_api_not_kaggle": [],
            "match_count": 0,
            "error": str(e),
        }


def generate_report(validation_results: Dict[str, Any]) -> str:
    """
    Generate formatted data quality report.

    Args:
        validation_results: Dictionary containing all validation results

    Returns:
        Formatted report string with colored indicators

    Example:
        >>> results = {"record_counts": {...}, "completeness": {...}}
        >>> report = generate_report(results)
        >>> print(report)
        📊 Data Quality Validation Report
        ...
    """
    lines = []
    separator = "━" * 80

    # Header
    lines.append(f"{Colors.BOLD}📊 Data Quality Validation Report{Colors.RESET}")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    # Section 1: Record Counts
    lines.append(separator)
    lines.append("1. RECORD COUNTS")
    lines.append(separator)
    lines.append("")
    lines.append(f"{'Table':<20} | {'Actual':>8} | {'Expected':>10} | Status")
    lines.append("-" * 80)

    record_counts = validation_results.get("record_counts", {})
    for table_name, result in record_counts.items():
        actual = result.get("actual", 0)
        expected = result.get("expected", "N/A")
        status = result.get("status", "UNKNOWN")

        if status == "PASS":
            status_str = f"{Colors.GREEN}✓ PASS{Colors.RESET}"
        elif status == "WARN":
            status_str = f"{Colors.YELLOW}⚠ WARN{Colors.RESET}"
        else:
            status_str = f"{Colors.RED}✗ ERROR{Colors.RESET}"

        lines.append(f"{table_name:<20} | {actual:>8} | {expected:>10} | {status_str}")

    lines.append("")

    # Section 2: Completeness Check
    lines.append(separator)
    lines.append("2. COMPLETENESS CHECK")
    lines.append(separator)
    lines.append("")
    lines.append(f"{'Table':<20} | {'Field':<10} | {'Completeness':>13} | Status")
    lines.append("-" * 80)

    completeness = validation_results.get("completeness", {})
    for table_name, result in completeness.items():
        status = result.get("status", "UNKNOWN")
        for field, pct in result.items():
            if field == "status" or field == "error":
                continue

            if pct >= COMPLETENESS_THRESHOLD:
                status_str = f"{Colors.GREEN}✓ PASS{Colors.RESET}"
            else:
                status_str = f"{Colors.YELLOW}⚠ WARN{Colors.RESET}"

            lines.append(
                f"{table_name:<20} | {field:<10} | {pct:>12.1f}% | {status_str}"
            )

    lines.append("")

    # Section 3: Duplicate Detection
    lines.append(separator)
    lines.append("3. DUPLICATE DETECTION")
    lines.append(separator)
    lines.append("")

    duplicates = validation_results.get("duplicates", {})
    has_duplicates = False

    for table_name, result in duplicates.items():
        duplicate_ids = result.get("duplicate_ids", [])
        duplicate_titles = result.get("duplicate_titles", [])

        if duplicate_ids:
            has_duplicates = True
            msg = (
                f"{Colors.RED}✗{Colors.RESET} {table_name}: "
                f"Found {len(duplicate_ids)} duplicate IDs"
            )
            lines.append(msg)
            for dup in duplicate_ids[:5]:  # Show first 5
                lines.append(f"  - ID: {dup.get('id')} (count: {dup.get('count')})")

        if duplicate_titles:
            has_duplicates = True
            msg = (
                f"{Colors.RED}✗{Colors.RESET} {table_name}: "
                f"Found {len(duplicate_titles)} duplicate titles"
            )
            lines.append(msg)
            for dup in duplicate_titles[:5]:  # Show first 5
                lines.append(f"  - Title: {dup.get('title')} (count: {dup.get('count')})")

    if not has_duplicates:
        lines.append(f"{Colors.GREEN}✓ No duplicate IDs or titles found{Colors.RESET}")

    lines.append("")

    # Section 4: Referential Integrity
    lines.append(separator)
    lines.append("4. REFERENTIAL INTEGRITY")
    lines.append(separator)
    lines.append("")

    ref_integrity = validation_results.get("referential_integrity", {})
    orphaned_refs = ref_integrity.get("orphaned_references", [])

    if orphaned_refs:
        lines.append(
            f"{Colors.RED}✗{Colors.RESET} Found {len(orphaned_refs)} orphaned film references"
        )
        for ref in orphaned_refs[:10]:  # Show first 10
            if ref.get("error"):
                lines.append(f"  - {ref['table']}: {ref['error']}")
            else:
                msg = (
                    f"  - {ref['table']} record {ref['record_id']} → "
                    f"missing film {ref['missing_film_id']}"
                )
                lines.append(msg)
    else:
        lines.append(
            f"{Colors.GREEN}✓ All film references valid (0 orphaned references){Colors.RESET}"
        )

    lines.append("")

    # Section 5: Kaggle-API Cross-Reference
    lines.append(separator)
    lines.append("5. KAGGLE-API CROSS-REFERENCE")
    lines.append(separator)
    lines.append("")

    crossref = validation_results.get("kaggle_api_crossref", {})
    match_count = crossref.get("match_count", 0)
    match_details = crossref.get("match_details", {})
    in_kaggle_not_api = crossref.get("in_kaggle_not_api", [])
    in_api_not_kaggle = crossref.get("in_api_not_kaggle", [])

    exact_matches = match_details.get("exact", 0)
    fuzzy_matches = match_details.get("fuzzy", 0)

    lines.append(f"Total matching films: {match_count}")
    if match_details:
        lines.append(f"  - Exact matches: {exact_matches}")
        lines.append(f"  - Fuzzy matches: {fuzzy_matches}")
    lines.append("")

    if in_kaggle_not_api:
        lines.append("Films in Kaggle but not API:")
        for title in in_kaggle_not_api:
            lines.append(f"  - {title}")
        lines.append("")

    if in_api_not_kaggle:
        lines.append("Films in API but not Kaggle:")
        for title in in_api_not_kaggle:
            lines.append(f"  - {title}")
        lines.append("")

    if not in_kaggle_not_api and not in_api_not_kaggle:
        lines.append(f"{Colors.GREEN}✓ All films match between sources{Colors.RESET}")
        lines.append("")

    # Summary
    lines.append(separator)
    lines.append("SUMMARY")
    lines.append(separator)
    lines.append("")

    # Determine overall status
    critical_failures = (
        has_duplicates or
        (orphaned_refs and len(orphaned_refs) > 0) or
        any(
            result.get("status") != "PASS"
            for result in completeness.values()
        )
    )

    warnings_count = sum(
        1 for result in record_counts.values() if result.get("status") == "WARN"
    )

    if critical_failures:
        lines.append(f"{Colors.RED}✗ CRITICAL FAILURES DETECTED{Colors.RESET}")
    else:
        lines.append(f"{Colors.GREEN}✓ All critical checks PASSED{Colors.RESET}")

    if warnings_count > 0:
        lines.append(f"{Colors.YELLOW}⚠ {warnings_count} warnings (non-critical){Colors.RESET}")

    lines.append("")

    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="Run data quality checks on ingested Ghibli data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python src/ingestion/data_quality_check.py
  python src/ingestion/data_quality_check.py --output custom_report.txt
  python src/ingestion/data_quality_check.py --verbose
        """,
    )

    parser.add_argument(
        "--output",
        type=str,
        default="data/processed/data_quality_report.txt",
        help="Path to save data quality report (default: data/processed/data_quality_report.txt)",
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging (DEBUG level)",
    )

    return parser.parse_args()


def main() -> int:
    """
    Run data quality checks and return exit code.

    Returns:
        0 if all critical checks pass, 1 if critical failures detected
    """
    args = parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger.info("Starting data quality validation...")

    try:
        # Get DuckDB connection
        conn = get_duckdb_connection()

        # Run all validation checks
        validation_results = {
            "record_counts": validate_record_counts(conn),
            "completeness": validate_completeness(conn),
            "duplicates": validate_no_duplicates(conn),
            "referential_integrity": validate_referential_integrity(conn),
            "kaggle_api_crossref": validate_kaggle_api_crossref(conn),
        }

        # Generate report
        report = generate_report(validation_results)

        # Print to console
        print(report)

        # Save to file
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Remove ANSI color codes for file output
        import re

        ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
        report_no_color = ansi_escape.sub("", report)

        with open(output_path, "w") as f:
            f.write(report_no_color)

        logger.info(f"Report saved to: {output_path}")

        # Determine exit code based on critical failures
        has_duplicates = any(
            result.get("status") == "FAIL"
            for result in validation_results["duplicates"].values()
        )
        has_orphaned_refs = (
            validation_results["referential_integrity"]["status"] == "FAIL"
        )
        has_null_critical_fields = any(
            result.get("status") != "PASS"
            for result in validation_results["completeness"].values()
        )

        critical_failures = (
            has_duplicates or has_orphaned_refs or has_null_critical_fields
        )

        if critical_failures:
            logger.error(f"{Colors.RED}✗ Critical failures detected{Colors.RESET}")
            return 1
        else:
            logger.info(f"{Colors.GREEN}✓ All checks passed{Colors.RESET}")
            return 0

    except Exception as e:
        logger.error(f"Fatal error during validation: {e}")
        import traceback

        traceback.print_exc()
        return 1

    finally:
        # Close connection if it exists
        try:
            if "conn" in locals():
                conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
