#!/usr/bin/env python3
"""
Test script to validate data quality warnings for 9 FAIL validations.

Verifies that get_validation_status() correctly identifies all film-language
combinations with FAIL status from Story 3.6.5 analysis.

[Source: Epic 3.6.5 - Data Quality Validation Layer Testing]
"""

import sys
from pathlib import Path

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "app"))

from utils.data_loader import get_validation_status
from tabulate import tabulate


# Expected FAIL combinations from FAIL_VALIDATIONS_ANALYSIS.md
EXPECTED_FAILS = [
    ("The Cat Returns", "es", 139),
    ("My Neighbors the Yamadas", "ar", 77),
    ("The Wind Rises", "fr", 60),  # v2
    ("My Neighbors the Yamadas", "fr", 24),
    ("The Wind Rises", "nl", 24),  # v2
    ("Tales from Earthsea", "es", 19),
    ("Whisper of the Heart", "es", 18),
    ("Whisper of the Heart", "nl", 18),
    ("The Tale of the Princess Kaguya", "ar", 13),
]


def get_film_id_map():
    """Get mapping of film titles to UUIDs from database."""
    import duckdb
    from utils.config import DUCKDB_PATH

    conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)

    query = """
    SELECT DISTINCT
        e.film_title,
        e.film_id
    FROM main_marts.mart_film_emotion_timeseries e
    WHERE e.film_id IS NOT NULL
    """

    result = conn.execute(query).fetch_df()
    conn.close()

    # Create mapping
    return {row["film_title"]: row["film_id"] for _, row in result.iterrows()}


def test_validation_warnings():
    """Test that all 9 FAIL validations are correctly identified."""
    print("=" * 80)
    print("Data Quality Warning Test - Epic 3.6.5")
    print("=" * 80)
    print()

    # Get film ID mappings
    print("Loading film ID mappings...")
    film_id_map = get_film_id_map()
    print(f"Found {len(film_id_map)} films in database\n")

    results = []
    fail_count = 0
    missing_count = 0

    for film_title, language_code, expected_overrun in EXPECTED_FAILS:
        # Get film ID
        if film_title not in film_id_map:
            results.append([
                film_title,
                language_code.upper(),
                "❌ FILM NOT FOUND",
                "-",
                "-"
            ])
            missing_count += 1
            continue

        film_id = film_id_map[film_title]

        # Get validation status
        validation_data = get_validation_status(film_id, language_code)

        if validation_data is None:
            results.append([
                film_title,
                language_code.upper(),
                "❌ NO VALIDATION DATA",
                "-",
                "-"
            ])
            missing_count += 1
            continue

        status = validation_data["validation_status"]
        overrun = validation_data["overrun_minutes"]

        if status == "FAIL":
            severity = "🔴 CRITICAL" if overrun > 50 else "🟠 SEVERE" if overrun > 20 else "🟡 MODERATE"
            results.append([
                film_title,
                language_code.upper(),
                f"✅ FAIL ({severity})",
                f"{overrun:.0f} min",
                f"{expected_overrun} min"
            ])
            fail_count += 1
        else:
            results.append([
                film_title,
                language_code.upper(),
                f"❌ {status} (Expected FAIL)",
                f"{overrun:.0f} min" if overrun else "N/A",
                f"{expected_overrun} min"
            ])

    # Print results table
    print("\nTest Results:")
    print(tabulate(
        results,
        headers=["Film", "Lang", "Status", "Actual Overrun", "Expected Overrun"],
        tablefmt="grid"
    ))

    # Summary
    print("\n" + "=" * 80)
    print("Summary:")
    print(f"  ✅ Correctly identified FAIL status: {fail_count}/9")
    print(f"  ❌ Missing/incorrect validations: {9 - fail_count}/9")
    if missing_count > 0:
        print(f"  ⚠️  Films not found in database: {missing_count}")
    print("=" * 80)

    if fail_count == 9:
        print("\n🎉 SUCCESS: All 9 FAIL validations correctly identified!")
        return 0
    else:
        print("\n❌ FAILURE: Some validations not correctly identified.")
        return 1


if __name__ == "__main__":
    try:
        exit_code = test_validation_warnings()
        sys.exit(exit_code)
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
