"""
Add validation metadata columns to DuckDB film_emotions table.

This script adds columns to track subtitle version validation results:
- film_version_reference: VARCHAR (e.g., "Blu-ray", "Disney+ US")
- subtitle_timing_validated: BOOLEAN
- timing_drift_percent: FLOAT
"""

import json
import logging
from pathlib import Path
from typing import Dict

import duckdb

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("spiriteddata.validation.add_validation_metadata")


def add_validation_columns(db_path: str = "data/ghibli.duckdb") -> None:
    """
    Add validation metadata columns to raw.film_emotions table.

    Args:
        db_path: Path to DuckDB database file
    """
    logger.info(f"Connecting to database: {db_path}")

    conn = duckdb.connect(db_path)

    try:
        # Check if columns already exist
        existing_columns = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'raw' AND table_name = 'film_emotions'"
        ).fetchall()

        existing_column_names = [col[0] for col in existing_columns]

        # Add film_version_reference column
        if "film_version_reference" not in existing_column_names:
            logger.info("Adding column: film_version_reference")
            conn.execute(
                "ALTER TABLE raw.film_emotions ADD COLUMN film_version_reference VARCHAR"
            )
        else:
            logger.info("Column film_version_reference already exists")

        # Add subtitle_timing_validated column
        if "subtitle_timing_validated" not in existing_column_names:
            logger.info("Adding column: subtitle_timing_validated")
            conn.execute(
                "ALTER TABLE raw.film_emotions ADD COLUMN subtitle_timing_validated BOOLEAN"
            )
        else:
            logger.info("Column subtitle_timing_validated already exists")

        # Add timing_drift_percent column
        if "timing_drift_percent" not in existing_column_names:
            logger.info("Adding column: timing_drift_percent")
            conn.execute(
                "ALTER TABLE raw.film_emotions ADD COLUMN timing_drift_percent FLOAT"
            )
        else:
            logger.info("Column timing_drift_percent already exists")

        logger.info("✅ Validation columns added successfully")

    except Exception as e:
        logger.error(f"Failed to add validation columns: {e}", exc_info=True)
        raise
    finally:
        conn.close()


def populate_validation_metadata(
    db_path: str = "data/ghibli.duckdb",
    validation_results_path: str = "data/processed/subtitle_validation_results.json",
    film_versions_path: str = "data/metadata/film_versions.json",
) -> None:
    """
    Populate validation metadata columns with results from validation.

    Args:
        db_path: Path to DuckDB database file
        validation_results_path: Path to validation results JSON
        film_versions_path: Path to film versions metadata
    """
    logger.info("Populating validation metadata...")

    # Load validation results
    validation_results_file = Path(validation_results_path)
    if not validation_results_file.exists():
        logger.warning(
            f"Validation results not found: {validation_results_path}. "
            "Run validate_subtitle_timing.py first to generate validation results."
        )
        return

    with open(validation_results_file) as f:
        validation_results = json.load(f)

    # Load film versions
    with open(film_versions_path) as f:
        film_versions = json.load(f)
        film_versions.pop("_documentation", None)

    conn = duckdb.connect(db_path)

    try:
        # Update each film's validation metadata
        for film_slug, data in validation_results.items():
            # Get film version reference
            film_version_ref = film_versions.get(film_slug, {}).get("reference_source", "Unknown")

            # Process each language
            for lang, validation in data.get("per_language", {}).items():
                timing_drift = validation.get("timing_drift_percent")
                is_validated = validation.get("status") == "PASS"

                # Update database
                film_slug_with_lang = f"{film_slug}_{lang}"

                conn.execute(
                    """
                    UPDATE raw.film_emotions
                    SET
                        film_version_reference = ?,
                        subtitle_timing_validated = ?,
                        timing_drift_percent = ?
                    WHERE film_slug = ?
                    """,
                    [film_version_ref, is_validated, timing_drift, film_slug_with_lang],
                )

                logger.debug(
                    f"Updated {film_slug_with_lang}: ref={film_version_ref}, "
                    f"validated={is_validated}, drift={timing_drift}%"
                )

        logger.info("✅ Validation metadata populated successfully")

        # Show summary
        summary = conn.execute(
            """
            SELECT
                subtitle_timing_validated,
                COUNT(DISTINCT film_slug) as film_count
            FROM raw.film_emotions
            WHERE subtitle_timing_validated IS NOT NULL
            GROUP BY subtitle_timing_validated
            ORDER BY subtitle_timing_validated DESC
            """
        ).fetchall()

        logger.info("Validation summary:")
        for row in summary:
            validated, count = row
            status = "VALIDATED" if validated else "NOT VALIDATED"
            logger.info(f"  {status}: {count} films")

    except Exception as e:
        logger.error(f"Failed to populate validation metadata: {e}", exc_info=True)
        raise
    finally:
        conn.close()


def export_validation_results_to_json(
    subtitle_dir: str = "data/processed/subtitles",
    output_path: str = "data/processed/subtitle_validation_results.json",
) -> None:
    """
    Export validation results to JSON file for database population.

    This function loads the validation script and generates results.

    Args:
        subtitle_dir: Directory containing parsed subtitle files
        output_path: Path to save JSON results
    """
    from src.validation.validate_subtitle_timing import (
        generate_validation_report,
        load_film_versions,
        validate_subtitle_timing,
        validate_cross_language_consistency,
    )

    logger.info("Generating validation results for database...")

    # Load film versions
    film_versions = load_film_versions()

    # Validate all subtitle files
    results = {}
    subtitle_path = Path(subtitle_dir)

    for subtitle_file in sorted(subtitle_path.glob("*_parsed.json")):
        # Extract film slug and language
        filename_parts = subtitle_file.stem.rsplit("_", 2)
        if len(filename_parts) < 3:
            continue

        film_slug = filename_parts[0]
        lang = filename_parts[1]

        # Initialize results structure for this film
        if film_slug not in results:
            results[film_slug] = {"per_language": {}, "cross_language": None}

        # Validate timing for this language
        validation = validate_subtitle_timing(subtitle_file, film_versions)
        results[film_slug]["per_language"][lang] = validation

    # Perform cross-language validation for each film
    for film_slug in results:
        results[film_slug]["cross_language"] = validate_cross_language_consistency(
            film_slug, subtitle_dir
        )

    # Save to JSON
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)

    logger.info(f"✅ Validation results exported to: {output_path}")

    return results


def main():
    """Main execution function."""
    import argparse

    parser = argparse.ArgumentParser(description="Add validation metadata to DuckDB")
    parser.add_argument(
        "--db-path", default="data/ghibli.duckdb", help="Path to DuckDB database"
    )
    parser.add_argument(
        "--add-columns",
        action="store_true",
        help="Add validation metadata columns to database",
    )
    parser.add_argument(
        "--populate",
        action="store_true",
        help="Populate validation metadata from validation results",
    )
    parser.add_argument(
        "--export-results",
        action="store_true",
        help="Export validation results to JSON file",
    )

    args = parser.parse_args()

    try:
        if args.export_results:
            export_validation_results_to_json()

        if args.add_columns:
            add_validation_columns(args.db_path)

        if args.populate:
            populate_validation_metadata(args.db_path)

        if not (args.add_columns or args.populate or args.export_results):
            # Default: do everything
            logger.info("Running full workflow: export -> add columns -> populate")
            export_validation_results_to_json()
            add_validation_columns(args.db_path)
            populate_validation_metadata(args.db_path)

    except Exception as e:
        logger.error(f"Operation failed: {e}", exc_info=True)
        exit(1)


if __name__ == "__main__":
    main()
