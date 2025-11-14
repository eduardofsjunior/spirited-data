#!/usr/bin/env python3
"""
Simple script to view emotion analysis results from DuckDB.

Usage:
    python scripts/view_emotion_results.py [--film FILM_SLUG] [--language LANG] [--limit N]
"""
import argparse
import sys
from pathlib import Path

import duckdb
import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.nlp.analyze_emotions import GOEMOTIONS_LABELS


def view_emotion_results(
    db_path: str = "data/ghibli.duckdb",
    film_slug: str = None,
    language_code: str = None,
    limit: int = 10,
):
    """View emotion analysis results from DuckDB."""
    conn = duckdb.connect(db_path)

    try:
        # Check if table exists
        table_exists = conn.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = 'raw' AND table_name = 'film_emotions'
            """
        ).fetchone()[0]

        if not table_exists:
            print("❌ No emotion data found. Please run emotion analysis first:")
            print("   python -m src.nlp.analyze_emotions")
            return

        # Build query
        query = """
            SELECT 
                film_slug,
                film_id,
                language_code,
                minute_offset,
                dialogue_count,
                emotion_joy,
                emotion_sadness,
                emotion_fear,
                emotion_anger,
                emotion_surprise,
                emotion_neutral,
                loaded_at
            FROM raw.film_emotions
            WHERE 1=1
        """
        params = []

        if film_slug:
            query += " AND film_slug LIKE ?"
            params.append(f"%{film_slug}%")

        if language_code:
            query += " AND language_code = ?"
            params.append(language_code)

        query += " ORDER BY film_slug, language_code, minute_offset"
        query += f" LIMIT {limit}"

        # Execute query
        df = conn.execute(query, params).df()

        if df.empty:
            print("❌ No results found matching your criteria.")
            return

        print(f"\n📊 Emotion Analysis Results ({len(df)} rows)")
        print("=" * 80)

        # Group by film and language
        for (film, lang), group in df.groupby(["film_slug", "language_code"]):
            print(f"\n🎬 Film: {film} ({lang.upper()})")
            print("-" * 80)
            print(
                f"{'Minute':<8} {'Dialogue':<10} {'Joy':<8} {'Sadness':<10} {'Fear':<8} {'Anger':<10} {'Surprise':<10} {'Neutral':<10}"
            )
            print("-" * 80)

            for _, row in group.head(20).iterrows():
                print(
                    f"{row['minute_offset']:<8} {row['dialogue_count']:<10} "
                    f"{row['emotion_joy']:<8.3f} {row['emotion_sadness']:<10.3f} "
                    f"{row['emotion_fear']:<8.3f} {row['emotion_anger']:<10.3f} "
                    f"{row['emotion_surprise']:<10.3f} {row['emotion_neutral']:<10.3f}"
                )

        # Summary statistics
        print("\n" + "=" * 80)
        print("📈 Summary Statistics")
        print("-" * 80)

        summary_query = """
            SELECT 
                COUNT(DISTINCT film_slug) as num_films,
                COUNT(DISTINCT language_code) as num_languages,
                COUNT(*) as total_records,
                SUM(dialogue_count) as total_dialogues,
                AVG(emotion_joy) as avg_joy,
                AVG(emotion_sadness) as avg_sadness,
                AVG(emotion_fear) as avg_fear
            FROM raw.film_emotions
        """
        summary = conn.execute(summary_query).fetchone()

        print(f"Films processed: {summary[0]}")
        print(f"Languages: {summary[1]}")
        print(f"Total records: {summary[2]}")
        print(f"Total dialogues: {summary[3]}")
        print(f"\nAverage Emotion Scores:")
        print(f"  Joy: {summary[4]:.3f}")
        print(f"  Sadness: {summary[5]:.3f}")
        print(f"  Fear: {summary[6]:.3f}")

        # Top emotions by film
        print("\n" + "=" * 80)
        print("🏆 Top Emotions by Film")
        print("-" * 80)

        top_emotions_query = """
            SELECT 
                film_slug,
                language_code,
                emotion_joy,
                emotion_sadness,
                emotion_fear,
                emotion_anger,
                emotion_surprise
            FROM raw.film_emotions
            ORDER BY emotion_joy DESC
            LIMIT 5
        """
        top_emotions = conn.execute(top_emotions_query).df()

        if not top_emotions.empty:
            for _, row in top_emotions.iterrows():
                print(
                    f"{row['film_slug']} ({row['language_code']}): "
                    f"Joy={row['emotion_joy']:.3f}, "
                    f"Sadness={row['emotion_sadness']:.3f}, "
                    f"Fear={row['emotion_fear']:.3f}"
                )

    finally:
        conn.close()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="View emotion analysis results from DuckDB"
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default="data/ghibli.duckdb",
        help="Path to DuckDB database (default: data/ghibli.duckdb)",
    )
    parser.add_argument(
        "--film",
        type=str,
        default=None,
        help="Filter by film slug (e.g., 'spirited_away')",
    )
    parser.add_argument(
        "--language",
        type=str,
        default=None,
        help="Filter by language code (e.g., 'en', 'fr')",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum number of records to display (default: 10)",
    )

    args = parser.parse_args()

    view_emotion_results(
        db_path=args.db_path,
        film_slug=args.film,
        language_code=args.language,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()









