#!/usr/bin/env python3
"""
Quick demo script to run emotion analysis on a small sample and view results.

This processes just 1-2 films so you can see results quickly (instead of waiting 1-2 hours).

IMPORTANT: Make sure to activate the virtual environment first:
    source venv/bin/activate
"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Check if transformers is available
try:
    import transformers
except ImportError:
    print("❌ Error: 'transformers' module not found!")
    print("\nPlease activate the virtual environment first:")
    print("   source venv/bin/activate")
    print("\nOr install dependencies:")
    print("   pip install transformers torch")
    sys.exit(1)

from src.nlp.analyze_emotions import process_all_films


def main():
    """Run emotion analysis demo on small sample."""
    print("🎬 Emotion Analysis Demo")
    print("=" * 60)
    print("\nThis will process 2 films (Spirited Away & Princess Mononoke) in English only.")
    print("This should take ~2-5 minutes instead of 1-2 hours for all films.\n")

    subtitle_dir = Path("data/processed/subtitles")
    db_path = Path("data/ghibli.duckdb")

    if not subtitle_dir.exists():
        print(f"❌ Error: Subtitle directory not found: {subtitle_dir}")
        sys.exit(1)

    if not db_path.exists():
        print(f"❌ Error: Database not found: {db_path}")
        print("   Please run data ingestion first (Epic 1)")
        sys.exit(1)

    # Process just 2 films in English
    # Note: film_filter matches base slug (without language suffix)
    results = process_all_films(
        subtitle_dir=subtitle_dir,
        db_path=db_path,
        film_filter=["spirited_away", "princess_mononoke"],
        language_filter=["en"],
    )

    print("\n" + "=" * 60)
    print("📊 Processing Results")
    print("=" * 60)

    success_count = sum(1 for r in results if r["success"])
    total_records = sum(r["records_loaded"] for r in results)

    for result in results:
        status = "✓" if result["success"] else "✗"
        print(
            f"{status} {result['film_slug']} ({result['language_code']}): "
            f"{result['records_loaded']} records"
        )
        if not result["success"]:
            print(f"   Error: {result['error_message']}")

    print(f"\n✅ Successfully processed {success_count}/{len(results)} films")
    print(f"📈 Total records loaded: {total_records}")

    print("\n" + "=" * 60)
    print("🔍 View Results")
    print("=" * 60)
    print("\nTo view the emotion analysis results, run:")
    print("   python scripts/view_emotion_results.py")
    print("\nOr with filters:")
    print("   python scripts/view_emotion_results.py --film spirited_away --language en")
    print("   python scripts/view_emotion_results.py --limit 20")


if __name__ == "__main__":
    main()


