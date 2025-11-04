"""
Test different rolling average window sizes to find optimal smoothing for emotion data.

Compares noise levels (standard deviation) across different window sizes.
"""

import duckdb
import pandas as pd
import numpy as np
from pathlib import Path
from src.shared.config import DUCKDB_PATH

def analyze_noise_by_window(conn: duckdb.DuckDBPyConnection, window_size: int, film_slug: str = "spirited_away_en", lang: str = "en") -> dict:
    """
    Apply rolling average with specified window and calculate noise metrics.

    Args:
        conn: DuckDB connection
        window_size: Rolling window size in minutes
        film_slug: Film to analyze
        lang: Language code

    Returns:
        Dict with noise metrics
    """
    # Get raw emotion data for one film
    query = f"""
        SELECT minute_offset, emotion_joy, emotion_fear, emotion_anger, emotion_sadness, emotion_love
        FROM raw.film_emotions
        WHERE film_slug = '{film_slug}'
          AND language_code = '{lang}'
        ORDER BY minute_offset
    """

    df = conn.execute(query).df()

    if df.empty:
        return {"error": "No data found"}

    # Apply rolling average
    emotion_cols = ['emotion_joy', 'emotion_fear', 'emotion_anger', 'emotion_sadness', 'emotion_love']
    smoothed = df.copy()

    for col in emotion_cols:
        smoothed[f'{col}_smoothed'] = df[col].rolling(window=window_size, center=True, min_periods=1).mean()

    # Calculate noise metrics (standard deviation of first-order differences)
    noise_metrics = {}
    raw_volatilities = []

    for col in emotion_cols:
        raw_diff = df[col].diff().abs()
        smoothed_diff = smoothed[f'{col}_smoothed'].diff().abs()

        raw_vol = raw_diff.std()
        smoothed_vol = smoothed_diff.std()
        raw_volatilities.append(raw_vol)

        noise_metrics[col] = {
            'raw_volatility': raw_vol,
            'smoothed_volatility': smoothed_vol,
            'noise_reduction_pct': ((raw_vol - smoothed_vol) / raw_vol * 100) if raw_vol > 0 and not np.isnan(raw_vol) else 0
        }

    # Calculate overall metrics
    valid_reductions = [m['noise_reduction_pct'] for m in noise_metrics.values() if not np.isnan(m['noise_reduction_pct'])]
    valid_volatilities = [m['smoothed_volatility'] for m in noise_metrics.values() if not np.isnan(m['smoothed_volatility'])]

    avg_noise_reduction = np.mean(valid_reductions) if valid_reductions else 0.0
    avg_smoothed_volatility = np.mean(valid_volatilities) if valid_volatilities else 0.0

    return {
        'window_size': window_size,
        'avg_noise_reduction_pct': avg_noise_reduction,
        'avg_volatility': avg_smoothed_volatility,
        'by_emotion': noise_metrics,
        'data_points': len(df)
    }


def main():
    """Test multiple window sizes and display comparison."""
    conn = duckdb.connect(str(DUCKDB_PATH))

    # Test different window sizes
    window_sizes = [1, 3, 5, 7, 10, 15]  # 1 = no smoothing

    print("=" * 80)
    print("EMOTION DATA SMOOTHING ANALYSIS")
    print("=" * 80)
    print("\nAnalyzing: Spirited Away (EN)")
    print("\nTesting window sizes:", window_sizes)
    print("\n" + "=" * 80)

    results = []
    for window in window_sizes:
        result = analyze_noise_by_window(conn, window)
        results.append(result)

        print(f"\nWindow Size: {window} minutes")
        print(f"  Avg Noise Reduction: {result['avg_noise_reduction_pct']:.1f}%")
        print(f"  Avg Volatility: {result['avg_volatility']:.4f}")
        print(f"  Data Points: {result['data_points']}")

        # Show per-emotion breakdown
        print("  Per-Emotion Noise Reduction:")
        for emotion, metrics in result['by_emotion'].items():
            print(f"    {emotion}: {metrics['noise_reduction_pct']:.1f}% reduction")

    print("\n" + "=" * 80)
    print("RECOMMENDATIONS")
    print("=" * 80)

    # Find optimal window (best noise reduction without over-smoothing)
    best_window = max(results[1:], key=lambda x: x['avg_noise_reduction_pct'])  # Skip window=1

    print(f"\nCurrent setting: window=3 minutes")
    print(f"  Noise reduction: {results[1]['avg_noise_reduction_pct']:.1f}%")
    print(f"  Volatility: {results[1]['avg_volatility']:.4f}")

    print(f"\nOptimal setting: window={best_window['window_size']} minutes")
    print(f"  Noise reduction: {best_window['avg_noise_reduction_pct']:.1f}%")
    print(f"  Volatility: {best_window['avg_volatility']:.4f}")
    print(f"  Improvement: {best_window['avg_noise_reduction_pct'] - results[1]['avg_noise_reduction_pct']:.1f}% better")

    print("\n" + "=" * 80)
    conn.close()


if __name__ == "__main__":
    main()
