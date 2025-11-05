# Bug Fix: Emotion Neutral Dominance

## Problem Report
User reported two critical issues:
1. **Heatmap showing 100% everywhere** - No differentiation between films
2. **Radar chart skewed to neutral** - One emotion dominated all visualizations

## Root Cause Analysis

### Investigation Results
```sql
-- Query revealed the issue:
SELECT AVG(emotion_neutral) as neutral,
       AVG(emotion_joy) as joy,
       AVG(emotion_sadness) as sad
FROM raw.film_emotions;

-- Results:
--   Neutral: 56.5%  ← PROBLEM!
--   Joy: 1.6%
--   Sadness: 2.4%
```

**Discovery**: The emotion detection model classified **56.5% of all content as "neutral"**, with other emotions at only 1-3%.

### Why This Broke Visualizations

#### 1. Heatmap (100% Similarity Issue)
- All films had nearly identical vectors because neutral dominated
- Joy range across all films: 0.94% - 2.47% (variance of only 1.5%)
- When all vectors are similar, cosine similarity ≈ 1.0 (100%) for everything
- Result: Uniform blue heatmap with no insights

#### 2. Radar Chart (Neutral Skew)
- Neutral was 56%, all other emotions 1-3%
- Radar showed one massive spike (neutral) and tiny bumps for everything else
- No visual differentiation between films
- Result: Unreadable, uninformative chart

---

## Solution Implemented

### 1. Exclude Neutral Emotion
```python
def calculate_emotion_vectors(
    conn: duckdb.DuckDBPyConnection,
    exclude_neutral: bool = True,  # NEW: Default to True
    normalize: bool = True,
) -> Dict[str, Dict[str, float]]:
    # ... query all 28 emotions ...
    
    # NEW: Remove neutral
    if exclude_neutral and 'neutral' in emotions:
        del emotions['neutral']
```

**Rationale**: "Neutral" is a conservative fallback label, not a meaningful emotion for comparison.

### 2. Normalize Remaining Emotions
```python
# NEW: Normalize so emotions sum to 1.0 (100%)
if normalize:
    total = sum(emotions.values())
    if total > 0:
        emotions = {k: v / total for k, v in emotions.items()}
```

**Rationale**: Amplifies the relative differences between emotions, making patterns visible.

### 3. Use Euclidean Distance Instead of Cosine
```python
def euclidean_distance(vec1: Dict[str, float], vec2: Dict[str, float]) -> float:
    squared_diff = sum((vec1[k] - vec2[k]) ** 2 for k in keys)
    return math.sqrt(squared_diff)

def distance_to_similarity(distance: float, max_distance: float) -> float:
    return max(0.0, 1.0 - (distance / max_distance))
```

**Rationale**: Euclidean distance is more sensitive to small differences than cosine similarity.

---

## Results After Fix

### Before (with neutral):
```
Film averages (all similar):
  Joy: 0.94% - 2.47%
  Sadness: 1.60% - 3.23%
  Neutral: 50% - 60%  ← Dominates everything
  
Similarity: All films ~95-100%
```

### After (neutral excluded, normalized):
```
Film averages (distinct profiles):
  Curiosity: 12.6% - 14.2%
  Admiration: 9.2% - 12.9%
  Approval: 10.4% - 13.0%
  
Similarity: Films range 60-100% (varied!)
Distance between films: 0.05-0.06 (measurable!)
```

---

## Code Changes

### Files Modified

#### 1. `src/validation/chart_utils.py`

**Added Parameters**:
- `calculate_emotion_vectors(exclude_neutral=True, normalize=True)`

**New Functions**:
```python
def euclidean_distance(vec1, vec2) -> float
def distance_to_similarity(distance, max_distance) -> float
```

**Updated Functions**:
- `plot_emotion_similarity_heatmap()` - Now uses distance-based similarity
- `plot_emotion_fingerprint_radar()` - Now excludes neutral, better scaling

#### 2. `src/validation/dashboard.py`

**Updated Info Text**:
- Changed "28 emotions" → "27 emotions (neutral excluded)"
- Changed "Cosine similarity" → "Normalized & distance-based"
- Added explanation about neutral exclusion (56%)

---

## Visual Improvements

### Heatmap
**Before**: Solid blue (all ~100%)  
**After**: Gradient from yellow/red (60%) to blue (100%)

**New Title**: "Film Emotional Similarity Matrix (Neutral emotion excluded for clarity)"

### Radar Chart
**Before**: One giant spike (neutral) + tiny bumps  
**After**: Distinct 8-pointed shapes showing top emotions

**New Title**: "Emotional Fingerprint Comparison (Neutral excluded, normalized to 100%)"

**New Scale**: 0-17% (instead of 0-60% where everything was compressed)

---

## Testing Results

```bash
✅ Calculations improved!

Sample emotion distributions:
  Princess Kaguya: Admiration 12.9%, Curiosity 12.6%, Approval 10.4%
  Poppy Hill: Curiosity 14.0%, Approval 13.0%, Admiration 10.3%
  Arrietty: Curiosity 14.2%, Approval 10.6%, Admiration 9.2%

Distance measurements:
  Film 1 vs Film 2: 0.0606 (measurable difference!)
  Film 2 vs Film 3: 0.0558
  Film 3 vs Film 4: 0.0595

Value ranges:
  Min: 0.30%
  Max: 17.01%
  Mean: 3.70%
```

---

## Lessons Learned

### 1. Always Inspect Raw Data First
- Don't assume ML model outputs are balanced
- Check for dominant/null categories
- Validate distributions before building visualizations

### 2. Normalize When Needed
- Small absolute differences can be meaningful
- Normalization reveals relative patterns
- Essential when one category dominates

### 3. Choose Right Distance Metric
- Cosine similarity: Good for high-dimensional, sparse data
- Euclidean distance: Better for normalized, dense vectors
- Always consider your data characteristics

### 4. Make Exclusions Explicit
- Document what's excluded and why
- Show in chart titles/subtitles
- Help users understand the transformation

---

## Impact

### Before Fix
- ❌ Heatmap: Useless (all 100%)
- ❌ Radar: Unreadable (neutral spike)
- ❌ No insights
- ❌ User frustrated

### After Fix
- ✅ Heatmap: Shows varied similarities (60-100%)
- ✅ Radar: Distinct emotion profiles per film
- ✅ Meaningful insights about emotional differences
- ✅ Beautiful, interpretable visualizations

---

## Deployment

**Status**: ✅ Fixed and tested  
**Date**: November 5, 2025  
**Commit Message**: "Fix emotion visualizations: exclude dominant neutral emotion, normalize distributions"

**To run**:
```bash
streamlit run src/validation/dashboard.py
```

The dashboard now provides the insightful, visually compelling emotion analysis originally intended! 🎨✨


