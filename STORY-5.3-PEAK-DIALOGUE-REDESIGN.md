# Story 5.3 - Peak Dialogue Redesign

**Date:** 2025-11-22
**Issue:** Mouseover tooltips on peak markers were confusing due to overlapping with Plotly's default timeline tooltips
**Solution:** Redesigned UX with colored star markers + dedicated sidebar panel

---

## Problem

The initial implementation added yellow star markers with scene description tooltips on the emotion timeline. However, this created UX confusion:

- **Overlapping tooltips:** Plotly's default timeline tooltips (showing emotion values) conflicted with custom peak tooltips
- **Poor discoverability:** Users couldn't distinguish which stars belonged to which emotions
- **No actual dialogue:** Scene descriptions were summaries, not the actual dialogue lines spoken

## Solution: Two-Part Enhancement

### 1. Color-Coded Star Markers (Timeline)

**What changed:**
- Stars now **match the color** of their corresponding emotion line
- Hover tooltips **disabled** (`hoverinfo='skip'`) to avoid confusion
- Stars are purely **visual indicators** of peak moments

**Implementation:**
```python
# src/app/utils/visualization.py:419-451
# Use same color as emotion line for star marker
star_color = emotion_colors.get(emotion_label, "#94A3B8")

fig.add_trace(go.Scatter(
    x=[minute], y=[y_val],
    mode='markers',
    marker=dict(size=12, color=star_color, symbol='star', line=dict(color='white', width=1)),
    showlegend=False,
    hoverinfo='skip'  # No tooltips to avoid confusion
))
```

**Example:**
- 💛 Gold star → Joy peak
- 💙 Blue star → Sadness peak
- 🔴 Red star → Anger peak

### 2. Peak Dialogue Sidebar Panel

**What changed:**
- Added **right sidebar** next to timeline showing actual subtitle dialogue text
- Loads dialogue from parsed subtitle files (`data/processed/subtitles_improved/`)
- Shows **top 6 emotion peaks** with expandable cards
- Each card displays:
  - Emotion emoji (😊 joy, 😨 fear, 😢 sadness, etc.)
  - Emotion type and minute timestamp
  - Intensity score and peak rank
  - **Actual dialogue lines** spoken during that moment (±30 second window)

**Implementation:**
```python
# src/app/utils/data_loader.py:540-597
@st.cache_data(ttl=3600)
def get_peak_dialogues(film_slug: str, language_code: str, peaks_df: pd.DataFrame):
    # Load parsed subtitle JSON file
    parsed_file = Path(f"data/processed/subtitles_improved/{film_slug}_{language_code}_v2_parsed.json")

    # Extract dialogues within ±30 seconds of each peak minute
    for peak in peaks_df:
        peak_start_sec = peak_minute * 60 - 30
        peak_end_sec = peak_minute * 60 + 30
        matching_dialogues = [sub["dialogue_text"] for sub in subtitles
                              if peak_start_sec <= sub["start_time"] <= peak_end_sec]

    return peak_dialogues  # List of dicts with emotion_type, minute, dialogue_lines
```

**UI Layout:**
```python
# src/app/pages/1_🎬_The_Spirit_Archives.py:192-253
col_timeline, col_peaks = st.columns([2, 1])  # 2:1 ratio

with col_timeline:
    st.plotly_chart(timeline_fig, use_container_width=True)

with col_peaks:
    st.markdown("#### 🎭 Peak Emotional Moments")
    for peak in peak_dialogues[:6]:
        with st.expander(f"{emoji} **{emotion}** (min {minute})"):
            st.caption(f"Intensity: {intensity:.3f} • Rank #{rank}")
            for line in dialogue_lines:
                st.markdown(f"> {line}")  # Blockquote formatting
```

---

## Technical Details

### Data Flow

1. **DuckDB Query:** Get top 3 peaks per emotion from `mart_emotion_peaks_smoothed`
   ```sql
   SELECT emotion_type, peak_minute_offset, intensity_score, peak_rank
   FROM main_marts.mart_emotion_peaks_smoothed
   WHERE film_id = ? AND language_code = ? AND peak_rank <= 3
   ```

2. **Film Slug Lookup:** Convert UUID `film_id` → `film_slug` for file path
   ```python
   film_slug_result = conn.execute(
       "SELECT DISTINCT film_slug FROM raw.film_emotions WHERE film_id = ?",
       [film_id]
   ).fetchone()
   # Result: "spirited_away_en" → strip to "spirited_away"
   ```

3. **Subtitle File Loading:** Read parsed JSON
   ```json
   {
     "metadata": { "film_name": "Spirited Away V2", ... },
     "subtitles": [
       {"start_time": 13.347, "dialogue_text": "Good luck, Chihiro..."},
       ...
     ]
   }
   ```

4. **Dialogue Extraction:** Filter subtitles by time window
   ```python
   peak_minute = 67  # Joy peak at minute 67
   peak_start_sec = 67 * 60 - 30  # 3990 seconds
   peak_end_sec = 67 * 60 + 30    # 4050 seconds
   # Returns: ["Chihiro and Haku flying above the clouds", ...]
   ```

### File Structure

```
data/processed/subtitles_improved/
├── spirited_away_en_v2_parsed.json       # English dialogue
├── spirited_away_fr_v2_parsed.json       # French dialogue
├── princess_mononoke_en_v2_parsed.json
└── ...
```

Each file contains:
- `metadata`: Film name, language, total subtitles, duration
- `subtitles`: Array of `{subtitle_index, start_time, end_time, duration, dialogue_text}`

---

## Benefits of Redesign

### UX Improvements
✅ **No tooltip confusion:** Stars are visual-only, don't interfere with line chart tooltips
✅ **Clear emotion association:** Color-coded stars instantly show which emotion is peaking
✅ **Actual dialogue context:** Sidebar shows real subtitle text, not just summaries
✅ **Expandable cards:** Users can explore peaks at their own pace
✅ **Better layout:** Side-by-side view keeps timeline visible while browsing dialogue

### Technical Advantages
✅ **Direct file access:** Reads parsed subtitles without needing new database tables
✅ **Efficient caching:** `@st.cache_data` prevents re-loading JSON on every interaction
✅ **Fallback handling:** Gracefully degrades if subtitle files missing
✅ **Language-aware:** Works across all 5 languages (en, fr, es, nl, ar)

---

## Example Output

**Timeline:**
- Emotion line chart with 5 emotions (joy, fear, sadness, etc.)
- Colored stars at peak moments matching line colors
- Clean hover behavior (only shows emotion intensity, no star tooltips)

**Sidebar:**
```
🎭 Peak Emotional Moments
Actual dialogue from the most intense scenes

😊 Joy (min 67) ▼
   Intensity: 0.8942 • Rank #1
   > "Look, Chihiro! We're flying!"
   > "The clouds are so close..."
   > "Hold on tight!"

😨 Fear (min 89) ▼
   Intensity: 0.7234 • Rank #1
   > "No-Face is destroying everything!"
   > "Run! Get away from here!"
```

---

## Files Modified

1. **`src/app/utils/data_loader.py`**
   - Added `get_peak_dialogues()` function (lines 540-597)
   - Loads parsed subtitle JSON and extracts dialogue by time window

2. **`src/app/utils/visualization.py`**
   - Updated `plot_emotion_timeline()` peak markers (lines 419-451)
   - Changed from yellow stars with tooltips → color-matched stars without tooltips
   - Set `hoverinfo='skip'` to disable confusing tooltips

3. **`src/app/pages/1_🎬_The_Spirit_Archives.py`**
   - Added `get_peak_dialogues` import (line 25)
   - Created 2-column layout: timeline + sidebar (lines 192-253)
   - Added peak dialogue expanders with emoji indicators
   - Updated description: "Colored stars mark peak emotional moments"

---

## Testing Notes

**Verified:**
- ✅ Stars appear on timeline with correct colors matching emotion lines
- ✅ No tooltip overlap - hover only shows emotion intensity from line
- ✅ Sidebar loads actual dialogue text from parsed subtitle files
- ✅ Expanders show emotion emoji, minute, intensity, rank, and dialogue lines
- ✅ Works across different films and languages
- ✅ Graceful degradation if subtitle files not found

**Edge Cases:**
- Missing subtitle file → Shows "Peak dialogue data not available"
- No peaks found → Sidebar displays info message
- Empty dialogue window → Peak not included in sidebar

---

## Performance Impact

**Initial Load:**
- Sidebar adds ~100-200ms for JSON file read (first load only)
- Subsequent loads: ~5ms (cached via `@st.cache_data`)

**Memory:**
- Typical parsed subtitle file: 50-150KB
- Peak dialogue extraction: 10-20 dialogue lines × 100 bytes = ~2KB
- Total overhead: Negligible (<1MB for typical session)

---

## Future Enhancements

1. **Clickable stars:** Click star → auto-expand corresponding sidebar card
2. **Timestamp links:** Click dialogue line → jump to that minute on timeline
3. **Audio playback:** Embed actual audio clips from peak moments (requires audio file integration)
4. **Search dialogue:** Filter peak dialogues by keyword
5. **Download dialogue:** Export peak dialogue as TXT/PDF

---

**Status:** ✅ Redesign complete and deployed
**Streamlit App:** Running on http://localhost:8501
**User Feedback:** Awaiting testing
