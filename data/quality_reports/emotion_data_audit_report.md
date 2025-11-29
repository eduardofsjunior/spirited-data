# Emotion Data Quality Audit Report

**Audit Date:** 2025-11-26 14:51:37
**Database:** `data/ghibli.duckdb`
**Story:** 3.6.1 - Comprehensive Emotion Data Audit
**Scope:** 100 film-language combinations

---

## Executive Summary

- ✅ **PASS:** 48 combinations (48.0%)
- ⚠️  **WARN:** 10 combinations (10.0%)
- ❌ **FAIL:** 2 combinations (2.0%)
- ❓ **MISSING:** 40 combinations (no subtitle metadata found)
- 🔧 **Total requiring re-processing:** 12 combinations (WARN + FAIL)

**Pass Rate:** 80.0% (excluding MISSING combinations)

### Key Findings

- 2 film-language combinations have **major runtime overruns** (>5 minutes)
  - Average overrun for FAIL cases: 22.5 minutes
- 10 combinations have **minor runtime overruns** (1-5 minutes)
  - Average overrun for WARN cases: 2.9 minutes

---

## Top 10 Worst Runtime Overruns

| Rank | Film Title | Language | Emotion Max (min) | Subtitle Max (min) | Overrun (min) | Status |
|------|------------|----------|-------------------|--------------------|---------------|--------|
| 1 | The Red Turtle | en | 117.0 | 77.9 | 39.1 | ❌ FAIL |
| 2 | Ponyo | nl | 100.0 | 94.1 | 5.9 | ❌ FAIL |
| 3 | Ponyo | es | 99.0 | 94.4 | 4.6 | ⚠️ WARN |
| 4 | The Wind Rises | fr | 121.0 | 116.5 | 4.5 | ⚠️ WARN |
| 5 | Ponyo | fr | 100.0 | 95.8 | 4.2 | ⚠️ WARN |
| 6 | Ponyo | en | 100.0 | 96.1 | 3.9 | ⚠️ WARN |
| 7 | The Red Turtle | ar | 81.0 | 78.3 | 2.7 | ⚠️ WARN |
| 8 | The Red Turtle | es | 62.0 | 59.7 | 2.3 | ⚠️ WARN |
| 9 | My Neighbor Totoro | nl | 86.0 | 83.9 | 2.1 | ⚠️ WARN |
| 10 | The Red Turtle | fr | 80.0 | 78.2 | 1.8 | ⚠️ WARN |

---

## Detailed Findings (All Combinations)

| Film Title | Language | Emotion Max (min) | Subtitle Max (min) | Overrun (min) | Status |
|------------|----------|-------------------|--------------------|---------------|--------|
| Arrietty | ar | 90.0 | 90.0 | -0.0 | ✅ PASS |
| Arrietty | en | 94.0 | 93.7 | 0.3 | ✅ PASS |
| Arrietty | es | 93.0 | 93.9 | -0.9 | ✅ PASS |
| Arrietty | fr | 93.0 | 93.9 | -0.9 | ✅ PASS |
| Arrietty | nl | 93.0 | 93.9 | -0.9 | ✅ PASS |
| Castle in the Sky | ar | 119.0 | N/A | N/A | ❓ MISSING |
| Castle in the Sky | en | 41.0 | N/A | N/A | ❓ MISSING |
| Castle in the Sky | es | 65.0 | N/A | N/A | ❓ MISSING |
| Castle in the Sky | fr | 62.0 | N/A | N/A | ❓ MISSING |
| Castle in the Sky | nl | 41.0 | N/A | N/A | ❓ MISSING |
| Earwig and the Witch | ar | 111.0 | N/A | N/A | ❓ MISSING |
| Earwig and the Witch | en | 109.0 | N/A | N/A | ❓ MISSING |
| Earwig and the Witch | es | 111.0 | N/A | N/A | ❓ MISSING |
| Earwig and the Witch | fr | 112.0 | N/A | N/A | ❓ MISSING |
| Earwig and the Witch | nl | 92.0 | N/A | N/A | ❓ MISSING |
| From Up on Poppy Hill | ar | 90.0 | N/A | N/A | ❓ MISSING |
| From Up on Poppy Hill | en | 89.0 | N/A | N/A | ❓ MISSING |
| From Up on Poppy Hill | es | 90.0 | N/A | N/A | ❓ MISSING |
| From Up on Poppy Hill | fr | 90.0 | N/A | N/A | ❓ MISSING |
| From Up on Poppy Hill | nl | 90.0 | N/A | N/A | ❓ MISSING |
| Grave of the Fireflies | ar | 89.0 | N/A | N/A | ❓ MISSING |
| Grave of the Fireflies | en | 41.0 | N/A | N/A | ❓ MISSING |
| Grave of the Fireflies | es | 41.0 | N/A | N/A | ❓ MISSING |
| Grave of the Fireflies | fr | 41.0 | N/A | N/A | ❓ MISSING |
| Grave of the Fireflies | nl | 85.0 | N/A | N/A | ❓ MISSING |
| My Neighbor Totoro | ar | 86.0 | 85.8 | 0.2 | ✅ PASS |
| My Neighbor Totoro | en | 82.0 | 82.3 | -0.3 | ✅ PASS |
| My Neighbor Totoro | es | 86.0 | 85.6 | 0.4 | ✅ PASS |
| My Neighbor Totoro | fr | 86.0 | 86.2 | -0.2 | ✅ PASS |
| My Neighbor Totoro | nl | 86.0 | 83.9 | 2.1 | ⚠️ WARN |
| My Neighbors the Yamadas | ar | 99.0 | N/A | N/A | ❓ MISSING |
| My Neighbors the Yamadas | en | 103.0 | N/A | N/A | ❓ MISSING |
| My Neighbors the Yamadas | es | 103.0 | N/A | N/A | ❓ MISSING |
| My Neighbors the Yamadas | fr | 99.0 | N/A | N/A | ❓ MISSING |
| My Neighbors the Yamadas | nl | 103.0 | N/A | N/A | ❓ MISSING |
| Only Yesterday | ar | 118.0 | 117.9 | 0.1 | ✅ PASS |
| Only Yesterday | en | 117.0 | 118.6 | -1.6 | ✅ PASS |
| Only Yesterday | es | 118.0 | 118.1 | -0.1 | ✅ PASS |
| Only Yesterday | fr | 56.0 | 56.4 | -0.4 | ✅ PASS |
| Only Yesterday | nl | 113.0 | 113.8 | -0.8 | ✅ PASS |
| Pom Poko | ar | 56.0 | 56.1 | -0.1 | ✅ PASS |
| Pom Poko | en | 119.0 | 119.0 | 0.0 | ✅ PASS |
| Pom Poko | es | 119.0 | 119.1 | -0.1 | ✅ PASS |
| Pom Poko | fr | 117.0 | 117.1 | -0.1 | ✅ PASS |
| Pom Poko | nl | 111.0 | 110.9 | 0.1 | ✅ PASS |
| Ponyo | ar | 100.0 | 179.0 | -79.0 | ✅ PASS |
| Ponyo | en | 100.0 | 96.1 | 3.9 | ⚠️ WARN |
| Ponyo | es | 99.0 | 94.4 | 4.6 | ⚠️ WARN |
| Ponyo | fr | 100.0 | 95.8 | 4.2 | ⚠️ WARN |
| Ponyo | nl | 100.0 | 94.1 | 5.9 | ❌ FAIL |
| Porco Rosso | ar | 89.0 | 89.7 | -0.7 | ✅ PASS |
| Porco Rosso | en | 92.0 | 92.9 | -0.9 | ✅ PASS |
| Porco Rosso | es | 93.0 | 91.5 | 1.5 | ⚠️ WARN |
| Porco Rosso | fr | 93.0 | 93.1 | -0.1 | ✅ PASS |
| Porco Rosso | nl | 93.0 | 93.1 | -0.1 | ✅ PASS |
| Princess Mononoke | ar | 128.0 | 128.7 | -0.7 | ✅ PASS |
| Princess Mononoke | en | 133.0 | 133.2 | -0.2 | ✅ PASS |
| Princess Mononoke | es | 131.0 | 130.9 | 0.1 | ✅ PASS |
| Princess Mononoke | fr | 123.0 | 122.6 | 0.4 | ✅ PASS |
| Princess Mononoke | nl | 133.0 | 133.4 | -0.4 | ✅ PASS |
| Spirited Away | ar | 120.0 | 121.1 | -1.1 | ✅ PASS |
| Spirited Away | en | 124.0 | 124.3 | -0.3 | ✅ PASS |
| Spirited Away | es | 64.0 | 65.0 | -1.0 | ✅ PASS |
| Spirited Away | fr | 65.0 | 65.5 | -0.5 | ✅ PASS |
| Spirited Away | nl | 62.0 | 62.3 | -0.3 | ✅ PASS |
| Tales from Earthsea | ar | 111.0 | N/A | N/A | ❓ MISSING |
| Tales from Earthsea | en | 115.0 | N/A | N/A | ❓ MISSING |
| Tales from Earthsea | es | 109.0 | N/A | N/A | ❓ MISSING |
| Tales from Earthsea | fr | 115.0 | N/A | N/A | ❓ MISSING |
| Tales from Earthsea | nl | 115.0 | N/A | N/A | ❓ MISSING |
| The Cat Returns | ar | 74.0 | 74.8 | -0.8 | ✅ PASS |
| The Cat Returns | en | 71.0 | 70.9 | 0.1 | ✅ PASS |
| The Cat Returns | es | 71.0 | 71.5 | -0.5 | ✅ PASS |
| The Cat Returns | fr | 74.0 | 74.8 | -0.8 | ✅ PASS |
| The Cat Returns | nl | 71.0 | 71.0 | -0.0 | ✅ PASS |
| The Red Turtle | ar | 81.0 | 78.3 | 2.7 | ⚠️ WARN |
| The Red Turtle | en | 117.0 | 77.9 | 39.1 | ❌ FAIL |
| The Red Turtle | es | 62.0 | 59.7 | 2.3 | ⚠️ WARN |
| The Red Turtle | fr | 80.0 | 78.2 | 1.8 | ⚠️ WARN |
| The Red Turtle | nl | 104.0 | 102.7 | 1.3 | ⚠️ WARN |
| The Tale of the Princess Kaguya | ar | 131.0 | N/A | N/A | ❓ MISSING |
| The Tale of the Princess Kaguya | en | 136.0 | N/A | N/A | ❓ MISSING |
| The Tale of the Princess Kaguya | es | 136.0 | N/A | N/A | ❓ MISSING |
| The Tale of the Princess Kaguya | fr | 137.0 | N/A | N/A | ❓ MISSING |
| The Tale of the Princess Kaguya | nl | 136.0 | N/A | N/A | ❓ MISSING |
| The Wind Rises | ar | 123.0 | 124.0 | -1.0 | ✅ PASS |
| The Wind Rises | en | 126.0 | 126.3 | -0.3 | ✅ PASS |
| The Wind Rises | es | 126.0 | 126.1 | -0.1 | ✅ PASS |
| The Wind Rises | fr | 121.0 | 116.5 | 4.5 | ⚠️ WARN |
| The Wind Rises | nl | 117.0 | 212.1 | -95.1 | ✅ PASS |
| When Marnie Was There | ar | 102.0 | 102.5 | -0.5 | ✅ PASS |
| When Marnie Was There | en | 102.0 | 102.4 | -0.4 | ✅ PASS |
| When Marnie Was There | es | 98.0 | 98.4 | -0.4 | ✅ PASS |
| When Marnie Was There | fr | 102.0 | 102.5 | -0.5 | ✅ PASS |
| When Marnie Was There | nl | 102.0 | 102.4 | -0.4 | ✅ PASS |
| Whisper of the Heart | ar | 110.0 | N/A | N/A | ❓ MISSING |
| Whisper of the Heart | en | 107.0 | N/A | N/A | ❓ MISSING |
| Whisper of the Heart | es | 107.0 | N/A | N/A | ❓ MISSING |
| Whisper of the Heart | fr | 60.0 | N/A | N/A | ❓ MISSING |
| Whisper of the Heart | nl | 106.0 | N/A | N/A | ❓ MISSING |

---

## Data Completeness

### Missing from Emotion Data (74 combinations)

These film-language combinations have subtitle files but no emotion data:

- Arrietty (ja)
- Castle In The Sky (ar)
- Castle In The Sky (en)
- Castle In The Sky (es)
- Castle In The Sky (fr)
- Castle In The Sky (ja)
- Castle In The Sky (nl)
- Earwig And The Witch (ar)
- Earwig And The Witch (en)
- Earwig And The Witch (es)
- Earwig And The Witch (fr)
- Earwig And The Witch (ja)
- Earwig And The Witch (nl)
- Film1 (en)
- Film1 (ja)
- From Up On Poppy Hill (ar)
- From Up On Poppy Hill (en)
- From Up On Poppy Hill (es)
- From Up On Poppy Hill (fr)
- From Up On Poppy Hill (ja)
- From Up On Poppy Hill (nl)
- Grave Of The Fireflies (ar)
- Grave Of The Fireflies (en)
- Grave Of The Fireflies (es)
- Grave Of The Fireflies (fr)
- Grave Of The Fireflies (ja)
- Grave Of The Fireflies (nl)
- Howls Moving Castle (ar)
- Howls Moving Castle (en)
- Howls Moving Castle (es)
- Howls Moving Castle (fr)
- Howls Moving Castle (ja)
- Howls Moving Castle (nl)
- Kikis Delivery Service (ar)
- Kikis Delivery Service (en)
- Kikis Delivery Service (es)
- Kikis Delivery Service (fr)
- Kikis Delivery Service (ja)
- Kikis Delivery Service (nl)
- My Neighbor Totoro (ja)
- My Neighbors The Yamadas (ar)
- My Neighbors The Yamadas (en)
- My Neighbors The Yamadas (es)
- My Neighbors The Yamadas (fr)
- My Neighbors The Yamadas (ja)
- My Neighbors The Yamadas (nl)
- Only Yesterday (ja)
- Pom Poko (ja)
- Ponyo (ja)
- Porco Rosso (ja)
- Princess Mononoke (ja)
- Spirited Away (ja)
- Talesom Earthsea (ar)
- Talesom Earthsea (en)
- Talesom Earthsea (es)
- Talesom Earthsea (fr)
- Talesom Earthsea (ja)
- Talesom Earthsea (nl)
- The Cat Returns (ja)
- The Red Turtle (ja)
- The Tale Of The Princess Kaguya (ar)
- The Tale Of The Princess Kaguya (en)
- The Tale Of The Princess Kaguya (es)
- The Tale Of The Princess Kaguya (fr)
- The Tale Of The Princess Kaguya (ja)
- The Tale Of The Princess Kaguya (nl)
- The Wind Rises (ja)
- When Marnie Was There (ja)
- Whisper Of The Heart (ar)
- Whisper Of The Heart (en)
- Whisper Of The Heart (es)
- Whisper Of The Heart (fr)
- Whisper Of The Heart (ja)
- Whisper Of The Heart (nl)

### Unexpected in Emotion Data (40 combinations)

These film-language combinations have emotion data but no matching subtitle files:

- Castle in the Sky (ar)
- Castle in the Sky (en)
- Castle in the Sky (es)
- Castle in the Sky (fr)
- Castle in the Sky (nl)
- Earwig and the Witch (ar)
- Earwig and the Witch (en)
- Earwig and the Witch (es)
- Earwig and the Witch (fr)
- Earwig and the Witch (nl)
- From Up on Poppy Hill (ar)
- From Up on Poppy Hill (en)
- From Up on Poppy Hill (es)
- From Up on Poppy Hill (fr)
- From Up on Poppy Hill (nl)
- Grave of the Fireflies (ar)
- Grave of the Fireflies (en)
- Grave of the Fireflies (es)
- Grave of the Fireflies (fr)
- Grave of the Fireflies (nl)
- My Neighbors the Yamadas (ar)
- My Neighbors the Yamadas (en)
- My Neighbors the Yamadas (es)
- My Neighbors the Yamadas (fr)
- My Neighbors the Yamadas (nl)
- Tales from Earthsea (ar)
- Tales from Earthsea (en)
- Tales from Earthsea (es)
- Tales from Earthsea (fr)
- Tales from Earthsea (nl)
- The Tale of the Princess Kaguya (ar)
- The Tale of the Princess Kaguya (en)
- The Tale of the Princess Kaguya (es)
- The Tale of the Princess Kaguya (fr)
- The Tale of the Princess Kaguya (nl)
- Whisper of the Heart (ar)
- Whisper of the Heart (en)
- Whisper of the Heart (es)
- Whisper of the Heart (fr)
- Whisper of the Heart (nl)

---

## Films Requiring Re-processing

**Total:** 12 film-language combinations

### ❌ FAIL (2 combinations)

- **The Red Turtle** (en): Overrun 39.1 minutes
- **Ponyo** (nl): Overrun 5.9 minutes

### ⚠️ WARN (10 combinations)

- **Ponyo** (es): Overrun 4.6 minutes
- **The Wind Rises** (fr): Overrun 4.5 minutes
- **Ponyo** (fr): Overrun 4.2 minutes
- **Ponyo** (en): Overrun 3.9 minutes
- **The Red Turtle** (ar): Overrun 2.7 minutes
- **The Red Turtle** (es): Overrun 2.3 minutes
- **My Neighbor Totoro** (nl): Overrun 2.1 minutes
- **The Red Turtle** (fr): Overrun 1.8 minutes
- **Porco Rosso** (es): Overrun 1.5 minutes
- **The Red Turtle** (nl): Overrun 1.3 minutes

---

## Recommendations for Downstream Stories

### Story 3.6.2: Root Cause Investigation

**Priority films to investigate:**

- The Red Turtle (en): 39.1 minute overrun
- Ponyo (nl): 5.9 minute overrun

**Investigation focus:**
- Verify `src/nlp/analyze_emotions.py` uses correct duration source
- Check if Kaggle metadata duration differs from subtitle duration
- Validate emotion pipeline logic for minute_offset calculation

### Story 3.6.3: Pipeline Fix Scope

**Scope:** Fix applies to **all 100 film-language combinations**
- 12 combinations require re-processing
- 48 combinations already pass (verify fix doesn't regress)

### Story 3.6.4: Re-processing Targets

**Re-process these 12 combinations:**

- **My Neighbor Totoro**: nl
- **Ponyo**: en, es, fr, nl
- **Porco Rosso**: es
- **The Red Turtle**: ar, en, es, fr, nl
- **The Wind Rises**: fr

**Estimated effort:**
- 12 emotion analysis runs
- ~24 minutes processing time (estimated)

### Story 3.6.6: Automated Test Baselines

**Test coverage targets:**

- Validate emotion max_minute <= subtitle_duration for all 100 combinations
- Target pass rate: 100% (currently 80.0%)
- Test tolerance: ±1.0 minute buffer acceptable

---

## Audit Metadata

- **Script:** `scripts/audit_emotion_data_quality.py`
- **Generated:** 2025-11-26 14:51:37
- **Database:** `data/ghibli.duckdb` (read-only access)
- **Total combinations audited:** 100
- **Subtitle files scanned:** 100

---

*Report generated by Story 3.6.1: Comprehensive Emotion Data Audit*
