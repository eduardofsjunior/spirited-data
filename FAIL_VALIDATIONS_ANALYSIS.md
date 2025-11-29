# Detailed Analysis of 9 FAIL Validations

**Generated**: 2025-11-29 (Story 3.6.5 Implementation)
**Source**: `int_emotion_data_quality_checks` dbt validation model

---

## Executive Summary

9 film-language combinations have emotion data extending **beyond** the film runtime + 10-minute buffer. These represent legitimate data quality issues requiring investigation and remediation.

**Severity Breakdown**:
- 🔴 **CRITICAL** (>50 min overrun): 3 films
- 🟠 **SEVERE** (20-50 min overrun): 2 films
- 🟡 **MODERATE** (10-20 min overrun): 4 films

---

## Detailed Film Analysis

### 🔴 CRITICAL Issues (>50 minute overrun)

#### 1. **The Cat Returns** (Spanish - ES)
- **Film Slug**: `the_cat_returns_es`
- **Expected Runtime**: 75 minutes
- **Emotion Data Extends To**: Minute 214
- **Overrun**: **139 minutes** (185% beyond film duration!)
- **Status**: 🔴 CRITICAL

**Analysis**:
This is clearly a **wrong subtitle file**. The emotion data is almost **3x the film length**. Possible causes:
- Subtitle file from a completely different film
- Compilation of multiple episodes mistakenly treated as one film
- Severely corrupted subtitle timing

**Recommended Action**:
1. Check if `the_cat_returns_es` subtitle file exists
2. Verify subtitle file matches the actual film
3. Delete existing emotion data: `DELETE FROM raw.film_emotions WHERE film_slug = 'the_cat_returns_es'`
4. Re-download correct Spanish subtitles
5. Re-run emotion analysis

---

#### 2. **My Neighbors the Yamadas** (Arabic - AR)
- **Film Slug**: `my_neighbors_the_yamadas_ar`
- **Expected Runtime**: 104 minutes
- **Emotion Data Extends To**: Minute 181
- **Overrun**: **77 minutes** (74% beyond film duration)
- **Status**: 🔴 CRITICAL

**Analysis**:
Significant overrun suggesting **wrong film cut or version**. "My Neighbors the Yamadas" is episodic in nature (collection of vignettes about family life). Possible causes:
- Extended/director's cut subtitle file (if such exists)
- Subtitle file includes end credits or bonus content timing
- Wrong film version (theatrical vs home video)

**Recommended Action**:
1. Verify actual film runtime - check if there's a 181-minute version
2. If no extended cut exists, this is wrong subtitle data
3. Delete and re-process with correct subtitles

---

#### 3. **The Wind Rises** (French v2 - FR)
- **Film Slug**: `the_wind_rises_fr_v2`
- **Expected Runtime**: 126 minutes
- **Emotion Data Extends To**: Minute 186
- **Overrun**: **60 minutes** (48% beyond film duration)
- **Status**: 🔴 CRITICAL

**Analysis**:
The "v2" suffix indicates this is an **improved/alternative subtitle version**. The massive overrun suggests:
- v2 subtitle file is for a different cut (possibly including documentaries/extras)
- Subtitle timing completely wrong
- File corruption during v2 subtitle processing

**Recommended Action**:
1. Compare with non-v2 version: Check `the_wind_rises_fr` (if exists) for validation status
2. Re-download v2 French subtitles
3. Consider falling back to v1 if v2 is consistently problematic

---

### 🟠 SEVERE Issues (20-50 minute overrun)

#### 4. **My Neighbors the Yamadas** (French - FR)
- **Film Slug**: `my_neighbors_the_yamadas_fr`
- **Expected Runtime**: 104 minutes
- **Emotion Data Extends To**: Minute 128
- **Overrun**: **24 minutes** (23% beyond film duration)
- **Status**: 🟠 SEVERE

**Analysis**:
Same film as #2 but different language. The **24-minute overrun** is more reasonable than the Arabic version (77 min), but still fails the 10-minute buffer. Possible causes:
- Different subtitle source with different end credit timing
- PAL vs NTSC speed differences (PAL runs 4% faster, could explain ~4 minutes)
- Subtitle file includes post-credit scenes or bonus content

**Recommended Action**:
1. Compare with Arabic version timing (181 min) - these should be similar
2. Verify if 128 minutes is plausible (film + credits + extras)
3. If legitimate extended content, consider increasing buffer for this specific film
4. Otherwise, re-download French subtitles

---

#### 5. **The Wind Rises** (Dutch v2 - NL)
- **Film Slug**: `the_wind_rises_nl_v2`
- **Expected Runtime**: 126 minutes
- **Emotion Data Extends To**: Minute 150
- **Overrun**: **24 minutes** (19% beyond film duration)
- **Status**: 🟠 SEVERE

**Analysis**:
Same film as #3 (French v2) but less severe overrun. This is **also a v2 subtitle**, suggesting systematic issue with "The Wind Rises" v2 subtitles across languages:
- French v2: 60 min overrun
- Dutch v2: 24 min overrun

Both v2 versions have timing issues! This points to **v2 subtitle source problem** for this specific film.

**Recommended Action**:
1. Check if v1 versions exist and validate correctly
2. Investigate v2 subtitle source for "The Wind Rises"
3. Consider flagging all "The Wind Rises" v2 subtitles as problematic
4. Re-download or revert to v1

---

### 🟡 MODERATE Issues (10-20 minute overrun)

#### 6. **Tales from Earthsea** (Spanish - ES)
- **Film Slug**: `tales_from_earthsea_es`
- **Expected Runtime**: 115 minutes
- **Emotion Data Extends To**: Minute 134
- **Overrun**: **19 minutes** (17% beyond film duration)
- **Status**: 🟡 MODERATE

**Analysis**:
Close to the 10-minute buffer threshold. Could be:
- Legitimate extended cut (115 min theatrical vs 134 min with credits/extras)
- Different regional release (US vs Japanese vs European cuts)
- Subtitle timing drift accumulated over film duration

**Recommended Action**:
1. Verify if 134-minute version exists for Tales from Earthsea
2. Check other language versions for same film to compare runtimes
3. If others are ~115 min, re-process Spanish subtitles
4. Consider this a **borderline case** - may be acceptable

---

#### 7 & 8. **Whisper of the Heart** (Spanish ES & Dutch NL)
- **Film Slugs**: `whisper_of_the_heart_es`, `whisper_of_the_heart_nl`
- **Expected Runtime**: 111 minutes (both)
- **Emotion Data Extends To**: Minute 129 (both!)
- **Overrun**: **18 minutes** (16% beyond film duration)
- **Status**: 🟡 MODERATE

**Analysis**:
**Identical overruns** across two different languages (ES and NL both extend to exactly minute 129) suggests:
- Both subtitle files from **same source** with same timing
- Legitimate extended version (111 min + 18 min credits = 129 min total)
- Systematic timing issue in subtitle source

**Recommended Action**:
1. Check if other languages (EN, FR) also extend to ~129 minutes
2. If yes: This might be legitimate runtime (film + long credits)
3. If no: ES and NL subtitles need replacement
4. Research if Whisper of the Heart has known extended cuts

---

#### 9. **The Tale of the Princess Kaguya** (Arabic - AR)
- **Film Slug**: `the_tale_of_the_princess_kaguya_ar`
- **Expected Runtime**: 137 minutes
- **Emotion Data Extends To**: Minute 150
- **Overrun**: **13 minutes** (9.5% beyond film duration)
- **Status**: 🟡 MODERATE (borderline)

**Analysis**:
**Smallest overrun** of all failures - only **3 minutes beyond the 10-minute buffer**. This is borderline acceptable and could be:
- Legitimate timing difference (PAL speed, different cuts)
- Long end credits (Princess Kaguya is 137 min + credits could be 150 min total)
- Minor subtitle timing drift

**Recommended Action**:
1. **Lowest priority** - only 3 minutes beyond buffer
2. Verify against other language versions
3. Consider increasing buffer to 15 minutes if multiple films show 13-15 min overruns
4. May be acceptable as-is depending on project requirements

---

## Pattern Analysis

### By Film (Unique Films with Issues):
1. **The Cat Returns** - 1 language (ES) - CRITICAL
2. **My Neighbors the Yamadas** - 2 languages (AR, FR) - CRITICAL/SEVERE
3. **The Wind Rises** - 2 languages (FR v2, NL v2) - **BOTH v2 versions!**
4. **Tales from Earthsea** - 1 language (ES) - MODERATE
5. **Whisper of the Heart** - 2 languages (ES, NL) - **Identical overruns!**
6. **The Tale of the Princess Kaguya** - 1 language (AR) - MODERATE (borderline)

### By Language:
- **Spanish (ES)**: 3 films affected (Cat Returns, Tales, Whisper)
- **Arabic (AR)**: 2 films affected (Yamadas, Kaguya)
- **French (FR)**: 2 films affected (both Yamadas and Wind Rises v2)
- **Dutch (NL)**: 2 films affected (Wind Rises v2, Whisper)

### By Severity:
- **v2 subtitles problematic**: Both "Wind Rises" failures are v2 versions
- **Episodic films**: "My Neighbors the Yamadas" (episodic structure) has issues in 2 languages
- **Shared sources**: Whisper of the Heart ES/NL have identical minute 129 endpoint

---

## Recommended Remediation Plan

### Immediate Actions (CRITICAL):
1. **The Cat Returns (ES)** - Delete and re-download subtitles (139 min overrun is catastrophic)
2. **My Neighbors Yamadas (AR)** - Investigate if 181-minute version exists; if not, replace
3. **The Wind Rises (FR v2)** - Check v1 version; revert from v2 if necessary

### Medium Priority (SEVERE):
4. **My Neighbors Yamadas (FR)** - Compare with AR version, standardize
5. **The Wind Rises (NL v2)** - Part of v2 subtitle issue; address with FR v2

### Lower Priority (MODERATE):
6. **Tales from Earthsea (ES)** - Verify runtime; may be legitimate
7. **Whisper of the Heart (ES/NL)** - Check if 129 min is standard across all languages
8. **Princess Kaguya (AR)** - Only 3 min over buffer; lowest priority

### Systematic Fixes:
- **Audit all v2 subtitles** - "The Wind Rises" v2 issues suggest broader problem
- **Cross-language validation** - Compare runtimes across languages for each film
- **Source validation** - Verify subtitle file sources and quality
- **Consider buffer increase** - If many films legitimately need 13-15 min buffer, increase from 10 to 15

---

## Next Story Recommendation

**Story 3.6.6**: "Remediate 9 Failed Emotion Validations"

**Tasks**:
1. Investigate each of the 9 films individually
2. Verify actual film runtimes (including credits, extended cuts)
3. Replace incorrect subtitle files
4. Re-run emotion analysis for affected combinations
5. Validate all 9 combinations pass the 10-minute buffer test
6. Document any films requiring buffer exceptions

**Success Criteria**: All 174 combinations achieve PASS or UNKNOWN status (0 FAIL).

---

**End of Analysis**
