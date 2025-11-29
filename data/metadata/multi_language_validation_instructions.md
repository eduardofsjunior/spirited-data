# Multi-Language Validation Instructions

**Story:** 4.X.5 - Task 4

---

## Step 1: Run Validation on Acquired Files

After acquiring subtitles (Task 3), run validation:

```bash
python src/validation/validate_subtitle_timing.py \
    --subtitle-dir data/processed/subtitles/ \
    --metadata data/metadata/film_versions.json \
    --output data/processed/subtitle_validation_multi_language_v2.json
```

## Step 2: Generate Comparison Report

After validation completes:

```bash
python scripts/analyze_multi_language_validation.py
```

This will generate:
- `data/metadata/multi_language_validation_comparison.md`

---

**Expected Outcomes:**
- Phase 2 pass rate: 75%+ (100+/134 files)
- Average timing drift: <2%
- Cross-language consistency: <3% drift within films

---
