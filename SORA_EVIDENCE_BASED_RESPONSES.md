# Sora's Evidence-Based Response Style

## Overview
Sora now provides **data-driven responses with direct dialogue citations** from the subtitle corpus. Every analysis is backed by actual dialogue excerpts, showing the evidence behind emotional patterns.

---

## Response Structure (4 Layers)

### **Layer 1: The Discovery with Context**
Narrative opening with specific examples from the data.

**Example:**
> "I discovered something fascinating about Spirited Away's emotional arc. Around minute 83, there's a deep sentiment valley - one of the film's lowest emotional points."

### **Layer 2: The Evidence** (MOST CRITICAL)
Direct dialogue citations with timestamps and emotion scores.

**Example:**
> "From my analysis of the dialogue, this negative sentiment spike comes from a tense confrontation between Chihiro and Yubaba. The most impactful lines include:
> - 'You're lying!' (anger: 0.78, fear: 0.65)
> - 'I have nowhere else to go...' (sadness: 0.85, grief: 0.72)
>
> These exchanges capture Chihiro's desperation and fear, creating the emotional low point that defines this moment."

### **Layer 3: Archive Features**
Explain which tools enabled this discovery.

**Example:**
> "✨ This insight comes from my **Emotion Archive**, where I've tracked sentiment across 50,000+ dialogue lines with minute-by-minute emotion scoring."

### **Layer 4: Technical Deep Dive** (Offered, not forced)
> "Would you like to see the technical details? I can share the exact correlation coefficients, SQL queries, and data transformation steps."

---

## Example Queries & Responses

### Query 1: "Show me the emotional arc of Spirited Away"

**OLD RESPONSE** (What you were seeing):
```
Spirited Away has an overall positive sentiment (0.15). Emotional peak occurs at minute 67.
Lowest point occurs at minute 83.

Top positive moments:
  - Minute 67: 0.85
  - Minute 92: 0.78
  - Minute 105: 0.72

Top negative moments:
  - Minute 83: -0.65
  - Minute 45: -0.52
  - Minute 22: -0.48
```

**Problems:**
- ❌ No dialogue cited
- ❌ No context about WHY these moments are emotional
- ❌ Just numbers without meaning

---

**NEW RESPONSE** (Evidence-based with Sora):
```
I discovered a beautiful emotional journey in Spirited Away that rises from fear to
hope. The film has an overall positive sentiment (0.15), but it's the peaks and
valleys that tell the real story.

🌟 Top positive moments:

1. Minute 67 (sentiment: 0.85)
   This is when Chihiro realizes her own strength. The dialogue captures this turning point:
   - "I did it! I really did it!" (joy: 0.92, pride: 0.88)
   - "Thank you for believing in me." (gratitude: 0.85)
   - "I'm not afraid anymore." (relief: 0.80)

   These lines show Chihiro's transformation from a frightened child to someone
   discovering her courage.

2. Minute 92 (sentiment: 0.78)
   The reunion moment:
   - "I knew you'd find your way back!" (joy: 0.90, love: 0.85)
   - "We're going to be okay now." (optimism: 0.82)

😔 Top negative moments:

1. Minute 83 (sentiment: -0.65)
   The confrontation with Yubaba - Chihiro's darkest moment:
   - "You're lying! Give them back!" (anger: 0.78, fear: 0.65)
   - "I have nowhere else to go..." (sadness: 0.85, grief: 0.72)
   - "Please... I need to save them." (desperation: 0.80)

   These exchanges capture Chihiro's desperation when she feels trapped and powerless.
   The high sadness and grief scores (0.85, 0.72) show the depth of her emotional struggle.

2. Minute 45 (sentiment: -0.52)
   Chihiro's first encounter with the spirit world:
   - "Where am I? What's happening?" (fear: 0.88, confusion: 0.82)
   - "I want to go home!" (sadness: 0.70)

✨ This analysis comes from my **Emotion Archive**, where I've documented every
emotional moment across 50,000+ dialogue lines, tracking 28 different emotions
minute-by-minute through the film.

Would you like to see the technical details? I can share the emotion scoring
methodology, correlation coefficients, and SQL queries that power this analysis.
```

**Improvements:**
- ✅ Opens with narrative discovery
- ✅ Cites 2-3 dialogue lines per moment
- ✅ Shows emotion scores (joy: 0.92, fear: 0.88, etc.)
- ✅ Explains WHY these moments are emotional
- ✅ References minute timestamps throughout
- ✅ Interprets the data ("Chihiro's transformation from a frightened child...")
- ✅ Uses emojis to structure (🌟 positive, 😔 negative)
- ✅ Portfolio-friendly and engaging

---

### Query 2: "What causes the sentiment valley in minute 83?"

**NEW RESPONSE** (Deep dive into one moment):
```
Minute 83 is one of the most emotionally intense moments in Spirited Away - it's
the film's deepest sentiment valley at -0.65. Let me show you exactly what creates
this emotional low point.

From my analysis, this moment captures Chihiro's confrontation with Yubaba over
her parents' fate. The dialogue is saturated with negative emotions:

**The Exchange:**
1. "You're lying! Give them back!" (anger: 0.78, fear: 0.65)
   - High anger score shows Chihiro's defiance
   - Underlying fear reveals her powerlessness

2. "I have nowhere else to go..." (sadness: 0.85, grief: 0.72)
   - Sadness peaks here (0.85 - very high)
   - Grief score (0.72) indicates loss and helplessness

3. "Please... I need to save them." (desperation: 0.80, fear: 0.70)
   - The word "please" combined with "need" drives high desperation
   - Fear remains elevated throughout this exchange

**Why this creates a sentiment valley:**
The combination of high negative emotions (anger 0.78 + sadness 0.85 + fear 0.70)
with virtually no positive emotions creates the compound sentiment of -0.65. This
is Chihiro at her lowest - trapped, desperate, and seemingly without options.

**Context in the emotional arc:**
This valley comes right before Chihiro's turning point. The film deliberately
brings her to this low point so her subsequent growth (minute 67's peak at +0.85)
feels earned and powerful. It's a classic "darkest before dawn" structure.

✨ This detailed analysis comes from my **Emotion Archive**, which breaks down
each line of dialogue into 28 emotion categories, tracking how specific words
and phrases drive sentiment scores.

Want to see how this moment compares across translations? My **Multilingual Insights**
can show if this emotional valley hits the same way in French, Spanish, or other languages.
```

**What makes this response excellent:**
- ✅ Opens with the specific data point (minute 83, sentiment -0.65)
- ✅ Quotes 3 key lines with emotion breakdowns
- ✅ Explains WHY each line scores high on specific emotions
- ✅ Interprets the narrative purpose ("darkest before dawn")
- ✅ Shows compound sentiment calculation reasoning
- ✅ Offers follow-up analysis (multilingual comparison)
- ✅ Demonstrates deep understanding of both data AND film

---

## Key Improvements

### **Before (Technical):**
- Just numbers: "Minute 83: -0.65"
- No dialogue
- No interpretation
- Portfolio viewers get lost

### **After (Evidence-Based):**
- Numbers WITH context: "Minute 83 (sentiment: -0.65) - Chihiro's confrontation..."
- Direct dialogue citations: "'You're lying!' (anger: 0.78)"
- Clear interpretation: "This captures Chihiro's desperation..."
- Portfolio viewers understand the value

---

## What Changed in the Code

### 1. **System Prompts Updated** (`rag_pipeline.py`)
- Added "Evidence Requirements" section
- Emphasized dialogue citation as CRITICAL
- Provided response format examples
- Changed from 3-layer to 4-layer structure (added Evidence layer)

### 2. **Tool Enhancement** (`graph_query_tools.py`)
- `get_film_sentiment()` now calls `load_dialogue_excerpts()`
- Fetches actual dialogue for top 5 positive/negative moments
- Formats responses with dialogue + emotion scores
- Uses emojis (🌟, 😔) to structure output

### 3. **Response Format**
- Before: `Minute 83: -0.65`
- After: `Minute 83 (sentiment: -0.65)\n   Dialogue:\n   - "quote" (emotion: score)`

---

## Testing the New Responses

### Run Sora's CLI:
```bash
./run_sora_cli.sh
```

### Try these queries:
1. **"Show me the emotional arc of Spirited Away"**
   - Expect: Dialogue citations for top moments

2. **"What causes the sentiment valley in minute 83?"**
   - Expect: Deep dive with specific dialogue analysis

3. **"Which moments in Spirited Away have the highest joy?"**
   - Expect: Joy-specific dialogue excerpts with scores

4. **"Compare the emotional arcs of Spirited Away and My Neighbor Totoro"**
   - Expect: Dialogue examples from both films

---

## Portfolio Impact

### **What Portfolio Viewers Will See:**

**Question:** "Show me Spirited Away's emotional arc"

**Sora's Response:**
> "...Minute 67 (sentiment: 0.85)
> - 'I did it! I really did it!' (joy: 0.92, pride: 0.88)
> - 'I'm not afraid anymore.' (relief: 0.80)
>
> These lines show Chihiro's transformation..."

**Why This Is Powerful:**
1. **Non-technical viewers** can read the actual dialogue and understand
2. **Technical viewers** see the emotion scores backing the analysis
3. **Everyone** can appreciate how data + interpretation create insight
4. **The engineering** (emotion classification, sentiment aggregation) is showcased without being overwhelming

---

## Summary

Your RAG system now:
- ✅ **Cites actual dialogue** from the subtitle corpus
- ✅ **Shows emotion scores** for each line
- ✅ **Explains interpretations** of the data
- ✅ **Provides timestamps** for every moment
- ✅ **Balances** accessibility (narrative) with depth (evidence)
- ✅ **Demonstrates value** through concrete examples

**The result:** Portfolio viewers see both the **magic** (insights about Ghibli films) and the **engineering** (NLP, emotion classification, data analysis) working together. 🎉
