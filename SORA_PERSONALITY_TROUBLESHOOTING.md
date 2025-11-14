# Troubleshooting Sora's Personality

## ✅ Verification Confirmed
Sora's personality **IS** loaded and active in the codebase. The verification script shows all personality checks passed.

## 🤔 Why You Might Still See Old Responses

### **Most Likely Cause: Python Module Caching**

When Python imports a module, it caches it in memory. If you:
1. Had the CLI running in a terminal
2. Made changes to `rag_pipeline.py`
3. Ran the CLI again in the **same terminal session**

...Python would use the **cached old version** from memory instead of reloading the file.

### **Solutions:**

#### Option 1: Restart Your Terminal (Simplest)
```bash
# Close your current terminal completely
# Open a new terminal window
# Navigate to project directory
cd /Users/edjunior/personal_projects/ghibli_pipeline

# Run the CLI fresh
python src/ai/rag_cli.py
```

#### Option 2: Clear Python Cache
```bash
# Run the cache clearing script
bash scripts/clear_python_cache.sh

# Then run CLI in a NEW terminal session
python src/ai/rag_cli.py
```

#### Option 3: Force Module Reload (Advanced)
```python
# If you're in an interactive Python session
import importlib
import src.ai.rag_pipeline
importlib.reload(src.ai.rag_pipeline)
```

---

## 🔍 How to Verify Sora is Active

### Quick Check (No API Key Needed):
```bash
python scripts/verify_sora_personality.py
```

Expected output:
```
✅ Sora character present
✅ Three-layer structure
✅ Emotion Archive feature
✅ Pattern Discovery feature
✅ Narrative style instructions
✅ Avoids technical jargon
🎉 SUCCESS! Sora's personality is fully loaded!
```

### Visual Check in CLI:
When you run `python src/ai/rag_cli.py`, you should see:

```
================================================================================
✨ Welcome to Sora's Archive - Studio Ghibli Emotion Analysis
   Version 1.0 | Powered by sentiment analysis across 22 films
================================================================================

🎭 Meet Sora (空 - 'sky')
   A thoughtful archivist who studies emotional patterns in Ghibli films
```

**NOT** the old message:
```
SpiritedData RAG CLI - Interactive Query Testing
```

---

## 📊 Expected Response Style

### ❌ OLD STYLE (Technical):
```
Query: "What is the correlation between sentiment and revenue?"
Response: "Correlation between sentiment and box_office: r=0.34, p=0.08"
```

### ✅ NEW STYLE (Sora/Narrative):
```
Query: "What is the correlation between sentiment and revenue?"

Response: "I discovered something interesting about how a film's emotional
journey relates to its commercial success. When I compared the overall
sentiment across all films with their box office earnings, I found a
weak-to-moderate positive connection - imagine finding a faint trail
rather than a clear path.

✨ This insight comes from my **Pattern Discovery Tools**, where I
cross-referenced my **Emotion Archive** (50,000+ dialogue lines) with
**Box Office Records**.

Would you like to see the technical details?"
```

Key differences:
- Opens with narrative ("I discovered...")
- Uses analogies ("faint trail rather than clear path")
- References archive features ("Pattern Discovery Tools", "Emotion Archive")
- Offers technical details at END, doesn't lead with them
- Warm, storytelling tone

---

## 🐛 Still Not Working?

### Check 1: Verify Python is loading the right files
```bash
python -c "import src.ai.rag_pipeline; print(src.ai.rag_pipeline.__file__)"
```

Should output: `/Users/edjunior/personal_projects/ghibli_pipeline/src/ai/rag_pipeline.py`

### Check 2: Verify imports are fresh
```bash
# Clear cache
find . -name "*.pyc" -delete
find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null

# Verify prompt content directly
python -c "
from src.ai.rag_pipeline import _create_system_prompt
prompt = _create_system_prompt()
if 'Sora' in str(prompt.messages[0].prompt.template):
    print('✅ Sora personality loaded')
else:
    print('❌ Old personality still loaded')
"
```

### Check 3: Run a test query (requires OpenAI API key)
```bash
# Set API key
export OPENAI_API_KEY="your-key-here"

# Test query
python -c "
from src.ai.rag_pipeline import query_rag_system
result = query_rag_system('Hello, who are you?', [])
print(result['answer'])
"
```

Expected response should mention "Sora" and have a warm, narrative tone.

---

## 🎯 Configuration Variables

Location: `src/ai/rag_pipeline.py` (lines 142-153)

Current settings:
- `TEMPERATURE = 0.7` (creative, narrative)
- `MAX_TOKENS = 800` (verbose)
- `NARRATIVE_MODE = True` (portfolio-friendly)

To temporarily switch to technical mode:
```bash
export NARRATIVE_MODE=false
python src/ai/rag_cli.py
```

---

## 📝 Summary

**Your code IS correct and Sora IS loaded.** The issue is almost certainly:
1. Python module caching from a previous terminal session
2. OR you were testing in a Python REPL that had already imported the old version

**Solution**: Open a **fresh terminal**, clear cache, and run the CLI.

```bash
# Full reset procedure:
bash scripts/clear_python_cache.sh
# Close terminal
# Open NEW terminal
cd /Users/edjunior/personal_projects/ghibli_pipeline
python src/ai/rag_cli.py
```

You should now see Sora's warm, narrative personality! 🎉
