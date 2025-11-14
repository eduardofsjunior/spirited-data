"""
Demo script to showcase Sora's Ghibli-inspired personality in the RAG system.

This script demonstrates how the system now responds with:
- Warm, narrative language
- Portfolio-friendly explanations
- Three-layer response structure (Discovery → Magic → Technical)
- Feature-based citations instead of technical jargon
"""

# Example queries to showcase the personality
DEMO_QUERIES = [
    {
        "query": "What is the correlation between sentiment and box office revenue?",
        "why": "Demonstrates Pattern Discovery + storytelling statistics"
    },
    {
        "query": "Show me the emotional arc of Spirited Away",
        "why": "Demonstrates Emotion Archive feature"
    },
    {
        "query": "Do films with rising emotional trajectories perform better with critics?",
        "why": "Demonstrates Success Correlation Studies"
    },
    {
        "query": "How does the sentiment of Spirited Away differ across languages?",
        "why": "Demonstrates Multilingual Insights"
    },
]

# Example of expected response style:

EXAMPLE_OLD_STYLE = """
Query: "What is the correlation between sentiment and box office revenue?"

Response: "Correlation between sentiment and box_office: r=0.34, p=0.08 (weak positive, not statistically significant), n=18 films"

❌ Problems:
- Leads with raw statistics (r=0.34, p=0.08)
- Uses technical terms (box_office, n=18)
- No context about which features enabled this
- Not accessible to non-technical viewers
"""

EXAMPLE_NEW_STYLE = """
Query: "What is the correlation between sentiment and box office revenue?"

Response:
"I discovered something interesting about how a film's emotional journey relates to its commercial success. When I compared the overall sentiment across all films with their box office earnings, I found a weak-to-moderate positive connection - imagine finding a faint trail rather than a clear path. Films with more positive emotional arcs tended to earn slightly more, though this pattern wasn't strong enough to be considered definitive.

✨ This insight comes from my **Pattern Discovery Tools**, where I cross-referenced data from my **Emotion Archive** (tracking sentiment across 50,000+ dialogue lines) with **Box Office Records** from the Kaggle dataset. I analyzed 18 films that had complete data for both emotional patterns and revenue.

Would you like to see the technical details? I can share the specific correlation coefficient (Pearson r), statistical significance (p-value), and the exact data transformation steps that powered this analysis."

✅ Improvements:
- Opens with narrative discovery
- Uses accessible analogies ("faint trail rather than clear path")
- Explains which archive features were used (Pattern Discovery, Emotion Archive)
- Counts presented as "50,000+ dialogue lines" and "18 films"
- Offers technical details but doesn't force them
- Portfolio-friendly and engaging
"""

def print_demo():
    """Print demo examples to console."""
    print("=" * 80)
    print("SORA (空) - GHIBLI-INSPIRED RAG SYSTEM PERSONALITY DEMO")
    print("=" * 80)
    print()

    print("🎭 CHARACTER: Sora (空 - 'sky')")
    print("   A thoughtful film archivist studying emotional patterns in Studio Ghibli films")
    print()

    print("✨ ARCHIVE FEATURES:")
    print("   🎭 Emotion Archive: Sentiment from 50K+ dialogue lines (22 films, 5 languages)")
    print("   📊 Pattern Discovery: Correlations between emotional arcs and reception")
    print("   🌍 Multilingual Insights: Cross-translation emotion comparison")
    print("   🎯 Success Studies: Emotional patterns vs box office & critics")
    print()

    print("-" * 80)
    print("BEFORE & AFTER COMPARISON")
    print("-" * 80)
    print()

    print(EXAMPLE_OLD_STYLE)
    print()
    print("-" * 80)
    print()
    print(EXAMPLE_NEW_STYLE)
    print()
    print("=" * 80)
    print()

    print("📚 DEMO QUERIES TO TRY:")
    for i, demo in enumerate(DEMO_QUERIES, 1):
        print(f"   {i}. {demo['query']}")
        print(f"      → {demo['why']}")
        print()

    print("=" * 80)
    print("💡 TIP: Set NARRATIVE_MODE=false to return to technical mode")
    print("=" * 80)

if __name__ == "__main__":
    print_demo()
