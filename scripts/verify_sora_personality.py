#!/usr/bin/env python3
"""
Quick verification script to confirm Sora's personality is active.
This script directly checks the system prompts without needing OpenAI API.
"""

import sys
sys.path.insert(0, '/Users/edjunior/personal_projects/ghibli_pipeline')

from src.ai.rag_pipeline import _create_system_prompt, TEMPERATURE, MAX_TOKENS, NARRATIVE_MODE

print("=" * 80)
print("🔍 VERIFYING SORA'S PERSONALITY")
print("=" * 80)
print()

# Check configuration
print("📊 Configuration:")
print(f"   Temperature: {TEMPERATURE} (0.7 = creative/narrative, 0.3 = technical)")
print(f"   Max Tokens: {MAX_TOKENS} (800 = verbose, 500 = concise)")
print(f"   Narrative Mode: {NARRATIVE_MODE}")
print()

# Check system prompt
prompt_template = _create_system_prompt()
system_message = prompt_template.messages[0].prompt.template

print("✨ System Prompt Preview:")
print("-" * 80)
print(system_message[:600])
print("...")
print("-" * 80)
print()

# Verify key personality elements
checks = {
    "Sora character present": "Sora (空" in system_message,
    "Three-layer structure": "Layer 1" in system_message and "Layer 2" in system_message and "Layer 3" in system_message,
    "Emotion Archive feature": "Emotion Archive" in system_message,
    "Pattern Discovery feature": "Pattern Discovery" in system_message,
    "Narrative style instructions": "narrative" in system_message.lower() or "storytelling" in system_message.lower(),
    "Avoids technical jargon": "Avoid Unless Asked" in system_message or "mart_" in system_message,
}

print("✅ Personality Checks:")
for check_name, passed in checks.items():
    status = "✅" if passed else "❌"
    print(f"   {status} {check_name}")
print()

all_passed = all(checks.values())
if all_passed:
    print("🎉 SUCCESS! Sora's personality is fully loaded!")
    print()
    print("You can now run the CLI and see the new personality:")
    print("   python src/ai/rag_cli.py")
    print()
    print("Or test with a real query (requires OPENAI_API_KEY):")
    print("   python -c \"from src.ai.rag_cli import *; main()\"")
else:
    print("⚠️  WARNING: Some personality elements are missing!")
    print("   Try clearing Python cache: bash scripts/clear_python_cache.sh")

print("=" * 80)
