#!/bin/bash
# Clear Python bytecode cache to force reload of modules

echo "🧹 Clearing Python cache files..."

# Find and remove all __pycache__ directories
find /Users/edjunior/personal_projects/ghibli_pipeline/src -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null

# Find and remove all .pyc files
find /Users/edjunior/personal_projects/ghibli_pipeline/src -name "*.pyc" -delete 2>/dev/null

echo "✅ Python cache cleared!"
echo ""
echo "Now run the CLI to see Sora's new personality:"
echo "  python src/ai/rag_cli.py"
echo ""
echo "Or run a quick test:"
echo "  python src/ai/demo_sora_personality.py"
