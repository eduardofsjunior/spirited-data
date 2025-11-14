#!/bin/bash
# Launcher script for Sora's CLI with proper environment setup

cd "$(dirname "$0")"

# Activate virtual environment
source venv/bin/activate

# Set PYTHONPATH
export PYTHONPATH=/Users/edjunior/personal_projects/ghibli_pipeline

# Clear Python cache to ensure fresh module loading
echo "🧹 Clearing Python cache..."
find src -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null
find src -name "*.pyc" -delete 2>/dev/null

echo ""
echo "✨ Starting Sora's Archive CLI..."
echo ""

# Run CLI
python src/ai/rag_cli.py "$@"
