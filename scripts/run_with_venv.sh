#!/bin/bash
# Helper script to run Python scripts with virtual environment activated

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$( cd "$SCRIPT_DIR/.." && pwd )"

# Activate virtual environment
if [ -d "$PROJECT_DIR/venv" ]; then
    source "$PROJECT_DIR/venv/bin/activate"
else
    echo "❌ Error: Virtual environment not found at $PROJECT_DIR/venv"
    echo "   Please create it first: python -m venv venv"
    exit 1
fi

# Run the script with all arguments
cd "$PROJECT_DIR"
exec "$@"

