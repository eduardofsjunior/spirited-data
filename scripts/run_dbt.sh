#!/bin/bash
# dbt Transformation Execution Script
# Runs dbt transformations with connection validation

set -e  # Exit on error

# Check if DUCKDB_PATH environment variable is set
if [ -z "$DUCKDB_PATH" ]; then
    echo "❌ Error: DUCKDB_PATH environment variable not set"
    echo "   Please add DUCKDB_PATH to your .env file"
    exit 1
fi

echo "✓ DUCKDB_PATH set to: $DUCKDB_PATH"

# Navigate to transformation directory
cd "$(dirname "$0")/../src/transformation" || exit 1

echo ""
echo "================================================"
echo "  dbt Transformation Execution"
echo "================================================"
echo ""

# Run dbt debug to verify connection
echo "Step 1: Verifying dbt connection..."
if dbt debug --quiet; then
    echo "✓ Connection verified"
else
    echo "❌ Connection failed. Run 'dbt debug' for details."
    exit 1
fi

echo ""
echo "Step 2: Running staging models..."
dbt run --select staging.*

echo ""
echo "Step 3: Counting transformed models..."
MODEL_COUNT=$(dbt list --select staging.* --resource-type model | wc -l | tr -d ' ')

echo ""
echo "================================================"
echo "  ✓ dbt Transformation Complete"
echo "  Models executed: $MODEL_COUNT"
echo "================================================"
