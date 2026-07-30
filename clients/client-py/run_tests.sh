#!/bin/bash

# Test runner script for Queen Python client
# Equivalent to Node.js "npm test" or "node test-v2/run.js"

set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}Queen Python Client - Test Runner${NC}"
echo "======================================"
echo ""

# Check if virtual environment is activated
if [ -z "$VIRTUAL_ENV" ]; then
    echo -e "${RED}⚠️  Warning: Virtual environment not activated${NC}"
    echo "Consider running: source venv/bin/activate"
    echo ""
fi

# Check dependencies
if ! python -c "import queen" 2>/dev/null; then
    echo -e "${RED}❌ Queen client not installed${NC}"
    echo "Run: pip install -e ."
    exit 1
fi

if ! python -c "import pytest" 2>/dev/null; then
    echo -e "${RED}❌ Pytest not installed${NC}"
    echo "Run: pip install -e \".[dev]\""
    exit 1
fi

# Check if Queen server is running
if ! curl -s http://localhost:6632/health > /dev/null 2>&1; then
    echo -e "${RED}❌ Queen server not running on http://localhost:6632${NC}"
    echo ""
    echo "Start the server:"
    echo "  docker run -p 6632:6632 -e PG_HOST=... ghcr.io/queen-mq/queen:latest"
    echo ""
    exit 1
fi

echo -e "${GREEN}✅ Queen server is running${NC}"
echo ""

# Parse arguments
MODE="${1:-all}"

case "$MODE" in
    "human")
        echo "Running human-written tests..."
        python -m tests.run_tests human
        ;;
    "pytest")
        echo "Running all tests with pytest..."
        pytest tests/ -v
        ;;
    "pytest-s")
        echo "Running all tests with pytest (with output)..."
        pytest tests/ -vs
        ;;
    "quick")
        echo "Running quick smoke tests..."
        pytest tests/test_queue.py tests/test_push.py tests/test_pop.py -v
        ;;
    *)
        if [ "$MODE" = "all" ]; then
            echo "Running all tests with pytest..."
            pytest tests/ -v
        else
            echo "Running specific test: $MODE"
            python -m tests.run_tests "$MODE"
        fi
        ;;
esac

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✅ Tests completed successfully${NC}"
else
    echo -e "${RED}❌ Some tests failed${NC}"
fi

exit $EXIT_CODE

