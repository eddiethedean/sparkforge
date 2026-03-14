#!/bin/bash
# Run tests with sparkless backend (default)
export SPARKLESS_TEST_MODE=sparkless
echo "🔧 Running tests with SPARKLESS backend"
python -m pytest "$@"
