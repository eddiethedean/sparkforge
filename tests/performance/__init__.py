"""
Performance testing module for SparkForge.

This module provides performance testing infrastructure including:
- Performance baseline measurements
- Regression detection
- Memory usage monitoring
- Timing analysis
"""

# Import only the performance monitor to avoid circular imports
# Performance tests will be discovered by pytest
import sys
from pathlib import Path

# Add this directory to sys.path for imports
perf_dir = Path(__file__).parent
if str(perf_dir) not in sys.path:
    sys.path.insert(0, str(perf_dir))

__all__ = []
