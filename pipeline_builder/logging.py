"""
Simplified logging system for the framework.

This module re-exports logging classes from pipeline_builder_base
for backward compatibility.
"""

from __future__ import annotations

# Re-export from base for backward compatibility
from pipeline_builder_base.logging import PipelineLogger

__all__ = ["PipelineLogger"]
