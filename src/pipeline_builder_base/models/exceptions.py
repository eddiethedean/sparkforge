"""
Custom exceptions for the Pipeline Builder models.
"""


class PipelineConfigurationError(ValueError):
    """Raised when pipeline configuration is invalid."""

    pass


class PipelineExecutionError(RuntimeError):
    """Raised when pipeline execution fails."""

    pass
