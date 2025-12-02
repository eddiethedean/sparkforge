"""
Writer-specific exceptions.
"""

from ..errors import SparkForgeError


class WriterError(SparkForgeError):
    """Base exception for writer errors."""

    pass


class WriterConfigurationError(WriterError):
    """Raised when writer configuration is invalid."""

    pass


class WriterValidationError(WriterError):
    """Raised when writer validation fails."""

    pass


class WriterTableError(WriterError):
    """Raised when table operations fail."""

    pass


class WriterDataQualityError(WriterError):
    """Raised when data quality checks fail."""

    pass


class WriterPerformanceError(WriterError):
    """Raised when performance issues are detected."""

    pass
