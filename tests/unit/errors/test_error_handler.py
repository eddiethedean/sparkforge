"""Tests for ErrorHandler."""

from pipeline_builder.errors.error_handler import ErrorHandler


class TestErrorHandler:
    """Tests for ErrorHandler."""

    def test_error_handler_initialization(self):
        """Test ErrorHandler can be initialized."""
        handler = ErrorHandler()
        assert handler is not None
        assert handler.logger is not None
