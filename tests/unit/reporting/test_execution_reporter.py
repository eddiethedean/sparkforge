"""Tests for ExecutionReporter."""

from pipeline_builder.reporting.execution_reporter import ExecutionReporter


class TestExecutionReporter:
    """Tests for ExecutionReporter."""

    def test_execution_reporter_initialization(self):
        """Test ExecutionReporter can be initialized."""
        reporter = ExecutionReporter()
        assert reporter is not None
