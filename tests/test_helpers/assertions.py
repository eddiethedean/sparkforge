"""
Custom assertion helpers for tests.

This module provides custom assertion functions for common test scenarios.
"""

from typing import List, Optional

from pipeline_builder.compat import DataFrame


class TestAssertions:
    """Utility class for common test assertions."""

    @staticmethod
    def assert_pipeline_success(result) -> None:
        """Assert that a pipeline execution was successful."""
        assert result.status.value == "completed", f"Pipeline failed: {result.status}"
        assert result.metrics.failed_steps == 0, (
            f"Failed steps: {result.metrics.failed_steps}"
        )
        assert result.metrics.successful_steps > 0, "No successful steps"

    @staticmethod
    def assert_pipeline_failure(
        result, expected_failed_steps: Optional[int] = None
    ) -> None:
        """Assert that a pipeline execution failed as expected."""
        assert result.status.value == "failed", (
            f"Pipeline should have failed: {result.status}"
        )
        if expected_failed_steps is not None:
            assert result.metrics.failed_steps == expected_failed_steps, (
                f"Expected {expected_failed_steps} failed steps, got {result.metrics.failed_steps}"
            )

    @staticmethod
    def assert_dataframe_has_columns(
        df: DataFrame, expected_columns: List[str], exact: bool = False
    ) -> None:
        """
        Assert that a DataFrame has the expected columns.

        Args:
            df: DataFrame to check
            expected_columns: List of expected column names
            exact: If True, assert exact match (no extra columns)
        """
        actual_columns = set(df.columns)
        expected_columns_set = set(expected_columns)
        missing_columns = expected_columns_set - actual_columns
        extra_columns = actual_columns - expected_columns_set

        assert not missing_columns, f"Missing columns: {missing_columns}"
        if exact and extra_columns:
            raise AssertionError(f"Extra columns found: {extra_columns}")

    @staticmethod
    def assert_dataframe_not_empty(df: DataFrame) -> None:
        """Assert that a DataFrame is not empty."""
        assert df.count() > 0, "DataFrame is empty"

    @staticmethod
    def assert_dataframe_empty(df: DataFrame) -> None:
        """Assert that a DataFrame is empty."""
        assert df.count() == 0, f"DataFrame is not empty, has {df.count()} rows"

    @staticmethod
    def assert_dataframe_row_count(df: DataFrame, expected_count: int) -> None:
        """Assert that a DataFrame has the expected number of rows."""
        actual_count = df.count()
        assert actual_count == expected_count, (
            f"Expected {expected_count} rows, got {actual_count}"
        )

    @staticmethod
    def assert_dataframe_has_data(df: DataFrame, expected_data: List[tuple]) -> None:
        """
        Assert that a DataFrame contains the expected data.

        Args:
            df: DataFrame to check
            expected_data: List of tuples representing expected rows
        """
        actual_data = df.collect()
        assert len(actual_data) == len(expected_data), (
            f"Expected {len(expected_data)} rows, got {len(actual_data)}"
        )
        # Compare data (simplified - may need schema-aware comparison)
        for i, (actual_row, expected_row) in enumerate(zip(actual_data, expected_data)):
            assert actual_row == expected_row, (
                f"Row {i} mismatch: expected {expected_row}, got {actual_row}"
            )
