"""
Query builder module for common PySpark DataFrame operations.

This module provides reusable query builders and common aggregations
to reduce code duplication across the writer modules.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    count,
    date_format,
    desc,
    lag,
    lit,
    max,
    min,
    stddev,
)
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import when
from pyspark.sql.window import Window


class QueryBuilder:
    """Builder class for common PySpark DataFrame operations."""

    @staticmethod
    def filter_by_date_range(df: DataFrame, days: int = 30) -> DataFrame:
        """
        Filter DataFrame by date range.

        Args:
            df: Input DataFrame
            days: Number of days to look back

        Returns:
            Filtered DataFrame
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        return df.filter(col("created_at") >= lit(start_date.strftime("%Y-%m-%d")))

    @staticmethod
    def add_date_column(
        df: DataFrame,
        date_column: str = "created_at",
        output_column: str = "date",
        format: str = "yyyy-MM-dd",
    ) -> DataFrame:
        """
        Add formatted date column to DataFrame.

        Args:
            df: Input DataFrame
            date_column: Source date column name
            output_column: Output column name
            format: Date format string

        Returns:
            DataFrame with added date column
        """
        return df.withColumn(output_column, date_format(col(date_column), format))

    @staticmethod
    def get_common_aggregations() -> Dict[str, Any]:
        """
        Get common aggregation functions.

        Returns:
            Dictionary of common aggregations
        """
        return {
            "count_all": count("*").alias("total_executions"),
            "count_rows": count("*").alias("execution_count"),
            "avg_validation_rate": avg("validation_rate").alias("avg_validation_rate"),
            "min_validation_rate": min("validation_rate").alias("min_validation_rate"),
            "max_validation_rate": max("validation_rate").alias("max_validation_rate"),
            "stddev_validation_rate": stddev("validation_rate").alias(
                "stddev_validation_rate"
            ),
            "avg_execution_time": avg("execution_time").alias("avg_execution_time"),
            "min_execution_time": min("execution_time").alias("min_execution_time"),
            "max_execution_time": max("execution_time").alias("max_execution_time"),
            "stddev_execution_time": stddev("execution_time").alias(
                "stddev_execution_time"
            ),
            "sum_rows_written": spark_sum("rows_written").alias("total_rows_written"),
            "successful_executions": spark_sum(
                when(col("success") == True, 1).otherwise(0)
            ).alias("successful_executions"),
            "failed_executions": spark_sum(
                when(col("success") == False, 1).otherwise(0)
            ).alias("failed_executions"),
            "high_quality_executions": spark_sum(
                when(col("validation_rate") >= 95.0, 1).otherwise(0)
            ).alias("high_quality_executions"),
            "low_quality_executions": spark_sum(
                when(col("validation_rate") < 80.0, 1).otherwise(0)
            ).alias("low_quality_executions"),
        }

    @staticmethod
    def get_quality_aggregations() -> Dict[str, Any]:
        """
        Get quality-specific aggregations.

        Returns:
            Dictionary of quality aggregations
        """
        aggs = QueryBuilder.get_common_aggregations()
        return {
            "total_executions": aggs["count_all"],
            "avg_validation_rate": aggs["avg_validation_rate"],
            "min_validation_rate": aggs["min_validation_rate"],
            "max_validation_rate": aggs["max_validation_rate"],
            "stddev_validation_rate": aggs["stddev_validation_rate"],
            "high_quality_executions": aggs["high_quality_executions"],
            "low_quality_executions": aggs["low_quality_executions"],
        }

    @staticmethod
    def get_performance_aggregations() -> Dict[str, Any]:
        """
        Get performance-specific aggregations.

        Returns:
            Dictionary of performance aggregations
        """
        aggs = QueryBuilder.get_common_aggregations()
        return {
            "execution_count": aggs["count_rows"],
            "avg_execution_time": aggs["avg_execution_time"],
            "min_execution_time": aggs["min_execution_time"],
            "max_execution_time": aggs["max_execution_time"],
            "stddev_execution_time": aggs["stddev_execution_time"],
            "avg_validation_rate": aggs["avg_validation_rate"],
            "total_rows_written": aggs["sum_rows_written"],
            "successful_executions": aggs["successful_executions"],
        }

    @staticmethod
    def get_trend_aggregations() -> Dict[str, Any]:
        """
        Get trend-specific aggregations.

        Returns:
            Dictionary of trend aggregations
        """
        aggs = QueryBuilder.get_common_aggregations()
        return {
            "daily_executions": aggs["count_all"],
            "successful_executions": aggs["successful_executions"],
            "failed_executions": aggs["failed_executions"],
            "avg_execution_time": aggs["avg_execution_time"],
            "total_rows_written": aggs["sum_rows_written"],
        }

    @staticmethod
    def build_daily_trends_query(df: DataFrame, days: int = 30) -> DataFrame:
        """
        Build daily trends query with common aggregations.

        Args:
            df: Input DataFrame
            days: Number of days to analyze

        Returns:
            DataFrame with daily trends
        """
        filtered_df = QueryBuilder.filter_by_date_range(df, days)
        aggs = QueryBuilder.get_trend_aggregations()

        return (
            filtered_df.transform(lambda df: QueryBuilder.add_date_column(df))
            .groupBy("date")
            .agg(**aggs)
            .orderBy("date")
        )

    @staticmethod
    def build_phase_trends_query(df: DataFrame, days: int = 30) -> DataFrame:
        """
        Build phase trends query with common aggregations.

        Args:
            df: Input DataFrame
            days: Number of days to analyze

        Returns:
            DataFrame with phase trends
        """
        filtered_df = QueryBuilder.filter_by_date_range(df, days)
        aggs = QueryBuilder.get_performance_aggregations()

        return filtered_df.groupBy("phase").agg(**aggs).orderBy("phase")

    @staticmethod
    def build_step_trends_query(df: DataFrame, days: int = 30) -> DataFrame:
        """
        Build step trends query with common aggregations.

        Args:
            df: Input DataFrame
            days: Number of days to analyze

        Returns:
            DataFrame with step trends
        """
        filtered_df = QueryBuilder.filter_by_date_range(df, days)
        aggs = QueryBuilder.get_performance_aggregations()

        return (
            filtered_df.groupBy("step").agg(**aggs).orderBy(desc("avg_execution_time"))
        )

    @staticmethod
    def build_quality_trends_query(df: DataFrame, days: int = 30) -> DataFrame:
        """
        Build quality trends query with common aggregations.

        Args:
            df: Input DataFrame
            days: Number of days to analyze

        Returns:
            DataFrame with quality trends
        """
        filtered_df = QueryBuilder.filter_by_date_range(df, days)
        aggs = QueryBuilder.get_quality_aggregations()

        return (
            filtered_df.transform(lambda df: QueryBuilder.add_date_column(df))
            .groupBy("date")
            .agg(**aggs)
            .orderBy("date")
        )

    @staticmethod
    def build_overall_metrics_query(df: DataFrame, days: int = 30) -> DataFrame:
        """
        Build overall metrics query.

        Args:
            df: Input DataFrame
            days: Number of days to analyze

        Returns:
            DataFrame with overall metrics
        """
        filtered_df = QueryBuilder.filter_by_date_range(df, days)
        aggs = QueryBuilder.get_quality_aggregations()

        return filtered_df.agg(**aggs)

    @staticmethod
    def build_anomaly_detection_query(
        df: DataFrame, threshold_column: str, threshold_value: float
    ) -> DataFrame:
        """
        Build anomaly detection query.

        Args:
            df: Input DataFrame
            threshold_column: Column to check against threshold
            threshold_value: Threshold value

        Returns:
            DataFrame with anomalies
        """
        return df.filter(col(threshold_column) < threshold_value)

    @staticmethod
    def build_performance_anomaly_query(
        df: DataFrame, performance_threshold: float
    ) -> DataFrame:
        """
        Build performance anomaly detection query.

        Args:
            df: Input DataFrame
            performance_threshold: Performance threshold value

        Returns:
            DataFrame with performance anomalies
        """
        return df.filter(
            (col("execution_time") > performance_threshold)
            | (col("validation_rate") < 80.0)
            | (col("success") == False)
        )

    @staticmethod
    def build_quality_anomaly_query(
        df: DataFrame, quality_threshold: float = 90.0
    ) -> DataFrame:
        """
        Build quality anomaly detection query.

        Args:
            df: Input DataFrame
            quality_threshold: Quality threshold value

        Returns:
            DataFrame with quality anomalies
        """
        return df.filter(col("validation_rate") < quality_threshold)

    @staticmethod
    def build_temporal_anomaly_query(
        df: DataFrame, change_threshold: float = -10.0
    ) -> DataFrame:
        """
        Build temporal anomaly detection query.

        Args:
            df: Input DataFrame
            change_threshold: Change threshold value

        Returns:
            DataFrame with temporal anomalies
        """
        # First, calculate daily quality metrics
        daily_quality_df = (
            df.transform(lambda df: QueryBuilder.add_date_column(df))
            .groupBy("date")
            .agg(avg("validation_rate").alias("daily_avg_validation_rate"))
            .orderBy("date")
        )

        # Use window function to calculate lag and quality change
        window_spec = Window.orderBy("date")
        return (
            daily_quality_df.withColumn(
                "prev_avg_validation_rate",
                lag("daily_avg_validation_rate", 1).over(window_spec),
            )
            .withColumn(
                "quality_change",
                col("daily_avg_validation_rate") - col("prev_avg_validation_rate"),
            )
            .filter(col("quality_change") < change_threshold)
            .orderBy("quality_change")
        )

    @staticmethod
    def calculate_statistics(df: DataFrame, column: str) -> Dict[str, float]:
        """
        Calculate basic statistics for a column.

        Args:
            df: Input DataFrame
            column: Column name to calculate statistics for

        Returns:
            Dictionary with statistics
        """
        stats_df = df.agg(
            avg(column).alias("avg"),
            stddev(column).alias("stddev"),
            min(column).alias("min"),
            max(column).alias("max"),
        )

        result = stats_df.collect()[0]
        return {
            "avg": result["avg"],
            "stddev": result["stddev"],
            "min": result["min"],
            "max": result["max"],
        }

    @staticmethod
    def build_recent_performance_query(df: DataFrame, days: int = 7) -> DataFrame:
        """
        Build recent performance query.

        Args:
            df: Input DataFrame
            days: Number of recent days to analyze

        Returns:
            DataFrame with recent performance
        """
        filtered_df = QueryBuilder.filter_by_date_range(df, days)
        aggs = {
            "daily_executions": count("*").alias("daily_executions"),
            "avg_execution_time": avg("execution_time").alias("avg_execution_time"),
            "avg_validation_rate": avg("validation_rate").alias("avg_validation_rate"),
        }

        return (
            filtered_df.transform(lambda df: QueryBuilder.add_date_column(df))
            .groupBy("date")
            .agg(**aggs)
            .orderBy("date")
        )
