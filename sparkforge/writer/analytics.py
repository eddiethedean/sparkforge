"""
Writer analytics module for data quality and trend analysis.

This module provides comprehensive analytics capabilities for analyzing
pipeline execution data, detecting trends, and generating insights.

# Depends on:
#   compat
#   logging
#   writer.exceptions
#   writer.query_builder
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, Literal, TypedDict, Union, cast

from ..compat import DataFrame, F, SparkSession
from ..logging import PipelineLogger
from .exceptions import WriterError
from .query_builder import QueryBuilder

# Alias for convenience
col = F.col


# ============================================================================
# TypedDict Definitions
# ============================================================================


class AnalysisPeriod(TypedDict):
    """Analysis period structure."""

    start_date: str
    end_date: str
    days_analyzed: int


class DailyQualityTrend(TypedDict):
    """Daily quality trend data point."""

    date: str
    total_executions: int
    avg_validation_rate: float
    min_validation_rate: float
    max_validation_rate: float
    stddev_validation_rate: float
    high_quality_executions: int
    low_quality_executions: int
    quality_score: str


class OverallQualityMetrics(TypedDict):
    """Overall quality metrics."""

    total_executions: int
    avg_validation_rate: float
    min_validation_rate: float
    max_validation_rate: float
    stddev_validation_rate: float


class DegradationAlert(TypedDict):
    """Quality degradation alert."""

    type: str
    message: str
    severity: Literal["high", "medium", "low"]


class QualityTrends(TypedDict):
    """Quality trends analysis structure."""

    analysis_period: AnalysisPeriod
    daily_trends: list[DailyQualityTrend]
    overall_metrics: OverallQualityMetrics
    degradation_alerts: list[DegradationAlert]
    quality_grade: str


class ValidationAnomaly(TypedDict):
    """Validation anomaly data point."""

    step: str
    phase: str
    validation_rate: float
    valid_rows: int
    invalid_rows: int
    timestamp: str


class StepAnomaly(TypedDict):
    """Step-level anomaly data point."""

    step: str
    execution_count: int
    avg_validation_rate: float
    min_validation_rate: float
    stddev_validation_rate: float
    anomaly_score: float


class TemporalAnomaly(TypedDict):
    """Temporal anomaly data point."""

    date: str
    daily_avg_validation_rate: float
    prev_avg_validation_rate: float
    quality_change: float


class AnomalySummary(TypedDict):
    """Anomaly summary statistics."""

    total_validation_anomalies: int
    total_step_anomalies: int
    total_temporal_anomalies: int
    overall_anomaly_score: float


class QualityAnomalies(TypedDict):
    """Quality anomalies analysis structure."""

    validation_anomalies: list[ValidationAnomaly]
    step_anomalies: list[StepAnomaly]
    temporal_anomalies: list[TemporalAnomaly]
    anomaly_summary: AnomalySummary


class VolumeTrendPoint(TypedDict):
    """Volume trend data point."""

    date: str
    daily_executions: int
    successful_executions: int
    failed_executions: int
    success_rate: float
    avg_execution_time: float
    total_rows_written: int


class PhaseTrendPoint(TypedDict):
    """Phase trend data point."""

    phase: str
    execution_count: int
    avg_execution_time: float
    avg_validation_rate: float
    total_rows_written: int
    success_rate: float


class StepTrendPoint(TypedDict):
    """Step trend data point."""

    step: str
    execution_count: int
    avg_execution_time: float
    avg_validation_rate: float
    stddev_execution_time: float
    min_execution_time: float
    max_execution_time: float
    performance_grade: str


class TrendIndicators(TypedDict):
    """Trend indicators."""

    execution_volume_trend: str
    success_rate_trend: str
    recent_executions: int
    historical_avg_executions: float
    recent_success_rate: float
    historical_success_rate: float


class ExecutionTrends(TypedDict):
    """Execution trends analysis structure."""

    analysis_period: AnalysisPeriod
    volume_trends: list[VolumeTrendPoint]
    phase_trends: list[PhaseTrendPoint]
    step_trends: list[StepTrendPoint]
    trend_indicators: TrendIndicators


class DataQualityAnalyzer:
    """Analyzes data quality metrics and trends."""

    def __init__(self, spark: SparkSession, logger: PipelineLogger | None = None):
        """Initialize the data quality analyzer."""
        self.spark = spark
        if logger is None:
            self.logger = PipelineLogger("DataQualityAnalyzer")
        else:
            self.logger = logger

    def analyze_quality_trends(self, df: DataFrame, days: int = 30) -> QualityTrends:
        """
        Analyze data quality trends over time.

        Args:
            df: DataFrame containing log data
            days: Number of days to analyze

        Returns:
            Dictionary containing quality trend analysis
        """
        try:
            self.logger.info(f"Analyzing data quality trends for last {days} days")

            # Use query builder for quality trends
            quality_trends_df = QueryBuilder.build_quality_trends_query(df, days)
            quality_trends = quality_trends_df.collect()

            # Use query builder for overall metrics
            overall_metrics_df = QueryBuilder.build_overall_metrics_query(df, days)
            overall_metrics = overall_metrics_df.collect()[0]

            # Detect quality degradation
            degradation_alerts = []
            if len(quality_trends) > 1:
                recent_avg = quality_trends[-1]["avg_validation_rate"]
                historical_avg = sum(
                    row["avg_validation_rate"] for row in quality_trends[:-1]
                ) / len(quality_trends[:-1])

                if recent_avg < historical_avg - 5.0:  # 5% degradation threshold
                    degradation_alerts.append(
                        {
                            "type": "quality_degradation",
                            "message": f"Recent validation rate ({recent_avg:.1f}%) is significantly lower than historical average ({historical_avg:.1f}%)",
                            "severity": (
                                "high"
                                if recent_avg < historical_avg - 10.0
                                else "medium"
                            ),
                        }
                    )

            # Get date range for analysis period
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)

            analysis_result = {
                "analysis_period": {
                    "start_date": start_date.strftime("%Y-%m-%d"),
                    "end_date": end_date.strftime("%Y-%m-%d"),
                    "days_analyzed": days,
                },
                "daily_trends": [
                    {
                        "date": row["date"].strftime("%Y-%m-%d"),
                        "total_executions": row["total_executions"],
                        "avg_validation_rate": round(row["avg_validation_rate"], 2),
                        "min_validation_rate": round(row["min_validation_rate"], 2),
                        "max_validation_rate": round(row["max_validation_rate"], 2),
                        "stddev_validation_rate": round(
                            row["stddev_validation_rate"], 2
                        ),
                        "high_quality_executions": row["high_quality_executions"],
                        "low_quality_executions": row["low_quality_executions"],
                        "quality_score": self._calculate_quality_score(row.asDict()),
                    }
                    for row in quality_trends
                ],
                "overall_metrics": {
                    "total_executions": overall_metrics["total_executions"],
                    "avg_validation_rate": round(
                        overall_metrics["overall_avg_validation_rate"], 2
                    ),
                    "min_validation_rate": round(
                        overall_metrics["overall_min_validation_rate"], 2
                    ),
                    "max_validation_rate": round(
                        overall_metrics["overall_max_validation_rate"], 2
                    ),
                    "stddev_validation_rate": round(
                        overall_metrics["overall_stddev_validation_rate"], 2
                    ),
                },
                "degradation_alerts": degradation_alerts,
                "quality_grade": self._calculate_quality_grade(
                    overall_metrics["overall_avg_validation_rate"]
                ),
            }

            self.logger.info("Data quality trends analysis completed")
            return cast(QualityTrends, analysis_result)

        except Exception as e:
            self.logger.error(f"Failed to analyze quality trends: {e}")
            raise WriterError(f"Failed to analyze quality trends: {e}") from e

    def detect_quality_anomalies(self, df: DataFrame) -> QualityAnomalies:
        """
        Detect data quality anomalies.

        Args:
            df: DataFrame containing log data

        Returns:
            Dictionary containing anomaly detection results
        """
        try:
            self.logger.info("Detecting data quality anomalies")

            # Calculate overall statistics for anomaly detection
            overall_stats = QueryBuilder.calculate_statistics(df, "validation_rate")
            threshold = overall_stats["avg"] - (2 * overall_stats["stddev"])

            # Detect validation rate anomalies using query builder
            validation_anomalies_df = (
                QueryBuilder.build_anomaly_detection_query(
                    df, "validation_rate", threshold
                )
                .select(
                    "step",
                    "phase",
                    "validation_rate",
                    "valid_rows",
                    "invalid_rows",
                    "created_at",
                )
                .orderBy("validation_rate")
            )

            validation_anomalies = validation_anomalies_df.collect()

            # Detect step-specific anomalies using query builder
            step_anomalies_df = (
                df.groupBy("step")
                .agg(**QueryBuilder.get_performance_aggregations())
                .filter(
                    (col("avg_validation_rate") < 90.0)
                    | (col("stddev_validation_rate") > 10.0)
                )
                .orderBy("avg_validation_rate")
            )

            step_anomalies = step_anomalies_df.collect()

            # Detect temporal anomalies using query builder
            temporal_anomalies_df = QueryBuilder.build_temporal_anomaly_query(df)
            temporal_anomalies = temporal_anomalies_df.collect()

            anomaly_result = {
                "validation_anomalies": [
                    {
                        "step": row["step"],
                        "phase": row["phase"],
                        "validation_rate": round(row["validation_rate"], 2),
                        "valid_rows": row["valid_rows"],
                        "invalid_rows": row["invalid_rows"],
                        "timestamp": row["created_at"].strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    for row in validation_anomalies
                ],
                "step_anomalies": [
                    {
                        "step": row["step"],
                        "execution_count": row["execution_count"],
                        "avg_validation_rate": round(row["avg_validation_rate"], 2),
                        "min_validation_rate": round(row["min_validation_rate"], 2),
                        "stddev_validation_rate": round(
                            row["stddev_validation_rate"], 2
                        ),
                        "anomaly_score": self._calculate_anomaly_score(row.asDict()),
                    }
                    for row in step_anomalies
                ],
                "temporal_anomalies": [
                    {
                        "date": row["date"].strftime("%Y-%m-%d"),
                        "daily_avg_validation_rate": round(
                            row["daily_avg_validation_rate"], 2
                        ),
                        "prev_avg_validation_rate": round(
                            row["prev_avg_validation_rate"], 2
                        ),
                        "quality_change": round(row["quality_change"], 2),
                    }
                    for row in temporal_anomalies
                ],
                "anomaly_summary": {
                    "total_validation_anomalies": len(validation_anomalies),
                    "total_step_anomalies": len(step_anomalies),
                    "total_temporal_anomalies": len(temporal_anomalies),
                    "overall_anomaly_score": self._calculate_overall_anomaly_score(
                        len(validation_anomalies),
                        len(step_anomalies),
                        len(temporal_anomalies),
                    ),
                },
            }

            self.logger.info(
                f"Quality anomaly detection completed: {len(validation_anomalies)} validation anomalies found"
            )
            return cast(QualityAnomalies, anomaly_result)

        except Exception as e:
            self.logger.error(f"Failed to detect quality anomalies: {e}")
            raise WriterError(f"Failed to detect quality anomalies: {e}") from e

    def _calculate_quality_score(self, row: Dict[str, Union[int, float]]) -> str:
        """Calculate quality score for a row."""
        avg_rate = row["avg_validation_rate"]
        if avg_rate >= 95.0:
            return "A"
        elif avg_rate >= 90.0:
            return "B"
        elif avg_rate >= 80.0:
            return "C"
        else:
            return "D"

    def _calculate_quality_grade(self, avg_validation_rate: float) -> str:
        """Calculate overall quality grade."""
        if avg_validation_rate >= 95.0:
            return "A"
        elif avg_validation_rate >= 90.0:
            return "B"
        elif avg_validation_rate >= 80.0:
            return "C"
        else:
            return "D"

    def _calculate_anomaly_score(self, row: Dict[str, Union[int, float]]) -> float:
        """Calculate anomaly score for a step."""
        avg_rate = row["avg_validation_rate"]
        stddev_rate = row["stddev_validation_rate"]

        # Lower average rate and higher standard deviation = higher anomaly score
        anomaly_score = (100 - avg_rate) + (stddev_rate * 2)
        return float(round(min(anomaly_score, 100.0), 2))

    def _calculate_overall_anomaly_score(
        self, validation_anomalies: int, step_anomalies: int, temporal_anomalies: int
    ) -> float:
        """Calculate overall anomaly score."""
        total_anomalies = validation_anomalies + step_anomalies + temporal_anomalies

        if total_anomalies == 0:
            return 0.0

        # Weight different types of anomalies
        weighted_score = (
            (validation_anomalies * 1.0)
            + (step_anomalies * 0.8)
            + (temporal_anomalies * 1.2)
        )
        return round(min(weighted_score, 100.0), 2)


class TrendAnalyzer:
    """Analyzes execution trends and patterns."""

    def __init__(self, spark: SparkSession, logger: PipelineLogger | None = None):
        """Initialize the trend analyzer."""
        self.spark = spark
        if logger is None:
            self.logger = PipelineLogger("TrendAnalyzer")
        else:
            self.logger = logger

    def analyze_execution_trends(self, df: DataFrame, days: int = 30) -> ExecutionTrends:
        """
        Analyze execution trends over time.

        Args:
            df: DataFrame containing log data
            days: Number of days to analyze

        Returns:
            Dictionary containing trend analysis
        """
        try:
            self.logger.info(f"Analyzing execution trends for last {days} days")

            # Use query builder for all trend analyses
            volume_trends_df = QueryBuilder.build_daily_trends_query(df, days)
            volume_trends = volume_trends_df.collect()

            phase_trends_df = QueryBuilder.build_phase_trends_query(df, days)
            phase_trends = phase_trends_df.collect()

            step_trends_df = QueryBuilder.build_step_trends_query(df, days)
            step_trends = step_trends_df.collect()

            # Calculate trend indicators
            trend_indicators = self._calculate_trend_indicators(
                [row.asDict() for row in volume_trends]
            )

            # Get date range for analysis period
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)

            analysis_result = {
                "analysis_period": {
                    "start_date": start_date.strftime("%Y-%m-%d"),
                    "end_date": end_date.strftime("%Y-%m-%d"),
                    "days_analyzed": days,
                },
                "volume_trends": [
                    {
                        "date": row["date"].strftime("%Y-%m-%d"),
                        "daily_executions": row["daily_executions"],
                        "successful_executions": row["successful_executions"],
                        "failed_executions": row["failed_executions"],
                        "success_rate": (
                            round(
                                (row["successful_executions"] / row["daily_executions"])
                                * 100,
                                2,
                            )
                            if row["daily_executions"] > 0
                            else 0
                        ),
                        "avg_execution_time": round(row["avg_execution_time"], 2),
                        "total_rows_written": row["total_rows_written"],
                    }
                    for row in volume_trends
                ],
                "phase_trends": [
                    {
                        "phase": row["phase"],
                        "execution_count": row["execution_count"],
                        "avg_execution_time": round(row["avg_execution_time"], 2),
                        "avg_validation_rate": round(row["avg_validation_rate"], 2),
                        "total_rows_written": row["total_rows_written"],
                        "success_rate": round(
                            (row["successful_executions"] / row["execution_count"])
                            * 100,
                            2,
                        ),
                    }
                    for row in phase_trends
                ],
                "step_trends": [
                    {
                        "step": row["step"],
                        "execution_count": row["execution_count"],
                        "avg_execution_time": round(row["avg_execution_time"], 2),
                        "avg_validation_rate": round(row["avg_validation_rate"], 2),
                        "stddev_execution_time": round(row["stddev_execution_time"], 2),
                        "min_execution_time": round(row["min_execution_time"], 2),
                        "max_execution_time": round(row["max_execution_time"], 2),
                        "performance_grade": self._calculate_performance_grade(
                            row.asDict()
                        ),
                    }
                    for row in step_trends
                ],
                "trend_indicators": trend_indicators,
            }

            self.logger.info("Execution trends analysis completed")
            return cast(ExecutionTrends, analysis_result)

        except Exception as e:
            self.logger.error(f"Failed to analyze execution trends: {e}")
            raise WriterError(f"Failed to analyze execution trends: {e}") from e

    def _calculate_trend_indicators(
        self, volume_trends: list[Dict[str, Union[int, float]]]
    ) -> TrendIndicators:
        """Calculate trend indicators from volume trends."""
        if len(volume_trends) < 2:
            return {
                "execution_volume_trend": "insufficient_data",
                "success_rate_trend": "insufficient_data",
                "recent_executions": 0,
                "historical_avg_executions": 0.0,
                "recent_success_rate": 0.0,
                "historical_success_rate": 0.0,
            }

        # Calculate execution volume trend
        recent_executions = volume_trends[-1]["daily_executions"]
        historical_avg = sum(
            row["daily_executions"] for row in volume_trends[:-1]
        ) / len(volume_trends[:-1])

        execution_trend = (
            "increasing"
            if recent_executions > historical_avg * 1.1
            else "decreasing"
            if recent_executions < historical_avg * 0.9
            else "stable"
        )

        # Calculate success rate trend
        recent_success_rate = (
            (
                volume_trends[-1]["successful_executions"]
                / volume_trends[-1]["daily_executions"]
            )
            * 100
            if volume_trends[-1]["daily_executions"] > 0
            else 0
        )
        historical_success_rate = sum(
            (row["successful_executions"] / row["daily_executions"]) * 100
            for row in volume_trends[:-1]
            if row["daily_executions"] > 0
        ) / len([row for row in volume_trends[:-1] if row["daily_executions"] > 0])

        success_trend = (
            "improving"
            if recent_success_rate > historical_success_rate + 2
            else (
                "declining"
                if recent_success_rate < historical_success_rate - 2
                else "stable"
            )
        )

        return {
            "execution_volume_trend": execution_trend,
            "success_rate_trend": success_trend,
            "recent_executions": int(recent_executions),
            "historical_avg_executions": round(historical_avg, 2),
            "recent_success_rate": round(recent_success_rate, 2),
            "historical_success_rate": round(historical_success_rate, 2),
        }

    def _calculate_performance_grade(self, row: Dict[str, Union[int, float]]) -> str:
        """Calculate performance grade for a step."""
        avg_time = row["avg_execution_time"]
        stddev_time = row["stddev_execution_time"]

        # Consider both average time and consistency (low stddev)
        if avg_time < 60 and stddev_time < 30:  # Fast and consistent
            return "A"
        elif avg_time < 120 and stddev_time < 60:  # Reasonable and somewhat consistent
            return "B"
        elif avg_time < 300:  # Acceptable
            return "C"
        else:  # Slow
            return "D"
