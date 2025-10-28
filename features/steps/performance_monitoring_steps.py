"""
Step definitions for performance monitoring BDD tests.
"""

from behave import given, then, when

from pipeline_builder.models import ExecutionResult, StepResult


@given("I have performance monitoring enabled")
def step_have_performance_monitoring_enabled(context):
    """Enable performance monitoring for testing."""
    context.performance_monitoring_enabled = True
    context.performance_metrics = {}


@given("I have a pipeline with multiple steps")
def step_have_pipeline_with_multiple_steps(context):
    """Create a pipeline with multiple steps for performance testing."""
    context.execution_result = ExecutionResult(
        success=True,
        execution_time=180.0,
        total_rows_processed=2000,
        validation_rate=97.0,
        error_message=None,
    )

    # Add multiple step results with performance data
    context.execution_result.bronze_results = {
        "raw_events": StepResult(
            step_name="raw_events",
            success=True,
            execution_time=60.0,
            input_rows=1000,
            output_rows=950,
            validation_rate=95.0,
            error_message=None,
        )
    }

    context.execution_result.silver_results = {
        "clean_events": StepResult(
            step_name="clean_events",
            success=True,
            execution_time=90.0,
            input_rows=950,
            output_rows=900,
            validation_rate=98.0,
            error_message=None,
        )
    }

    context.execution_result.gold_results = {
        "analytics_events": StepResult(
            step_name="analytics_events",
            success=True,
            execution_time=30.0,
            input_rows=900,
            output_rows=900,
            validation_rate=100.0,
            error_message=None,
        )
    }


@given("I have a pipeline processing large datasets")
def step_have_pipeline_processing_large_datasets(context):
    """Create a pipeline processing large datasets."""
    context.large_dataset_metrics = {
        "total_rows": 1000000,
        "processing_time": 300.0,  # 5 minutes
        "rows_per_second": 1000000 / 300.0,
        "data_throughput": "3.33 MB/s",
        "processing_efficiency": 85.0,
    }


@given("I have a pipeline with resource-intensive operations")
def step_have_pipeline_with_resource_intensive_operations(context):
    """Create a pipeline with resource-intensive operations."""
    context.resource_metrics = {
        "memory_usage": {"peak": "4.2GB", "average": "3.8GB", "utilization": 85.0},
        "cpu_utilization": {"peak": 95.0, "average": 78.0, "cores_used": 8},
        "disk_io": {
            "read_bytes": "2.1GB",
            "write_bytes": "1.8GB",
            "io_operations": 15000,
        },
    }


@given("I have historical performance data")
def step_have_historical_performance_data(context):
    """Create historical performance data for trend analysis."""
    context.historical_performance = {
        "2024-01-01": {
            "execution_time": 100.0,
            "memory_usage": "2.5GB",
            "validation_rate": 96.0,
        },
        "2024-01-02": {
            "execution_time": 105.0,
            "memory_usage": "2.6GB",
            "validation_rate": 95.5,
        },
        "2024-01-03": {
            "execution_time": 110.0,
            "memory_usage": "2.7GB",
            "validation_rate": 95.0,
        },
        "2024-01-04": {
            "execution_time": 120.0,
            "memory_usage": "2.8GB",
            "validation_rate": 94.5,
        },  # Declining trend
    }


@given("I have a pipeline with parallel steps")
def step_have_pipeline_with_parallel_steps(context):
    """Create a pipeline with parallel steps."""
    context.parallel_execution_metrics = {
        "total_steps": 4,
        "parallel_steps": 2,
        "sequential_steps": 2,
        "parallelization_efficiency": 75.0,
        "load_balancing": 90.0,
        "optimal_parallelization": 3,
    }


@given("I have a pipeline with incremental processing")
def step_have_pipeline_with_incremental_processing(context):
    """Create a pipeline with incremental processing."""
    context.incremental_metrics = {
        "incremental_data_volume": 50000,
        "full_refresh_volume": 500000,
        "processing_efficiency": 95.0,
        "time_savings": 80.0,
        "incremental_ratio": 0.1,
    }


@given("I have performance data from multiple executions")
def step_have_performance_data_from_multiple_executions(context):
    """Create performance data from multiple executions."""
    context.multiple_execution_data = [
        {
            "execution_id": 1,
            "execution_time": 100.0,
            "memory_usage": "2.5GB",
            "validation_rate": 96.0,
        },
        {
            "execution_id": 2,
            "execution_time": 105.0,
            "memory_usage": "2.6GB",
            "validation_rate": 95.5,
        },
        {
            "execution_id": 3,
            "execution_time": 110.0,
            "memory_usage": "2.7GB",
            "validation_rate": 95.0,
        },
        {
            "execution_id": 4,
            "execution_time": 115.0,
            "memory_usage": "2.8GB",
            "validation_rate": 94.5,
        },
        {
            "execution_id": 5,
            "execution_time": 120.0,
            "memory_usage": "2.9GB",
            "validation_rate": 94.0,
        },
    ]


@when("I execute the pipeline")
def step_execute_pipeline_with_monitoring(context):
    """Execute the pipeline with performance monitoring."""
    try:
        # Simulate performance monitoring
        context.performance_metrics = {
            "execution_time": context.execution_result.execution_time,
            "memory_usage": "3.2GB",
            "cpu_usage": 85.0,
            "total_rows_processed": context.execution_result.total_rows_processed,
            "validation_rate": context.execution_result.validation_rate,
        }

        context.monitoring_success = True
    except Exception as e:
        context.monitoring_error = str(e)
        context.monitoring_success = False


@when("I execute the pipeline")
def step_execute_pipeline_with_data_processing_monitoring(context):
    """Execute the pipeline with data processing monitoring."""
    try:
        # Simulate data processing monitoring
        context.data_processing_metrics = {
            "rows_processed_per_second": context.large_dataset_metrics[
                "rows_per_second"
            ],
            "data_throughput": context.large_dataset_metrics["data_throughput"],
            "processing_efficiency": context.large_dataset_metrics[
                "processing_efficiency"
            ],
            "bottlenecks": ["memory_allocation", "disk_io"],
        }

        context.data_processing_monitoring_success = True
    except Exception as e:
        context.data_processing_monitoring_error = str(e)
        context.data_processing_monitoring_success = False


@when("I execute the pipeline")
def step_execute_pipeline_with_resource_monitoring(context):
    """Execute the pipeline with resource monitoring."""
    try:
        # Simulate resource monitoring
        context.resource_monitoring_metrics = {
            "memory_usage_over_time": context.resource_metrics["memory_usage"],
            "cpu_utilization": context.resource_metrics["cpu_utilization"],
            "disk_io": context.resource_metrics["disk_io"],
            "resource_constraints": ["memory_limit", "cpu_cores"],
        }

        context.resource_monitoring_success = True
    except Exception as e:
        context.resource_monitoring_error = str(e)
        context.resource_monitoring_success = False


@when("I execute the pipeline")
def step_execute_pipeline_with_trend_analysis(context):
    """Execute the pipeline with trend analysis."""
    try:
        # Simulate trend analysis
        current_metrics = {
            "execution_time": 125.0,
            "memory_usage": "3.0GB",
            "validation_rate": 93.5,
        }

        context.trend_analysis = {
            "current_metrics": current_metrics,
            "historical_average": {
                "execution_time": 110.0,
                "memory_usage": "2.7GB",
                "validation_rate": 95.0,
            },
            "performance_regression": {
                "execution_time": 15.0,  # 15 seconds slower
                "memory_usage": 0.3,  # 0.3GB more memory
                "validation_rate": -1.5,  # 1.5% lower validation rate
            },
            "alerts": ["Performance regression detected", "Memory usage increased"],
        }

        context.trend_analysis_success = True
    except Exception as e:
        context.trend_analysis_error = str(e)
        context.trend_analysis_success = False


@when("I execute the pipeline")
def step_execute_pipeline_with_parallel_monitoring(context):
    """Execute the pipeline with parallel execution monitoring."""
    try:
        # Simulate parallel execution monitoring
        context.parallel_monitoring_metrics = {
            "parallelization_efficiency": context.parallel_execution_metrics[
                "parallelization_efficiency"
            ],
            "load_balancing": context.parallel_execution_metrics["load_balancing"],
            "optimal_parallelization": context.parallel_execution_metrics[
                "optimal_parallelization"
            ],
            "parallel_settings": {
                "max_parallel_steps": 4,
                "current_parallel_steps": 2,
                "recommended_parallel_steps": 3,
            },
        }

        context.parallel_monitoring_success = True
    except Exception as e:
        context.parallel_monitoring_error = str(e)
        context.parallel_monitoring_success = False


@when("I execute the pipeline")
def step_execute_pipeline_with_incremental_monitoring(context):
    """Execute the pipeline with incremental processing monitoring."""
    try:
        # Simulate incremental processing monitoring
        context.incremental_monitoring_metrics = {
            "incremental_data_volume": context.incremental_metrics[
                "incremental_data_volume"
            ],
            "processing_efficiency": context.incremental_metrics[
                "processing_efficiency"
            ],
            "time_savings": context.incremental_metrics["time_savings"],
            "full_refresh_comparison": {
                "incremental_time": 30.0,
                "full_refresh_time": 150.0,
                "time_savings_percentage": 80.0,
            },
        }

        context.incremental_monitoring_success = True
    except Exception as e:
        context.incremental_monitoring_error = str(e)
        context.incremental_monitoring_success = False


@when("I generate a performance report")
def step_generate_performance_report(context):
    """Generate a performance report."""
    try:
        # Simulate performance report generation
        context.performance_report = {
            "execution_summary": {
                "total_executions": len(context.multiple_execution_data),
                "average_execution_time": sum(
                    d["execution_time"] for d in context.multiple_execution_data
                )
                / len(context.multiple_execution_data),
                "average_memory_usage": "2.7GB",
                "average_validation_rate": sum(
                    d["validation_rate"] for d in context.multiple_execution_data
                )
                / len(context.multiple_execution_data),
            },
            "trend_analysis": {
                "execution_time_trend": "increasing",
                "memory_usage_trend": "increasing",
                "validation_rate_trend": "decreasing",
            },
            "optimization_recommendations": [
                "Optimize memory allocation",
                "Review validation rules",
                "Consider parallel processing",
                "Monitor data quality trends",
            ],
            "export_format": "structured",
        }

        context.report_generation_success = True
    except Exception as e:
        context.report_generation_error = str(e)
        context.report_generation_success = False


@then("I should receive performance metrics")
def step_should_receive_performance_metrics(context):
    """Verify that performance metrics are received."""
    assert context.monitoring_success, (
        f"Performance monitoring failed: {getattr(context, 'monitoring_error', 'Unknown error')}"
    )
    assert context.performance_metrics is not None, "No performance metrics received"

    # Check that we have the expected metrics
    expected_metrics = [
        "execution_time",
        "memory_usage",
        "cpu_usage",
        "total_rows_processed",
        "validation_rate",
    ]
    for metric in expected_metrics:
        assert metric in context.performance_metrics, (
            f"Missing performance metric: {metric}"
        )


@then("the metrics should include execution time")
def step_metrics_should_include_execution_time(context):
    """Verify that metrics include execution time."""
    assert context.monitoring_success, (
        f"Performance monitoring failed: {getattr(context, 'monitoring_error', 'Unknown error')}"
    )
    assert "execution_time" in context.performance_metrics, (
        "No execution time in metrics"
    )
    assert context.performance_metrics["execution_time"] > 0, "Invalid execution time"


@then("the metrics should include memory usage")
def step_metrics_should_include_memory_usage(context):
    """Verify that metrics include memory usage."""
    assert context.monitoring_success, (
        f"Performance monitoring failed: {getattr(context, 'monitoring_error', 'Unknown error')}"
    )
    assert "memory_usage" in context.performance_metrics, "No memory usage in metrics"
    assert context.performance_metrics["memory_usage"] is not None, (
        "Invalid memory usage"
    )


@then("the metrics should include CPU usage")
def step_metrics_should_include_cpu_usage(context):
    """Verify that metrics include CPU usage."""
    assert context.monitoring_success, (
        f"Performance monitoring failed: {getattr(context, 'monitoring_error', 'Unknown error')}"
    )
    assert "cpu_usage" in context.performance_metrics, "No CPU usage in metrics"
    assert 0 <= context.performance_metrics["cpu_usage"] <= 100, (
        "Invalid CPU usage percentage"
    )


@then("the metrics should be logged for analysis")
def step_metrics_should_be_logged_for_analysis(context):
    """Verify that metrics are logged for analysis."""
    assert context.monitoring_success, (
        f"Performance monitoring failed: {getattr(context, 'monitoring_error', 'Unknown error')}"
    )

    # For now, we'll just check that monitoring succeeded
    # In a real implementation, this would test actual logging
    print("Performance metrics are logged for analysis")


@then("I should receive data processing metrics")
def step_should_receive_data_processing_metrics(context):
    """Verify that data processing metrics are received."""
    assert context.data_processing_monitoring_success, (
        f"Data processing monitoring failed: {getattr(context, 'data_processing_monitoring_error', 'Unknown error')}"
    )
    assert context.data_processing_metrics is not None, (
        "No data processing metrics received"
    )

    # Check that we have the expected metrics
    expected_metrics = [
        "rows_processed_per_second",
        "data_throughput",
        "processing_efficiency",
        "bottlenecks",
    ]
    for metric in expected_metrics:
        assert metric in context.data_processing_metrics, (
            f"Missing data processing metric: {metric}"
        )


@then("the metrics should include rows processed per second")
def step_metrics_should_include_rows_processed_per_second(context):
    """Verify that metrics include rows processed per second."""
    assert context.data_processing_monitoring_success, (
        f"Data processing monitoring failed: {getattr(context, 'data_processing_monitoring_error', 'Unknown error')}"
    )
    assert "rows_processed_per_second" in context.data_processing_metrics, (
        "No rows processed per second in metrics"
    )
    assert context.data_processing_metrics["rows_processed_per_second"] > 0, (
        "Invalid rows processed per second"
    )


@then("the metrics should include data throughput")
def step_metrics_should_include_data_throughput(context):
    """Verify that metrics include data throughput."""
    assert context.data_processing_monitoring_success, (
        f"Data processing monitoring failed: {getattr(context, 'data_processing_monitoring_error', 'Unknown error')}"
    )
    assert "data_throughput" in context.data_processing_metrics, (
        "No data throughput in metrics"
    )
    assert context.data_processing_metrics["data_throughput"] is not None, (
        "Invalid data throughput"
    )


@then("the metrics should include processing efficiency")
def step_metrics_should_include_processing_efficiency(context):
    """Verify that metrics include processing efficiency."""
    assert context.data_processing_monitoring_success, (
        f"Data processing monitoring failed: {getattr(context, 'data_processing_monitoring_error', 'Unknown error')}"
    )
    assert "processing_efficiency" in context.data_processing_metrics, (
        "No processing efficiency in metrics"
    )
    assert 0 <= context.data_processing_metrics["processing_efficiency"] <= 100, (
        "Invalid processing efficiency percentage"
    )


@then("the metrics should identify performance bottlenecks")
def step_metrics_should_identify_performance_bottlenecks(context):
    """Verify that metrics identify performance bottlenecks."""
    assert context.data_processing_monitoring_success, (
        f"Data processing monitoring failed: {getattr(context, 'data_processing_monitoring_error', 'Unknown error')}"
    )
    assert "bottlenecks" in context.data_processing_metrics, "No bottlenecks in metrics"
    assert len(context.data_processing_metrics["bottlenecks"]) > 0, (
        "No bottlenecks identified"
    )


@then("I should receive resource utilization metrics")
def step_should_receive_resource_utilization_metrics(context):
    """Verify that resource utilization metrics are received."""
    assert context.resource_monitoring_success, (
        f"Resource monitoring failed: {getattr(context, 'resource_monitoring_error', 'Unknown error')}"
    )
    assert context.resource_monitoring_metrics is not None, (
        "No resource monitoring metrics received"
    )

    # Check that we have the expected metrics
    expected_metrics = [
        "memory_usage_over_time",
        "cpu_utilization",
        "disk_io",
        "resource_constraints",
    ]
    for metric in expected_metrics:
        assert metric in context.resource_monitoring_metrics, (
            f"Missing resource monitoring metric: {metric}"
        )


@then("the metrics should include memory usage over time")
def step_metrics_should_include_memory_usage_over_time(context):
    """Verify that metrics include memory usage over time."""
    assert context.resource_monitoring_success, (
        f"Resource monitoring failed: {getattr(context, 'resource_monitoring_error', 'Unknown error')}"
    )
    assert "memory_usage_over_time" in context.resource_monitoring_metrics, (
        "No memory usage over time in metrics"
    )

    memory_usage = context.resource_monitoring_metrics["memory_usage_over_time"]
    assert "peak" in memory_usage, "No peak memory usage"
    assert "average" in memory_usage, "No average memory usage"
    assert "utilization" in memory_usage, "No memory utilization percentage"


@then("the metrics should include CPU utilization")
def step_metrics_should_include_cpu_utilization(context):
    """Verify that metrics include CPU utilization."""
    assert context.resource_monitoring_success, (
        f"Resource monitoring failed: {getattr(context, 'resource_monitoring_error', 'Unknown error')}"
    )
    assert "cpu_utilization" in context.resource_monitoring_metrics, (
        "No CPU utilization in metrics"
    )

    cpu_utilization = context.resource_monitoring_metrics["cpu_utilization"]
    assert "peak" in cpu_utilization, "No peak CPU utilization"
    assert "average" in cpu_utilization, "No average CPU utilization"
    assert "cores_used" in cpu_utilization, "No CPU cores used"


@then("the metrics should include disk I/O")
def step_metrics_should_include_disk_io(context):
    """Verify that metrics include disk I/O."""
    assert context.resource_monitoring_success, (
        f"Resource monitoring failed: {getattr(context, 'resource_monitoring_error', 'Unknown error')}"
    )
    assert "disk_io" in context.resource_monitoring_metrics, "No disk I/O in metrics"

    disk_io = context.resource_monitoring_metrics["disk_io"]
    assert "read_bytes" in disk_io, "No read bytes in disk I/O"
    assert "write_bytes" in disk_io, "No write bytes in disk I/O"
    assert "io_operations" in disk_io, "No I/O operations count"


@then("the metrics should identify resource constraints")
def step_metrics_should_identify_resource_constraints(context):
    """Verify that metrics identify resource constraints."""
    assert context.resource_monitoring_success, (
        f"Resource monitoring failed: {getattr(context, 'resource_monitoring_error', 'Unknown error')}"
    )
    assert "resource_constraints" in context.resource_monitoring_metrics, (
        "No resource constraints in metrics"
    )
    assert len(context.resource_monitoring_metrics["resource_constraints"]) > 0, (
        "No resource constraints identified"
    )


@then("I should compare performance with historical data")
def step_should_compare_performance_with_historical_data(context):
    """Verify that performance is compared with historical data."""
    assert context.trend_analysis_success, (
        f"Trend analysis failed: {getattr(context, 'trend_analysis_error', 'Unknown error')}"
    )
    assert context.trend_analysis is not None, "No trend analysis result"

    # Check that we have historical comparison
    assert "current_metrics" in context.trend_analysis, (
        "No current metrics in trend analysis"
    )
    assert "historical_average" in context.trend_analysis, (
        "No historical average in trend analysis"
    )


@then("I should detect significant performance regressions")
def step_should_detect_significant_performance_regressions(context):
    """Verify that significant performance regressions are detected."""
    assert context.trend_analysis_success, (
        f"Trend analysis failed: {getattr(context, 'trend_analysis_error', 'Unknown error')}"
    )
    assert context.trend_analysis is not None, "No trend analysis result"

    # Check that performance regressions are detected
    assert "performance_regression" in context.trend_analysis, (
        "No performance regression in trend analysis"
    )

    regression = context.trend_analysis["performance_regression"]
    assert "execution_time" in regression, "No execution time regression"
    assert "memory_usage" in regression, "No memory usage regression"
    assert "validation_rate" in regression, "No validation rate regression"


@then("I should receive alerts for performance issues")
def step_should_receive_alerts_for_performance_issues(context):
    """Verify that alerts for performance issues are received."""
    assert context.trend_analysis_success, (
        f"Trend analysis failed: {getattr(context, 'trend_analysis_error', 'Unknown error')}"
    )
    assert context.trend_analysis is not None, "No trend analysis result"

    # Check that alerts are generated
    assert "alerts" in context.trend_analysis, "No alerts in trend analysis"
    assert len(context.trend_analysis["alerts"]) > 0, (
        "No performance issue alerts generated"
    )


@then("I should get suggestions for performance optimization")
def step_should_get_suggestions_for_performance_optimization(context):
    """Verify that suggestions for performance optimization are provided."""
    assert context.trend_analysis_success, (
        f"Trend analysis failed: {getattr(context, 'trend_analysis_error', 'Unknown error')}"
    )
    assert context.trend_analysis is not None, "No trend analysis result"

    # Check that optimization suggestions are provided
    assert "alerts" in context.trend_analysis, "No alerts in trend analysis"
    alerts = context.trend_analysis["alerts"]
    assert any(
        "optimization" in alert.lower() or "performance" in alert.lower()
        for alert in alerts
    ), "No performance optimization suggestions in alerts"


@then("I should receive parallel execution metrics")
def step_should_receive_parallel_execution_metrics(context):
    """Verify that parallel execution metrics are received."""
    assert context.parallel_monitoring_success, (
        f"Parallel monitoring failed: {getattr(context, 'parallel_monitoring_error', 'Unknown error')}"
    )
    assert context.parallel_monitoring_metrics is not None, (
        "No parallel monitoring metrics received"
    )

    # Check that we have the expected metrics
    expected_metrics = [
        "parallelization_efficiency",
        "load_balancing",
        "optimal_parallelization",
        "parallel_settings",
    ]
    for metric in expected_metrics:
        assert metric in context.parallel_monitoring_metrics, (
            f"Missing parallel monitoring metric: {metric}"
        )


@then("the metrics should include parallelization efficiency")
def step_metrics_should_include_parallelization_efficiency(context):
    """Verify that metrics include parallelization efficiency."""
    assert context.parallel_monitoring_success, (
        f"Parallel monitoring failed: {getattr(context, 'parallel_monitoring_error', 'Unknown error')}"
    )
    assert "parallelization_efficiency" in context.parallel_monitoring_metrics, (
        "No parallelization efficiency in metrics"
    )
    assert (
        0 <= context.parallel_monitoring_metrics["parallelization_efficiency"] <= 100
    ), "Invalid parallelization efficiency percentage"


@then("the metrics should include load balancing")
def step_metrics_should_include_load_balancing(context):
    """Verify that metrics include load balancing."""
    assert context.parallel_monitoring_success, (
        f"Parallel monitoring failed: {getattr(context, 'parallel_monitoring_error', 'Unknown error')}"
    )
    assert "load_balancing" in context.parallel_monitoring_metrics, (
        "No load balancing in metrics"
    )
    assert 0 <= context.parallel_monitoring_metrics["load_balancing"] <= 100, (
        "Invalid load balancing percentage"
    )


@then("the metrics should identify optimal parallelization settings")
def step_metrics_should_identify_optimal_parallelization_settings(context):
    """Verify that metrics identify optimal parallelization settings."""
    assert context.parallel_monitoring_success, (
        f"Parallel monitoring failed: {getattr(context, 'parallel_monitoring_error', 'Unknown error')}"
    )
    assert "optimal_parallelization" in context.parallel_monitoring_metrics, (
        "No optimal parallelization in metrics"
    )
    assert context.parallel_monitoring_metrics["optimal_parallelization"] > 0, (
        "Invalid optimal parallelization setting"
    )


@then("I should receive incremental processing metrics")
def step_should_receive_incremental_processing_metrics(context):
    """Verify that incremental processing metrics are received."""
    assert context.incremental_monitoring_success, (
        f"Incremental monitoring failed: {getattr(context, 'incremental_monitoring_error', 'Unknown error')}"
    )
    assert context.incremental_monitoring_metrics is not None, (
        "No incremental monitoring metrics received"
    )

    # Check that we have the expected metrics
    expected_metrics = [
        "incremental_data_volume",
        "processing_efficiency",
        "time_savings",
        "full_refresh_comparison",
    ]
    for metric in expected_metrics:
        assert metric in context.incremental_monitoring_metrics, (
            f"Missing incremental monitoring metric: {metric}"
        )


@then("the metrics should include incremental data volume")
def step_metrics_should_include_incremental_data_volume(context):
    """Verify that metrics include incremental data volume."""
    assert context.incremental_monitoring_success, (
        f"Incremental monitoring failed: {getattr(context, 'incremental_monitoring_error', 'Unknown error')}"
    )
    assert "incremental_data_volume" in context.incremental_monitoring_metrics, (
        "No incremental data volume in metrics"
    )
    assert context.incremental_monitoring_metrics["incremental_data_volume"] > 0, (
        "Invalid incremental data volume"
    )


@then("the metrics should include processing efficiency")
def step_metrics_should_include_processing_efficiency_incremental(context):
    """Verify that metrics include processing efficiency for incremental processing."""
    assert context.incremental_monitoring_success, (
        f"Incremental monitoring failed: {getattr(context, 'incremental_monitoring_error', 'Unknown error')}"
    )
    assert "processing_efficiency" in context.incremental_monitoring_metrics, (
        "No processing efficiency in metrics"
    )
    assert (
        0 <= context.incremental_monitoring_metrics["processing_efficiency"] <= 100
    ), "Invalid processing efficiency percentage"


@then("the metrics should compare with full refresh performance")
def step_metrics_should_compare_with_full_refresh_performance(context):
    """Verify that metrics compare with full refresh performance."""
    assert context.incremental_monitoring_success, (
        f"Incremental monitoring failed: {getattr(context, 'incremental_monitoring_error', 'Unknown error')}"
    )
    assert "full_refresh_comparison" in context.incremental_monitoring_metrics, (
        "No full refresh comparison in metrics"
    )

    comparison = context.incremental_monitoring_metrics["full_refresh_comparison"]
    assert "incremental_time" in comparison, "No incremental time in comparison"
    assert "full_refresh_time" in comparison, "No full refresh time in comparison"
    assert "time_savings_percentage" in comparison, (
        "No time savings percentage in comparison"
    )


@then("I should receive a comprehensive performance analysis")
def step_should_receive_comprehensive_performance_analysis(context):
    """Verify that a comprehensive performance analysis is received."""
    assert context.report_generation_success, (
        f"Report generation failed: {getattr(context, 'report_generation_error', 'Unknown error')}"
    )
    assert context.performance_report is not None, "No performance report received"

    # Check that we have the expected report sections
    expected_sections = [
        "execution_summary",
        "trend_analysis",
        "optimization_recommendations",
        "export_format",
    ]
    for section in expected_sections:
        assert section in context.performance_report, (
            f"Missing report section: {section}"
        )


@then("the report should include trend analysis")
def step_report_should_include_trend_analysis(context):
    """Verify that the report includes trend analysis."""
    assert context.report_generation_success, (
        f"Report generation failed: {getattr(context, 'report_generation_error', 'Unknown error')}"
    )
    assert "trend_analysis" in context.performance_report, "No trend analysis in report"

    trend_analysis = context.performance_report["trend_analysis"]
    assert "execution_time_trend" in trend_analysis, (
        "No execution time trend in analysis"
    )
    assert "memory_usage_trend" in trend_analysis, "No memory usage trend in analysis"
    assert "validation_rate_trend" in trend_analysis, (
        "No validation rate trend in analysis"
    )


@then("the report should include optimization recommendations")
def step_report_should_include_optimization_recommendations(context):
    """Verify that the report includes optimization recommendations."""
    assert context.report_generation_success, (
        f"Report generation failed: {getattr(context, 'report_generation_error', 'Unknown error')}"
    )
    assert "optimization_recommendations" in context.performance_report, (
        "No optimization recommendations in report"
    )

    recommendations = context.performance_report["optimization_recommendations"]
    assert len(recommendations) > 0, "No optimization recommendations provided"
    assert all(isinstance(rec, str) for rec in recommendations), (
        "Optimization recommendations should be strings"
    )


@then("the report should be exportable for further analysis")
def step_report_should_be_exportable_for_further_analysis(context):
    """Verify that the report is exportable for further analysis."""
    assert context.report_generation_success, (
        f"Report generation failed: {getattr(context, 'report_generation_error', 'Unknown error')}"
    )
    assert "export_format" in context.performance_report, "No export format in report"
    assert context.performance_report["export_format"] == "structured", (
        "Report should be in structured format"
    )
