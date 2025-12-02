#!/usr/bin/env python
"""
Demo showing LogWriter creating and appending to a log table.
This shows how execution results are persisted across multiple pipeline runs.
"""

import sys
import time
from datetime import datetime
from pathlib import Path

from mock_spark import (
    SparkSession,
)

# Add src to path
project_root = Path(__file__).parent.parent
src_dir = project_root / "src"
sys.path.insert(0, str(src_dir))

from pipeline_builder.models.execution import (  # noqa: E402
    ExecutionContext,
    ExecutionMode,
    ExecutionResult,
    StepExecutionResult,
    StepStatus,
    StepType,
)
from pipeline_builder.writer import LogWriter  # noqa: E402

print("\n" + "=" * 80)
print("DEMO: LOGWRITER - PERSISTING PIPELINE EXECUTION LOGS")
print("=" * 80)
print("\nThis demo shows how LogWriter creates and maintains execution history.")
print("=" * 80 + "\n")

# Create mock Spark session
builder = SparkSession.builder
if builder is not None:
    spark = builder.getOrCreate()
else:
    raise RuntimeError("Failed to create SparkSession builder")

# Create LogWriter
print("📝 Creating LogWriter for 'analytics.pipeline_logs' table...")
writer = LogWriter(spark=spark, schema="analytics", table_name="pipeline_logs")
print("✅ LogWriter initialized\n")

# Simulate execution results from Run 1
print("=" * 80)
print("RUN 1: Initial Pipeline Execution")
print("=" * 80 + "\n")

run1_results = [
    StepExecutionResult(
        step_name="bronze_events",
        step_type=StepType.BRONZE,
        status=StepStatus.COMPLETED,
        start_time=datetime.now(),
        end_time=datetime.now(),
        duration=0.51,
        rows_processed=1000,
        output_table=None,
    ),
    StepExecutionResult(
        step_name="bronze_profiles",
        step_type=StepType.BRONZE,
        status=StepStatus.COMPLETED,
        start_time=datetime.now(),
        end_time=datetime.now(),
        duration=0.50,
        rows_processed=500,
        output_table=None,
    ),
    StepExecutionResult(
        step_name="silver_purchases",
        step_type=StepType.SILVER,
        status=StepStatus.COMPLETED,
        start_time=datetime.now(),
        end_time=datetime.now(),
        duration=0.81,
        rows_processed=350,
        output_table="analytics.silver_purchases",
    ),
    StepExecutionResult(
        step_name="silver_customers",
        step_type=StepType.SILVER,
        status=StepStatus.COMPLETED,
        start_time=datetime.now(),
        end_time=datetime.now(),
        duration=0.80,
        rows_processed=500,
        output_table="analytics.silver_customers",
    ),
    StepExecutionResult(
        step_name="gold_customer_summary",
        step_type=StepType.GOLD,
        status=StepStatus.COMPLETED,
        start_time=datetime.now(),
        end_time=datetime.now(),
        duration=0.61,
        rows_processed=150,
        output_table="analytics.gold_customer_summary",
    ),
]

print("📊 Pipeline executed successfully!")
print("   - 2 Bronze steps completed")
print("   - 2 Silver steps completed")
print("   - 1 Gold step completed")
print("   - Total duration: 3.23s")
print("   - Total rows processed: 2,500")

print("\n💾 Writing Run 1 results to log table...")
try:
    # Create ExecutionContext
    context = ExecutionContext(
        mode=ExecutionMode.INITIAL,
        start_time=datetime.now(),
        end_time=datetime.now(),
        duration_secs=3.23,
        run_id="run_001",
        execution_id="run_001",
        pipeline_id="customer_pipeline_v1",
        schema="analytics",
        config={"pipeline_name": "customer_pipeline_v1"},
    )

    # Create ExecutionResult from context and step results
    run1_execution = ExecutionResult.from_context_and_results(context, run1_results)

    writer.write_execution_result(
        execution_result=run1_execution,
        run_id="run_001",
        run_mode="initial",
        metadata={"pipeline_name": "customer_pipeline_v1"},
    )
    print("✅ Run 1 logged successfully!\n")
except Exception as e:
    print(f"⚠️  LogWriter simulation: {e}")
    print("   (In real usage, this would create the table)\n")

# Simulate execution results from Run 2 (with a failure)
print("=" * 80)
print("RUN 2: Second Pipeline Execution (with one failure)")
print("=" * 80 + "\n")

time.sleep(0.5)  # Small delay to show time difference

run2_results = [
    StepExecutionResult(
        step_name="bronze_events",
        step_type=StepType.BRONZE,
        status=StepStatus.COMPLETED,
        start_time=datetime.now(),
        end_time=datetime.now(),
        duration=0.48,
        rows_processed=1200,
        output_table=None,
    ),
    StepExecutionResult(
        step_name="bronze_profiles",
        step_type=StepType.BRONZE,
        status=StepStatus.COMPLETED,
        start_time=datetime.now(),
        end_time=datetime.now(),
        duration=0.52,
        rows_processed=520,
        output_table=None,
    ),
    StepExecutionResult(
        step_name="silver_purchases",
        step_type=StepType.SILVER,
        status=StepStatus.COMPLETED,
        start_time=datetime.now(),
        end_time=datetime.now(),
        duration=0.85,
        rows_processed=380,
        output_table="analytics.silver_purchases",
    ),
    StepExecutionResult(
        step_name="silver_customers",
        step_type=StepType.SILVER,
        status=StepStatus.FAILED,
        start_time=datetime.now(),
        end_time=datetime.now(),
        duration=0.12,
        rows_processed=0,
        output_table=None,
        error="Validation failed: 15% invalid rows (threshold: 95%)",
    ),
    StepExecutionResult(
        step_name="gold_customer_summary",
        step_type=StepType.GOLD,
        status=StepStatus.SKIPPED,
        start_time=datetime.now(),
        end_time=datetime.now(),
        duration=0.0,
        rows_processed=0,
        output_table=None,
    ),
]

print("⚠️  Pipeline completed with failures!")
print("   - 2 Bronze steps completed")
print("   - 1 Silver step completed")
print("   - 1 Silver step FAILED (validation error)")
print("   - 1 Gold step SKIPPED (dependency failed)")
print("   - Total duration: 1.97s")

print("\n💾 Appending Run 2 results to log table...")
try:
    # Create ExecutionContext for run 2
    context2 = ExecutionContext(
        mode=ExecutionMode.INITIAL,
        start_time=datetime.now(),
        end_time=datetime.now(),
        duration_secs=1.97,
        run_id="run_002",
        execution_id="run_002",
        pipeline_id="customer_pipeline_v1",
        schema="analytics",
        config={"pipeline_name": "customer_pipeline_v1"},
    )

    # Create ExecutionResult from context and step results
    run2_execution = ExecutionResult.from_context_and_results(context2, run2_results)

    writer.write_execution_result(
        execution_result=run2_execution,
        run_id="run_002",
        run_mode="initial",
        metadata={"pipeline_name": "customer_pipeline_v1"},
    )
    print("✅ Run 2 logged successfully!\n")
except Exception as e:
    print(f"⚠️  LogWriter simulation: {e}")
    print("   (In real usage, this would append to the table)\n")

# Show what the log table data would look like
print("=" * 80)
print("📋 LOG TABLE CONTENTS: analytics.pipeline_logs")
print("=" * 80 + "\n")

print("Sample rows from the log table:\n")
print(
    "| execution_id | pipeline_id          | step_name              | step_type | status    | duration | rows_processed | timestamp           |"
)
print(
    "|--------------|----------------------|------------------------|-----------|-----------|----------|----------------|---------------------|"
)
print(
    "| run_001      | customer_pipeline_v1 | bronze_events          | bronze    | completed | 0.51s    | 1,000          | 2025-10-17 13:08:09 |"
)
print(
    "| run_001      | customer_pipeline_v1 | bronze_profiles        | bronze    | completed | 0.50s    | 500            | 2025-10-17 13:08:09 |"
)
print(
    "| run_001      | customer_pipeline_v1 | silver_purchases       | silver    | completed | 0.81s    | 350            | 2025-10-17 13:08:10 |"
)
print(
    "| run_001      | customer_pipeline_v1 | silver_customers       | silver    | completed | 0.80s    | 500            | 2025-10-17 13:08:11 |"
)
print(
    "| run_001      | customer_pipeline_v1 | gold_customer_summary  | gold      | completed | 0.61s    | 150            | 2025-10-17 13:08:12 |"
)
print(
    "| run_002      | customer_pipeline_v1 | bronze_events          | bronze    | completed | 0.48s    | 1,200          | 2025-10-17 13:08:45 |"
)
print(
    "| run_002      | customer_pipeline_v1 | bronze_profiles        | bronze    | completed | 0.52s    | 520            | 2025-10-17 13:08:45 |"
)
print(
    "| run_002      | customer_pipeline_v1 | silver_purchases       | silver    | completed | 0.85s    | 380            | 2025-10-17 13:08:46 |"
)
print(
    "| run_002      | customer_pipeline_v1 | silver_customers       | silver    | failed    | 0.12s    | 0              | 2025-10-17 13:08:46 |"
)
print(
    "| run_002      | customer_pipeline_v1 | gold_customer_summary  | gold      | skipped   | 0.00s    | 0              | 2025-10-17 13:08:46 |"
)

print("\n" + "=" * 80)
print("📊 ANALYTICS QUERIES YOU CAN RUN")
print("=" * 80 + "\n")

print("1. Track pipeline success rate over time:")
print("   ```sql")
print("   SELECT execution_id, ")
print("          COUNT(*) as total_steps,")
print("          SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,")
print("          SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed")
print("   FROM analytics.pipeline_logs")
print("   GROUP BY execution_id;")
print("   ```\n")

print("2. Find slowest steps across all runs:")
print("   ```sql")
print("   SELECT step_name, step_type, ")
print("          AVG(duration) as avg_duration,")
print("          MAX(duration) as max_duration")
print("   FROM analytics.pipeline_logs")
print("   WHERE status = 'completed'")
print("   GROUP BY step_name, step_type")
print("   ORDER BY avg_duration DESC;")
print("   ```\n")

print("3. Identify steps with failures:")
print("   ```sql")
print("   SELECT step_name, execution_id, error_message, timestamp")
print("   FROM analytics.pipeline_logs")
print("   WHERE status = 'failed'")
print("   ORDER BY timestamp DESC;")
print("   ```\n")

print("4. Monitor data volume trends:")
print("   ```sql")
print("   SELECT execution_id, timestamp,")
print("          SUM(rows_processed) as total_rows")
print("   FROM analytics.pipeline_logs")
print("   GROUP BY execution_id, timestamp")
print("   ORDER BY timestamp;")
print("   ```\n")

print("=" * 80)
print("KEY BENEFITS OF LOGWRITER")
print("=" * 80)
print("✅ Automatic table creation on first write")
print("✅ Appends to existing table on subsequent writes")
print("✅ Captures detailed step-level metrics")
print("✅ Tracks execution history over time")
print("✅ Enables SQL-based analytics and monitoring")
print("✅ Helps identify performance bottlenecks")
print("✅ Tracks failure patterns and error messages")
print("✅ Supports debugging and troubleshooting")
print("=" * 80 + "\n")

print("💡 TIP: Use LogWriter in your production pipelines to maintain")
print("   execution history for monitoring, alerting, and analysis!")
print()
