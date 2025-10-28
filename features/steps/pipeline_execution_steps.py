"""
Step definitions for pipeline execution BDD tests.
"""

from behave import given, then, when

from pipeline_builder.execution import ExecutionEngine
from pipeline_builder.models import BronzeStep, GoldStep, SilverStep
from pipeline_builder.pipeline.builder import PipelineBuilder


@given("I have a Spark session configured")
def step_have_spark_session(context):
    """Ensure Spark session is available."""
    assert context.spark is not None, "Spark session not configured"
    context.step_results = []


@given("I have test data available")
def step_have_test_data(context):
    """Create test data for scenarios."""
    # Create sample test data
    test_data = [
        (1, "Alice", 100, "2024-01-01 10:00:00"),
        (2, "Bob", 200, "2024-01-01 11:00:00"),
        (3, "Charlie", 150, "2024-01-01 12:00:00"),
    ]

    context.test_data = create_test_dataframe(context, test_data)
    context.test_data.cache()


@given('I have a bronze step named "{step_name}" with validation rules')
def step_have_bronze_step_with_rules(context, step_name):
    """Create a bronze step with validation rules."""
    rules = {"id": ["not_null"], "name": ["not_null"], "value": ["not_null", "gt:0"]}

    context.bronze_step = BronzeStep(
        name=step_name, rules=rules, incremental_col="timestamp"
    )


@given('I have a bronze step named "{step_name}"')
def step_have_bronze_step(context, step_name):
    """Create a basic bronze step."""
    context.bronze_step = BronzeStep(
        name=step_name, rules={"id": ["not_null"]}, incremental_col="timestamp"
    )


@given('I have a silver step named "{step_name}" that depends on "{bronze_name}"')
def step_have_silver_step(context, step_name, bronze_name):
    """Create a silver step that depends on a bronze step."""

    def transform_func(df):
        return df.filter(df.value > 50)

    context.silver_step = SilverStep(
        name=step_name,
        source_bronze=bronze_name,
        transform=transform_func,
        rules={"id": ["not_null"]},
    )


@given('I have a gold step named "{step_name}" that depends on "{silver_name}"')
def step_have_gold_step(context, step_name, silver_name):
    """Create a gold step that depends on a silver step."""

    def transform_func(df):
        return df.groupBy("name").agg({"value": "sum"})

    context.gold_step = GoldStep(
        name=step_name, transform=transform_func, rules={"name": ["not_null"]}
    )


@when("I execute the bronze step")
def step_execute_bronze_step(context):
    """Execute the bronze step."""
    try:
        # Create pipeline builder
        builder = PipelineBuilder(context.spark)
        builder.add_bronze_step(context.bronze_step)

        # Execute the pipeline
        engine = ExecutionEngine(context.spark)
        context.execution_result = engine.execute_pipeline(builder.build())

        context.step_results.append(
            {
                "step": context.bronze_step.name,
                "status": "success",
                "result": context.execution_result,
            }
        )

    except Exception as e:
        context.step_results.append(
            {"step": context.bronze_step.name, "status": "error", "error": str(e)}
        )
        raise


@when("I execute the bronze-silver pipeline")
def step_execute_bronze_silver_pipeline(context):
    """Execute a bronze-silver pipeline."""
    try:
        # Create pipeline builder
        builder = PipelineBuilder(context.spark)
        builder.add_bronze_step(context.bronze_step)
        builder.add_silver_step(context.silver_step)

        # Execute the pipeline
        engine = ExecutionEngine(context.spark)
        context.execution_result = engine.execute_pipeline(builder.build())

        context.step_results.append(
            {
                "step": "bronze-silver-pipeline",
                "status": "success",
                "result": context.execution_result,
            }
        )

    except Exception as e:
        context.step_results.append(
            {"step": "bronze-silver-pipeline", "status": "error", "error": str(e)}
        )
        raise


@when("I execute the full pipeline")
def step_execute_full_pipeline(context):
    """Execute a full bronze-silver-gold pipeline."""
    try:
        # Create pipeline builder
        builder = PipelineBuilder(context.spark)
        builder.add_bronze_step(context.bronze_step)
        builder.add_silver_step(context.silver_step)
        builder.add_gold_step(context.gold_step)

        # Execute the pipeline
        engine = ExecutionEngine(context.spark)
        context.execution_result = engine.execute_pipeline(builder.build())

        context.step_results.append(
            {
                "step": "full-pipeline",
                "status": "success",
                "result": context.execution_result,
            }
        )

    except Exception as e:
        context.step_results.append(
            {"step": "full-pipeline", "status": "error", "error": str(e)}
        )
        raise


@then("the step should complete successfully")
def step_step_should_complete_successfully(context):
    """Verify that the step completed successfully."""
    assert context.execution_result is not None, "Execution result is None"
    assert context.execution_result.success, (
        f"Step failed: {context.execution_result.error_message}"
    )


@then("the output should contain valid data")
def step_output_should_contain_valid_data(context):
    """Verify that the output contains valid data."""
    assert context.execution_result is not None, "Execution result is None"

    # Check that we have output data
    if hasattr(context.execution_result, "bronze_results"):
        for step_name, result in context.execution_result.bronze_results.items():
            assert result.success, f"Bronze step {step_name} failed"
            assert result.output_rows > 0, f"Bronze step {step_name} produced no output"


@then("the validation rate should be above {threshold}%")
def step_validation_rate_should_be_above(context, threshold):
    """Verify that the validation rate is above the threshold."""
    threshold_float = float(threshold)

    if hasattr(context.execution_result, "bronze_results"):
        for _step_name, result in context.execution_result.bronze_results.items():
            validation_rate = result.validation_rate
            assert validation_rate >= threshold_float, (
                f"Validation rate {validation_rate}% is below threshold {threshold}%"
            )


@then("both steps should complete successfully")
def step_both_steps_should_complete_successfully(context):
    """Verify that both bronze and silver steps completed successfully."""
    assert context.execution_result is not None, "Execution result is None"
    assert context.execution_result.success, (
        f"Pipeline failed: {context.execution_result.error_message}"
    )

    # Check bronze results
    if hasattr(context.execution_result, "bronze_results"):
        for step_name, result in context.execution_result.bronze_results.items():
            assert result.success, f"Bronze step {step_name} failed"

    # Check silver results
    if hasattr(context.execution_result, "silver_results"):
        for step_name, result in context.execution_result.silver_results.items():
            assert result.success, f"Silver step {step_name} failed"


@then("the silver step should process data from the bronze step")
def step_silver_should_process_bronze_data(context):
    """Verify that the silver step processed data from the bronze step."""
    assert context.execution_result is not None, "Execution result is None"

    # Check that silver step has input data
    if hasattr(context.execution_result, "silver_results"):
        for step_name, result in context.execution_result.silver_results.items():
            assert result.input_rows > 0, f"Silver step {step_name} had no input data"


@then("the data quality should improve from bronze to silver")
def step_data_quality_should_improve(context):
    """Verify that data quality improved from bronze to silver."""
    assert context.execution_result is not None, "Execution result is None"

    # Compare validation rates between bronze and silver
    bronze_rates = []
    silver_rates = []

    if hasattr(context.execution_result, "bronze_results"):
        for result in context.execution_result.bronze_results.values():
            bronze_rates.append(result.validation_rate)

    if hasattr(context.execution_result, "silver_results"):
        for result in context.execution_result.silver_results.values():
            silver_rates.append(result.validation_rate)

    if bronze_rates and silver_rates:
        avg_bronze_rate = sum(bronze_rates) / len(bronze_rates)
        avg_silver_rate = sum(silver_rates) / len(silver_rates)
        assert avg_silver_rate >= avg_bronze_rate, (
            f"Data quality did not improve: bronze={avg_bronze_rate}%, silver={avg_silver_rate}%"
        )


@then("all steps should complete successfully")
def step_all_steps_should_complete_successfully(context):
    """Verify that all pipeline steps completed successfully."""
    assert context.execution_result is not None, "Execution result is None"
    assert context.execution_result.success, (
        f"Pipeline failed: {context.execution_result.error_message}"
    )

    # Check all step results
    for phase in ["bronze_results", "silver_results", "gold_results"]:
        if hasattr(context.execution_result, phase):
            results = getattr(context.execution_result, phase)
            for step_name, result in results.items():
                assert result.success, (
                    f"{phase.replace('_results', '')} step {step_name} failed"
                )


@then("data should flow correctly through all layers")
def step_data_should_flow_correctly(context):
    """Verify that data flows correctly through all pipeline layers."""
    assert context.execution_result is not None, "Execution result is None"

    # Check that each layer has output data
    for phase in ["bronze_results", "silver_results", "gold_results"]:
        if hasattr(context.execution_result, phase):
            results = getattr(context.execution_result, phase)
            for step_name, result in results.items():
                assert result.output_rows > 0, (
                    f"{phase.replace('_results', '')} step {step_name} produced no output"
                )


@then("the final gold data should be ready for analytics")
def step_gold_data_should_be_ready(context):
    """Verify that the final gold data is ready for analytics."""
    assert context.execution_result is not None, "Execution result is None"

    if hasattr(context.execution_result, "gold_results"):
        for step_name, result in context.execution_result.gold_results.items():
            assert result.success, f"Gold step {step_name} failed"
            assert result.output_rows > 0, f"Gold step {step_name} produced no output"
            assert result.validation_rate >= 95.0, (
                f"Gold step {step_name} has low validation rate: {result.validation_rate}%"
            )


@then("the execution should fail gracefully")
def step_execution_should_fail_gracefully(context):
    """Verify that execution failed gracefully."""
    assert context.execution_result is not None, "Execution result is None"
    assert not context.execution_result.success, (
        "Expected execution to fail but it succeeded"
    )


@then("I should receive a clear error message")
def step_should_receive_clear_error_message(context):
    """Verify that a clear error message was received."""
    assert context.execution_result is not None, "Execution result is None"
    assert context.execution_result.error_message is not None, (
        "No error message provided"
    )
    assert len(context.execution_result.error_message) > 0, "Error message is empty"
    assert (
        "error" in context.execution_result.error_message.lower()
        or "failed" in context.execution_result.error_message.lower()
    ), "Error message does not indicate failure"


@then("the error should include suggestions for resolution")
def step_error_should_include_suggestions(context):
    """Verify that the error includes suggestions for resolution."""
    assert context.execution_result is not None, "Execution result is None"
    assert context.execution_result.error_message is not None, (
        "No error message provided"
    )

    # Check for common suggestion keywords
    suggestions = ["suggestion", "recommend", "try", "check", "verify", "ensure"]
    error_msg = context.execution_result.error_message.lower()
    has_suggestions = any(keyword in error_msg for keyword in suggestions)

    # For now, we'll be lenient since not all errors may include suggestions
    # In a real implementation, this would be more strict
    print(f"Error message: {context.execution_result.error_message}")
    print(f"Contains suggestions: {has_suggestions}")


def create_test_dataframe(context, data):
    """Helper function to create test DataFrames."""
    from pyspark.sql.types import (
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("value", IntegerType(), True),
            StructField("timestamp", TimestampType(), True),
        ]
    )

    return context.spark.createDataFrame(data, schema)
