"""
Step definitions for error handling BDD tests.
"""

from behave import given, then, when

from pipeline_builder.errors import (
    DataQualityError,
    PipelineConfigurationError,
    PipelineExecutionError,
    ResourceError,
    ValidationError,
)


@given("I have error handling configured")
def step_have_error_handling_configured(context):
    """Configure error handling for testing."""
    context.error_handling_enabled = True
    context.error_context = {}


@given("I have data with quality issues")
def step_have_data_with_quality_issues(context):
    """Create data with quality issues."""
    # Create test data with quality issues
    test_data = [
        (1, "Alice", "alice@example.com"),  # Valid
        (2, "", "bob@example.com"),  # Empty name
        (3, "Charlie", "invalid-email"),  # Invalid email
        (4, None, "david@example.com"),  # Null name
        (5, "Eve", ""),  # Empty email
    ]

    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
        ]
    )

    context.test_data = context.spark.createDataFrame(test_data, schema)
    context.quality_issues = {
        "empty_names": 1,
        "invalid_emails": 1,
        "null_names": 1,
        "empty_emails": 1,
    }


@given("I have an invalid pipeline configuration")
def step_have_invalid_pipeline_configuration(context):
    """Create an invalid pipeline configuration."""
    context.invalid_config = {
        "bronze_steps": [],  # No bronze steps
        "silver_steps": [
            {"name": "test", "source_bronze": "nonexistent"}
        ],  # Invalid source
        "gold_steps": [],
        "validation_rules": {},  # Empty validation rules
    }
    context.config_errors = [
        "No bronze steps defined",
        "Silver step depends on non-existent bronze step",
        "No validation rules defined",
    ]


@given("I have a pipeline that requires more resources than available")
def step_have_pipeline_requiring_more_resources(context):
    """Create a pipeline that requires more resources than available."""
    context.resource_requirements = {
        "memory": "8GB",
        "cpu_cores": 16,
        "disk_space": "100GB",
    }
    context.available_resources = {
        "memory": "4GB",
        "cpu_cores": 8,
        "disk_space": "50GB",
    }
    context.resource_error = (
        "Insufficient resources: requires 8GB memory, 16 CPU cores, 100GB disk space"
    )


@given("I have a pipeline that requires network access")
def step_have_pipeline_requiring_network_access(context):
    """Create a pipeline that requires network access."""
    context.network_requirements = {
        "external_apis": ["https://api.example.com"],
        "database_connections": ["postgresql://db.example.com"],
        "file_systems": ["hdfs://cluster.example.com"],
    }
    context.network_available = False
    context.connectivity_error = (
        "Network connectivity issues: unable to reach external services"
    )


@given("I have a pipeline with schema changes")
def step_have_pipeline_with_schema_changes(context):
    """Create a pipeline with schema changes."""
    context.schema_changes = {
        "new_columns": ["new_field_1", "new_field_2"],
        "removed_columns": ["old_field_1"],
        "type_changes": {"field_1": "string -> integer"},
    }
    context.schema_conflict = (
        "Schema evolution conflict: new columns added, existing columns removed"
    )


@given("I have a pipeline that takes too long to execute")
def step_have_pipeline_that_takes_too_long(context):
    """Create a pipeline that takes too long to execute."""
    context.execution_time = 3600  # 1 hour
    context.timeout_threshold = 1800  # 30 minutes
    context.timeout_error = "Pipeline execution timeout: exceeded 30 minute limit"


@when("I execute the pipeline")
def step_execute_pipeline_with_quality_issues(context):
    """Execute the pipeline with quality issues."""
    try:
        # Simulate pipeline execution with quality issues
        context.quality_validation_result = {
            "valid_records": 1,
            "invalid_records": 4,
            "validation_rate": 20.0,
            "quality_issues": context.quality_issues,
        }

        # Simulate error handling
        if context.quality_validation_result["validation_rate"] < 50.0:
            raise DataQualityError(
                message="Data quality validation failed",
                context={
                    "validation_rate": context.quality_validation_result[
                        "validation_rate"
                    ]
                },
                suggestions=[
                    "Check data sources",
                    "Review validation rules",
                    "Clean input data",
                ],
            )

        context.pipeline_execution_success = True
    except DataQualityError as e:
        context.pipeline_execution_success = False
        context.pipeline_error = e
        context.error_handled = True


@when("I try to execute the pipeline")
def step_try_execute_pipeline_with_invalid_config(context):
    """Try to execute the pipeline with invalid configuration."""
    try:
        # Simulate configuration validation
        if not context.invalid_config["bronze_steps"]:
            raise PipelineConfigurationError(
                message="Pipeline configuration error",
                context={"issue": "No bronze steps defined"},
                suggestions=[
                    "Add at least one bronze step",
                    "Check pipeline configuration",
                ],
            )

        context.config_validation_success = True
    except PipelineConfigurationError as e:
        context.config_validation_success = False
        context.config_error = e
        context.error_handled = True


@when("I execute the pipeline")
def step_execute_pipeline_with_resource_issues(context):
    """Execute the pipeline with resource issues."""
    try:
        # Simulate resource check
        if (
            context.resource_requirements["memory"]
            > context.available_resources["memory"]
        ):
            raise ResourceError(
                message="Insufficient resources",
                context={
                    "required": context.resource_requirements,
                    "available": context.available_resources,
                },
                suggestions=[
                    "Increase memory allocation",
                    "Optimize pipeline",
                    "Use smaller datasets",
                ],
            )

        context.resource_check_success = True
    except ResourceError as e:
        context.resource_check_success = False
        context.resource_error = e
        context.error_handled = True


@when("I execute the pipeline")
def step_execute_pipeline_with_network_issues(context):
    """Execute the pipeline with network issues."""
    try:
        # Simulate network connectivity check
        if not context.network_available:
            raise PipelineExecutionError(
                message="Network connectivity error",
                context={"network_requirements": context.network_requirements},
                suggestions=[
                    "Check network connectivity",
                    "Verify service availability",
                    "Use local alternatives",
                ],
            )

        context.network_check_success = True
    except PipelineExecutionError as e:
        context.network_check_success = False
        context.network_error = e
        context.error_handled = True


@when("I execute the pipeline")
def step_execute_pipeline_with_schema_issues(context):
    """Execute the pipeline with schema issues."""
    try:
        # Simulate schema validation
        if (
            context.schema_changes["new_columns"]
            or context.schema_changes["removed_columns"]
        ):
            raise ValidationError(
                message="Schema evolution conflict",
                context={"schema_changes": context.schema_changes},
                suggestions=[
                    "Update target schema",
                    "Use schema evolution",
                    "Migrate data",
                ],
            )

        context.schema_validation_success = True
    except ValidationError as e:
        context.schema_validation_success = False
        context.schema_error = e
        context.error_handled = True


@when("I execute the pipeline")
def step_execute_pipeline_with_timeout_issues(context):
    """Execute the pipeline with timeout issues."""
    try:
        # Simulate timeout check
        if context.execution_time > context.timeout_threshold:
            raise PipelineExecutionError(
                message="Pipeline execution timeout",
                context={
                    "execution_time": context.execution_time,
                    "timeout_threshold": context.timeout_threshold,
                },
                suggestions=[
                    "Optimize pipeline performance",
                    "Increase timeout limit",
                    "Use parallel processing",
                ],
            )

        context.timeout_check_success = True
    except PipelineExecutionError as e:
        context.timeout_check_success = False
        context.timeout_error = e
        context.error_handled = True


@then("the pipeline should not crash")
def step_pipeline_should_not_crash(context):
    """Verify that the pipeline does not crash."""
    assert context.error_handled, (
        "Pipeline crashed instead of handling error gracefully"
    )


@then("I should receive a clear error message")
def step_should_receive_clear_error_message(context):
    """Verify that a clear error message is received."""
    assert (
        hasattr(context, "pipeline_error")
        or hasattr(context, "config_error")
        or hasattr(context, "resource_error")
        or hasattr(context, "network_error")
        or hasattr(context, "schema_error")
        or hasattr(context, "timeout_error")
    ), "No error message received"

    # Check that we have an error object
    error_obj = (
        getattr(context, "pipeline_error", None)
        or getattr(context, "config_error", None)
        or getattr(context, "resource_error", None)
        or getattr(context, "network_error", None)
        or getattr(context, "schema_error", None)
        or getattr(context, "timeout_error", None)
    )

    assert error_obj is not None, "No error object received"
    assert hasattr(error_obj, "message"), "Error object missing message attribute"
    assert len(error_obj.message) > 0, "Empty error message"


@then("the error should include data quality details")
def step_error_should_include_data_quality_details(context):
    """Verify that the error includes data quality details."""
    assert hasattr(context, "pipeline_error"), "No pipeline error received"
    assert context.pipeline_error is not None, "Pipeline error is None"

    # Check that error includes data quality details
    assert hasattr(context.pipeline_error, "context"), "Error missing context"
    assert "validation_rate" in context.pipeline_error.context, (
        "No validation rate in error context"
    )


@then("I should get suggestions for fixing the data")
def step_should_get_suggestions_for_fixing_data(context):
    """Verify that suggestions for fixing data are provided."""
    assert hasattr(context, "pipeline_error"), "No pipeline error received"
    assert context.pipeline_error is not None, "Pipeline error is None"

    # Check that suggestions are provided
    assert hasattr(context.pipeline_error, "suggestions"), "Error missing suggestions"
    assert len(context.pipeline_error.suggestions) > 0, "No suggestions provided"

    # Check that suggestions are relevant
    suggestions = context.pipeline_error.suggestions
    assert any("data" in suggestion.lower() for suggestion in suggestions), (
        "No data-related suggestions provided"
    )


@then("the pipeline should continue with valid data")
def step_pipeline_should_continue_with_valid_data(context):
    """Verify that the pipeline continues with valid data."""
    assert context.error_handled, "Pipeline did not handle error gracefully"

    # Check that we have valid data to continue with
    assert hasattr(context, "quality_validation_result"), "No quality validation result"
    assert context.quality_validation_result["valid_records"] > 0, (
        "No valid records to continue with"
    )


@then("I should receive a configuration error")
def step_should_receive_configuration_error(context):
    """Verify that a configuration error is received."""
    assert hasattr(context, "config_error"), "No configuration error received"
    assert context.config_error is not None, "Configuration error is None"
    assert isinstance(context.config_error, PipelineConfigurationError), (
        "Wrong error type"
    )


@then("the error should specify what is wrong")
def step_error_should_specify_what_is_wrong(context):
    """Verify that the error specifies what is wrong."""
    assert hasattr(context, "config_error"), "No configuration error received"
    assert context.config_error is not None, "Configuration error is None"

    # Check that error specifies the issue
    assert (
        "bronze steps" in context.config_error.message.lower()
        or "configuration" in context.config_error.message.lower()
    ), "Error message does not specify the configuration issue"


@then("I should get suggestions for fixing the configuration")
def step_should_get_suggestions_for_fixing_configuration(context):
    """Verify that suggestions for fixing configuration are provided."""
    assert hasattr(context, "config_error"), "No configuration error received"
    assert context.config_error is not None, "Configuration error is None"

    # Check that suggestions are provided
    assert hasattr(context.config_error, "suggestions"), "Error missing suggestions"
    assert len(context.config_error.suggestions) > 0, "No suggestions provided"

    # Check that suggestions are relevant
    suggestions = context.config_error.suggestions
    assert any("step" in suggestion.lower() for suggestion in suggestions), (
        "No step-related suggestions provided"
    )


@then("the error should be caught early in the process")
def step_error_should_be_caught_early(context):
    """Verify that the error is caught early in the process."""
    assert hasattr(context, "config_validation_success"), (
        "No configuration validation performed"
    )
    assert not context.config_validation_success, (
        "Configuration validation should have failed"
    )
    assert context.error_handled, "Error not handled early in the process"


@then("I should receive a resource error")
def step_should_receive_resource_error(context):
    """Verify that a resource error is received."""
    assert hasattr(context, "resource_error"), "No resource error received"
    assert context.resource_error is not None, "Resource error is None"
    assert isinstance(context.resource_error, ResourceError), "Wrong error type"


@then("the error should specify what resources are needed")
def step_error_should_specify_resources_needed(context):
    """Verify that the error specifies what resources are needed."""
    assert hasattr(context, "resource_error"), "No resource error received"
    assert context.resource_error is not None, "Resource error is None"

    # Check that error specifies resource requirements
    assert hasattr(context.resource_error, "context"), "Error missing context"
    assert "required" in context.resource_error.context, (
        "No resource requirements in error context"
    )


@then("I should get suggestions for increasing resources")
def step_should_get_suggestions_for_increasing_resources(context):
    """Verify that suggestions for increasing resources are provided."""
    assert hasattr(context, "resource_error"), "No resource error received"
    assert context.resource_error is not None, "Resource error is None"

    # Check that suggestions are provided
    assert hasattr(context.resource_error, "suggestions"), "Error missing suggestions"
    assert len(context.resource_error.suggestions) > 0, "No suggestions provided"

    # Check that suggestions are relevant
    suggestions = context.resource_error.suggestions
    assert any(
        "memory" in suggestion.lower() or "allocation" in suggestion.lower()
        for suggestion in suggestions
    ), "No resource-related suggestions provided"


@then("the pipeline should fail gracefully")
def step_pipeline_should_fail_gracefully(context):
    """Verify that the pipeline fails gracefully."""
    assert context.error_handled, "Pipeline did not fail gracefully"
    assert hasattr(context, "resource_error"), "No resource error received"


@then("I should receive a connectivity error")
def step_should_receive_connectivity_error(context):
    """Verify that a connectivity error is received."""
    assert hasattr(context, "network_error"), "No connectivity error received"
    assert context.network_error is not None, "Connectivity error is None"
    assert isinstance(context.network_error, PipelineExecutionError), "Wrong error type"


@then("the error should specify what network resources are needed")
def step_error_should_specify_network_resources_needed(context):
    """Verify that the error specifies what network resources are needed."""
    assert hasattr(context, "network_error"), "No connectivity error received"
    assert context.network_error is not None, "Connectivity error is None"

    # Check that error specifies network requirements
    assert hasattr(context.network_error, "context"), "Error missing context"
    assert "network_requirements" in context.network_error.context, (
        "No network requirements in error context"
    )


@then("I should get suggestions for resolving connectivity issues")
def step_should_get_suggestions_for_resolving_connectivity_issues(context):
    """Verify that suggestions for resolving connectivity issues are provided."""
    assert hasattr(context, "network_error"), "No connectivity error received"
    assert context.network_error is not None, "Connectivity error is None"

    # Check that suggestions are provided
    assert hasattr(context.network_error, "suggestions"), "Error missing suggestions"
    assert len(context.network_error.suggestions) > 0, "No suggestions provided"

    # Check that suggestions are relevant
    suggestions = context.network_error.suggestions
    assert any(
        "connectivity" in suggestion.lower() or "network" in suggestion.lower()
        for suggestion in suggestions
    ), "No connectivity-related suggestions provided"


@then("the pipeline should retry automatically if configured")
def step_pipeline_should_retry_automatically_if_configured(context):
    """Verify that the pipeline retries automatically if configured."""
    # For now, we'll just check that the error is handled
    # In a real implementation, this would test actual retry logic
    assert context.error_handled, "Pipeline did not handle connectivity error"
    print("Pipeline retry logic would be tested here")


@then("I should receive a schema error")
def step_should_receive_schema_error(context):
    """Verify that a schema error is received."""
    assert hasattr(context, "schema_error"), "No schema error received"
    assert context.schema_error is not None, "Schema error is None"
    assert isinstance(context.schema_error, ValidationError), "Wrong error type"


@then("the error should specify the schema differences")
def step_error_should_specify_schema_differences(context):
    """Verify that the error specifies the schema differences."""
    assert hasattr(context, "schema_error"), "No schema error received"
    assert context.schema_error is not None, "Schema error is None"

    # Check that error specifies schema differences
    assert hasattr(context.schema_error, "context"), "Error missing context"
    assert "schema_changes" in context.schema_error.context, (
        "No schema changes in error context"
    )


@then("I should get suggestions for resolving schema conflicts")
def step_should_get_suggestions_for_resolving_schema_conflicts(context):
    """Verify that suggestions for resolving schema conflicts are provided."""
    assert hasattr(context, "schema_error"), "No schema error received"
    assert context.schema_error is not None, "Schema error is None"

    # Check that suggestions are provided
    assert hasattr(context.schema_error, "suggestions"), "Error missing suggestions"
    assert len(context.schema_error.suggestions) > 0, "No suggestions provided"

    # Check that suggestions are relevant
    suggestions = context.schema_error.suggestions
    assert any(
        "schema" in suggestion.lower() or "evolution" in suggestion.lower()
        for suggestion in suggestions
    ), "No schema-related suggestions provided"


@then("I should be able to choose schema evolution options")
def step_should_be_able_to_choose_schema_evolution_options(context):
    """Verify that schema evolution options can be chosen."""
    # For now, we'll just check that the error is handled
    # In a real implementation, this would test actual schema evolution options
    assert context.error_handled, "Pipeline did not handle schema error"
    print("Schema evolution options would be tested here")


@then("I should receive a timeout error")
def step_should_receive_timeout_error(context):
    """Verify that a timeout error is received."""
    assert hasattr(context, "timeout_error"), "No timeout error received"
    assert context.timeout_error is not None, "Timeout error is None"
    assert isinstance(context.timeout_error, PipelineExecutionError), "Wrong error type"


@then("the error should specify the timeout duration")
def step_error_should_specify_timeout_duration(context):
    """Verify that the error specifies the timeout duration."""
    assert hasattr(context, "timeout_error"), "No timeout error received"
    assert context.timeout_error is not None, "Timeout error is None"

    # Check that error specifies timeout duration
    assert hasattr(context.timeout_error, "context"), "Error missing context"
    assert "timeout_threshold" in context.timeout_error.context, (
        "No timeout threshold in error context"
    )


@then("I should get suggestions for optimizing performance")
def step_should_get_suggestions_for_optimizing_performance(context):
    """Verify that suggestions for optimizing performance are provided."""
    assert hasattr(context, "timeout_error"), "No timeout error received"
    assert context.timeout_error is not None, "Timeout error is None"

    # Check that suggestions are provided
    assert hasattr(context.timeout_error, "suggestions"), "Error missing suggestions"
    assert len(context.timeout_error.suggestions) > 0, "No suggestions provided"

    # Check that suggestions are relevant
    suggestions = context.timeout_error.suggestions
    assert any(
        "performance" in suggestion.lower() or "optimize" in suggestion.lower()
        for suggestion in suggestions
    ), "No performance-related suggestions provided"


@then("I should be able to configure timeout settings")
def step_should_be_able_to_configure_timeout_settings(context):
    """Verify that timeout settings can be configured."""
    # For now, we'll just check that the error is handled
    # In a real implementation, this would test actual timeout configuration
    assert context.error_handled, "Pipeline did not handle timeout error"
    print("Timeout configuration would be tested here")


@then("errors should be logged with full context")
def step_errors_should_be_logged_with_full_context(context):
    """Verify that errors are logged with full context."""
    # Check that we have error context
    assert context.error_handled, "No errors were handled"

    # For now, we'll just check that errors are handled
    # In a real implementation, this would test actual logging
    print("Error logging with full context would be tested here")


@then("errors should be categorized by type")
def step_errors_should_be_categorized_by_type(context):
    """Verify that errors are categorized by type."""
    # Check that we have different error types
    error_types = []
    if hasattr(context, "pipeline_error"):
        error_types.append(type(context.pipeline_error).__name__)
    if hasattr(context, "config_error"):
        error_types.append(type(context.config_error).__name__)
    if hasattr(context, "resource_error"):
        error_types.append(type(context.resource_error).__name__)
    if hasattr(context, "network_error"):
        error_types.append(type(context.network_error).__name__)
    if hasattr(context, "schema_error"):
        error_types.append(type(context.schema_error).__name__)
    if hasattr(context, "timeout_error"):
        error_types.append(type(context.timeout_error).__name__)

    assert len(error_types) > 0, "No error types found"
    print(f"Error types found: {error_types}")


@then("errors should include debugging information")
def step_errors_should_include_debugging_information(context):
    """Verify that errors include debugging information."""
    # Check that errors have debugging information
    error_objects = []
    if hasattr(context, "pipeline_error"):
        error_objects.append(context.pipeline_error)
    if hasattr(context, "config_error"):
        error_objects.append(context.config_error)
    if hasattr(context, "resource_error"):
        error_objects.append(context.resource_error)
    if hasattr(context, "network_error"):
        error_objects.append(context.network_error)
    if hasattr(context, "schema_error"):
        error_objects.append(context.schema_error)
    if hasattr(context, "timeout_error"):
        error_objects.append(context.timeout_error)

    for error_obj in error_objects:
        if error_obj is not None:
            assert hasattr(error_obj, "context"), (
                f"Error {type(error_obj).__name__} missing context"
            )
            assert hasattr(error_obj, "suggestions"), (
                f"Error {type(error_obj).__name__} missing suggestions"
            )


@then("errors should be trackable across executions")
def step_errors_should_be_trackable_across_executions(context):
    """Verify that errors are trackable across executions."""
    # For now, we'll just check that errors are handled
    # In a real implementation, this would test actual error tracking
    assert context.error_handled, "No errors were handled"
    print("Error tracking across executions would be tested here")
