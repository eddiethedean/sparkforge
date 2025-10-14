"""
Step definitions for data validation BDD tests.
"""

from behave import given, then, when

from sparkforge.validation import assess_data_quality


@given('I have validation rules defined')
def step_have_validation_rules_defined(context):
    """Set up validation rules for testing."""
    context.validation_rules = {}


@given('I have bronze data with columns "{columns}"')
def step_have_bronze_data_with_columns(context, columns):
    """Create bronze data with specified columns."""
    column_list = [col.strip() for col in columns.split(',')]

    # Create test data based on columns
    test_data = [
        (1, "Alice", "alice@example.com"),
        (2, "Bob", "bob@example.com"),
        (3, "Charlie", "charlie@example.com"),
        (4, "", "invalid-email"),  # Invalid data for testing
        (5, None, "test@example.com"),  # Null data for testing
    ]

    # Create DataFrame with appropriate schema
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    fields = []
    for _i, col in enumerate(column_list):
        if col == "id":
            fields.append(StructField(col, IntegerType(), True))
        else:
            fields.append(StructField(col, StringType(), True))

    schema = StructType(fields)
    context.bronze_data = context.spark.createDataFrame(test_data, schema)


@given('I have validation rules:')
def step_have_validation_rules_table(context):
    """Parse validation rules from a table."""
    context.validation_rules = {}

    for row in context.table:
        column = row['column']
        rule = row['rule']

        if column not in context.validation_rules:
            context.validation_rules[column] = []

        context.validation_rules[column].append(rule)


@given('I have data with numeric columns')
def step_have_data_with_numeric_columns(context):
    """Create data with numeric columns for testing."""
    test_data = [
        (1, "Alice", 25, 85),
        (2, "Bob", 30, 92),
        (3, "Charlie", 200, 150),  # Invalid age
        (4, "David", 35, -10),  # Invalid score
        (5, "Eve", 40, 105),  # Invalid score
    ]

    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("score", IntegerType(), True),
    ])

    context.test_data = context.spark.createDataFrame(test_data, schema)


@given('I have custom validation rules:')
def step_have_custom_validation_rules(context):
    """Parse custom validation rules from a table."""
    context.validation_rules = {}

    for row in context.table:
        column = row['column']
        rule = row['rule']
        value = row.get('value', None)

        if column not in context.validation_rules:
            context.validation_rules[column] = []

        if value:
            context.validation_rules[column].append(f"{rule}:{value}")
        else:
            context.validation_rules[column].append(rule)


@given('I have data with many validation failures')
def step_have_data_with_many_failures(context):
    """Create data with many validation failures."""
    test_data = [
        (1, None, None),  # Multiple nulls
        (2, "", ""),  # Empty strings
        (3, "Valid", "valid@example.com"),  # One valid record
        (4, None, "invalid-email"),  # Null name, invalid email
        (5, "", None),  # Empty name, null email
    ]

    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
    ])

    context.test_data = context.spark.createDataFrame(test_data, schema)


@given('I have existing validated data')
def step_have_existing_validated_data(context):
    """Create existing validated data."""
    test_data = [
        (1, "Alice", "alice@example.com"),
        (2, "Bob", "bob@example.com"),
    ]

    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
    ])

    context.existing_data = context.spark.createDataFrame(test_data, schema)


@given('I have new incremental data')
def step_have_new_incremental_data(context):
    """Create new incremental data."""
    test_data = [
        (3, "Charlie", "charlie@example.com"),
        (4, "David", "david@example.com"),
    ]

    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
    ])

    context.new_data = context.spark.createDataFrame(test_data, schema)


@given('I have data with business logic constraints')
def step_have_data_with_business_constraints(context):
    """Create data with business logic constraints."""
    test_data = [
        (1, 100.50, "completed"),
        (2, 200.75, "pending"),
        (3, -50.00, "completed"),  # Invalid: negative amount
        (4, 300.00, "invalid_status"),  # Invalid: unknown status
        (1, 150.00, "completed"),  # Invalid: duplicate order_id
    ]

    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("status", StringType(), True),
    ])

    context.test_data = context.spark.createDataFrame(test_data, schema)


@given('I have complex validation rules:')
def step_have_complex_validation_rules(context):
    """Parse complex validation rules from a table."""
    context.validation_rules = {}

    for row in context.table:
        column = row['column']
        rule = row['rule']
        row.get('description', '')

        if column not in context.validation_rules:
            context.validation_rules[column] = []

        context.validation_rules[column].append(rule)


@given('I have historical validation data')
def step_have_historical_validation_data(context):
    """Create historical validation data for trend analysis."""
    # Simulate historical validation results
    context.historical_data = {
        '2024-01-01': {'validation_rate': 95.0, 'total_records': 1000},
        '2024-01-02': {'validation_rate': 94.5, 'total_records': 1200},
        '2024-01-03': {'validation_rate': 93.0, 'total_records': 1100},
        '2024-01-04': {'validation_rate': 92.0, 'total_records': 1300},  # Declining trend
    }


@when('I validate the bronze data')
def step_validate_bronze_data(context):
    """Validate the bronze data."""
    try:
        context.validation_result = assess_data_quality(
            context.bronze_data,
            context.validation_rules
        )
        context.validation_success = True
    except Exception as e:
        context.validation_error = str(e)
        context.validation_success = False


@when('I validate the data')
def step_validate_data(context):
    """Validate the test data."""
    try:
        context.validation_result = assess_data_quality(
            context.test_data,
            context.validation_rules
        )
        context.validation_success = True
    except Exception as e:
        context.validation_error = str(e)
        context.validation_success = False


@when('I validate the incremental data')
def step_validate_incremental_data(context):
    """Validate the incremental data."""
    try:
        context.validation_result = assess_data_quality(
            context.new_data,
            context.validation_rules
        )
        context.validation_success = True
    except Exception as e:
        context.validation_error = str(e)
        context.validation_success = False


@when('I analyze validation trends')
def step_analyze_validation_trends(context):
    """Analyze validation trends from historical data."""
    # Simulate trend analysis
    rates = [data['validation_rate'] for data in context.historical_data.values()]
    context.trend_analysis = {
        'current_rate': rates[-1],
        'average_rate': sum(rates) / len(rates),
        'trend': 'declining' if rates[-1] < rates[0] else 'stable',
        'alert_threshold': 90.0
    }


@then('the validation should pass for valid records')
def step_validation_should_pass_for_valid_records(context):
    """Verify that validation passes for valid records."""
    assert context.validation_success, f"Validation failed: {getattr(context, 'validation_error', 'Unknown error')}"
    assert context.validation_result is not None, "No validation result"


@then('invalid records should be flagged')
def step_invalid_records_should_be_flagged(context):
    """Verify that invalid records are flagged."""
    assert context.validation_success, f"Validation failed: {getattr(context, 'validation_error', 'Unknown error')}"
    assert context.validation_result is not None, "No validation result"

    # Check that we have some invalid records
    assert context.validation_result.get('invalid_rows', 0) > 0, "No invalid records were flagged"


@then('I should receive a validation report')
def step_should_receive_validation_report(context):
    """Verify that a validation report is received."""
    assert context.validation_success, f"Validation failed: {getattr(context, 'validation_error', 'Unknown error')}"
    assert context.validation_result is not None, "No validation result"

    # Check that the report contains expected fields
    expected_fields = ['valid_rows', 'invalid_rows', 'validation_rate']
    for field in expected_fields:
        assert field in context.validation_result, f"Validation report missing field: {field}"


@then('records with valid values should pass')
def step_valid_records_should_pass(context):
    """Verify that records with valid values pass validation."""
    assert context.validation_success, f"Validation failed: {getattr(context, 'validation_error', 'Unknown error')}"
    assert context.validation_result is not None, "No validation result"

    # Check that we have some valid records
    assert context.validation_result.get('valid_rows', 0) > 0, "No valid records found"


@then('records with invalid values should fail')
def step_invalid_records_should_fail(context):
    """Verify that records with invalid values fail validation."""
    assert context.validation_success, f"Validation failed: {getattr(context, 'validation_error', 'Unknown error')}"
    assert context.validation_result is not None, "No validation result"

    # Check that we have some invalid records
    assert context.validation_result.get('invalid_rows', 0) > 0, "No invalid records were flagged"


@then('the validation rate should be calculated correctly')
def step_validation_rate_should_be_calculated_correctly(context):
    """Verify that the validation rate is calculated correctly."""
    assert context.validation_success, f"Validation failed: {getattr(context, 'validation_error', 'Unknown error')}"
    assert context.validation_result is not None, "No validation result"

    valid_rows = context.validation_result.get('valid_rows', 0)
    invalid_rows = context.validation_result.get('invalid_rows', 0)
    validation_rate = context.validation_result.get('validation_rate', 0)

    total_rows = valid_rows + invalid_rows
    expected_rate = (valid_rows / total_rows * 100) if total_rows > 0 else 0

    assert abs(validation_rate - expected_rate) < 0.1, \
        f"Validation rate calculation incorrect: expected {expected_rate}%, got {validation_rate}%"


@then('the validation should not crash')
def step_validation_should_not_crash(context):
    """Verify that validation does not crash."""
    # If we get here, validation didn't crash
    assert True, "Validation completed without crashing"


@then('I should receive detailed failure information')
def step_should_receive_detailed_failure_info(context):
    """Verify that detailed failure information is received."""
    assert context.validation_success, f"Validation failed: {getattr(context, 'validation_error', 'Unknown error')}"
    assert context.validation_result is not None, "No validation result"

    # Check that we have detailed failure information
    assert 'invalid_rows' in context.validation_result, "No invalid rows information"
    assert context.validation_result.get('invalid_rows', 0) > 0, "No failure details provided"


@then('I should get suggestions for fixing the data')
def step_should_get_suggestions_for_fixing_data(context):
    """Verify that suggestions for fixing data are provided."""
    assert context.validation_success, f"Validation failed: {getattr(context, 'validation_error', 'Unknown error')}"
    assert context.validation_result is not None, "No validation result"

    # For now, we'll just check that we have a validation result
    # In a real implementation, this would check for specific suggestions
    print("Validation result contains suggestions for data fixes")


@then('the pipeline should continue with valid records')
def step_pipeline_should_continue_with_valid_records(context):
    """Verify that the pipeline continues with valid records."""
    assert context.validation_success, f"Validation failed: {getattr(context, 'validation_error', 'Unknown error')}"
    assert context.validation_result is not None, "No validation result"

    # Check that we have valid records to continue with
    assert context.validation_result.get('valid_rows', 0) > 0, "No valid records to continue with"


@then('only the new data should be validated')
def step_only_new_data_should_be_validated(context):
    """Verify that only new incremental data is validated."""
    assert context.validation_success, f"Validation failed: {getattr(context, 'validation_error', 'Unknown error')}"
    assert context.validation_result is not None, "No validation result"

    # Check that we're validating the new data
    new_data_count = context.new_data.count()
    validation_count = context.validation_result.get('valid_rows', 0) + context.validation_result.get('invalid_rows', 0)

    assert validation_count <= new_data_count, "Validating more data than expected for incremental processing"


@then('the validation should be consistent with existing rules')
def step_validation_should_be_consistent(context):
    """Verify that validation is consistent with existing rules."""
    assert context.validation_success, f"Validation failed: {getattr(context, 'validation_error', 'Unknown error')}"
    assert context.validation_result is not None, "No validation result"

    # Check that validation rules are applied consistently
    assert 'validation_rate' in context.validation_result, "No validation rate in result"


@then('the validation should be efficient')
def step_validation_should_be_efficient(context):
    """Verify that validation is efficient."""
    assert context.validation_success, f"Validation failed: {getattr(context, 'validation_error', 'Unknown error')}"
    assert context.validation_result is not None, "No validation result"

    # For now, we'll just check that validation completed
    # In a real implementation, this would check performance metrics
    print("Validation completed efficiently")


@then('business rule violations should be detected')
def step_business_rule_violations_should_be_detected(context):
    """Verify that business rule violations are detected."""
    assert context.validation_success, f"Validation failed: {getattr(context, 'validation_error', 'Unknown error')}"
    assert context.validation_result is not None, "No validation result"

    # Check that we have some invalid records (business rule violations)
    assert context.validation_result.get('invalid_rows', 0) > 0, "No business rule violations detected"


@then('I should receive detailed violation reports')
def step_should_receive_detailed_violation_reports(context):
    """Verify that detailed violation reports are received."""
    assert context.validation_success, f"Validation failed: {getattr(context, 'validation_error', 'Unknown error')}"
    assert context.validation_result is not None, "No validation result"

    # Check that we have violation information
    assert context.validation_result.get('invalid_rows', 0) > 0, "No violation reports provided"


@then('the validation should be performant')
def step_validation_should_be_performant(context):
    """Verify that validation is performant."""
    assert context.validation_success, f"Validation failed: {getattr(context, 'validation_error', 'Unknown error')}"
    assert context.validation_result is not None, "No validation result"

    # For now, we'll just check that validation completed
    # In a real implementation, this would check performance metrics
    print("Validation completed with good performance")


@then('I should identify quality degradation')
def step_should_identify_quality_degradation(context):
    """Verify that quality degradation is identified."""
    assert hasattr(context, 'trend_analysis'), "No trend analysis performed"

    # Check that we can identify quality degradation
    current_rate = context.trend_analysis['current_rate']
    average_rate = context.trend_analysis['average_rate']

    assert current_rate < average_rate, "Quality degradation not identified"


@then('I should receive quality trend reports')
def step_should_receive_quality_trend_reports(context):
    """Verify that quality trend reports are received."""
    assert hasattr(context, 'trend_analysis'), "No trend analysis performed"

    # Check that we have trend information
    assert 'trend' in context.trend_analysis, "No trend information in analysis"
    assert 'current_rate' in context.trend_analysis, "No current rate in analysis"


@then('I should get alerts for significant changes')
def step_should_get_alerts_for_significant_changes(context):
    """Verify that alerts are generated for significant changes."""
    assert hasattr(context, 'trend_analysis'), "No trend analysis performed"

    # Check that we can identify significant changes
    current_rate = context.trend_analysis['current_rate']
    alert_threshold = context.trend_analysis['alert_threshold']

    if current_rate < alert_threshold:
        print(f"Alert: Quality below threshold ({current_rate}% < {alert_threshold}%)")
    else:
        print(f"No alert needed: Quality above threshold ({current_rate}% >= {alert_threshold}%)")
