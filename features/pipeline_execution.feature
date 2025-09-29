Feature: Pipeline Execution
  As a data engineer
  I want to execute SparkForge pipelines
  So that I can process data through bronze, silver, and gold layers

  Background:
    Given I have a Spark session configured
    And I have test data available

  Scenario: Execute a simple bronze pipeline
    Given I have a bronze step named "events" with validation rules
    When I execute the bronze step
    Then the step should complete successfully
    And the output should contain valid data
    And the validation rate should be above 95%

  Scenario: Execute a bronze-silver pipeline
    Given I have a bronze step named "raw_events"
    And I have a silver step named "clean_events" that depends on "raw_events"
    When I execute the bronze-silver pipeline
    Then both steps should complete successfully
    And the silver step should process data from the bronze step
    And the data quality should improve from bronze to silver

  Scenario: Execute a full bronze-silver-gold pipeline
    Given I have a bronze step named "raw_data"
    And I have a silver step named "processed_data" that depends on "raw_data"
    And I have a gold step named "analytics_data" that depends on "processed_data"
    When I execute the full pipeline
    Then all steps should complete successfully
    And data should flow correctly through all layers
    And the final gold data should be ready for analytics

  Scenario: Handle pipeline execution errors
    Given I have a bronze step with invalid data
    When I execute the pipeline
    Then the execution should fail gracefully
    And I should receive a clear error message
    And the error should include suggestions for resolution

  Scenario: Execute pipeline with incremental processing
    Given I have a bronze step with incremental column "timestamp"
    And I have previous data in the target table
    When I execute the pipeline in incremental mode
    Then only new data should be processed
    And existing data should remain unchanged
    And the processing should be more efficient

  Scenario: Execute pipeline with parallel steps
    Given I have multiple independent bronze steps
    When I execute the pipeline with parallel processing enabled
    Then the independent steps should run in parallel
    And the total execution time should be reduced
    And all steps should complete successfully

  Scenario: Monitor pipeline execution metrics
    Given I have a pipeline with multiple steps
    When I execute the pipeline
    Then I should receive detailed execution metrics
    And the metrics should include timing information
    And the metrics should include data quality metrics
    And the metrics should include resource usage information
