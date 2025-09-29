Feature: Error Handling
  As a data engineer
  I want comprehensive error handling in my pipelines
  So that I can quickly identify and resolve issues

  Background:
    Given I have a Spark session configured
    And I have error handling configured

  Scenario: Handle data quality errors gracefully
    Given I have data with quality issues
    When I execute the pipeline
    Then the pipeline should not crash
    And I should receive a clear error message
    And the error should include data quality details
    And I should get suggestions for fixing the data
    And the pipeline should continue with valid data

  Scenario: Handle configuration errors
    Given I have an invalid pipeline configuration
    When I try to execute the pipeline
    Then I should receive a configuration error
    And the error should specify what is wrong
    And I should get suggestions for fixing the configuration
    And the error should be caught early in the process

  Scenario: Handle resource errors
    Given I have a pipeline that requires more resources than available
    When I execute the pipeline
    Then I should receive a resource error
    And the error should specify what resources are needed
    And I should get suggestions for increasing resources
    And the pipeline should fail gracefully

  Scenario: Handle network and connectivity errors
    Given I have a pipeline that requires network access
    And the network is unavailable
    When I execute the pipeline
    Then I should receive a connectivity error
    And the error should specify what network resources are needed
    And I should get suggestions for resolving connectivity issues
    And the pipeline should retry automatically if configured

  Scenario: Handle schema evolution errors
    Given I have a pipeline with schema changes
    And the target table has a different schema
    When I execute the pipeline
    Then I should receive a schema error
    And the error should specify the schema differences
    And I should get suggestions for resolving schema conflicts
    And I should be able to choose schema evolution options

  Scenario: Handle timeout errors
    Given I have a pipeline that takes too long to execute
    When I execute the pipeline
    Then I should receive a timeout error
    And the error should specify the timeout duration
    And I should get suggestions for optimizing performance
    And I should be able to configure timeout settings

  Scenario: Log and track errors
    Given I have a pipeline that encounters errors
    When I execute the pipeline
    Then errors should be logged with full context
    And errors should be categorized by type
    And errors should include debugging information
    And errors should be trackable across executions
