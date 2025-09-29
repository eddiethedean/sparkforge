Feature: Example BDD Test
  As a developer
  I want to see how BDD tests work
  So that I can understand the testing framework

  Background:
    Given I have a Spark session configured
    And I have test data available

  Scenario: Simple data processing
    Given I have a bronze step named "test_events" with validation rules
    When I execute the bronze step
    Then the step should complete successfully
    And the output should contain valid data
    And the validation rate should be above 95%

  @smoke
  Scenario: Smoke test for basic functionality
    Given I have a bronze step named "smoke_test"
    When I execute the bronze step
    Then the step should complete successfully

  @integration
  Scenario: Integration test for full pipeline
    Given I have a bronze step named "raw_data"
    And I have a silver step named "clean_data" that depends on "raw_data"
    When I execute the bronze-silver pipeline
    Then both steps should complete successfully
    And the silver step should process data from the bronze step
