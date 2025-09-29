Feature: Dependency Analysis
  As a data engineer
  I want to analyze pipeline dependencies
  So that I can optimize execution order and detect issues

  Background:
    Given I have a Spark session configured
    And I have pipeline steps with dependencies

  Scenario: Analyze simple pipeline dependencies
    Given I have a bronze step named "raw_data"
    And I have a silver step named "clean_data" that depends on "raw_data"
    When I analyze the dependencies
    Then I should identify that "clean_data" depends on "raw_data"
    And the execution order should be "raw_data" then "clean_data"
    And no circular dependencies should be detected

  Scenario: Detect circular dependencies
    Given I have steps with circular dependencies:
      | step_a | depends_on | step_b |
      | step_b | depends_on | step_c |
      | step_c | depends_on | step_a |
    When I analyze the dependencies
    Then I should detect the circular dependency
    And I should receive a clear error message
    And I should get suggestions for resolving the circular dependency

  Scenario: Optimize execution order for parallel processing
    Given I have multiple independent steps
    And I have steps with dependencies
    When I analyze the dependencies
    Then I should identify independent steps that can run in parallel
    And I should identify the optimal execution order
    And I should receive execution group recommendations

  Scenario: Analyze complex pipeline dependencies
    Given I have a complex pipeline with multiple layers
    And I have steps with multiple dependencies
    When I analyze the dependencies
    Then I should identify all dependencies correctly
    And I should identify the critical path
    And I should identify potential bottlenecks
    And I should provide optimization recommendations

  Scenario: Handle missing dependencies
    Given I have a step that depends on a non-existent step
    When I analyze the dependencies
    Then I should detect the missing dependency
    And I should receive a clear error message
    And I should get suggestions for fixing the dependency

  Scenario: Analyze dependency performance impact
    Given I have steps with different execution times
    And I have steps with dependencies
    When I analyze the dependencies
    Then I should calculate the total execution time
    And I should identify the critical path
    And I should provide performance optimization suggestions
