Feature: Performance Monitoring
  As a data engineer
  I want to monitor pipeline performance
  So that I can optimize execution and identify bottlenecks

  Background:
    Given I have a Spark session configured
    And I have performance monitoring enabled

  Scenario: Monitor basic pipeline performance
    Given I have a pipeline with multiple steps
    When I execute the pipeline
    Then I should receive performance metrics
    And the metrics should include execution time
    And the metrics should include memory usage
    And the metrics should include CPU usage
    And the metrics should be logged for analysis

  Scenario: Monitor data processing performance
    Given I have a pipeline processing large datasets
    When I execute the pipeline
    Then I should receive data processing metrics
    And the metrics should include rows processed per second
    And the metrics should include data throughput
    And the metrics should include processing efficiency
    And the metrics should identify performance bottlenecks

  Scenario: Monitor resource utilization
    Given I have a pipeline with resource-intensive operations
    When I execute the pipeline
    Then I should receive resource utilization metrics
    And the metrics should include memory usage over time
    And the metrics should include CPU utilization
    And the metrics should include disk I/O
    And the metrics should identify resource constraints

  Scenario: Detect performance regressions
    Given I have historical performance data
    And I have a new pipeline execution
    When I execute the pipeline
    Then I should compare performance with historical data
    And I should detect significant performance regressions
    And I should receive alerts for performance issues
    And I should get suggestions for performance optimization

  Scenario: Monitor parallel execution performance
    Given I have a pipeline with parallel steps
    When I execute the pipeline
    Then I should receive parallel execution metrics
    And the metrics should include parallelization efficiency
    And the metrics should include load balancing
    And the metrics should identify optimal parallelization settings

  Scenario: Monitor incremental processing performance
    Given I have a pipeline with incremental processing
    When I execute the pipeline
    Then I should receive incremental processing metrics
    And the metrics should include incremental data volume
    And the metrics should include processing efficiency
    And the metrics should compare with full refresh performance

  Scenario: Generate performance reports
    Given I have performance data from multiple executions
    When I generate a performance report
    Then I should receive a comprehensive performance analysis
    And the report should include trend analysis
    And the report should include optimization recommendations
    And the report should be exportable for further analysis
