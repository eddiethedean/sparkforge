Feature: Data Validation
  As a data engineer
  I want to validate data quality at each pipeline stage
  So that I can ensure data integrity and catch issues early

  Background:
    Given I have a Spark session configured
    And I have validation rules defined

  Scenario: Validate bronze data with basic rules
    Given I have bronze data with columns "id", "name", "email"
    And I have validation rules:
      | column | rule        |
      | id     | not_null    |
      | name   | not_null    |
      | email  | valid_email |
    When I validate the bronze data
    Then the validation should pass for valid records
    And invalid records should be flagged
    And I should receive a validation report

  Scenario: Validate data with custom rules
    Given I have data with numeric columns
    And I have custom validation rules:
      | column | rule    | value |
      | age    | gt      | 0     |
      | age    | lt      | 150   |
      | score  | between | 0,100 |
    When I validate the data
    Then records with valid values should pass
    And records with invalid values should fail
    And the validation rate should be calculated correctly

  Scenario: Handle validation failures gracefully
    Given I have data with many validation failures
    When I validate the data
    Then the validation should not crash
    And I should receive detailed failure information
    And I should get suggestions for fixing the data
    And the pipeline should continue with valid records

  Scenario: Validate incremental data
    Given I have existing validated data
    And I have new incremental data
    When I validate the incremental data
    Then only the new data should be validated
    And the validation should be consistent with existing rules
    And the validation should be efficient

  Scenario: Validate data with complex business rules
    Given I have data with business logic constraints
    And I have complex validation rules:
      | column | rule           | description                    |
      | order_id | unique       | Order ID must be unique        |
      | amount   | gt           | Amount must be positive        |
      | status   | in           | Status must be valid           |
    When I validate the data
    Then business rule violations should be detected
    And I should receive detailed violation reports
    And the validation should be performant

  Scenario: Validate data quality trends
    Given I have historical validation data
    When I analyze validation trends
    Then I should identify quality degradation
    And I should receive quality trend reports
    And I should get alerts for significant changes
