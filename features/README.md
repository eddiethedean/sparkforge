# SparkForge BDD Tests

This directory contains Behavior-Driven Development (BDD) tests for the SparkForge pipeline framework using the `behave` framework.

## Overview

The BDD tests provide comprehensive coverage of SparkForge functionality through natural language scenarios that describe the expected behavior of the system.

## Test Structure

```
features/
├── environment.py              # Test environment setup and teardown
├── behave.ini                 # Behave configuration
├── requirements.txt           # BDD testing dependencies
├── README.md                  # This file
├── reports/                   # Test reports and logs
├── fixtures/                  # Test fixtures and data
├── steps/                     # Step definitions
│   ├── pipeline_execution_steps.py
│   ├── data_validation_steps.py
│   ├── writer_logging_steps.py
│   ├── dependency_analysis_steps.py
│   ├── error_handling_steps.py
│   └── performance_monitoring_steps.py
└── *.feature                  # Feature files
    ├── pipeline_execution.feature
    ├── data_validation.feature
    ├── writer_logging.feature
    ├── dependency_analysis.feature
    ├── error_handling.feature
    └── performance_monitoring.feature
```

## Features Tested

### 1. Pipeline Execution
- **File**: `pipeline_execution.feature`
- **Steps**: `pipeline_execution_steps.py`
- **Coverage**: Bronze, silver, and gold pipeline execution
- **Scenarios**: Simple pipelines, complex pipelines, error handling, incremental processing

### 2. Data Validation
- **File**: `data_validation.feature`
- **Steps**: `data_validation_steps.py`
- **Coverage**: Data quality validation, custom rules, business logic
- **Scenarios**: Basic validation, complex rules, quality trends

### 3. Writer Logging
- **File**: `writer_logging.feature`
- **Steps**: `writer_logging_steps.py`
- **Coverage**: Execution logging, performance metrics, data quality tracking
- **Scenarios**: Successful execution, error logging, high-volume logging

### 4. Dependency Analysis
- **File**: `dependency_analysis.feature`
- **Steps**: `dependency_analysis_steps.py`
- **Coverage**: Pipeline dependencies, circular dependency detection, optimization
- **Scenarios**: Simple dependencies, complex pipelines, performance analysis

### 5. Error Handling
- **File**: `error_handling.feature`
- **Steps**: `error_handling_steps.py`
- **Coverage**: Comprehensive error handling, graceful failures, debugging
- **Scenarios**: Data quality errors, configuration errors, resource errors

### 6. Performance Monitoring
- **File**: `performance_monitoring.feature`
- **Steps**: `performance_monitoring_steps.py`
- **Coverage**: Performance metrics, resource monitoring, trend analysis
- **Scenarios**: Basic monitoring, resource utilization, performance optimization

## Setup and Installation

### 1. Install Dependencies

```bash
# Install BDD testing dependencies
pip install -r features/requirements.txt

# Install SparkForge in development mode
pip install -e .
```

### 2. Configure Environment

```bash
# Set up Java environment for Spark
export JAVA_HOME=/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home
export SPARK_LOCAL_IP=127.0.0.1

# Create reports directory
mkdir -p features/reports
```

### 3. Run Tests

```bash
# Run all BDD tests
behave features/

# Run specific feature
behave features/pipeline_execution.feature

# Run with specific tags
behave features/ --tags @smoke

# Run with verbose output
behave features/ --format pretty --verbose

# Run with JUnit reporting
behave features/ --junit --junit-directory features/reports/junit
```

## Test Configuration

### Behave Configuration (`behave.ini`)

The `behave.ini` file contains comprehensive configuration for the BDD tests:

- **Output Formats**: Pretty, JUnit, HTML, JSON
- **Logging**: Detailed logging with timestamps
- **Performance**: Timeout settings and failure limits
- **Reporting**: Multiple report formats
- **Debugging**: Verbose output and traceback display

### Environment Setup (`environment.py`)

The environment file provides:

- **Spark Session Management**: Automatic setup and teardown
- **Test Data Creation**: Helper functions for test data
- **Resource Cleanup**: Automatic cleanup after tests
- **Error Handling**: Graceful error handling and reporting

## Writing New Tests

### 1. Create Feature File

```gherkin
Feature: New Feature
  As a data engineer
  I want to test new functionality
  So that I can ensure it works correctly

  Scenario: Test new functionality
    Given I have the prerequisites
    When I perform the action
    Then I should see the expected result
```

### 2. Create Step Definitions

```python
from behave import given, when, then

@given('I have the prerequisites')
def step_have_prerequisites(context):
    # Setup code here
    pass

@when('I perform the action')
def step_perform_action(context):
    # Action code here
    pass

@then('I should see the expected result')
def step_should_see_expected_result(context):
    # Assertion code here
    pass
```

### 3. Add to Test Suite

```bash
# Run the new feature
behave features/new_feature.feature
```

## Best Practices

### 1. Test Organization
- Group related scenarios in feature files
- Use descriptive scenario names
- Keep scenarios focused and atomic

### 2. Step Definitions
- Use clear, descriptive step names
- Implement proper error handling
- Add helpful assertions and error messages

### 3. Test Data
- Use realistic test data
- Clean up test data after tests
- Use fixtures for complex data setup

### 4. Performance
- Monitor test execution time
- Use appropriate timeouts
- Optimize slow tests

## Troubleshooting

### Common Issues

1. **Spark Session Issues**
   ```bash
   # Check Java installation
   java -version
   
   # Set JAVA_HOME
   export JAVA_HOME=/path/to/java
   ```

2. **Import Errors**
   ```bash
   # Install SparkForge in development mode
   pip install -e .
   
   # Check Python path
   python -c "import sparkforge"
   ```

3. **Test Failures**
   ```bash
   # Run with verbose output
   behave features/ --verbose
   
   # Check logs
   tail -f features/reports/behave.log
   ```

### Debug Mode

```bash
# Run with debug output
behave features/ --format pretty --verbose --show-source

# Run specific scenario
behave features/pipeline_execution.feature --name "Execute a simple bronze pipeline"
```

## Continuous Integration

### GitHub Actions

```yaml
name: BDD Tests
on: [push, pull_request]
jobs:
  bdd-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: 3.8
      - name: Install dependencies
        run: |
          pip install -r features/requirements.txt
          pip install -e .
      - name: Run BDD tests
        run: behave features/
```

### Jenkins Pipeline

```groovy
pipeline {
    agent any
    stages {
        stage('Setup') {
            steps {
                sh 'pip install -r features/requirements.txt'
                sh 'pip install -e .'
            }
        }
        stage('BDD Tests') {
            steps {
                sh 'behave features/ --junit --junit-directory reports/junit'
            }
        }
        stage('Publish Reports') {
            steps {
                publishTestResults testResultsPattern: 'reports/junit/*.xml'
            }
        }
    }
}
```

## Reporting

### Test Reports

The BDD tests generate multiple types of reports:

- **Pretty Format**: Human-readable console output
- **JUnit XML**: For CI/CD integration
- **HTML Reports**: Detailed HTML reports
- **JSON Reports**: Machine-readable JSON output

### Report Locations

- **Console Output**: Real-time test execution
- **Log Files**: `features/reports/behave.log`
- **JUnit Reports**: `features/reports/junit/`
- **HTML Reports**: `features/reports/html/`

## Contributing

### Adding New Tests

1. Create feature file in `features/`
2. Implement step definitions in `features/steps/`
3. Add test data in `features/fixtures/`
4. Update documentation

### Code Style

- Follow PEP 8 for Python code
- Use descriptive names for steps
- Add docstrings to step functions
- Include type hints where appropriate

### Testing

- Test your new tests thoroughly
- Ensure they pass in isolation
- Check for proper cleanup
- Verify error handling

## Support

For issues with BDD tests:

1. Check the logs in `features/reports/behave.log`
2. Run tests with verbose output
3. Check the SparkForge documentation
4. Create an issue in the repository

## License

The BDD tests are part of the SparkForge project and follow the same license terms.
