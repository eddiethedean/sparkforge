# Test Troubleshooting Guide

Common issues and solutions when running SparkForge tests.

## Test Failures

### Tests fail with "Delta Lake not configured"

**Problem**: Tests requiring Delta Lake fail with configuration errors.

**Solution**:
1. Ensure Delta Lake is installed: `pip install delta-spark`
2. Check Java version: `java -version` (should be Java 8, 11, or 17)
3. Set `JAVA_HOME` environment variable
4. For tests that don't require Delta Lake, use `--marker "not delta"`

### Tests fail with "Spark session not found"

**Problem**: Tests fail because Spark session fixture is not available.

**Solution**:
1. Ensure you're using the `spark_session` fixture
2. Check that conftest files are in the correct location
3. Verify pytest can discover fixtures: `pytest --fixtures`

### Tests fail in parallel mode

**Problem**: Tests pass individually but fail when run in parallel.

**Solution**:
1. Use unique schema names (use `get_unique_schema()` helper)
2. Mark tests with `@pytest.mark.no_parallel` if they can't run in parallel
3. Check for shared state between tests
4. Use `isolated_spark_session` fixture for complete isolation

### Tests are slow

**Problem**: Tests take too long to run.

**Solution**:
1. Use mock mode for faster execution: `--mode mock`
2. Run only fast tests: `--marker fast`
3. Exclude slow tests: `--marker "not slow"`
4. Use parallel execution: `--parallel --workers 4`

## Environment Issues

### Python version mismatch

**Problem**: Tests fail due to Python version incompatibility.

**Solution**:
1. Ensure Python 3.8+ is used
2. Check virtual environment is activated
3. Verify `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` are set correctly

### Java not found

**Problem**: Real mode tests fail because Java is not found.

**Solution**:
1. Install Java 8, 11, or 17
2. Set `JAVA_HOME` environment variable
3. Add Java to `PATH`

## Import Errors

### "Cannot import name X"

**Problem**: Import errors when running tests.

**Solution**:
1. Ensure project is installed: `pip install -e .`
2. Check `PYTHONPATH` includes `src/` directory
3. Verify imports use correct paths

### Circular import errors

**Problem**: Tests fail with circular import errors.

**Solution**:
1. Check import order in test files
2. Use absolute imports
3. Avoid importing from `__init__.py` files that cause cycles

## Fixture Issues

### Fixture not found

**Problem**: Pytest reports fixture not found.

**Solution**:
1. Check fixture is defined in appropriate conftest.py
2. Verify fixture name matches exactly
3. Ensure conftest.py is in the correct directory

### Fixture scope issues

**Problem**: Tests interfere with each other due to fixture scope.

**Solution**:
1. Use function-scoped fixtures for isolation
2. Use `isolated_spark_session` for complete isolation
3. Clean up state in fixture teardown

## Coverage Issues

### Coverage not generated

**Problem**: Coverage reports are not generated.

**Solution**:
1. Use `--coverage` flag: `python tests/run_tests.py --coverage`
2. Ensure pytest-cov is installed: `pip install pytest-cov`
3. Check coverage files in `htmlcov/` directory

## Getting Help

If you encounter issues not covered here:

1. Check test logs for detailed error messages
2. Run tests with verbose output: `-v` flag
3. Run specific test to isolate issue
4. Check test documentation in `tests/` directory
