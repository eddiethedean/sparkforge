# Test Failure Storage and Rerun Scripts

This directory contains scripts to efficiently manage test failures by storing them and rerunning only failed tests.

## Usage

### Python Script (Recommended)

```bash
# Store failures from a test run
python scripts/store_and_rerun_failed_tests.py store pyspark

# Rerun only the previously stored failures
python scripts/store_and_rerun_failed_tests.py rerun pyspark

# Clear stored failures
python scripts/store_and_rerun_failed_tests.py clear pyspark
```

### Bash Script (Alternative)

```bash
# Store failures
./scripts/store_and_rerun_failed_tests.sh store pyspark

# Rerun failures
./scripts/store_and_rerun_failed_tests.sh rerun pyspark

# Clear failures
./scripts/store_and_rerun_failed_tests.sh clear pyspark
```

## Modes

- **pyspark** (default): Runs tests with real PySpark
- **mock**: Runs tests with mock-spark

## Workflow

1. **Initial run**: Store failures
   ```bash
   python scripts/store_and_rerun_failed_tests.py store pyspark
   ```
   This runs all tests and saves failures to `failed_tests_pyspark.txt`

2. **Make fixes**: Edit code to fix failing tests

3. **Verify fixes**: Rerun only failed tests (much faster!)
   ```bash
   python scripts/store_and_rerun_failed_tests.py rerun pyspark
   ```

4. **Repeat**: If some tests still fail, go back to step 2. If all pass, run step 1 again to check for any new failures.

5. **Clean up**: When done, clear the failure list
   ```bash
   python scripts/store_and_rerun_failed_tests.py clear pyspark
   ```

## Files

- `failed_tests_pyspark.txt`: Stores list of failed PySpark tests
- `failed_tests_mock.txt`: Stores list of failed mock-spark tests (if used)
- `/tmp/sparkforge_*_tests.log`: Full test logs from the store command

## Benefits

- **Faster iteration**: Rerun only ~50-100 failed tests instead of 1700+
- **Focus**: See exactly which tests need fixing
- **Tracking**: Maintain history of what was failing
- **Efficiency**: Especially important for slow pyspark tests
