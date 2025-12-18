GUIDE: CONCURRENT TESTING OF PYSPARK CODE WITH PYTEST-XDIST

OVERVIEW
This document describes best practices for testing PySpark code concurrently using pytest-xdist. The goal is to achieve reliable, deterministic, and scalable test execution without SparkContext collisions, deadlocks, or flaky behavior.

CORE PRINCIPLE
Spark is parallel internally but not concurrency-safe externally. Concurrency must be achieved through process isolation, not threading or async execution.

In short:
pytest-xdist provides concurrency via isolation.
Spark provides parallelism via executors.
Never mix the two.

⸻

REQUIRED TOOLS
	•	pytest
	•	pytest-xdist
	•	pyspark

Install:
pip install pytest pytest-xdist pyspark

⸻

HOW TO RUN TESTS CONCURRENTLY

Use pytest-xdist to run tests in parallel processes:

pytest -n auto

Each worker runs in its own OS process, which allows Spark to be safely isolated.

⸻

SPARK LIFECYCLE RULES
	1.	One SparkSession per pytest worker
	2.	Never share SparkSession across workers
	3.	Never share SparkSession across threads
	4.	Always stop Spark after the test
	5.	Use local[1] for all tests

Violating any of these rules will result in flaky tests or deadlocks.

⸻

CANONICAL SPARK FIXTURE (SAFE TO COPY)

Place this in conftest.py:

import os
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope=“function”)
def spark():
worker_id = os.environ.get(“PYTEST_XDIST_WORKER”, “gw0”)

spark = (
    SparkSession.builder
    .master("local[1]")
    .appName(f"pytest-spark-{worker_id}")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.default.parallelism", "1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)

yield spark

spark.stop()

WHY THIS WORKS
	•	Each pytest worker runs in a separate process
	•	Each process starts its own JVM
	•	local[1] enforces deterministic execution
	•	Spark UI is disabled to avoid port conflicts

⸻

WHY LOCAL[*] MUST NEVER BE USED IN TESTS

Using local[*] causes:
	•	Multiple executor threads per test
	•	Python GIL contention
	•	Py4J gateway contention
	•	Nondeterministic execution order
	•	Random hangs and timeouts

Rule:
local[*] is for exploration only, never for tests.

⸻

MARKING SPARK TESTS

Add this to pytest.ini:

[pytest]
markers =
spark: tests that require pyspark

Use the marker:

import pytest

@pytest.mark.spark
def test_transformation(spark):
…

Run only Spark tests:

pytest -m spark -n auto

⸻

TEST SEPARATION STRATEGY

Not all tests should use Spark.

Recommended split:
	•	Unit tests: pure Python (no Spark)
	•	Schema tests: pure Python or Polars
	•	Transformation logic: Polars preferred
	•	Integration tests: Spark
	•	I/O tests: Spark

Rule:
Test logic without Spark whenever possible.
Use Spark only to verify integration.

⸻

DO NOT INTRODUCE CONCURRENCY INSIDE TESTS

Never do the following inside Spark tests:
	•	ThreadPoolExecutor
	•	asyncio
	•	Multiple Spark actions in parallel
	•	Parallel DataFrame actions

Spark already parallelizes internally. External concurrency breaks it.

⸻

OPTIONAL: WORKER-SCOPED SPARK (ADVANCED)

This can improve performance but is risky.

Only use this if:
	•	Tests are read-only
	•	No temp views
	•	No catalog mutations
	•	No global Spark config changes

Example:

@pytest.fixture(scope=“session”)
def spark_worker():
spark = (
SparkSession.builder
.master(“local[1]”)
.getOrCreate()
)
yield spark
spark.stop()

If any test mutates Spark state, this approach will cause test contamination.

⸻

CI-SAFE SPARK SETTINGS (RECOMMENDED)

Add these configs if running in CI:

.config(“spark.sql.execution.arrow.pyspark.enabled”, “false”)
.config(“spark.sql.adaptive.enabled”, “false”)

These reduce nondeterminism and resource contention.

⸻

COMMON FAILURE MODES AND FIXES

Symptom: SparkContext stopped
Cause: Shared SparkSession
Fix: One Spark per worker

Symptom: Tests hang indefinitely
Cause: Threaded Spark calls
Fix: Use pytest-xdist only

Symptom: Port binding errors
Cause: Spark UI enabled
Fix: Disable Spark UI

Symptom: Random test failures
Cause: local[*]
Fix: Use local[1]

Symptom: Slow CI
Cause: Too many Spark tests
Fix: Reduce Spark usage, test logic elsewhere

⸻

MENTAL MODEL TO REMEMBER

pytest-xdist = concurrency through process isolation
Spark = parallelism through executors

Never attempt to make Spark concurrent externally.

⸻

RECOMMENDED ARCHITECTURE

pytest -n auto
-> worker gw0 -> Spark JVM (local[1])
-> worker gw1 -> Spark JVM (local[1])
-> worker gw2 -> Spark JVM (local[1])

Each worker owns its own Spark lifecycle.

⸻

FINAL CHECKLIST
	•	Using pytest-xdist
	•	One SparkSession per worker
	•	local[1] only
	•	Function-scoped Spark fixture
	•	Spark UI disabled
	•	No threading or async
	•	Spark tests explicitly marked
	•	Non-Spark logic tested separately
