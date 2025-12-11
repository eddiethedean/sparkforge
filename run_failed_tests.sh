#!/bin/bash
# Script to run only the failed PySpark tests
# Usage: ./run_failed_tests.sh

SPARK_MODE=real pytest $(cat failed_tests_pyspark.txt | grep -v '^#' | grep -v '^$' | tr '\n' ' ')

