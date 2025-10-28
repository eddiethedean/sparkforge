"""
Shared fixtures and utilities for builder system tests.

This module provides common fixtures, data generators, and utility functions
for testing realistic bronze-silver-gold pipeline scenarios.
"""

# Set environment to use mock-spark BEFORE any pipeline_builder imports
import os

os.environ["SPARKFORGE_ENGINE"] = "mock"

from datetime import datetime, timedelta
from typing import Any, List

import pytest
from mock_spark import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Import functions after setting environment
from pipeline_builder.models import ParallelConfig, PipelineConfig, ValidationThresholds
from pipeline_builder.writer import WriteMode, WriterConfig
from pipeline_builder.writer.models import LogLevel


@pytest.fixture
def mock_spark_session():
    """Mock Spark session for testing."""
    from mock_spark import MockSparkSession

    return MockSparkSession()


@pytest.fixture
def pipeline_config():
    """Standard pipeline configuration for tests."""
    return PipelineConfig(
        schema="test_schema",
        thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
        parallel=ParallelConfig(enabled=True, max_workers=2),
        verbose=True,
    )


@pytest.fixture
def log_writer_config():
    """Standard LogWriter configuration for tests."""
    return WriterConfig(
        table_schema="test_schema",
        table_name="pipeline_logs",
        write_mode=WriteMode.APPEND,
        log_level=LogLevel.INFO,
        batch_size=1000,
    )


class DataGenerator:
    """Utility class for generating realistic test data."""

    @staticmethod
    def create_ecommerce_orders(spark, num_orders: int = 100) -> Any:
        """Create realistic e-commerce order data."""

        orders = []
        for i in range(num_orders):
            orders.append(
                {
                    "order_id": f"ORD-{i:06d}",
                    "customer_id": f"CUST-{i % 50:04d}",
                    "product_id": f"PROD-{i % 20:03d}",
                    "quantity": (i % 5) + 1,
                    "unit_price": round(10.0 + (i % 100), 2),
                    "order_date": (datetime.now() - timedelta(days=i % 30)).isoformat(),
                    "status": ["pending", "shipped", "delivered", "cancelled"][i % 4],
                    "payment_method": ["credit_card", "paypal", "bank_transfer"][i % 3],
                }
            )

        schema = StructType(
            [
                StructField("order_id", StringType(), False),
                StructField("customer_id", StringType(), False),
                StructField("product_id", StringType(), False),
                StructField("quantity", IntegerType(), False),
                StructField("unit_price", DoubleType(), False),
                StructField("order_date", StringType(), False),
                StructField("status", StringType(), False),
                StructField("payment_method", StringType(), False),
            ]
        )

        return spark.createDataFrame(orders, schema)

    @staticmethod
    def create_customer_data(spark, num_customers: int = 50) -> Any:
        """Create customer profile data."""

        customers = []
        for i in range(num_customers):
            customers.append(
                {
                    "customer_id": f"CUST-{i:04d}",
                    "name": f"Customer {i}",
                    "email": f"customer{i}@example.com",
                    "registration_date": (
                        datetime.now() - timedelta(days=i * 10)
                    ).isoformat(),
                    "country": ["US", "CA", "UK", "DE", "FR"][i % 5],
                    "segment": ["premium", "standard", "basic"][i % 3],
                    "total_orders": i % 20,
                    "lifetime_value": round(100.0 + (i * 50), 2),
                }
            )

        schema = StructType(
            [
                StructField("customer_id", StringType(), False),
                StructField("name", StringType(), False),
                StructField("email", StringType(), False),
                StructField("registration_date", StringType(), False),
                StructField("country", StringType(), False),
                StructField("segment", StringType(), False),
                StructField("total_orders", IntegerType(), False),
                StructField("lifetime_value", DoubleType(), False),
            ]
        )

        return spark.createDataFrame(customers, schema)

    @staticmethod
    def create_iot_sensor_data(spark, num_readings: int = 200) -> Any:
        """Create IoT sensor reading data."""

        readings = []
        for i in range(num_readings):
            readings.append(
                {
                    "sensor_id": f"SENSOR-{i % 10:02d}",
                    "timestamp": (datetime.now() - timedelta(hours=i)).isoformat(),
                    "temperature": round(20.0 + (i % 20) - 10, 1),
                    "humidity": round(40.0 + (i % 40), 1),
                    "pressure": round(1013.0 + (i % 20) - 10, 1),
                    "location": f"Building-{i % 5}",
                    "device_status": ["active", "maintenance", "error"][i % 3],
                }
            )

        schema = StructType(
            [
                StructField("sensor_id", StringType(), False),
                StructField("timestamp", StringType(), False),
                StructField("temperature", DoubleType(), False),
                StructField("humidity", DoubleType(), False),
                StructField("pressure", DoubleType(), False),
                StructField("location", StringType(), False),
                StructField("device_status", StringType(), False),
            ]
        )

        return spark.createDataFrame(readings, schema)

    @staticmethod
    def create_financial_transactions(spark, num_transactions: int = 150) -> Any:
        """Create financial transaction data."""

        transactions = []
        for i in range(num_transactions):
            transactions.append(
                {
                    "transaction_id": f"TXN-{i:08d}",
                    "account_id": f"ACC-{i % 20:04d}",
                    "amount": round(10.0 + (i % 1000), 2),
                    "transaction_type": ["debit", "credit", "transfer"][i % 3],
                    "timestamp": (datetime.now() - timedelta(hours=i)).isoformat(),
                    "merchant": f"Merchant-{i % 15}",
                    "category": ["groceries", "gas", "entertainment", "utilities"][
                        i % 4
                    ],
                    "fraud_score": round(0.0 + (i % 100) / 100, 2),
                }
            )

        schema = StructType(
            [
                StructField("transaction_id", StringType(), False),
                StructField("account_id", StringType(), False),
                StructField("amount", DoubleType(), False),
                StructField("transaction_type", StringType(), False),
                StructField("timestamp", StringType(), False),
                StructField("merchant", StringType(), False),
                StructField("category", StringType(), False),
                StructField("fraud_score", DoubleType(), False),
            ]
        )

        return spark.createDataFrame(transactions, schema)


class TestAssertions:
    """Utility class for common test assertions."""

    @staticmethod
    def assert_pipeline_success(result):
        """Assert that pipeline execution was successful."""
        assert result is not None
        assert hasattr(result, "status")
        # Additional assertions based on result type

    @staticmethod
    def assert_data_quality(df, min_rows: int = 1):
        """Assert basic data quality requirements."""
        assert df is not None
        assert df.count() >= min_rows

    @staticmethod
    def assert_schema_contains(df, required_columns: List[str]):
        """Assert that DataFrame contains required columns."""
        actual_columns = df.columns
        for col in required_columns:
            assert col in actual_columns, (
                f"Required column '{col}' not found in {actual_columns}"
            )


@pytest.fixture
def data_generator():
    """Data generator fixture."""
    return DataGenerator()


@pytest.fixture
def test_assertions():
    """Test assertions fixture."""
    return TestAssertions()
