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

# Import types based on SPARK_MODE
if os.environ.get("SPARK_MODE", "mock").lower() == "real":
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )
else:
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
    from mock_spark import SparkSession

    return SparkSession()


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

    @staticmethod
    def create_healthcare_patients(spark, num_patients: int = 50) -> Any:
        """Create healthcare patient demographics data."""

        patients = []
        for i in range(num_patients):
            patients.append(
                {
                    "patient_id": f"PAT-{i:06d}",
                    "first_name": f"Patient{i % 100}",
                    "last_name": f"LastName{i % 20}",
                    "date_of_birth": (
                        datetime.now() - timedelta(days=365 * (20 + i % 60))
                    ).strftime("%Y-%m-%d"),
                    "gender": ["M", "F", "Other"][i % 3],
                    "ethnicity": ["White", "Black", "Hispanic", "Asian", "Other"][
                        i % 5
                    ],
                    "address": f"123 Main St, City {i % 10}",
                    "insurance_provider": ["Medicare", "Medicaid", "Private", "None"][
                        i % 4
                    ],
                    "registration_date": (
                        datetime.now() - timedelta(days=i * 30)
                    ).isoformat(),
                }
            )

        schema = StructType(
            [
                StructField("patient_id", StringType(), False),
                StructField("first_name", StringType(), False),
                StructField("last_name", StringType(), False),
                StructField("date_of_birth", StringType(), False),
                StructField("gender", StringType(), False),
                StructField("ethnicity", StringType(), False),
                StructField("address", StringType(), False),
                StructField("insurance_provider", StringType(), False),
                StructField("registration_date", StringType(), False),
            ]
        )

        return spark.createDataFrame(patients, schema)

    @staticmethod
    def create_healthcare_labs(spark, num_results: int = 200) -> Any:
        """Create healthcare lab result data."""

        lab_results = []
        for i in range(num_results):
            lab_results.append(
                {
                    "lab_id": f"LAB-{i:08d}",
                    "patient_id": f"PAT-{(i % 50):06d}",
                    "test_date": (datetime.now() - timedelta(days=i % 90)).isoformat(),
                    "test_type": [
                        "glucose",
                        "cholesterol",
                        "hemoglobin",
                        "blood_pressure",
                        "lipid_panel",
                    ][i % 5],
                    "result_value": round(50.0 + (i % 200), 1),
                    "unit": ["mg/dL", "mg/dL", "g/dL", "mmHg", "mg/dL"][i % 5],
                    "reference_range_min": 50.0,
                    "reference_range_max": 150.0,
                    "status": ["normal", "abnormal", "critical"][i % 3],
                    "ordering_physician": f"DR-{i % 20:03d}",
                }
            )

        schema = StructType(
            [
                StructField("lab_id", StringType(), False),
                StructField("patient_id", StringType(), False),
                StructField("test_date", StringType(), False),
                StructField("test_type", StringType(), False),
                StructField("result_value", DoubleType(), False),
                StructField("unit", StringType(), False),
                StructField("reference_range_min", DoubleType(), False),
                StructField("reference_range_max", DoubleType(), False),
                StructField("status", StringType(), False),
                StructField("ordering_physician", StringType(), False),
            ]
        )

        return spark.createDataFrame(lab_results, schema)

    @staticmethod
    def create_healthcare_diagnoses(spark, num_diagnoses: int = 150) -> Any:
        """Create healthcare diagnosis data."""
        diagnoses_list = [
            "Hypertension",
            "Diabetes Type 2",
            "Asthma",
            "Obesity",
            "Depression",
            "Anxiety",
            "Migraine",
            "Arthritis",
            "COPD",
            "Heart Disease",
        ]

        diagnoses = []
        for i in range(num_diagnoses):
            diagnoses.append(
                {
                    "diagnosis_id": f"DX-{i:08d}",
                    "patient_id": f"PAT-{(i % 50):06d}",
                    "diagnosis_date": (
                        datetime.now() - timedelta(days=i % 180)
                    ).isoformat(),
                    "diagnosis_code": f"ICD10-{i % 1000:04d}",
                    "diagnosis_name": diagnoses_list[i % len(diagnoses_list)],
                    "severity": ["mild", "moderate", "severe"][i % 3],
                    "diagnosing_physician": f"DR-{i % 20:03d}",
                    "status": ["active", "resolved", "chronic"][i % 3],
                }
            )

        schema = StructType(
            [
                StructField("diagnosis_id", StringType(), False),
                StructField("patient_id", StringType(), False),
                StructField("diagnosis_date", StringType(), False),
                StructField("diagnosis_code", StringType(), False),
                StructField("diagnosis_name", StringType(), False),
                StructField("severity", StringType(), False),
                StructField("diagnosing_physician", StringType(), False),
                StructField("status", StringType(), False),
            ]
        )

        return spark.createDataFrame(diagnoses, schema)

    @staticmethod
    def create_healthcare_medications(spark, num_prescriptions: int = 180) -> Any:
        """Create healthcare medication/prescription data."""
        medications_list = [
            "Aspirin",
            "Metformin",
            "Lisinopril",
            "Atorvastatin",
            "Amlodipine",
            "Omeprazole",
            "Metoprolol",
            "Albuterol",
            "Gabapentin",
            "Sertraline",
        ]

        medications = []
        for i in range(num_prescriptions):
            medications.append(
                {
                    "prescription_id": f"RX-{i:08d}",
                    "patient_id": f"PAT-{(i % 50):06d}",
                    "prescription_date": (
                        datetime.now() - timedelta(days=i % 120)
                    ).isoformat(),
                    "medication_name": medications_list[i % len(medications_list)],
                    "dosage": f"{(i % 10) + 1 * 10}mg",
                    "frequency": ["daily", "twice_daily", "weekly", "as_needed"][i % 4],
                    "quantity": (i % 90) + 30,
                    "prescribing_physician": f"DR-{i % 20:03d}",
                    "refills_remaining": i % 5,
                }
            )

        schema = StructType(
            [
                StructField("prescription_id", StringType(), False),
                StructField("patient_id", StringType(), False),
                StructField("prescription_date", StringType(), False),
                StructField("medication_name", StringType(), False),
                StructField("dosage", StringType(), False),
                StructField("frequency", StringType(), False),
                StructField("quantity", IntegerType(), False),
                StructField("prescribing_physician", StringType(), False),
                StructField("refills_remaining", IntegerType(), False),
            ]
        )

        return spark.createDataFrame(medications, schema)

    @staticmethod
    def create_supply_chain_orders(spark, num_orders: int = 100) -> Any:
        """Create supply chain order data."""
        orders = []
        for i in range(num_orders):
            orders.append(
                {
                    "order_id": f"ORD-{i:08d}",
                    "customer_id": f"CUST-{i % 30:04d}",
                    "product_id": f"PROD-{i % 25:03d}",
                    "order_date": (datetime.now() - timedelta(days=i % 90)).isoformat(),
                    "quantity": (i % 50) + 10,
                    "unit_price": round(5.0 + (i % 100), 2),
                    "warehouse_id": f"WH-{i % 5:02d}",
                    "destination_city": f"City{i % 10}",
                    "priority": ["high", "medium", "low"][i % 3],
                }
            )

        schema = StructType(
            [
                StructField("order_id", StringType(), False),
                StructField("customer_id", StringType(), False),
                StructField("product_id", StringType(), False),
                StructField("order_date", StringType(), False),
                StructField("quantity", IntegerType(), False),
                StructField("unit_price", DoubleType(), False),
                StructField("warehouse_id", StringType(), False),
                StructField("destination_city", StringType(), False),
                StructField("priority", StringType(), False),
            ]
        )

        return spark.createDataFrame(orders, schema)

    @staticmethod
    def create_supply_chain_shipments(spark, num_shipments: int = 120) -> Any:
        """Create shipment tracking data."""
        shipments = []
        for i in range(num_shipments):
            shipments.append(
                {
                    "shipment_id": f"SHIP-{i:08d}",
                    "order_id": f"ORD-{(i % 100):08d}",
                    "shipping_date": (
                        datetime.now() - timedelta(days=i % 85)
                    ).isoformat(),
                    "delivery_date": (
                        datetime.now() - timedelta(days=(i % 85) - 3)
                    ).isoformat()
                    if i % 4 != 0
                    else None,
                    "carrier": ["UPS", "FedEx", "DHL", "USPS"][i % 4],
                    "tracking_number": f"TRACK{i:010d}",
                    "status": ["in_transit", "delivered", "pending", "delayed"][i % 4],
                    "cost": round(10.0 + (i % 50), 2),
                }
            )

        schema = StructType(
            [
                StructField("shipment_id", StringType(), False),
                StructField("order_id", StringType(), False),
                StructField("shipping_date", StringType(), False),
                StructField("delivery_date", StringType(), True),
                StructField("carrier", StringType(), False),
                StructField("tracking_number", StringType(), False),
                StructField("status", StringType(), False),
                StructField("cost", DoubleType(), False),
            ]
        )

        return spark.createDataFrame(shipments, schema)

    @staticmethod
    def create_supply_chain_inventory(spark, num_items: int = 200) -> Any:
        """Create inventory level data."""
        inventory = []
        for i in range(num_items):
            inventory.append(
                {
                    "inventory_id": f"INV-{i:08d}",
                    "product_id": f"PROD-{i % 25:03d}",
                    "warehouse_id": f"WH-{i % 5:02d}",
                    "snapshot_date": (
                        datetime.now() - timedelta(days=i % 30)
                    ).isoformat(),
                    "quantity_on_hand": (i % 1000) + 100,
                    "quantity_reserved": (i % 100),
                    "reorder_point": 200,
                    "max_stock": 1000,
                }
            )

        schema = StructType(
            [
                StructField("inventory_id", StringType(), False),
                StructField("product_id", StringType(), False),
                StructField("warehouse_id", StringType(), False),
                StructField("snapshot_date", StringType(), False),
                StructField("quantity_on_hand", IntegerType(), False),
                StructField("quantity_reserved", IntegerType(), False),
                StructField("reorder_point", IntegerType(), False),
                StructField("max_stock", IntegerType(), False),
            ]
        )

        return spark.createDataFrame(inventory, schema)

    @staticmethod
    def create_marketing_impressions(spark, num_impressions: int = 200) -> Any:
        """Create marketing ad impression data."""
        impressions = []
        for i in range(num_impressions):
            impressions.append(
                {
                    "impression_id": f"IMP-{i:08d}",
                    "campaign_id": f"CAMP-{i % 10:02d}",
                    "customer_id": f"CUST-{i % 40:04d}",
                    "impression_date": (
                        datetime.now() - timedelta(hours=i % 720)
                    ).isoformat(),
                    "channel": ["google", "facebook", "twitter", "email", "display"][
                        i % 5
                    ],
                    "ad_id": f"AD-{i % 20:03d}",
                    "cost_per_impression": round(0.01 + (i % 50) / 1000, 3),
                    "device_type": ["desktop", "mobile", "tablet"][i % 3],
                }
            )

        schema = StructType(
            [
                StructField("impression_id", StringType(), False),
                StructField("campaign_id", StringType(), False),
                StructField("customer_id", StringType(), False),
                StructField("impression_date", StringType(), False),
                StructField("channel", StringType(), False),
                StructField("ad_id", StringType(), False),
                StructField("cost_per_impression", DoubleType(), False),
                StructField("device_type", StringType(), False),
            ]
        )

        return spark.createDataFrame(impressions, schema)

    @staticmethod
    def create_marketing_clicks(spark, num_clicks: int = 80) -> Any:
        """Create marketing click/engagement data."""
        clicks = []
        for i in range(num_clicks):
            clicks.append(
                {
                    "click_id": f"CLK-{i:08d}",
                    "impression_id": f"IMP-{(i * 2) % 200:08d}",
                    "customer_id": f"CUST-{i % 40:04d}",
                    "click_date": (
                        datetime.now() - timedelta(hours=(i * 2) % 700)
                    ).isoformat(),
                    "channel": ["google", "facebook", "twitter", "email", "display"][
                        i % 5
                    ],
                    "time_to_click_seconds": (i % 300) + 10,
                }
            )

        schema = StructType(
            [
                StructField("click_id", StringType(), False),
                StructField("impression_id", StringType(), False),
                StructField("customer_id", StringType(), False),
                StructField("click_date", StringType(), False),
                StructField("channel", StringType(), False),
                StructField("time_to_click_seconds", IntegerType(), False),
            ]
        )

        return spark.createDataFrame(clicks, schema)

    @staticmethod
    def create_marketing_conversions(spark, num_conversions: int = 50) -> Any:
        """Create marketing conversion data."""
        conversions = []
        for i in range(num_conversions):
            conversions.append(
                {
                    "conversion_id": f"CONV-{i:08d}",
                    "customer_id": f"CUST-{i % 40:04d}",
                    "click_id": f"CLK-{i % 80:08d}",
                    "conversion_date": (
                        datetime.now() - timedelta(hours=(i * 3) % 680)
                    ).isoformat(),
                    "conversion_type": ["purchase", "signup", "download", "trial"][
                        i % 4
                    ],
                    "conversion_value": round(10.0 + (i % 200), 2),
                    "channel": ["google", "facebook", "twitter", "email", "display"][
                        i % 5
                    ],
                }
            )

        schema = StructType(
            [
                StructField("conversion_id", StringType(), False),
                StructField("customer_id", StringType(), False),
                StructField("click_id", StringType(), False),
                StructField("conversion_date", StringType(), False),
                StructField("conversion_type", StringType(), False),
                StructField("conversion_value", DoubleType(), False),
                StructField("channel", StringType(), False),
            ]
        )

        return spark.createDataFrame(conversions, schema)

    @staticmethod
    def create_data_quality_source_a(spark, num_records: int = 100) -> Any:
        """Create source A data with some quality issues."""
        records = []
        for i in range(num_records):
            # Introduce some quality issues
            records.append(
                {
                    "id": f"A-{i:06d}",
                    "customer_id": f"CUST-{i % 50:04d}"
                    if i % 20 != 0
                    else None,  # 5% missing
                    "transaction_date": (
                        datetime.now() - timedelta(days=i % 90)
                    ).isoformat(),
                    "amount": round(10.0 + (i % 200), 2)
                    if i % 15 != 0
                    else -5.0,  # Some negative amounts
                    "status": ["completed", "pending", "failed", None][
                        i % 4
                    ],  # 25% None
                    "category": ["food", "clothing", "electronics", ""][
                        i % 4
                    ],  # Some empty strings
                    "region": f"Region{i % 5}",
                }
            )

        schema = StructType(
            [
                StructField("id", StringType(), False),
                StructField("customer_id", StringType(), True),
                StructField("transaction_date", StringType(), False),
                StructField("amount", DoubleType(), False),
                StructField("status", StringType(), True),
                StructField("category", StringType(), False),
                StructField("region", StringType(), False),
            ]
        )

        return spark.createDataFrame(records, schema)

    @staticmethod
    def create_data_quality_source_b(spark, num_records: int = 120) -> Any:
        """Create source B data with different schema and some mismatches."""
        records = []
        for i in range(num_records):
            records.append(
                {
                    "record_id": f"B-{i:06d}",
                    "cust_id": f"CUST-{i % 50:04d}",
                    "date": (
                        datetime.now() - timedelta(days=(i + 5) % 90)
                    ).isoformat(),  # Slightly different dates
                    "value": round(10.0 + (i % 200), 2),
                    "transaction_status": ["done", "in_progress", "error"][i % 3],
                    "item_type": ["food", "clothing", "electronics", "other"][i % 4],
                    "location": f"Region{i % 5}",
                }
            )

        schema = StructType(
            [
                StructField("record_id", StringType(), False),
                StructField("cust_id", StringType(), False),
                StructField("date", StringType(), False),
                StructField("value", DoubleType(), False),
                StructField("transaction_status", StringType(), False),
                StructField("item_type", StringType(), False),
                StructField("location", StringType(), False),
            ]
        )

        return spark.createDataFrame(records, schema)

    @staticmethod
    def create_streaming_batch_events(spark, num_events: int = 150) -> Any:
        """Create streaming events data (simulates real-time data)."""
        events = []
        for i in range(num_events):
            events.append(
                {
                    "event_id": f"EVT-{i:08d}",
                    "user_id": f"USER-{i % 40:04d}",
                    "event_timestamp": (
                        datetime.now() - timedelta(minutes=i % 1440)
                    ).isoformat(),
                    "event_type": [
                        "click",
                        "view",
                        "purchase",
                        "search",
                        "add_to_cart",
                    ][i % 5],
                    "product_id": f"PROD-{i % 30:03d}",
                    "session_id": f"SESS-{i % 25:03d}",
                    "amount": round(10.0 + (i % 100), 2) if i % 5 == 0 else None,
                    "device": ["mobile", "desktop", "tablet"][i % 3],
                }
            )

        schema = StructType(
            [
                StructField("event_id", StringType(), False),
                StructField("user_id", StringType(), False),
                StructField("event_timestamp", StringType(), False),
                StructField("event_type", StringType(), False),
                StructField("product_id", StringType(), False),
                StructField("session_id", StringType(), False),
                StructField("amount", DoubleType(), True),
                StructField("device", StringType(), False),
            ]
        )

        return spark.createDataFrame(events, schema)

    @staticmethod
    def create_streaming_batch_history(spark, num_records: int = 200) -> Any:
        """Create historical batch data for backfill."""
        records = []
        for i in range(num_records):
            records.append(
                {
                    "event_id": f"HIST-{i:08d}",
                    "user_id": f"USER-{i % 40:04d}",
                    "event_timestamp": (
                        datetime.now() - timedelta(days=90 - (i % 60))
                    ).isoformat(),
                    "event_type": [
                        "click",
                        "view",
                        "purchase",
                        "search",
                        "add_to_cart",
                    ][i % 5],
                    "product_id": f"PROD-{i % 30:03d}",
                    "session_id": f"SESS-{i % 25:03d}",
                    "amount": round(10.0 + (i % 100), 2) if i % 5 == 0 else None,
                    "device": ["mobile", "desktop", "tablet"][i % 3],
                    "source": "batch",
                }
            )

        schema = StructType(
            [
                StructField("event_id", StringType(), False),
                StructField("user_id", StringType(), False),
                StructField("event_timestamp", StringType(), False),
                StructField("event_type", StringType(), False),
                StructField("product_id", StringType(), False),
                StructField("session_id", StringType(), False),
                StructField("amount", DoubleType(), True),
                StructField("device", StringType(), False),
                StructField("source", StringType(), False),
            ]
        )

        return spark.createDataFrame(records, schema)


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
