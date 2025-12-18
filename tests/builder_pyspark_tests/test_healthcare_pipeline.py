"""
Healthcare Analytics Pipeline Tests

This module tests a realistic healthcare analytics pipeline that demonstrates
Bronze → Silver → Gold medallion architecture with patient data, lab results,
diagnoses, medications, and population health insights.
"""

import os
import tempfile
from uuid import uuid4

import pytest

# Skip all tests in this module if SPARK_MODE is not "real"
if os.environ.get("SPARK_MODE", "mock").lower() != "real":
    pytestmark = pytest.mark.skip(
        reason="PySpark-specific tests require SPARK_MODE=real"
    )

from pyspark.sql import functions as F

from pipeline_builder.pipeline import PipelineBuilder
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from test_helpers.isolation import get_unique_schema


class TestHealthcarePipeline:
    """Test healthcare analytics pipeline with bronze-silver-gold architecture."""

    def test_complete_healthcare_pipeline_execution(
        self, spark_session, data_generator, test_assertions
    ):
        """Test complete healthcare pipeline: patients → medical records → health insights."""

        # Create realistic healthcare data
        patients_df = data_generator.create_healthcare_patients(
            spark_session, num_patients=40
        )
        labs_df = data_generator.create_healthcare_labs(spark_session, num_results=150)
        diagnoses_df = data_generator.create_healthcare_diagnoses(
            spark_session, num_diagnoses=120
        )
        medications_df = data_generator.create_healthcare_medications(
            spark_session, num_prescriptions=160
        )
        warehouse_dir = tempfile.mkdtemp(prefix="spark-warehouse-")
        unique_schema = f"bronze_{uuid4().hex[:16]}"
        escaped_dir = warehouse_dir.replace("'", "''")
        spark_session.sql(
            f"CREATE DATABASE IF NOT EXISTS {unique_schema} LOCATION '{escaped_dir}'"
        )

        # Create pipeline builder
        builder = PipelineBuilder(
            spark=spark_session,
            schema=unique_schema,
            min_bronze_rate=95.0,
            min_silver_rate=98.0,
            min_gold_rate=99.0,
            verbose=True,
        )

        # Bronze Layer: Raw healthcare data validation
        builder.with_bronze_rules(
            name="raw_patients",
            rules={
                "patient_id": ["not_null"],
                "first_name": ["not_null"],
                "date_of_birth": ["not_null"],
            },
        )

        builder.with_bronze_rules(
            name="raw_labs",
            rules={
                "lab_id": ["not_null"],
                "patient_id": ["not_null"],
                "test_date": ["not_null"],
                "result_value": [["gte", 0]],
            },
            incremental_col="test_date",
        )

        builder.with_bronze_rules(
            name="raw_diagnoses",
            rules={
                "diagnosis_id": ["not_null"],
                "patient_id": ["not_null"],
                "diagnosis_date": ["not_null"],
            },
            incremental_col="diagnosis_date",
        )

        builder.with_bronze_rules(
            name="raw_medications",
            rules={
                "prescription_id": ["not_null"],
                "patient_id": ["not_null"],
                "prescription_date": ["not_null"],
            },
            incremental_col="prescription_date",
        )

        # Silver Layer: Cleaned and normalized medical records
        def clean_patient_records_transform(spark, df, silvers):
            """Clean and normalize patient demographics."""
            return (
                df.withColumn(
                    "age",
                    F.floor(
                        F.datediff(
                            F.current_date(),
                            F.to_date(F.col("date_of_birth"), "yyyy-MM-dd"),
                        )
                        / 365.25
                    ),
                )
                .withColumn(
                    "full_name",
                    F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")),
                )
                .withColumn(
                    "age_group",
                    F.when(F.col("age") < 18, "pediatric")
                    .when(F.col("age") < 65, "adult")
                    .otherwise("senior"),
                )
                .select(
                    "patient_id",
                    "full_name",
                    "date_of_birth",
                    "age",
                    "age_group",
                    "gender",
                    "ethnicity",
                    "insurance_provider",
                    "registration_date",
                )
            )

        builder.add_silver_transform(
            name="clean_patients",
            source_bronze="raw_patients",
            transform=clean_patient_records_transform,
            rules={
                "patient_id": ["not_null"],
                "full_name": ["not_null"],
                "age": [["gte", 0]],
                "age_group": ["not_null"],
                "gender": ["not_null"],
                "insurance_provider": ["not_null"],
            },
            table_name="clean_patients",
        )

        def normalize_lab_results_transform(spark, df, silvers):
            """Normalize lab results and flag abnormal values."""
            return (
                df.withColumn(
                    "test_date_parsed",
                    F.to_timestamp(
                        F.col("test_date"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]"
                    ),
                )
                .withColumn(
                    "is_abnormal",
                    (F.col("result_value") < F.col("reference_range_min"))
                    | (F.col("result_value") > F.col("reference_range_max")),
                )
                .withColumn(
                    "result_category",
                    F.when(
                        F.col("result_value") > F.col("reference_range_max") * 1.5,
                        "critical_high",
                    )
                    .when(
                        F.col("result_value") > F.col("reference_range_max"),
                        "abnormal_high",
                    )
                    .when(
                        F.col("result_value") < F.col("reference_range_min") * 0.7,
                        "critical_low",
                    )
                    .when(
                        F.col("result_value") < F.col("reference_range_min"),
                        "abnormal_low",
                    )
                    .otherwise("normal"),
                )
                .select(
                    "lab_id",
                    "patient_id",
                    "test_date_parsed",
                    "test_type",
                    "result_value",
                    "unit",
                    "is_abnormal",
                    "result_category",
                    "ordering_physician",
                )
            )

        builder.add_silver_transform(
            name="normalized_labs",
            source_bronze="raw_labs",
            transform=normalize_lab_results_transform,
            rules={
                "patient_id": ["not_null"],
                "test_date_parsed": ["not_null"],
                "result_value": [["gte", 0]],
                "is_abnormal": ["not_null"],
                "result_category": ["not_null"],
            },
            table_name="normalized_labs",
        )

        def processed_diagnoses_transform(spark, df, silvers):
            """Process diagnoses with temporal information."""
            return (
                df.withColumn(
                    "diagnosis_date_parsed",
                    F.to_timestamp(
                        F.col("diagnosis_date"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]"
                    ),
                )
                .withColumn(
                    "is_chronic",
                    F.col("status") == "chronic",
                )
                .withColumn(
                    "risk_level",
                    F.when(F.col("severity") == "severe", "high")
                    .when(F.col("severity") == "moderate", "medium")
                    .otherwise("low"),
                )
                .select(
                    "diagnosis_id",
                    "patient_id",
                    "diagnosis_date_parsed",
                    "diagnosis_code",
                    "diagnosis_name",
                    "severity",
                    "risk_level",
                    "is_chronic",
                    "diagnosing_physician",
                    "status",
                )
            )

        builder.add_silver_transform(
            name="processed_diagnoses",
            source_bronze="raw_diagnoses",
            transform=processed_diagnoses_transform,
            rules={
                "patient_id": ["not_null"],
                "diagnosis_date_parsed": ["not_null"],
                "diagnosis_name": ["not_null"],
                "is_chronic": ["not_null"],
                "risk_level": ["not_null"],
            },
            table_name="processed_diagnoses",
        )

        # Gold Layer: Population health metrics and patient risk scores
        def patient_risk_scores_transform(spark, silvers):
            """Calculate patient risk scores based on labs, diagnoses, and medications."""
            clean_patients = silvers.get("clean_patients")
            normalized_labs = silvers.get("normalized_labs")
            processed_diagnoses = silvers.get("processed_diagnoses")

            # Handle None cases gracefully
            if (
                normalized_labs is None
                or processed_diagnoses is None
                or clean_patients is None
            ):
                return spark.createDataFrame(
                    [],
                    [
                        "patient_id",
                        "full_name",
                        "age_group",
                        "gender",
                        "insurance_provider",
                        "total_labs",
                        "abnormal_labs",
                        "critical_labs",
                        "abnormal_lab_rate",
                        "total_diagnoses",
                        "chronic_conditions",
                        "overall_risk_score",
                        "risk_category",
                    ],
                )

            # Calculate abnormal lab count per patient
            lab_metrics = normalized_labs.groupBy("patient_id").agg(
                F.count("*").alias("total_labs"),
                F.sum(F.when(F.col("is_abnormal"), 1).otherwise(0)).alias(
                    "abnormal_labs"
                ),
                F.sum(
                    F.when(
                        F.col("result_category").isin(
                            ["critical_high", "critical_low"]
                        ),
                        1,
                    ).otherwise(0)
                ).alias("critical_labs"),
            )

            # Calculate diagnosis risk metrics
            diagnosis_metrics = processed_diagnoses.groupBy("patient_id").agg(
                F.count("*").alias("total_diagnoses"),
                F.sum(F.when(F.col("is_chronic"), 1).otherwise(0)).alias(
                    "chronic_conditions"
                ),
                F.sum(
                    F.when(F.col("risk_level") == "high", 3)
                    .when(F.col("risk_level") == "medium", 2)
                    .otherwise(1)
                ).alias("risk_score_sum"),
            )

            # Combine metrics and calculate overall risk
            patient_risk = (
                clean_patients.join(lab_metrics, "patient_id", "left")
                .join(diagnosis_metrics, "patient_id", "left")
                .withColumn(
                    "abnormal_lab_rate",
                    F.when(
                        F.col("total_labs") > 0,
                        F.col("abnormal_labs") / F.col("total_labs") * 100,
                    ).otherwise(0),
                )
                .withColumn(
                    "overall_risk_score",
                    (
                        F.coalesce(F.col("risk_score_sum"), F.lit(0))
                        + F.coalesce(F.col("critical_labs") * 5, F.lit(0))
                        + F.coalesce(F.col("chronic_conditions") * 2, F.lit(0))
                    ),
                )
                .withColumn(
                    "risk_category",
                    F.when(F.col("overall_risk_score") >= 15, "high")
                    .when(F.col("overall_risk_score") >= 8, "medium")
                    .otherwise("low"),
                )
                .select(
                    "patient_id",
                    "full_name",
                    "age_group",
                    "gender",
                    "insurance_provider",
                    "total_labs",
                    "abnormal_labs",
                    "critical_labs",
                    "abnormal_lab_rate",
                    "total_diagnoses",
                    "chronic_conditions",
                    "overall_risk_score",
                    "risk_category",
                )
            )

            return patient_risk

        builder.add_gold_transform(
            name="patient_risk_scores",
            transform=patient_risk_scores_transform,
            rules={
                "patient_id": ["not_null"],
                "overall_risk_score": [["gte", 0]],
            },
            table_name="patient_risk_scores",
            source_silvers=["clean_patients", "normalized_labs", "processed_diagnoses"],
        )

        def population_health_metrics_transform(spark, silvers):
            """Calculate population-level health metrics."""
            clean_patients = silvers.get("clean_patients")
            normalized_labs = silvers.get("normalized_labs")
            processed_diagnoses = silvers.get("processed_diagnoses")

            # Handle None cases gracefully
            if clean_patients is None:
                return spark.createDataFrame(
                    [],
                    [
                        "total_patients",
                        "avg_age",
                        "total_lab_tests",
                        "abnormal_tests",
                        "abnormal_test_rate",
                        "total_diagnoses",
                        "chronic_diagnoses",
                    ],
                )

            if normalized_labs is None:
                normalized_labs = spark.createDataFrame(
                    [], ["lab_id", "patient_id", "is_abnormal", "result_value"]
                )
            if processed_diagnoses is None:
                processed_diagnoses = spark.createDataFrame(
                    [], ["diagnosis_id", "patient_id", "is_chronic"]
                )

            # Population demographics
            population_demo = clean_patients.agg(
                F.count("*").alias("total_patients"),
                F.countDistinct(F.col("age_group")).alias("age_groups"),
                F.countDistinct(F.col("insurance_provider")).alias("insurance_types"),
                F.avg("age").alias("avg_age"),
            )

            # Lab metrics
            lab_stats = normalized_labs.agg(
                F.count("*").alias("total_lab_tests"),
                F.sum(F.when(F.col("is_abnormal"), 1).otherwise(0)).alias(
                    "abnormal_tests"
                ),
                F.avg("result_value").alias("avg_result_value"),
            )

            # Diagnosis metrics
            diagnosis_stats = processed_diagnoses.agg(
                F.count("*").alias("total_diagnoses"),
                F.countDistinct("diagnosis_name").alias("unique_diagnoses"),
                F.sum(F.when(F.col("is_chronic"), 1).otherwise(0)).alias(
                    "chronic_diagnoses"
                ),
            )

            # Combine into summary
            summary = (
                population_demo.crossJoin(lab_stats)
                .crossJoin(diagnosis_stats)
                .withColumn(
                    "abnormal_test_rate",
                    F.when(
                        F.col("total_lab_tests") > 0,
                        F.col("abnormal_tests") / F.col("total_lab_tests") * 100,
                    ).otherwise(0),
                )
                .select(
                    "total_patients",
                    "avg_age",
                    "total_lab_tests",
                    "abnormal_tests",
                    "abnormal_test_rate",
                    "total_diagnoses",
                    "chronic_diagnoses",
                )
            )

            return summary

        builder.add_gold_transform(
            name="population_health_metrics",
            transform=population_health_metrics_transform,
            rules={
                "total_patients": [["gt", 0]],
            },
            table_name="population_health_metrics",
            source_silvers=["clean_patients", "normalized_labs", "processed_diagnoses"],
        )

        # Build and execute pipeline
        pipeline = builder.to_pipeline()

        result = pipeline.run_initial_load(
            bronze_sources={
                "raw_patients": patients_df,
                "raw_labs": labs_df,
                "raw_diagnoses": diagnoses_df,
                "raw_medications": medications_df,
            }
        )

        # Verify pipeline execution
        test_assertions.assert_pipeline_success(result)

        # Verify data quality
        assert result.status.value == "completed" or result.success
        assert "patient_risk_scores" in result.gold_results
        assert "population_health_metrics" in result.gold_results

        # Verify gold layer outputs
        patient_risk_result = result.gold_results["patient_risk_scores"]
        assert patient_risk_result.get("rows_processed", 0) > 0

        population_metrics_result = result.gold_results["population_health_metrics"]
        assert population_metrics_result.get("rows_processed", 0) > 0

        # Cleanup: drop schema created for this test
        try:
            import sys
            import os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
            from test_helpers.isolation import cleanup_test_tables
            cleanup_test_tables(spark_session, unique_schema)
        except Exception:
            pass  # Ignore cleanup errors

    def test_incremental_healthcare_processing(
        self, spark_session, data_generator, test_assertions
    ):
        """Test incremental processing of new healthcare data."""
        # Create initial data
        patients_df = data_generator.create_healthcare_patients(
            spark_session, num_patients=20
        )
        labs_initial = data_generator.create_healthcare_labs(
            spark_session, num_results=50
        )

        # Create pipeline builder
        warehouse_dir = tempfile.mkdtemp(prefix="spark-warehouse-")
        unique_schema = f"bronze_{uuid4().hex[:16]}"
        escaped_dir = warehouse_dir.replace("'", "''")
        spark_session.sql(
            f"CREATE DATABASE IF NOT EXISTS {unique_schema} LOCATION '{escaped_dir}'"
        )

        builder = PipelineBuilder(
            spark=spark_session,
            schema=unique_schema,
            min_bronze_rate=95.0,
            min_silver_rate=98.0,
            min_gold_rate=99.0,
            verbose=False,
        )

        builder.with_bronze_rules(
            name="raw_patients",
            rules={"patient_id": ["not_null"]},
        )

        builder.with_bronze_rules(
            name="raw_labs",
            rules={
                "lab_id": ["not_null"],
                "patient_id": ["not_null"],
            },
            incremental_col="test_date",
        )

        def normalize_labs_transform(spark, df, silvers):
            return df.withColumn(
                "test_date_parsed",
                F.to_timestamp(
                    F.col("test_date").cast("string"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]"
                ),
            )

        builder.add_silver_transform(
            name="normalized_labs",
            source_bronze="raw_labs",
            transform=normalize_labs_transform,
            rules={"patient_id": ["not_null"]},
            table_name="normalized_labs",
        )

        pipeline = builder.to_pipeline()

        # Initial load
        result1 = pipeline.run_initial_load(
            bronze_sources={
                "raw_patients": patients_df,
                "raw_labs": labs_initial,
            }
        )

        test_assertions.assert_pipeline_success(result1)

        # Incremental load with new lab results
        labs_incremental = data_generator.create_healthcare_labs(
            spark_session, num_results=30
        )

        result2 = pipeline.run_incremental(
            bronze_sources={
                "raw_patients": patients_df,
                "raw_labs": labs_incremental,
            }
        )

        test_assertions.assert_pipeline_success(result2)
        assert result2.mode.value == "incremental"

    @pytest.mark.pyspark
    def test_healthcare_logging(
        self, spark_session, data_generator, log_writer_config, test_assertions
    ):
        """Test comprehensive logging for healthcare pipeline."""
        # Skip if in mock mode (requires real PySpark with Delta Lake)
        if os.environ.get("SPARK_MODE", "mock").lower() == "mock":
            pytest.skip("Requires real PySpark with Delta Lake")
        from pipeline_builder.writer import LogWriter

        # Create test data
        patients_df = data_generator.create_healthcare_patients(
            spark_session, num_patients=15
        )
        labs_df = data_generator.create_healthcare_labs(spark_session, num_results=50)

        # Create unique schema for this test
        analytics_schema = get_unique_schema("analytics")
        spark_session.sql(f"CREATE DATABASE IF NOT EXISTS {analytics_schema}")

        # Create LogWriter
        log_writer = LogWriter(
            spark=spark_session,
            schema=analytics_schema,
            table_name="healthcare_logs",
        )

        # Create pipeline
        warehouse_dir = tempfile.mkdtemp(prefix="spark-warehouse-")
        unique_schema = f"bronze_{uuid4().hex[:16]}"
        escaped_dir = warehouse_dir.replace("'", "''")
        spark_session.sql(
            f"CREATE DATABASE IF NOT EXISTS {unique_schema} LOCATION '{escaped_dir}'"
        )

        builder = PipelineBuilder(
            spark=spark_session, schema=unique_schema, verbose=False
        )

        builder.with_bronze_rules(
            name="raw_patients", rules={"patient_id": ["not_null"]}
        )

        builder.with_bronze_rules(
            name="raw_labs",
            rules={"patient_id": ["not_null"]},
            incremental_col="test_date",
        )

        def normalize_labs_transform(spark, df, silvers):
            return df.withColumn(
                "test_date_parsed",
                F.to_timestamp(
                    F.col("test_date").cast("string"), "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]"
                ),
            )

        builder.add_silver_transform(
            name="normalized_labs",
            source_bronze="raw_labs",
            transform=normalize_labs_transform,
            rules={"patient_id": ["not_null"]},
            table_name="normalized_labs",
        )

        pipeline = builder.to_pipeline()

        # Execute pipeline
        result = pipeline.run_initial_load(
            bronze_sources={
                "raw_patients": patients_df,
                "raw_labs": labs_df,
            }
        )

        # Log execution results
        log_result = log_writer.append(result)

        # Verify logging was successful
        test_assertions.assert_pipeline_success(result)
        assert log_result is not None
        assert log_result.get("success") is True

        # Cleanup: drop schema created for this test
        try:
            import sys
            import os
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
            from test_helpers.isolation import cleanup_test_tables
            cleanup_test_tables(spark_session, analytics_schema)
        except Exception:
            pass  # Ignore cleanup errors
