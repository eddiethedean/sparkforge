# Use Case: IoT and Events

**Version**: 2.8.0  
**Last Updated**: February 2025

This guide builds an **IoT / event** pipeline: sensor ingestion, anomaly detection, and gold aggregates for zone analytics and device health. All content is self-contained; for pipeline basics see [Building Pipelines](./BUILDING_PIPELINES_GUIDE.md) and [Getting Started](./GETTING_STARTED_GUIDE.md).

## What You'll Build

- **Sensor ingestion:** Validate and land raw sensor readings
- **Processed sensors:** Time dimensions, anomaly flag, severity
- **Gold:** Zone-level aggregates, anomaly summary

## Prerequisites

- Python 3.8+ with SparkForge installed
- Engine configured (see [Getting Started](./GETTING_STARTED_GUIDE.md))

## Step 1: Setup and Data

```python
from pipeline_builder import PipelineBuilder
from pipeline_builder.engine_config import configure_engine
from pipeline_builder.functions import get_default_functions
from pyspark.sql import SparkSession, functions as F
from datetime import datetime, timedelta
import random

spark = SparkSession.builder \
    .appName("IoT") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
configure_engine(spark=spark)
F = get_default_functions()

base_time = datetime(2024, 1, 1, 0, 0, 0)
zones = ["Building_A", "Building_B", "Building_C"]
sensor_types = ["temperature", "humidity", "pressure"]

readings = []
for i in range(500):
    zone = random.choice(zones)
    st = random.choice(sensor_types)
    ts = base_time + timedelta(minutes=i * 5)
    val = round(20 + random.uniform(-5, 5), 2) if st == "temperature" else round(50 + random.uniform(-10, 10), 2)
    readings.append((f"{zone}_{st}_{i % 10}", st, zone, val, ts, "online"))

sensor_df = spark.createDataFrame(
    readings,
    ["sensor_id", "sensor_type", "zone", "value", "timestamp", "device_status"]
)
```

## Step 2: Build the Pipeline

```python
builder = PipelineBuilder(spark=spark, schema="iot_schema", min_bronze_rate=95.0, min_silver_rate=98.0, min_gold_rate=99.0)

# Bronze: sensor readings
builder.with_bronze_rules(
    name="sensor_readings",
    rules={
        "sensor_id": [F.col("sensor_id").isNotNull()],
        "sensor_type": [F.col("sensor_type").isNotNull()],
        "value": [F.col("value").isNotNull()],
        "timestamp": [F.col("timestamp").isNotNull()],
        "zone": [F.col("zone").isNotNull()],
    },
    incremental_col="timestamp",
)

# Silver: processed sensors with anomaly flag
def process_sensors(spark, bronze_df, prior_silvers):
    return (bronze_df
        .withColumn("hour", F.hour("timestamp"))
        .withColumn("is_anomaly",
            F.when((F.col("sensor_type") == "temperature") & ((F.col("value") > 60) | (F.col("value") < -10)), True)
            .when((F.col("sensor_type") == "humidity") & ((F.col("value") > 90) | (F.col("value") < 10)), True)
            .otherwise(False)
        )
        .withColumn("severity",
            F.when(F.col("is_anomaly") & (F.col("sensor_type") == "temperature") & (F.col("value") > 80), "critical")
            .when(F.col("is_anomaly"), "medium")
            .otherwise("normal")
        )
        .withColumn("processed_at", F.current_timestamp())
    )

builder.add_silver_transform(
    name="processed_sensors",
    source_bronze="sensor_readings",
    transform=process_sensors,
    rules={
        "is_anomaly": [F.col("is_anomaly").isNotNull()],
        "severity": [F.col("severity").isNotNull()],
        "processed_at": [F.col("processed_at").isNotNull()],
        "value": [F.col("value").isNotNull()],
    },
    table_name="processed_sensors",
)

# Gold: zone analytics
def zone_analytics(spark, silvers):
    return (silvers["processed_sensors"]
        .groupBy("zone", "sensor_type", "hour")
        .agg(
            F.count("*").alias("readings"),
            F.avg("value").alias("avg_value"),
            F.sum(F.when(F.col("is_anomaly"), 1).otherwise(0)).alias("anomalies"),
        )
        .withColumn("anomaly_rate", F.col("anomalies") / F.col("readings") * 100)
    )

builder.add_gold_transform(
    name="zone_analytics",
    transform=zone_analytics,
    rules={
        "zone": [F.col("zone").isNotNull()],
        "avg_value": [F.col("avg_value").isNotNull()],
        "readings": [F.col("readings") > 0],
    },
    table_name="zone_analytics",
    source_silvers=["processed_sensors"],
)
```

## Step 3: Run

```python
errors = builder.validate_pipeline()
if errors:
    raise RuntimeError(f"Validation failed: {errors}")

pipeline = builder.to_pipeline()
result = pipeline.run_initial_load(bronze_sources={"sensor_readings": sensor_df})

print(f"Status: {result.status.value}")
print(f"Silver rows written: {result.silver_results['processed_sensors']['rows_written']}")
print(f"Gold rows written: {result.gold_results['zone_analytics']['rows_written']}")
```

You can add more gold steps (e.g. anomaly summary by zone/severity, device health dashboard). See [Building Pipelines](./BUILDING_PIPELINES_GUIDE.md).

## Related Guides

- [Use Cases Index](./USE_CASES_INDEX.md)
- [Building Pipelines](./BUILDING_PIPELINES_GUIDE.md)
- [Execution Modes](./EXECUTION_MODES_GUIDE.md)
