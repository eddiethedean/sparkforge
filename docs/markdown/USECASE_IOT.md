# IoT Data Processing Quick Start

Build a complete IoT sensor data pipeline in 10 minutes! This guide shows you how to process sensor data, detect anomalies, and create real-time analytics for IoT devices.

## What You'll Build

- **Sensor Data Ingestion**: Temperature, humidity, pressure, vibration sensors
- **Anomaly Detection**: Identify unusual sensor readings and device failures
- **Real-time Analytics**: Zone-based monitoring and alerting
- **Device Health**: Sensor status and maintenance predictions
- **Time Series Analytics**: Trend analysis and forecasting

## Prerequisites

- Python 3.8+ with SparkForge installed
- Basic understanding of IoT and time series data

## Step 1: Setup (1 minute)

```python
from sparkforge import PipelineBuilder
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from datetime import datetime, timedelta
import random
import math

# Initialize Spark
spark = SparkSession.builder \
    .appName("IoT Data Processing") \
    .master("local[*]") \
    .getOrCreate()

# Create sample IoT sensor data
def create_sensor_data(spark, num_readings=5000):
    """Create realistic IoT sensor data with some anomalies."""
    readings = []
    base_time = datetime(2024, 1, 1, 0, 0, 0)

    # Define sensor zones and types
    zones = ["Building_A", "Building_B", "Building_C", "Building_D"]
    sensor_types = ["temperature", "humidity", "pressure", "vibration", "air_quality"]

    for i in range(num_readings):
        zone = random.choice(zones)
        sensor_type = random.choice(sensor_types)
        sensor_id = f"{zone}_{sensor_type}_{random.randint(1, 10):02d}"
        timestamp = base_time + timedelta(minutes=i * 5)  # Every 5 minutes

        # Generate realistic sensor values with some anomalies
        if sensor_type == "temperature":
            base_value = 20 + 10 * math.sin(i / 100)  # Daily cycle
            if random.random() < 0.02:  # 2% anomaly rate
                value = random.uniform(80, 120)  # Overheating
            else:
                value = base_value + random.uniform(-2, 2)
        elif sensor_type == "humidity":
            base_value = 45 + 15 * math.cos(i / 80)
            if random.random() < 0.01:  # 1% anomaly rate
                value = random.uniform(90, 100)  # Humidity spike
            else:
                value = base_value + random.uniform(-3, 3)
        elif sensor_type == "pressure":
            base_value = 1013 + 5 * math.sin(i / 200)
            if random.random() < 0.005:  # 0.5% anomaly rate
                value = random.uniform(500, 800)  # Pressure drop
            else:
                value = base_value + random.uniform(-1, 1)
        elif sensor_type == "vibration":
            base_value = random.uniform(0.1, 0.5)
            if random.random() < 0.03:  # 3% anomaly rate
                value = random.uniform(2, 5)  # High vibration
            else:
                value = base_value
        else:  # air_quality
            base_value = random.uniform(20, 60)
            if random.random() < 0.02:  # 2% anomaly rate
                value = random.uniform(150, 300)  # Poor air quality
            else:
                value = base_value

        readings.append({
            "sensor_id": sensor_id,
            "sensor_type": sensor_type,
            "zone": zone,
            "value": round(value, 2),
            "timestamp": timestamp,
            "device_status": "online" if random.random() > 0.005 else "offline",
            "battery_level": random.randint(20, 100),
            "signal_strength": random.randint(-100, -30)
        })

    return spark.createDataFrame(readings)

# Create the data
print("📡 Creating sample IoT sensor data...")
sensor_df = create_sensor_data(spark, 5000)
print(f"Created {sensor_df.count()} sensor readings")
sensor_df.show(5)
```

## Step 2: Build the Pipeline (5 minutes)

```python
# Configure pipeline for IoT data processing
builder = PipelineBuilder(
    spark=spark,
    schema="iot_analytics",
    min_bronze_rate=98.0,  # High quality required for sensor data
    min_silver_rate=99.0,  # Very high quality for processed data
    min_gold_rate=99.5,    # Near perfect for analytics
    enable_parallel_silver=True,
    max_parallel_workers=4,
    verbose=True
)

# Bronze Layer: Raw Sensor Data
print("🥉 Building Bronze Layer - Sensor Data Ingestion...")
builder.with_bronze_rules(
    name="sensor_readings",
    rules={
        "sensor_id": [F.col("sensor_id").isNotNull()],
        "sensor_type": [F.col("sensor_type").isNotNull()],
        "value": [F.col("value").isNotNull()],
        "timestamp": [F.col("timestamp").isNotNull()],
        "zone": [F.col("zone").isNotNull()]
    },
    incremental_col="timestamp",
    description="Raw sensor data ingestion with validation"
)

# Silver Layer 1: Processed Sensor Data with Anomaly Detection
print("🥈 Building Silver Layer - Sensor Data Processing...")

def process_sensor_data(spark, bronze_df, prior_silvers):
    """Process sensor data and detect anomalies."""
    return (bronze_df
        .withColumn("hour", F.hour("timestamp"))
        .withColumn("day_of_week", F.dayofweek("timestamp"))
        .withColumn("is_weekend", F.dayofweek("timestamp").isin([1, 7]))
        .withColumn("is_anomaly",
            # Temperature anomalies
            F.when((F.col("sensor_type") == "temperature") & (F.col("value") > 60), True)
            .when((F.col("sensor_type") == "temperature") & (F.col("value") < -10), True)
            # Humidity anomalies
            .when((F.col("sensor_type") == "humidity") & (F.col("value") > 90), True)
            .when((F.col("sensor_type") == "humidity") & (F.col("value") < 10), True)
            # Pressure anomalies
            .when((F.col("sensor_type") == "pressure") & (F.col("value") < 900), True)
            .when((F.col("sensor_type") == "pressure") & (F.col("value") > 1100), True)
            # Vibration anomalies
            .when((F.col("sensor_type") == "vibration") & (F.col("value") > 2), True)
            # Air quality anomalies
            .when((F.col("sensor_type") == "air_quality") & (F.col("value") > 100), True)
            .otherwise(False)
        )
        .withColumn("anomaly_severity",
            F.when((F.col("sensor_type") == "temperature") & (F.col("value") > 80), "critical")
            .when((F.col("sensor_type") == "temperature") & (F.col("value") > 60), "high")
            .when((F.col("sensor_type") == "air_quality") & (F.col("value") > 200), "critical")
            .when((F.col("sensor_type") == "vibration") & (F.col("value") > 3), "high")
            .when(F.col("is_anomaly"), "medium")
            .otherwise("normal")
        )
        .withColumn("processed_at", F.current_timestamp())
        .filter(F.col("device_status") == "online")  # Only process online devices
    )

builder.add_silver_transform(
    name="processed_sensors",
    source_bronze="sensor_readings",
    transform=process_sensor_data,
    rules={
        "is_anomaly": [F.col("is_anomaly").isNotNull()],
        "anomaly_severity": [F.col("anomaly_severity").isNotNull()],
        "processed_at": [F.col("processed_at").isNotNull()]
    },
    table_name="processed_sensors",
    watermark_col="timestamp",
    description="Sensor data with anomaly detection and severity classification"
)

# Silver Layer 2: Device Health Monitoring
print("🥈 Building Silver Layer - Device Health Monitoring...")

def monitor_device_health(spark, bronze_df, prior_silvers):
    """Monitor device health and connectivity."""
    return (bronze_df
        .groupBy("sensor_id", "sensor_type", "zone")
        .agg(
            F.count("*").alias("total_readings"),
            F.sum(F.when(F.col("device_status") == "online", 1).otherwise(0)).alias("online_readings"),
            F.avg("battery_level").alias("avg_battery_level"),
            F.min("battery_level").alias("min_battery_level"),
            F.avg("signal_strength").alias("avg_signal_strength"),
            F.min("signal_strength").alias("min_signal_strength"),
            F.min("timestamp").alias("first_seen"),
            F.max("timestamp").alias("last_seen")
        )
        .withColumn("uptime_percentage",
            (F.col("online_readings") / F.col("total_readings")) * 100)
        .withColumn("device_health",
            F.when(F.col("uptime_percentage") < 80, "poor")
            .when(F.col("min_battery_level") < 20, "low_battery")
            .when(F.col("min_signal_strength") < -80, "poor_signal")
            .when(F.col("uptime_percentage") < 95, "fair")
            .otherwise("good")
        )
        .withColumn("maintenance_needed",
            F.when(F.col("device_health").isin(["poor", "low_battery"]), True)
            .otherwise(False)
        )
    )

builder.add_silver_transform(
    name="device_health",
    source_bronze="sensor_readings",
    transform=monitor_device_health,
    rules={
        "device_health": [F.col("device_health").isNotNull()],
        "uptime_percentage": [F.col("uptime_percentage") >= 0],
        "maintenance_needed": [F.col("maintenance_needed").isNotNull()]
    },
    table_name="device_health",
    description="Device health monitoring and maintenance predictions"
)

# Gold Layer 1: Zone Analytics
print("🥇 Building Gold Layer - Zone Analytics...")

def zone_analytics(spark, silvers):
    """Zone-based sensor analytics and monitoring."""
    sensors_df = silvers["processed_sensors"]

    return (sensors_df
        .groupBy("zone", "sensor_type", F.date_trunc("hour", "timestamp").alias("hour"))
        .agg(
            F.count("*").alias("reading_count"),
            F.avg("value").alias("avg_value"),
            F.min("value").alias("min_value"),
            F.max("value").alias("max_value"),
            F.stddev("value").alias("value_stddev"),
            F.sum(F.when(F.col("is_anomaly"), 1).otherwise(0)).alias("anomaly_count"),
            F.countDistinct("sensor_id").alias("active_sensors")
        )
        .withColumn("anomaly_rate",
            (F.col("anomaly_count") / F.col("reading_count")) * 100)
        .withColumn("zone_status",
            F.when(F.col("anomaly_rate") > 10, "alert")
            .when(F.col("anomaly_rate") > 5, "warning")
            .otherwise("normal")
        )
    )

builder.add_gold_transform(
    name="zone_analytics",
    transform=zone_analytics,
    rules={
        "zone": [F.col("zone").isNotNull()],
        "avg_value": [F.col("avg_value").isNotNull()],
        "zone_status": [F.col("zone_status").isNotNull()]
    },
    table_name="zone_analytics",
    source_silvers=["processed_sensors"],
    description="Zone-based sensor analytics with anomaly monitoring"
)

# Gold Layer 2: Anomaly Summary
print("🥇 Building Gold Layer - Anomaly Summary...")

def anomaly_summary(spark, silvers):
    """Summary of anomalies and alerts."""
    sensors_df = silvers["processed_sensors"]

    return (sensors_df
        .filter(F.col("is_anomaly") == True)
        .groupBy("zone", "sensor_type", "anomaly_severity")
        .agg(
            F.count("*").alias("anomaly_count"),
            F.avg("value").alias("avg_anomaly_value"),
            F.min("value").alias("min_anomaly_value"),
            F.max("value").alias("max_anomaly_value"),
            F.min("timestamp").alias("first_anomaly"),
            F.max("timestamp").alias("last_anomaly")
        )
        .withColumn("requires_attention",
            F.when(F.col("anomaly_severity").isin(["critical", "high"]), True)
            .otherwise(False)
        )
        .orderBy(F.desc("anomaly_count"))
    )

builder.add_gold_transform(
    name="anomaly_summary",
    transform=anomaly_summary,
    rules={
        "anomaly_count": [F.col("anomaly_count") > 0],
        "anomaly_severity": [F.col("anomaly_severity").isNotNull()],
        "requires_attention": [F.col("requires_attention").isNotNull()]
    },
    table_name="anomaly_summary",
    source_silvers=["processed_sensors"],
    description="Anomaly summary with severity classification and attention flags"
)

# Gold Layer 3: Maintenance Dashboard
print("🥇 Building Gold Layer - Maintenance Dashboard...")

def maintenance_dashboard(spark, silvers):
    """Maintenance dashboard for device management."""
    health_df = silvers["device_health"]
    anomaly_df = silvers["processed_sensors"]

    # Get recent anomalies by device
    recent_anomalies = (anomaly_df
        .filter(F.col("is_anomaly") == True)
        .groupBy("sensor_id")
        .agg(
            F.count("*").alias("recent_anomalies"),
            F.max("timestamp").alias("last_anomaly")
        )
    )

    return (health_df
        .join(recent_anomalies, "sensor_id", "left")
        .fillna({"recent_anomalies": 0})
        .withColumn("priority_score",
            F.when(F.col("device_health") == "poor", 100)
            .when(F.col("device_health") == "low_battery", 80)
            .when(F.col("recent_anomalies") > 5, 70)
            .when(F.col("device_health") == "poor_signal", 60)
            .when(F.col("device_health") == "fair", 40)
            .otherwise(20)
        )
        .withColumn("maintenance_priority",
            F.when(F.col("priority_score") >= 80, "urgent")
            .when(F.col("priority_score") >= 60, "high")
            .when(F.col("priority_score") >= 40, "medium")
            .otherwise("low")
        )
        .orderBy(F.desc("priority_score"))
    )

builder.add_gold_transform(
    name="maintenance_dashboard",
    transform=maintenance_dashboard,
    rules={
        "sensor_id": [F.col("sensor_id").isNotNull()],
        "maintenance_priority": [F.col("maintenance_priority").isNotNull()],
        "priority_score": [F.col("priority_score") >= 0]
    },
    table_name="maintenance_dashboard",
    source_silvers=["device_health", "processed_sensors"],
    description="Maintenance dashboard with priority scoring for device management"
)
```

## Step 3: Execute the Pipeline (2 minutes)

```python
# Build and run the pipeline
print("🚀 Building complete IoT analytics pipeline...")
pipeline = builder.to_pipeline()

print("📊 Executing pipeline...")
result = pipeline.initial_load(bronze_sources={"sensor_readings": sensor_df})

# Check results
print(f"\n✅ Pipeline completed: {result.success}")
print(f"📈 Total rows processed: {result.totals['total_rows_written']}")
print(f"⏱️  Execution time: {result.totals['total_duration_secs']:.2f}s")
print(f"🎯 Overall validation rate: {result.totals.get('overall_validation_rate', 0):.1f}%")
```

## Step 4: Explore Your IoT Analytics (2 minutes)

```python
# Show all created tables
print("\n📋 Created IoT Analytics Tables:")
spark.sql("SHOW TABLES IN iot_analytics").show()

# Zone Analytics
print("\n🏢 Zone Analytics (by sensor type):")
spark.table("iot_analytics.zone_analytics").show(10)

# Anomaly Summary
print("\n⚠️  Anomaly Summary:")
spark.table("iot_analytics.anomaly_summary").show()

# Device Health
print("\n🔧 Device Health Status:")
spark.table("iot_analytics.device_health").show(10)

# Maintenance Dashboard
print("\n🛠️  Maintenance Dashboard (Priority Order):")
spark.table("iot_analytics.maintenance_dashboard").show(10)
```

## Step 5: IoT Insights & Alerts (Bonus)

```python
# Calculate key IoT metrics
print("\n💡 Key IoT Insights:")

# Total anomalies detected
total_anomalies = spark.table("iot_analytics.anomaly_summary").agg(F.sum("anomaly_count")).collect()[0][0]
print(f"🚨 Total Anomalies Detected: {total_anomalies}")

# Critical alerts
critical_alerts = spark.table("iot_analytics.anomaly_summary").filter(F.col("anomaly_severity") == "critical").count()
print(f"🔴 Critical Alerts: {critical_alerts}")

# Devices needing maintenance
urgent_maintenance = spark.table("iot_analytics.maintenance_dashboard").filter(F.col("maintenance_priority") == "urgent").count()
print(f"⚡ Devices Needing Urgent Maintenance: {urgent_maintenance}")

# Average zone health
avg_uptime = spark.table("iot_analytics.device_health").agg(F.avg("uptime_percentage")).collect()[0][0]
print(f"📊 Average Device Uptime: {avg_uptime:.1f}%")

# Battery levels
low_battery_devices = spark.table("iot_analytics.device_health").filter(F.col("min_battery_level") < 20).count()
print(f"🔋 Devices with Low Battery: {low_battery_devices}")

# Zone status summary
zone_status = spark.table("iot_analytics.zone_analytics").groupBy("zone_status").count().collect()
print("\n🏢 Zone Status Summary:")
for status in zone_status:
    print(f"   {status.zone_status}: {status['count']} zones")
```

## What You've Built

🎉 **Congratulations!** You've created a complete IoT analytics pipeline with:

- **Sensor Data Processing**: Real-time ingestion and validation of sensor data
- **Anomaly Detection**: Automatic detection of unusual sensor readings
- **Device Health Monitoring**: Battery levels, connectivity, and uptime tracking
- **Zone Analytics**: Building and area-based sensor monitoring
- **Maintenance Management**: Priority-based device maintenance scheduling
- **Alert System**: Severity-based anomaly classification and alerting

## Next Steps

### Try Real-time Processing
```python
# Simulate streaming data
streaming_data = create_sensor_data(spark, 100)  # New sensor readings
result = pipeline.run_incremental(bronze_sources={"sensor_readings": streaming_data})
```

### Debug Individual Components
```python
# Test anomaly detection
processed_result = pipeline.execute_silver_step("processed_sensors")
print(f"Anomalies detected: {processed_result.output_count}")

# Test device health monitoring
health_result = pipeline.execute_silver_step("device_health")
print(f"Devices monitored: {health_result.output_count}")
```

### Add Advanced Analytics
- **Predictive Maintenance**: ML models for failure prediction
- **Energy Optimization**: Power consumption analysis and optimization
- **Environmental Monitoring**: Air quality and environmental impact tracking
- **Security Analytics**: Unauthorized access and security breach detection
- **Performance Optimization**: Sensor network optimization and load balancing

## Customization Ideas

1. **Real Sensor Data**: Connect to your actual IoT devices and sensors
2. **Custom Anomaly Rules**: Add domain-specific anomaly detection logic
3. **Machine Learning**: Integrate ML models for predictive analytics
4. **Alert Integration**: Connect to your notification systems (email, Slack, SMS)
5. **Dashboard Integration**: Connect to Grafana, Tableau, or Power BI
6. **Edge Computing**: Deploy processing closer to sensors for real-time response

## IoT-Specific Features

### Time Series Analysis
```python
# Add time series analysis for trend detection
def trend_analysis(spark, silvers):
    # Moving averages, trend detection, seasonal patterns
    pass
```

### Geospatial Analytics
```python
# Add location-based analytics
def geospatial_analytics(spark, silvers):
    # Geographic clustering, proximity analysis, location-based alerts
    pass
```

### Multi-Sensor Correlation
```python
# Correlate data across different sensor types
def sensor_correlation(spark, silvers):
    # Temperature vs humidity, vibration vs pressure correlations
    pass
```

## Need Help?

- **[User Guide](USER_GUIDE.md)** - Learn advanced features
- **[API Reference](API_REFERENCE.md)** - Complete API documentation
- **[Examples](examples/)** - More working examples
- **[Troubleshooting](TROUBLESHOOTING.md)** - Common issues and solutions

---

**🚀 You're ready to build production IoT analytics!** Start with this foundation and customize it for your specific sensor networks and use cases.
