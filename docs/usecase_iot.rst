IoT Data Processing Use Case
============================

This guide demonstrates building an IoT sensor data pipeline using SparkForge for real-time analytics and monitoring.

Overview
--------

Process high-volume sensor data from IoT devices to create real-time dashboards and predictive analytics.

Key Features
------------

- **Stream Processing**: Handle high-frequency sensor data
- **Real-time Analytics**: Process data as it arrives
- **Anomaly Detection**: Identify unusual patterns
- **Predictive Maintenance**: Forecast equipment failures

Pipeline Components
------------------

**Bronze Layer**: Raw sensor data ingestion
**Silver Layer**: Data cleaning and feature engineering
**Gold Layer**: Analytics and alerts

Example Pipeline
---------------

.. code-block:: python

   from pyspark.sql import SparkSession
   from pipeline_builder import PipelineBuilder
   from pipeline_builder.engine_config import configure_engine
   from pipeline_builder.functions import get_default_functions

   # Configure engine (required!)
   spark = SparkSession.builder.appName("IoTPipeline").getOrCreate()
   configure_engine(spark=spark)
   F = get_default_functions()

   # Build pipeline
   builder = PipelineBuilder(spark=spark, schema="iot_analytics")
   
   # Bronze: Sensor data validation
   builder.with_bronze_rules(
       name="sensor_data",
       rules={
           "sensor_id": [F.col("sensor_id").isNotNull()],
           "temperature": [F.col("temperature").between(-50, 150)],
           "humidity": [F.col("humidity").between(0, 100)],
           "timestamp": [F.col("timestamp").isNotNull()]
       },
       incremental_col="timestamp"
   )
   
   # Silver: Feature engineering
   def enrich_sensor_data(spark, bronze_df, prior_silvers):
       F = get_default_functions()
       return bronze_df.withColumn(
           "temperature_anomaly",
           F.when(
               (F.col("temperature") < 0) | (F.col("temperature") > 100),
               True
           ).otherwise(False)
       )
   
   builder.add_silver_transform(
       name="enriched_sensor_data",
       source_bronze="sensor_data",
       transform=enrich_sensor_data,
       rules={"sensor_id": [F.col("sensor_id").isNotNull()]},
       table_name="enriched_sensor_data"
   )
   
   # Gold: Aggregated metrics
   def sensor_metrics(spark, silvers):
       F = get_default_functions()
       return silvers["enriched_sensor_data"].groupBy("sensor_id").agg(
           F.avg("temperature").alias("avg_temperature"),
           F.max("temperature").alias("max_temperature"),
           F.count("*").alias("reading_count")
       )
   
   builder.add_gold_transform(
       name="sensor_metrics",
       transform=sensor_metrics,
       rules={"sensor_id": [F.col("sensor_id").isNotNull()]},
       table_name="sensor_metrics",
       source_silvers=["enriched_sensor_data"]
   )
   
   # Execute
   pipeline = builder.to_pipeline()
   result = pipeline.run_initial_load(bronze_sources={"sensor_data": sensor_df})

Key Metrics
-----------

The pipeline produces these IoT metrics:

- **Sensor Health**: Average readings, anomaly detection, failure rates
- **Environmental Metrics**: Temperature trends, humidity patterns, pressure changes
- **Device Performance**: Uptime, data quality, transmission success rates
- **Predictive Alerts**: Maintenance schedules, failure predictions, threshold breaches

For more IoT examples, see the `Examples <../examples/>`_ directory.
