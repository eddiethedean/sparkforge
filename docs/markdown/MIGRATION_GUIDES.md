# Migration Guides - From Other Tools to SparkForge

This guide helps you migrate from popular data pipeline tools to SparkForge. Each section covers the key differences and provides migration examples.

## Table of Contents

1. [From Apache Airflow](#from-apache-airflow)
2. [From dbt (Data Build Tool)](#from-dbt-data-build-tool)
3. [From Custom Spark Scripts](#from-custom-spark-scripts)
4. [From Apache Beam](#from-apache-beam)
5. [From Luigi](#from-luigi)
6. [Migration Checklist](#migration-checklist)

---

## From Apache Airflow

### Key Differences

| Airflow | SparkForge |
|---------|------------|
| DAG-based orchestration | Pipeline-based processing |
| Task dependencies | Automatic dependency management |
| Python operators | Built-in Spark operations |
| Manual scheduling | Integrated execution modes |
| Complex error handling | Built-in validation and retry |

### Migration Example

**Before (Airflow DAG):**
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def extract_data():
    # Extract data from source
    pass

def transform_data():
    # Transform data
    pass

def load_data():
    # Load to destination
    pass

dag = DAG(
    'my_pipeline',
    default_args={
        'start_date': datetime(2024, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    schedule_interval='@daily'
)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    dag=dag
)

extract_task >> transform_task >> load_task
```

**After (SparkForge):**
```python
from sparkforge import PipelineBuilder
from pyspark.sql import functions as F

# Initialize Spark
spark = SparkSession.builder.appName("Migrated Pipeline").getOrCreate()

# Build pipeline
builder = PipelineBuilder(spark=spark, schema="my_schema")

# Bronze: Extract and validate
builder.with_bronze_rules(
    name="raw_data",
    rules={
        "id": [F.col("id").isNotNull()],
        "value": [F.col("value") > 0]
    }
)

# Silver: Transform
def transform_data(spark, bronze_df, prior_silvers):
    return bronze_df.withColumn("processed_at", F.current_timestamp())

builder.add_silver_transform(
    name="transformed_data",
    source_bronze="raw_data",
    transform=transform_data,
    rules={"processed_at": [F.col("processed_at").isNotNull()]},
    table_name="transformed_data"
)

# Gold: Load and aggregate
def create_analytics(spark, silvers):
    return silvers["transformed_data"].groupBy("category").count()

builder.add_gold_transform(
    name="analytics",
    transform=create_analytics,
    rules={"category": [F.col("category").isNotNull()]},
    table_name="analytics",
    source_silvers=["transformed_data"]
)

# Execute
pipeline = builder.to_pipeline()
result = pipeline.initial_load(bronze_sources={"raw_data": source_df})
```

### Migration Benefits

- **Simplified Orchestration**: No need to manage DAGs and task dependencies
- **Built-in Validation**: Automatic data quality checks
- **Integrated Scheduling**: Built-in execution modes (incremental, full refresh)
- **Better Error Handling**: Comprehensive validation and error reporting
- **Performance**: Parallel execution and optimization

---

## From dbt (Data Build Tool)

### Key Differences

| dbt | SparkForge |
|-----|------------|
| SQL-based transformations | Python + Spark transformations |
| Model dependencies | Automatic dependency management |
| Data warehouse focused | Spark + Delta Lake focused |
| Testing framework | Built-in validation |
| Incremental models | Built-in incremental processing |

### Migration Example

**Before (dbt models):**
```sql
-- models/staging/stg_events.sql
SELECT
    event_id,
    user_id,
    event_type,
    timestamp,
    properties
FROM {{ source('raw', 'events') }}
WHERE event_type IS NOT NULL

-- models/marts/daily_events.sql
SELECT
    DATE(timestamp) as event_date,
    event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users
FROM {{ ref('stg_events') }}
GROUP BY DATE(timestamp), event_type

-- models/schema.yml
version: 2
models:
  - name: stg_events
    tests:
      - not_null:
          column_name: event_id
      - not_null:
          column_name: user_id
  - name: daily_events
    tests:
      - not_null:
          column_name: event_date
```

**After (SparkForge):**
```python
from sparkforge import PipelineBuilder
from pyspark.sql import functions as F

# Build pipeline
builder = PipelineBuilder(
    spark=spark,
    schema="analytics",
    min_bronze_rate=95.0,
    min_silver_rate=98.0,
    min_gold_rate=99.0
)

# Bronze: Raw data ingestion (equivalent to dbt sources)
builder.with_bronze_rules(
    name="events",
    rules={
        "event_id": [F.col("event_id").isNotNull()],
        "user_id": [F.col("user_id").isNotNull()],
        "event_type": [F.col("event_type").isNotNull()],
        "timestamp": [F.col("timestamp").isNotNull()]
    },
    incremental_col="timestamp"
)

# Silver: Staging transformation (equivalent to dbt staging models)
def stage_events(spark, bronze_df, prior_silvers):
    return (bronze_df
        .filter(F.col("event_type").isNotNull())
        .withColumn("processed_at", F.current_timestamp())
    )

builder.add_silver_transform(
    name="stg_events",
    source_bronze="events",
    transform=stage_events,
    rules={
        "event_id": [F.col("event_id").isNotNull()],
        "user_id": [F.col("user_id").isNotNull()],
        "processed_at": [F.col("processed_at").isNotNull()]
    },
    table_name="stg_events"
)

# Gold: Mart transformation (equivalent to dbt mart models)
def daily_events_analytics(spark, silvers):
    events_df = silvers["stg_events"]
    return (events_df
        .withColumn("event_date", F.date_trunc("day", "timestamp"))
        .groupBy("event_date", "event_type")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("user_id").alias("unique_users")
        )
    )

builder.add_gold_transform(
    name="daily_events",
    transform=daily_events_analytics,
    rules={
        "event_date": [F.col("event_date").isNotNull()],
        "event_type": [F.col("event_type").isNotNull()],
        "event_count": [F.col("event_count") > 0]
    },
    table_name="daily_events",
    source_silvers=["stg_events"]
)

# Execute
pipeline = builder.to_pipeline()
result = pipeline.initial_load(bronze_sources={"events": events_df})
```

### Migration Benefits

- **Python Flexibility**: Use Python for complex transformations
- **Spark Performance**: Leverage Spark's distributed processing
- **Built-in Validation**: Automatic data quality checks
- **Delta Lake Integration**: ACID transactions and time travel
- **Parallel Execution**: Automatic parallelization of independent steps

---

## From Custom Spark Scripts

### Key Differences

| Custom Spark Scripts | SparkForge |
|----------------------|------------|
| Manual pipeline orchestration | Automated pipeline management |
| Custom validation logic | Built-in validation framework |
| Manual error handling | Comprehensive error handling |
| Custom monitoring | Integrated monitoring and logging |
| Manual dependency management | Automatic dependency resolution |

### Migration Example

**Before (Custom Spark Script):**
```python
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_data(df):
    """Custom validation function"""
    total_rows = df.count()
    valid_rows = df.filter(
        F.col("user_id").isNotNull() &
        F.col("amount") > 0
    ).count()

    validation_rate = (valid_rows / total_rows) * 100
    logger.info(f"Validation rate: {validation_rate:.2f}%")

    if validation_rate < 95:
        raise Exception(f"Validation failed: {validation_rate:.2f}% < 95%")

    return df.filter(F.col("user_id").isNotNull() & F.col("amount") > 0)

def transform_data(df):
    """Custom transformation function"""
    return (df
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("amount_category",
            F.when(F.col("amount") > 100, "high")
            .otherwise("low")
        )
    )

def create_analytics(df):
    """Custom analytics function"""
    return (df
        .groupBy("amount_category")
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("amount").alias("total_amount")
        )
    )

def main():
    spark = SparkSession.builder.appName("Custom Pipeline").getOrCreate()

    try:
        # Read data
        df = spark.read.csv("data/transactions.csv", header=True, inferSchema=True)
        logger.info(f"Read {df.count()} rows")

        # Validate data
        validated_df = validate_data(df)
        logger.info(f"Validated {validated_df.count()} rows")

        # Transform data
        transformed_df = transform_data(validated_df)
        logger.info(f"Transformed {transformed_df.count()} rows")

        # Create analytics
        analytics_df = create_analytics(transformed_df)
        logger.info(f"Created analytics for {analytics_df.count()} categories")

        # Write results
        analytics_df.write.mode("overwrite").parquet("output/analytics")
        logger.info("Pipeline completed successfully")

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
```

**After (SparkForge):**
```python
from sparkforge import PipelineBuilder
from pyspark.sql import SparkSession, functions as F

# Initialize Spark
spark = SparkSession.builder.appName("SparkForge Pipeline").getOrCreate()

# Build pipeline with built-in features
builder = PipelineBuilder(
    spark=spark,
    schema="analytics",
    min_bronze_rate=95.0,  # Built-in validation threshold
    min_silver_rate=98.0,
    min_gold_rate=99.0,
    verbose=True  # Built-in logging
)

# Bronze: Data ingestion with validation
builder.with_bronze_rules(
    name="transactions",
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "amount": [F.col("amount") > 0]
    }
)

# Silver: Data transformation
def transform_transactions(spark, bronze_df, prior_silvers):
    return (bronze_df
        .withColumn("processed_at", F.current_timestamp())
        .withColumn("amount_category",
            F.when(F.col("amount") > 100, "high")
            .otherwise("low")
        )
    )

builder.add_silver_transform(
    name="transformed_transactions",
    source_bronze="transactions",
    transform=transform_transactions,
    rules={
        "processed_at": [F.col("processed_at").isNotNull()],
        "amount_category": [F.col("amount_category").isNotNull()]
    },
    table_name="transformed_transactions"
)

# Gold: Analytics
def create_transaction_analytics(spark, silvers):
    transactions_df = silvers["transformed_transactions"]
    return (transactions_df
        .groupBy("amount_category")
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("amount").alias("total_amount")
        )
    )

builder.add_gold_transform(
    name="transaction_analytics",
    transform=create_transaction_analytics,
    rules={
        "amount_category": [F.col("amount_category").isNotNull()],
        "transaction_count": [F.col("transaction_count") > 0]
    },
    table_name="transaction_analytics",
    source_silvers=["transformed_transactions"]
)

# Execute pipeline
pipeline = builder.to_pipeline()

# Read source data
source_df = spark.read.csv("data/transactions.csv", header=True, inferSchema=True)

# Run pipeline
result = pipeline.initial_load(bronze_sources={"transactions": source_df})

print(f"Pipeline completed: {result.success}")
print(f"Rows processed: {result.totals['total_rows_written']}")
```

### Migration Benefits

- **Less Boilerplate**: No need for custom validation, logging, and error handling
- **Built-in Features**: Automatic validation, monitoring, and parallel execution
- **Better Error Handling**: Comprehensive error reporting and debugging
- **Performance Optimization**: Automatic parallelization and optimization
- **Maintainability**: Cleaner, more maintainable code

---

## From Apache Beam

### Key Differences

| Apache Beam | SparkForge |
|-------------|------------|
| Unified programming model | Spark-specific optimizations |
| Multi-language support | Python-focused |
| Complex pipeline definition | Simple pipeline builder |
| Manual windowing | Built-in time-based processing |
| Custom triggers | Built-in execution modes |

### Migration Example

**Before (Apache Beam):**
```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
from datetime import timedelta

def process_events(element):
    """Process individual events"""
    return {
        'user_id': element['user_id'],
        'event_type': element['event_type'],
        'processed_at': datetime.now().isoformat()
    }

def create_analytics(elements):
    """Create analytics from elements"""
    event_counts = {}
    for element in elements:
        event_type = element['event_type']
        event_counts[event_type] = event_counts.get(event_type, 0) + 1
    return event_counts

def run_pipeline():
    options = PipelineOptions()

    with beam.Pipeline(options=options) as pipeline:
        (pipeline
         | 'ReadEvents' >> beam.io.ReadFromText('data/events.json')
         | 'ParseJSON' >> beam.Map(lambda x: json.loads(x))
         | 'FilterValid' >> beam.Filter(lambda x: x.get('user_id') and x.get('event_type'))
         | 'ProcessEvents' >> beam.Map(process_events)
         | 'Window' >> beam.WindowInto(FixedWindows(3600))  # 1 hour windows
         | 'CreateAnalytics' >> beam.Map(create_analytics)
         | 'WriteResults' >> beam.io.WriteToText('output/analytics')
        )

if __name__ == '__main__':
    run_pipeline()
```

**After (SparkForge):**
```python
from sparkforge import PipelineBuilder
from pyspark.sql import SparkSession, functions as F

# Initialize Spark
spark = SparkSession.builder.appName("Migrated Beam Pipeline").getOrCreate()

# Build pipeline
builder = PipelineBuilder(
    spark=spark,
    schema="analytics",
    enable_parallel_silver=True,
    max_parallel_workers=4
)

# Bronze: Data ingestion
builder.with_bronze_rules(
    name="events",
    rules={
        "user_id": [F.col("user_id").isNotNull()],
        "event_type": [F.col("event_type").isNotNull()]
    },
    incremental_col="timestamp"
)

# Silver: Data processing
def process_events(spark, bronze_df, prior_silvers):
    return (bronze_df
        .withColumn("processed_at", F.current_timestamp())
        .filter(F.col("user_id").isNotNull() & F.col("event_type").isNotNull())
    )

builder.add_silver_transform(
    name="processed_events",
    source_bronze="events",
    transform=process_events,
    rules={
        "processed_at": [F.col("processed_at").isNotNull()],
        "event_type": [F.col("event_type").isNotNull()]
    },
    table_name="processed_events",
    watermark_col="timestamp"
)

# Gold: Analytics with windowing
def create_windowed_analytics(spark, silvers):
    events_df = silvers["processed_events"]
    return (events_df
        .withColumn("window", F.window("timestamp", "1 hour"))
        .groupBy("window", "event_type")
        .agg(F.count("*").alias("event_count"))
    )

builder.add_gold_transform(
    name="windowed_analytics",
    transform=create_windowed_analytics,
    rules={
        "window": [F.col("window").isNotNull()],
        "event_type": [F.col("event_type").isNotNull()],
        "event_count": [F.col("event_count") > 0]
    },
    table_name="windowed_analytics",
    source_silvers=["processed_events"]
)

# Execute
pipeline = builder.to_pipeline()

# Read source data
source_df = spark.read.json("data/events.json")

# Run pipeline
result = pipeline.initial_load(bronze_sources={"events": source_df})
```

### Migration Benefits

- **Simpler Syntax**: Less complex pipeline definition
- **Spark Optimization**: Leverage Spark's built-in optimizations
- **Built-in Windowing**: Automatic time-based processing
- **Better Performance**: Spark's mature execution engine
- **Easier Debugging**: Step-by-step execution capabilities

---

## From Luigi

### Key Differences

| Luigi | SparkForge |
|-------|------------|
| Task-based workflow | Pipeline-based processing |
| File-based dependencies | Automatic dependency management |
| Manual scheduling | Built-in execution modes |
| Custom validation | Built-in validation framework |
| Limited parallelization | Automatic parallel execution |

### Migration Example

**Before (Luigi):**
```python
import luigi
from luigi.contrib.spark import SparkSubmitTask
import json

class ExtractTask(luigi.Task):
    def output(self):
        return luigi.LocalTarget('data/raw_data.json')

    def run(self):
        # Extract data logic
        data = [{"id": 1, "value": 100}, {"id": 2, "value": 200}]
        with self.output().open('w') as f:
            json.dump(data, f)

class TransformTask(luigi.Task):
    def requires(self):
        return ExtractTask()

    def output(self):
        return luigi.LocalTarget('data/transformed_data.json')

    def run(self):
        # Transform data logic
        with self.input().open('r') as f:
            data = json.load(f)

        transformed_data = [{"id": item["id"], "processed_value": item["value"] * 2}
                           for item in data]

        with self.output().open('w') as f:
            json.dump(transformed_data, f)

class LoadTask(luigi.Task):
    def requires(self):
        return TransformTask()

    def output(self):
        return luigi.LocalTarget('data/final_analytics.json')

    def run(self):
        # Load and analytics logic
        with self.input().open('r') as f:
            data = json.load(f)

        analytics = {"total_records": len(data), "avg_value": sum(item["processed_value"] for item in data) / len(data)}

        with self.output().open('w') as f:
            json.dump(analytics, f)

if __name__ == '__main__':
    luigi.build([LoadTask()], local_scheduler=True)
```

**After (SparkForge):**
```python
from sparkforge import PipelineBuilder
from pyspark.sql import SparkSession, functions as F

# Initialize Spark
spark = SparkSession.builder.appName("Migrated Luigi Pipeline").getOrCreate()

# Build pipeline
builder = PipelineBuilder(spark=spark, schema="analytics")

# Bronze: Data extraction
builder.with_bronze_rules(
    name="raw_data",
    rules={
        "id": [F.col("id").isNotNull()],
        "value": [F.col("value") > 0]
    }
)

# Silver: Data transformation
def transform_data(spark, bronze_df, prior_silvers):
    return (bronze_df
        .withColumn("processed_value", F.col("value") * 2)
        .withColumn("processed_at", F.current_timestamp())
    )

builder.add_silver_transform(
    name="transformed_data",
    source_bronze="raw_data",
    transform=transform_data,
    rules={
        "processed_value": [F.col("processed_value") > 0],
        "processed_at": [F.col("processed_at").isNotNull()]
    },
    table_name="transformed_data"
)

# Gold: Analytics
def create_analytics(spark, silvers):
    transformed_df = silvers["transformed_data"]
    return (transformed_df
        .agg(
            F.count("*").alias("total_records"),
            F.avg("processed_value").alias("avg_value")
        )
    )

builder.add_gold_transform(
    name="analytics",
    transform=create_analytics,
    rules={
        "total_records": [F.col("total_records") > 0],
        "avg_value": [F.col("avg_value") > 0]
    },
    table_name="analytics",
    source_silvers=["transformed_data"]
)

# Execute
pipeline = builder.to_pipeline()

# Create sample data
data = [{"id": 1, "value": 100}, {"id": 2, "value": 200}]
source_df = spark.createDataFrame(data)

# Run pipeline
result = pipeline.initial_load(bronze_sources={"raw_data": source_df})
```

### Migration Benefits

- **Simplified Dependencies**: Automatic dependency management
- **Better Performance**: Spark's distributed processing
- **Built-in Validation**: No need for custom validation logic
- **Parallel Execution**: Automatic parallelization
- **Easier Maintenance**: Cleaner, more maintainable code

---

## Migration Checklist

Use this checklist to ensure a smooth migration to SparkForge:

### Pre-Migration
- [ ] **Analyze Current Pipeline**: Document existing pipeline structure and dependencies
- [ ] **Identify Data Sources**: List all data sources and their formats
- [ ] **Document Transformations**: Catalog all data transformations and business logic
- [ ] **Review Validation Rules**: Identify existing data quality checks
- [ ] **Assess Performance Requirements**: Document current performance metrics

### During Migration
- [ ] **Setup SparkForge Environment**: Install SparkForge and configure Spark
- [ ] **Migrate Bronze Layer**: Convert data ingestion and basic validation
- [ ] **Migrate Silver Layer**: Convert data transformations and cleaning
- [ ] **Migrate Gold Layer**: Convert analytics and aggregations
- [ ] **Configure Validation**: Set appropriate validation thresholds
- [ ] **Test Pipeline**: Validate pipeline execution and results

### Post-Migration
- [ ] **Performance Testing**: Compare performance with original pipeline
- [ ] **Data Quality Validation**: Ensure data quality is maintained or improved
- [ ] **Monitoring Setup**: Configure monitoring and alerting
- [ ] **Documentation Update**: Update documentation and runbooks
- [ ] **Team Training**: Train team on SparkForge concepts and features

### Migration Benefits Summary

| Feature | Before | After |
|---------|--------|-------|
| **Pipeline Definition** | Complex, tool-specific | Simple, Python-based |
| **Dependency Management** | Manual | Automatic |
| **Validation** | Custom implementation | Built-in framework |
| **Error Handling** | Manual | Comprehensive |
| **Performance** | Limited parallelization | Automatic optimization |
| **Monitoring** | Custom solutions | Integrated monitoring |
| **Maintenance** | High complexity | Simplified maintenance |

### Getting Help

- **[User Guide](USER_GUIDE.md)** - Learn SparkForge concepts
- **[Examples](examples/)** - Working examples for different scenarios
- **[API Reference](API_REFERENCE.md)** - Complete API documentation
- **[Troubleshooting](TROUBLESHOOTING.md)** - Common issues and solutions

---

**🚀 Ready to migrate?** Start with the [5-Minute Quick Start](QUICK_START_5_MIN.md) to get familiar with SparkForge, then use this guide to migrate your existing pipelines.
