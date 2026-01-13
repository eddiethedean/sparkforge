Progressive Examples
====================

This guide provides step-by-step examples that build complexity gradually, from simple pipelines to advanced use cases.

Learning Path
-------------

**Level 1: Basic Pipeline**
- Simple Bronze validation
- Single Silver transformation
- Basic Gold aggregation

**Level 2: Intermediate Pipeline**
- Multiple Bronze sources
- Silver dependencies
- Complex transformations

**Level 3: Advanced Pipeline**
- Incremental processing
- Error handling and recovery
- Service-oriented patterns

**Level 4: Production Pipeline**
- Performance optimization
- Monitoring and alerting
- Scalable architecture

Level 1: Basic Pipeline
------------------------

Simple Bronze → Silver → Gold pipeline with basic validation.

.. code-block:: python

   from pyspark.sql import SparkSession
   from pipeline_builder import PipelineBuilder
   from pipeline_builder.engine_config import configure_engine
   from pipeline_builder.functions import get_default_functions

   # Configure engine (required!)
   spark = SparkSession.builder.appName("BasicPipeline").getOrCreate()
   configure_engine(spark=spark)
   F = get_default_functions()

   # Create sample data
   data = [("user1", "click", 100), ("user2", "view", 200)]
   df = spark.createDataFrame(data, ["user_id", "action", "value"])

   # Build pipeline
   builder = PipelineBuilder(spark=spark, schema="basic")
   
   # Bronze: Validate raw data
   builder.with_bronze_rules(
       name="events",
       rules={
           "user_id": [F.col("user_id").isNotNull()],
           "action": [F.col("action").isNotNull()],
       }
   )
   
   # Silver: Clean data
   def clean_events(spark, bronze_df, prior_silvers):
       F = get_default_functions()
       return bronze_df.filter(F.col("value") > 0)
   
   builder.add_silver_transform(
       name="clean_events",
       source_bronze="events",
       transform=clean_events,
       rules={"user_id": [F.col("user_id").isNotNull()]},
       table_name="clean_events"
   )
   
   # Gold: Aggregate data
   def daily_summary(spark, silvers):
       F = get_default_functions()
       return silvers["clean_events"].groupBy("action").count()
   
   builder.add_gold_transform(
       name="daily_summary",
       transform=daily_summary,
       rules={"action": [F.col("action").isNotNull()]},
       table_name="daily_summary",
       source_silvers=["clean_events"]
   )
   
   # Execute
   pipeline = builder.to_pipeline()
   result = pipeline.run_initial_load(bronze_sources={"events": df})
   print(f"Status: {result.status.value}")

Level 2: Intermediate Pipeline
--------------------------------

Multiple sources with dependencies and complex transformations.

.. code-block:: python

   from pyspark.sql import SparkSession
   from pipeline_builder import PipelineBuilder
   from pipeline_builder.engine_config import configure_engine
   from pipeline_builder.functions import get_default_functions

   spark = SparkSession.builder.appName("IntermediatePipeline").getOrCreate()
   configure_engine(spark=spark)
   F = get_default_functions()

   # Multiple bronze sources
   orders_df = spark.createDataFrame([...], ["order_id", "customer_id", "amount"])
   customers_df = spark.createDataFrame([...], ["customer_id", "name", "segment"])

   builder = PipelineBuilder(spark=spark, schema="intermediate")
   
   # Bronze: Multiple sources
   builder.with_bronze_rules(
       name="orders",
       rules={"order_id": [F.col("order_id").isNotNull()]},
       incremental_col="order_date"
   )
   
   builder.with_bronze_rules(
       name="customers",
       rules={"customer_id": [F.col("customer_id").isNotNull()]}
   )
   
   # Silver: Join and enrich
   def enriched_orders(spark, bronze_df, prior_silvers):
       F = get_default_functions()
       customers = prior_silvers.get("clean_customers")
       if customers:
           return bronze_df.join(customers, "customer_id", "left")
       return bronze_df
   
   builder.add_silver_transform(
       name="enriched_orders",
       source_bronze="orders",
       transform=enriched_orders,
       rules={"order_id": [F.col("order_id").isNotNull()]},
       table_name="enriched_orders",
       source_silvers=["clean_customers"]
   )
   
   # Gold: Business metrics
   def customer_metrics(spark, silvers):
       F = get_default_functions()
       return silvers["enriched_orders"].groupBy("customer_segment").agg(
           F.count("*").alias("order_count"),
           F.sum("amount").alias("total_revenue")
       )
   
   builder.add_gold_transform(
       name="customer_metrics",
       transform=customer_metrics,
       rules={"customer_segment": [F.col("customer_segment").isNotNull()]},
       table_name="customer_metrics",
       source_silvers=["enriched_orders"]
   )
   
   pipeline = builder.to_pipeline()
   result = pipeline.run_initial_load(
       bronze_sources={"orders": orders_df, "customers": customers_df}
   )

Level 3: Advanced Pipeline
---------------------------

Incremental processing with error handling.

.. code-block:: python

   from pyspark.sql import SparkSession
   from pipeline_builder import PipelineBuilder
   from pipeline_builder.engine_config import configure_engine
   from pipeline_builder.functions import get_default_functions
   from pipeline_builder.models.exceptions import PipelineExecutionError

   spark = SparkSession.builder.appName("AdvancedPipeline").getOrCreate()
   configure_engine(spark=spark)
   F = get_default_functions()

   builder = PipelineBuilder(spark=spark, schema="advanced")
   
   # Bronze with incremental support
   builder.with_bronze_rules(
       name="events",
       rules={"user_id": [F.col("user_id").isNotNull()]},
       incremental_col="timestamp"
   )
   
   # Silver with error handling
   def safe_transform(spark, bronze_df, prior_silvers):
       F = get_default_functions()
       try:
           # Complex transformation logic
           return bronze_df.withColumn("processed", F.lit(True))
       except Exception as e:
           # Log error and return empty DataFrame
           print(f"Transform error: {e}")
           return spark.createDataFrame([], bronze_df.schema)
   
   builder.add_silver_transform(
       name="processed_events",
       source_bronze="events",
       transform=safe_transform,
       rules={"user_id": [F.col("user_id").isNotNull()]},
       table_name="processed_events"
   )
   
   pipeline = builder.to_pipeline()
   
   # Initial load
   result = pipeline.run_initial_load(bronze_sources={"events": historical_df})
   
   # Incremental load
   try:
       result = pipeline.run_incremental(bronze_sources={"events": new_data_df})
   except PipelineExecutionError as e:
       print(f"Execution failed: {e}")
       # Handle error appropriately

Level 4: Production Pipeline
-----------------------------

Production-ready pipeline with monitoring and optimization.

.. code-block:: python

   from pyspark.sql import SparkSession
   from pipeline_builder import PipelineBuilder
   from pipeline_builder.engine_config import configure_engine
   from pipeline_builder.functions import get_default_functions
   from pipeline_builder.writer import LogWriter

   # Production Spark configuration
   spark = SparkSession.builder \
       .appName("ProductionPipeline") \
       .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
       .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
       .config("spark.sql.adaptive.enabled", "true") \
       .getOrCreate()
   
   configure_engine(spark=spark)
   F = get_default_functions()

   # Build pipeline with strict validation
   builder = PipelineBuilder(
       spark=spark,
       schema="production",
       min_bronze_rate=95.0,
       min_silver_rate=98.0,
       min_gold_rate=99.0,
       verbose=True
   )
   
   # ... build pipeline steps ...
   
   pipeline = builder.to_pipeline()
   
   # Execute with logging
   log_writer = LogWriter(
       spark=spark,
       schema="monitoring",
       table_name="pipeline_logs"
   )
   
   result = pipeline.run_initial_load(bronze_sources={"events": source_df})
   
   # Log execution
   log_writer.append(result, run_id="run_123")
   
   # Monitor performance
   if result.duration_seconds > 300:  # 5 minutes
       print("⚠️  Pipeline execution exceeded threshold")
   
   # Check validation rates
   if result.metrics.total_rows_written < expected_rows:
       print("⚠️  Fewer rows written than expected")

Service-Oriented Patterns
--------------------------

Using services directly for advanced use cases.

.. code-block:: python

   from pipeline_builder.execution import ExecutionEngine
   from pipeline_builder.models import PipelineConfig, BronzeStep
   from pipeline_builder.models.enums import ExecutionMode

   # Create execution engine (services initialized internally)
   config = PipelineConfig.create_default(schema="production")
   engine = ExecutionEngine(spark=spark, config=config)
   
   # Access services directly
   # engine.validator - ExecutionValidator
   # engine.table_service - TableService
   # engine.write_service - WriteService
   # engine.transform_service - TransformService
   # engine.reporter - ExecutionReporter
   # engine.error_handler - ErrorHandler
   
   # Execute step with service composition
   bronze_step = BronzeStep(
       name="events",
       rules={"user_id": [F.col("user_id").isNotNull()]}
   )
   
   result = engine.execute_step(
       step=bronze_step,
       sources={"events": source_df},
       mode=ExecutionMode.INITIAL
   )

Error Handling Examples
-----------------------

Comprehensive error handling patterns.

.. code-block:: python

   from pipeline_builder.models.exceptions import (
       PipelineConfigurationError,
       PipelineExecutionError
   )
   
   try:
       # Validate pipeline
       errors = builder.validate_pipeline()
       if errors:
           raise PipelineConfigurationError(f"Validation errors: {errors}")
       
       # Execute pipeline
       result = pipeline.run_initial_load(bronze_sources={"events": df})
       
       if result.status.value != "completed":
           raise PipelineExecutionError(f"Pipeline failed: {result.errors}")
           
   except PipelineConfigurationError as e:
       print(f"Configuration error: {e}")
       # Fix configuration and retry
       
   except PipelineExecutionError as e:
       print(f"Execution error: {e}")
       # Log error, notify, and handle gracefully

Next Steps
----------

- Review the `User Guide <USER_GUIDE.md>`_ for comprehensive documentation
- Check `API Reference <api_reference.rst>`_ for detailed API documentation
- Explore `Examples <../examples/>`_ directory for more examples
- See `Architecture <Architecture.md>`_ for design patterns
