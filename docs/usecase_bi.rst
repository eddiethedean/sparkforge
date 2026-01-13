Business Intelligence Use Case
============================

This guide demonstrates building a comprehensive business intelligence pipeline using SparkForge for analytics and reporting.

Overview
--------

Build data pipelines that transform raw business data into actionable insights for decision-making.

Key Features
------------

- **Data Integration**: Combine multiple data sources
- **Real-time Analytics**: Process streaming data
- **Dashboard-Ready**: Create datasets for BI tools
- **Scalable Processing**: Handle large volumes of data

Pipeline Components
------------------

**Bronze Layer**: Raw data ingestion from various sources
**Silver Layer**: Data cleaning and standardization
**Gold Layer**: Business metrics and KPIs

Example Pipeline
---------------

.. code-block:: python

   from pyspark.sql import SparkSession
   from pipeline_builder import PipelineBuilder
   from pipeline_builder.engine_config import configure_engine
   from pipeline_builder.functions import get_default_functions

   # Configure engine (required!)
   spark = SparkSession.builder.appName("BIPipeline").getOrCreate()
   configure_engine(spark=spark)
   F = get_default_functions()

   # Build pipeline
   builder = PipelineBuilder(spark=spark, schema="bi_analytics")
   
   # Bronze: Multiple data sources
   builder.with_bronze_rules(
       name="sales",
       rules={"sale_id": [F.col("sale_id").isNotNull()]},
       incremental_col="sale_date"
   )
   
   builder.with_bronze_rules(
       name="customers",
       rules={"customer_id": [F.col("customer_id").isNotNull()]}
   )
   
   # Silver: Clean and standardize
   def clean_sales(spark, bronze_df, prior_silvers):
       F = get_default_functions()
       return bronze_df.filter(F.col("amount") > 0)
   
   builder.add_silver_transform(
       name="clean_sales",
       source_bronze="sales",
       transform=clean_sales,
       rules={"sale_id": [F.col("sale_id").isNotNull()]},
       table_name="clean_sales"
   )
   
   # Gold: Business metrics
   def sales_metrics(spark, silvers):
       F = get_default_functions()
       return silvers["clean_sales"].groupBy("region").agg(
           F.count("*").alias("total_sales"),
           F.sum("amount").alias("total_revenue")
       )
   
   builder.add_gold_transform(
       name="sales_metrics",
       transform=sales_metrics,
       rules={"region": [F.col("region").isNotNull()]},
       table_name="sales_metrics",
       source_silvers=["clean_sales"]
   )
   
   # Execute
   pipeline = builder.to_pipeline()
   result = pipeline.run_initial_load(
       bronze_sources={"sales": sales_df, "customers": customers_df}
   )

Key Metrics
-----------

The pipeline produces these business metrics:

- **Revenue Metrics**: Total revenue, average order value, growth rates
- **Customer Metrics**: Customer count, retention rates, lifetime value
- **Product Metrics**: Best sellers, category performance, inventory trends
- **Operational Metrics**: Processing times, error rates, data quality scores

For more BI examples, see the `Examples <../examples/>`_ directory.
