E-commerce Analytics Use Case
============================

This guide demonstrates how to build a comprehensive e-commerce analytics pipeline using SparkForge's Medallion Architecture.

Overview
--------

The e-commerce pipeline processes customer data, orders, and product information to create business intelligence dashboards and reports.

Data Sources
------------

- **Customer Data**: User profiles, demographics, preferences
- **Order Data**: Transactions, order items, shipping information
- **Product Data**: Product catalog, categories, pricing
- **Web Analytics**: Page views, clicks, session data

Pipeline Architecture
---------------------

Bronze Layer
~~~~~~~~~~~~

Raw data ingestion with basic validation:

.. code-block:: python

   builder = PipelineBuilder(spark=spark, schema="ecommerce")
   
   # Customer data validation
   builder.with_bronze_rules(
       name="customers",
       rules={
           "customer_id": [F.col("customer_id").isNotNull()],
           "email": [F.col("email").contains("@")],
           "created_at": [F.col("created_at").isNotNull()]
       },
       incremental_col="created_at"
   )
   
   # Order data validation
   builder.with_bronze_rules(
       name="orders",
       rules={
           "order_id": [F.col("order_id").isNotNull()],
           "customer_id": [F.col("customer_id").isNotNull()],
           "total_amount": [F.col("total_amount") > 0],
           "order_date": [F.col("order_date").isNotNull()]
       },
       incremental_col="order_date"
   )

Silver Layer
~~~~~~~~~~~~

Data cleaning and enrichment:

.. code-block:: python

   # Clean customer data
   def clean_customers(spark, bronze_df, prior_silvers):
       return (bronze_df
           .filter(F.col("customer_id").isNotNull())
           .withColumn("age_group", 
               F.when(F.col("age") < 25, "18-24")
               .when(F.col("age") < 35, "25-34")
               .when(F.col("age") < 45, "35-44")
               .otherwise("45+"))
           .withColumn("is_premium", F.col("membership_type") == "premium")
       )
   
   builder.add_silver_transform(
       name="clean_customers",
       source_bronze="customers",
       transform=clean_customers,
       rules={"customer_id": [F.col("customer_id").isNotNull()]},
       table_name="clean_customers",
       watermark_col="created_at"
   )
   
   # Enrich orders with customer data
   def enrich_orders(spark, bronze_df, prior_silvers):
       customers = prior_silvers["clean_customers"]
       return (bronze_df
           .join(customers, "customer_id", "left")
           .withColumn("order_value_tier",
               F.when(F.col("total_amount") > 1000, "high")
               .when(F.col("total_amount") > 500, "medium")
               .otherwise("low"))
       )
   
   builder.add_silver_transform(
       name="enriched_orders",
       source_bronze="orders",
       transform=enrich_orders,
       rules={"order_id": [F.col("order_id").isNotNull()]},
       table_name="enriched_orders",
       watermark_col="order_date",
       depends_on=["clean_customers"]
   )

Gold Layer
~~~~~~~~~~

Business analytics and KPIs:

.. code-block:: python

   # Customer analytics
   def customer_analytics(spark, silvers):
       orders = silvers["enriched_orders"]
       return (orders
           .groupBy("customer_id", "age_group", "is_premium")
           .agg(
               F.count("*").alias("total_orders"),
               F.sum("total_amount").alias("lifetime_value"),
               F.max("order_date").alias("last_order_date"),
               F.avg("total_amount").alias("avg_order_value")
           )
           .withColumn("customer_tier",
               F.when(F.col("lifetime_value") > 5000, "VIP")
               .when(F.col("lifetime_value") > 1000, "Premium")
               .otherwise("Standard"))
       )
   
   builder.add_gold_transform(
       name="customer_analytics",
       transform=customer_analytics,
       rules={"customer_id": [F.col("customer_id").isNotNull()]},
       table_name="customer_analytics",
       source_silvers=["enriched_orders"]
   )
   
   # Sales analytics
   def sales_analytics(spark, silvers):
       orders = silvers["enriched_orders"]
       return (orders
           .groupBy(F.date_trunc("month", "order_date").alias("month"))
           .agg(
               F.count("*").alias("total_orders"),
               F.sum("total_amount").alias("total_revenue"),
               F.countDistinct("customer_id").alias("unique_customers"),
               F.avg("total_amount").alias("avg_order_value")
           )
       )
   
   builder.add_gold_transform(
       name="sales_analytics",
       transform=sales_analytics,
       rules={"month": [F.col("month").isNotNull()]},
       table_name="sales_analytics",
       source_silvers=["enriched_orders"]
   )

Execution
---------

.. code-block:: python

   # Build and execute pipeline
   pipeline = builder.to_pipeline()
   
   # Initial load
   result = pipeline.initial_load(bronze_sources={
       "customers": customers_df,
       "orders": orders_df
   })
   
   # Incremental updates
   result = pipeline.run_incremental(bronze_sources={
       "customers": new_customers_df,
       "orders": new_orders_df
   })

Key Metrics
-----------

The pipeline produces these business metrics:

- **Customer Lifetime Value**: Total spending per customer
- **Order Frequency**: Average orders per customer
- **Revenue Trends**: Monthly revenue and growth
- **Customer Segmentation**: Tier-based customer classification
- **Product Performance**: Best-selling items and categories

For the complete e-commerce guide with more examples, see: `USECASE_ECOMMERCE.md <markdown/USECASE_ECOMMERCE.md>`_
