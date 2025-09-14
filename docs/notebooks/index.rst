Interactive Notebooks
=====================

This directory contains Jupyter notebooks for interactive learning with SparkForge.

Getting Started
---------------

Prerequisites
~~~~~~~~~~~~~

1. **Install Jupyter:**
   .. code-block:: bash

      pip install jupyter notebook

2. **Install SparkForge:**
   .. code-block:: bash

      pip install sparkforge pyspark

3. **Start Jupyter:**
   .. code-block:: bash

      jupyter notebook

Running the Notebooks
~~~~~~~~~~~~~~~~~~~~~

1. Navigate to this directory: ``cd sparkforge/notebooks``
2. Start Jupyter: ``jupyter notebook``
3. Open the notebook you want to run
4. Execute cells step by step (Shift+Enter)

Available Notebooks
-------------------

Hello World (``01_hello_world.ipynb``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Perfect for beginners!**

- Learn the basics of Bronze → Silver → Gold architecture
- Build your first simple pipeline
- Understand step-by-step debugging
- **Time**: 15-20 minutes

Progressive Learning (``02_progressive_examples.ipynb``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Build complexity gradually**

- Start with Bronze-only pipelines
- Add Silver transformations step by step
- Finish with complete Gold analytics
- **Time**: 30-45 minutes

E-commerce Analytics (``03_ecommerce_analytics.ipynb``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Real business use case**

- Build a complete e-commerce pipeline
- Customer segmentation and analytics
- Sales performance tracking
- **Time**: 45-60 minutes

IoT Data Processing (``04_iot_data_processing.ipynb``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Advanced time-series processing**

- Sensor data ingestion and validation
- Anomaly detection algorithms
- Real-time analytics and alerting
- **Time**: 60-90 minutes

Business Intelligence (``05_business_intelligence.ipynb``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Executive dashboards and KPIs**

- Build comprehensive BI analytics
- Customer lifetime value analysis
- Operational metrics and reporting
- **Time**: 60-90 minutes

Learning Path
-------------

Beginner (Start Here)
~~~~~~~~~~~~~~~~~~~~~

1. **Hello World** → Learn the basics
2. **Progressive Examples** → Build complexity gradually

Intermediate
~~~~~~~~~~~~

3. **E-commerce Analytics** → Real business use case
4. **Decision Trees** → Learn configuration choices

Advanced
~~~~~~~~

5. **IoT Data Processing** → Advanced time-series
6. **Business Intelligence** → Complex analytics

Tips for Success
----------------

Running Notebooks
~~~~~~~~~~~~~~~~~

- **Execute cells in order**: Don't skip ahead
- **Read the explanations**: Each cell has learning content
- **Try the exercises**: Hands-on practice is key
- **Experiment**: Modify code and see what happens

Troubleshooting
~~~~~~~~~~~~~~~

- **Spark errors**: Make sure Java is installed
- **Import errors**: Check SparkForge installation
- **Memory issues**: Restart kernel if needed
- **Performance**: Use smaller datasets for learning

Getting Help
~~~~~~~~~~~~

- **Check the documentation**: :doc:`../user_guide`
- **Look at examples**: :doc:`../examples/index` directory
- **Review API reference**: :doc:`../api_reference`
- **Ask questions**: Create an issue on GitHub

Customization
-------------

Using Your Own Data
~~~~~~~~~~~~~~~~~~~

Replace the sample data in notebooks with your own:

.. code-block:: python

   # Instead of sample data
   your_df = spark.read.parquet("path/to/your/data.parquet")

   # Use in pipeline
   result = pipeline.initial_load(bronze_sources={"your_table": your_df})

Adding Your Own Transformations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   def your_custom_transform(spark, bronze_df, prior_silvers):
       # Your custom logic here
       return bronze_df.withColumn("new_column", F.lit("value"))

   builder.add_silver_transform(
       name="your_step",
       source_bronze="source_table",
       transform=your_custom_transform,
       rules={"new_column": [F.col("new_column").isNotNull()]},
       table_name="your_table"
   )

Performance Tips
----------------

For Learning
~~~~~~~~~~~~

- Use small datasets (100-1000 rows)
- Run on local mode (``local[*]``)
- Enable verbose logging for debugging

For Production
~~~~~~~~~~~~~~

- Use larger datasets for realistic testing
- Configure appropriate parallel workers
- Enable unified execution for performance
- Use incremental processing for large datasets

Contributing
------------

Want to add a new notebook? Here's how:

1. **Create a new notebook** with descriptive name
2. **Follow the structure**: Introduction → Setup → Examples → Exercises → Summary
3. **Include learning objectives** and time estimates
4. **Add hands-on exercises** for practice
5. **Test thoroughly** before submitting
6. **Update this README** with the new notebook

Notebook Template
~~~~~~~~~~~~~~~~~

.. code-block:: python

   # Cell 1: Introduction and learning objectives
   # Cell 2: Setup and imports
   # Cell 3: Sample data creation
   # Cell 4: Pipeline building (step by step)
   # Cell 5: Execution and results
   # Cell 6: Hands-on exercises
   # Cell 7: Summary and next steps

.. admonition:: Happy Learning! 🚀

   Start with the Hello World notebook and work your way up to advanced topics. Each notebook builds on the previous ones, so follow the learning path for the best experience.
