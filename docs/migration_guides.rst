Migration Guides
================

This guide helps you migrate from other data pipeline frameworks to SparkForge.

Migration Overview
-----------------

**From Pandas/NumPy**
- Convert to PySpark DataFrames
- Use SparkForge validation rules
- Leverage distributed processing

**From Apache Airflow**
- Replace DAGs with PipelineBuilder
- Use built-in dependency management
- Simplify error handling

**From Custom Pipelines**
- Adopt Medallion Architecture
- Use standardized validation
- Leverage parallel execution

Migration Steps
---------------

1. **Assess Current Pipeline**
2. **Design Medallion Layers**
3. **Convert Transformations**
4. **Configure Validation**
5. **Test and Deploy**

For complete migration guides with examples, see: `MIGRATION_GUIDES.md <markdown/MIGRATION_GUIDES.md>`_
