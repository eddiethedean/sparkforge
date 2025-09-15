Decision Trees
==============

This guide provides decision trees to help you choose the right SparkForge configuration for your use case.

Pipeline Configuration
---------------------

**Starting a New Project**
- Use `PipelineBuilder` with default settings
- Begin with Bronze layer validation
- Add Silver transformations incrementally
- Create Gold analytics last

**Performance Optimization**
- Enable parallel execution for independent steps
- Use incremental processing with watermarking
- Configure appropriate quality thresholds
- Monitor execution metrics

**Data Quality**
- Set Bronze threshold to 95% for raw data
- Set Silver threshold to 98% for clean data
- Set Gold threshold to 99% for analytics
- Review validation failures regularly

For complete decision trees with detailed guidance, see: `DECISION_TREES.md <markdown/DECISION_TREES.md>`_
