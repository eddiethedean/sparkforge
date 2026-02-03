"""
Pipeline Builder - A production-ready data pipeline framework for Apache Spark & Delta Lake.

This framework transforms complex Spark + Delta Lake development into clean, maintainable code
using the proven Medallion Architecture (Bronze → Silver → Gold). Features include:

- **Robust Validation System**: Early error detection with clear validation messages
- **Simplified Pipeline Building**: 70% less boilerplate compared to raw Spark
- **Auto-inference**: Automatic dependency detection and validation
- **Step-by-step Debugging**: Easy troubleshooting of complex pipelines
- **Delta Lake Integration**: ACID transactions and time travel
- **Multi-schema Support**: Enterprise-ready cross-schema data flows
- **Comprehensive Error Handling**: Detailed error messages with suggestions
- **Extensive Test Coverage**: 1,400+ comprehensive tests ensuring reliability
- **Flexible Engine Support**: Works with PySpark or mock-spark

Quick Start:
    # Install: pip install pipeline_builder[pyspark]  # or pipeline_builder[mock]
    from pipeline_builder import PipelineBuilder
    from pipeline_builder.functions import get_default_functions as F

    # Initialize Spark (works with PySpark or mock-spark)
    spark = SparkSession.builder.appName("MyPipeline").getOrCreate()

    # Create sample data
    data = [("user1", "click", 100), ("user2", "purchase", 200)]
    df = spark.createDataFrame(data, ["user_id", "action", "value"])  # type: ignore[attr-defined]

    # Build pipeline with validation
    builder = PipelineBuilder(spark=spark, schema="analytics")

    # Bronze: Raw data validation (required)
    builder.with_bronze_rules(
        name="events",
        rules={"user_id": [F.col("user_id").isNotNull()]},
        incremental_col="timestamp"
    )

    # Silver: Data transformation (required)
    builder.add_silver_transform(
        name="clean_events",
        source_bronze="events",
        transform=lambda spark, df, silvers: df.filter(F.col("value")  # type: ignore[attr-defined] > 50),
        rules={"value": [F.col("value") > 50]},
        table_name="clean_events"
    )

    # Gold: Business analytics (required)
    builder.add_gold_transform(
        name="daily_metrics",
        transform=lambda spark, silvers: silvers["clean_events"].groupBy("action").agg(F.count("*").alias("count")),
        rules={"count": [F.col("count") > 0]},
        table_name="daily_metrics",
        source_silvers=["clean_events"]
    )

    # Execute pipeline
    pipeline = builder.to_pipeline()
    result = pipeline.run_initial_load(bronze_sources={"events": df})
    print(f"✅ Pipeline completed: {result.status}")

Validation Requirements:
    All pipeline steps must have validation rules. Invalid configurations are rejected
    with clear error messages to help you fix issues quickly.

    # ✅ Valid - has required validation rules
    BronzeStep(name="events", rules={"id": [F.col("id").isNotNull()]})

    # ❌ Invalid - empty rules rejected
    BronzeStep(name="events", rules={})  # ValidationError: Rules must be non-empty
"""

__version__ = "2.8.8"
__author__ = "Odos Matthews"
__email__ = "odosmatthews@gmail.com"
__description__ = "A simplified, production-ready data pipeline builder for Apache Spark and Delta Lake"


from typing import TYPE_CHECKING, Any

# Avoid eager imports so engine configuration can be set up first.
if TYPE_CHECKING:  # pragma: no cover - used only for type checkers
    from .pipeline import PipelineBuilder as PipelineBuilder
    from .pipeline import PipelineRunner as PipelineRunner
    from .writer import LogWriter as LogWriter


# Lazy import to avoid engine configuration during package import
def __getattr__(name: str) -> Any:
    if name in {"PipelineBuilder", "PipelineRunner"}:
        from .pipeline import builder as _builder, runner as _runner  # noqa: I001

        if name == "PipelineBuilder":
            return _builder.PipelineBuilder
        return _runner.PipelineRunner
    if name == "LogWriter":
        from .writer import core as _core  # noqa: I001

        return _core.LogWriter
    raise AttributeError(f"module {__name__} has no attribute {name}")


__all__ = ["PipelineBuilder", "PipelineRunner", "LogWriter"]
