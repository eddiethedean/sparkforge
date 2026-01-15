#!/usr/bin/env python3
"""Demo of execution order feature in PipelineBuilder."""

# Configure engine BEFORE importing pipeline_builder
from pipeline_builder.engine_config import configure_engine
from sparkless import SparkSession, functions as mock_functions, spark_types as mock_types
from sparkless import AnalysisException as MockAnalysisException, Window as MockWindow
from sparkless.functions import desc as mock_desc
from sparkless import DataFrame as MockDataFrame, SparkSession as MockSparkSession
from sparkless import Column as MockColumn

configure_engine(
    functions=mock_functions,
    types=mock_types,
    analysis_exception=MockAnalysisException,
    window=MockWindow,
    desc=mock_desc,
    engine_name="mock",
    dataframe_cls=MockDataFrame,
    spark_session_cls=MockSparkSession,
    column_cls=MockColumn,
)

# Now import pipeline_builder
from pipeline_builder import PipelineBuilder
from pipeline_builder.functions import get_default_functions

# Get functions
F = get_default_functions()

# Create Spark session
spark = SparkSession("ExecutionOrderDemo")

print("=" * 70)
print("PipelineBuilder Execution Order Demo")
print("=" * 70)
print()

# Create pipeline builder
builder = PipelineBuilder(spark=spark, schema="demo_schema")

print("📝 Building pipeline with multiple steps...")
print()

# Add bronze step
builder.with_bronze_rules(
    name="bronze_events",
    rules={"user_id": [F.col("user_id").isNotNull()]},
    incremental_col="timestamp",
)
print("✅ Added Bronze step: bronze_events")

# Add first silver step
builder.add_silver_transform(
    name="silver_clean",
    source_bronze="bronze_events",
    transform=lambda spark, bronze_df, prior_silvers: bronze_df.filter(
        F.col("user_id").isNotNull()
    ),
    rules={"user_id": [F.col("user_id").isNotNull()]},
    table_name="silver_clean_events",
)
print("✅ Added Silver step: silver_clean")

# Add second silver step that depends on first silver
builder.add_silver_transform(
    name="silver_enriched",
    source_bronze="bronze_events",
    transform=lambda spark, bronze_df, prior_silvers: bronze_df.withColumn(
        "enriched", F.lit(True)
    ),
    rules={"user_id": [F.col("user_id").isNotNull()]},
    table_name="silver_enriched_events",
    source_silvers=["silver_clean"],  # Depends on silver_clean
)
print("✅ Added Silver step: silver_enriched (depends on silver_clean)")

# Add gold step
builder.add_gold_transform(
    name="gold_metrics",
    transform=lambda spark, silvers: list(silvers.values())[0].groupBy("user_id").count(),
    rules={"count": [F.col("count") > 0]},
    table_name="gold_daily_metrics",
    source_silvers=["silver_enriched"],
)
print("✅ Added Gold step: gold_metrics")
print()

# Check execution order before validation
print("🔍 Checking execution order BEFORE validation:")
print(f"   execution_order = {builder.execution_order}")
print()

# Validate pipeline
print("🔧 Validating pipeline...")
validation_errors = builder.validate_pipeline()
print()

if validation_errors:
    print("❌ Validation failed with errors:")
    for error in validation_errors:
        print(f"   - {error}")
    print()
    print(f"   execution_order = {builder.execution_order}")
else:
    print("✅ Pipeline validation passed!")
    print()
    
    # Check execution order after validation
    print("🔍 Checking execution order AFTER validation:")
    print(f"   execution_order = {builder.execution_order}")
    print()
    
    if builder.execution_order:
        print("📋 Execution Order Details:")
        print(f"   Total steps: {len(builder.execution_order)}")
        print()
        print("   Step execution sequence:")
        for idx, step_name in enumerate(builder.execution_order, 1):
            # Determine step type
            if step_name in builder.bronze_steps:
                step_type = "BRONZE"
            elif step_name in builder.silver_steps:
                step_type = "SILVER"
            elif step_name in builder.gold_steps:
                step_type = "GOLD"
            else:
                step_type = "UNKNOWN"
            
            print(f"   {idx}. {step_name} ({step_type})")
        
        print()
        print("   Visual representation:")
        print(f"   {' → '.join(builder.execution_order)}")
        print()
        
        # Verify dependencies are respected
        print("   Dependency verification:")
        bronze_idx = builder.execution_order.index("bronze_events")
        silver_clean_idx = builder.execution_order.index("silver_clean")
        silver_enriched_idx = builder.execution_order.index("silver_enriched")
        gold_idx = builder.execution_order.index("gold_metrics")
        
        assert bronze_idx < silver_clean_idx, "Bronze should come before silver_clean"
        assert bronze_idx < silver_enriched_idx, "Bronze should come before silver_enriched"
        assert silver_clean_idx < silver_enriched_idx, "silver_clean should come before silver_enriched"
        assert silver_enriched_idx < gold_idx, "silver_enriched should come before gold"
        
        print("   ✅ All dependencies are correctly ordered!")
    else:
        print("   ⚠️  Execution order is empty")

print()
print("=" * 70)
print("Demo complete!")
print("=" * 70)
