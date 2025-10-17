#!/usr/bin/env python
"""
Demo showing the improved uniform logging format.
This shows what the log messages look like during pipeline execution.
"""

from sparkforge.logging import PipelineLogger

# Create logger
logger = PipelineLogger("PipelineRunner")

print("\n" + "="*80)
print("DEMO: UNIFORM LOGGING FORMAT FOR PIPELINE EXECUTION")
print("="*80)
print("\nAll messages now have consistent format:")
print("  HH:MM:SS - PipelineRunner - INFO/ERROR - [emoji] MESSAGE")
print("\n" + "="*80 + "\n")

# Simulate pipeline execution logging
logger.info("🚀 Pipeline built successfully with 2 bronze, 2 silver, 1 gold steps")

# Bronze step 1 - validates but doesn't write
logger.step_start("bronze", "bronze_events")
logger.step_complete("bronze", "bronze_events", 1.23, 
                    rows_processed=1000, rows_written=None, 
                    invalid_rows=0, validation_rate=100.0)

# Bronze step 2 - has some invalid rows
logger.step_start("bronze", "bronze_profiles")
logger.step_complete("bronze", "bronze_profiles", 0.87, 
                    rows_processed=500, rows_written=None,
                    invalid_rows=8, validation_rate=98.4)

# Silver step 1 - processes and writes
logger.step_start("silver", "silver_purchase_events")
logger.step_complete("silver", "silver_purchase_events", 2.15, 
                    rows_processed=350, rows_written=350,
                    invalid_rows=0, validation_rate=100.0)

# Silver step 2 - has some invalid rows
logger.step_start("silver", "silver_customer_profiles")
logger.step_complete("silver", "silver_customer_profiles", 1.92, 
                    rows_processed=500, rows_written=496,
                    invalid_rows=4, validation_rate=99.2)

# Gold step
logger.step_start("gold", "gold_customer_summary")
logger.step_complete("gold", "gold_customer_summary", 3.45, 
                    rows_processed=150, rows_written=150,
                    invalid_rows=0, validation_rate=100.0)

logger.pipeline_end("customer_analytics", 9.62, True)

print("\n" + "="*80)
print("KEY IMPROVEMENTS:")
print("="*80)
print("✅ All messages have timestamp prefix (HH:MM:SS - Logger - LEVEL)")
print("✅ Step types are UPPERCASE (BRONZE, SILVER, GOLD)")
print("✅ Consistent emojis: 🚀 Starting, ✅ Completed, ❌ Failed")
print("✅ Detailed info: duration, rows processed, rows written, invalid rows, validation %")
print("✅ Bronze steps show 'processed' only (they don't write to tables)")
print("✅ Silver/Gold steps show both processed and written")
print("✅ Invalid rows count shown when > 0")
print("✅ Proper newlines - no messages run together")
print("✅ No empty separator lines")
print("="*80 + "\n")

