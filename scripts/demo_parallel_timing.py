#!/usr/bin/env python
"""
Demo showing concurrent execution timing with simulated processing.
This shows how parallel execution saves time compared to sequential.
"""

import time

from sparkforge.logging import PipelineLogger

print("\n" + "="*80)
print("DEMO: CONCURRENT VS SEQUENTIAL EXECUTION TIMING")
print("="*80)
print("\nThis demo simulates pipeline execution to show timing differences.")
print("="*80 + "\n")

logger = PipelineLogger("PipelineRunner")

def simulate_step(step_type, step_name, duration, rows, parallel=True):
    """Simulate a step execution with realistic timing."""
    logger.step_start(step_type, step_name)
    start = time.time()
    time.sleep(duration)  # Simulate processing time
    elapsed = time.time() - start
    logger.step_complete(
        step_type,
        step_name,
        elapsed,
        rows_processed=rows,
        rows_written=rows if step_type != "bronze" else None,
        validation_rate=100.0
    )
    return elapsed

print("=" * 80)
print("SCENARIO 1: SEQUENTIAL EXECUTION (Old Approach)")
print("=" * 80)
logger.info("🔄 Running steps sequentially...")
seq_start = time.time()

# Bronze steps run one after another
simulate_step("bronze", "bronze_events", 0.5, 1000, parallel=False)
simulate_step("bronze", "bronze_profiles", 0.5, 500, parallel=False)

# Silver steps run one after another
simulate_step("silver", "silver_purchases", 0.8, 350, parallel=False)
simulate_step("silver", "silver_customers", 0.8, 500, parallel=False)

# Gold step
simulate_step("gold", "gold_customer_summary", 0.6, 150, parallel=False)

seq_total = time.time() - seq_start
logger.pipeline_end("sequential_pipeline", seq_total, True)

print("\n" + "=" * 80)
print("SCENARIO 2: PARALLEL EXECUTION (Current Approach)")
print("=" * 80)
logger.info("⚡ Running independent steps in parallel...")
par_start = time.time()

# Group 1: Bronze steps run concurrently (simulated with threading)
logger.info("📦 Executing group 1/3: 2 steps - bronze_events, bronze_profiles")
group1_start = time.time()
import threading

threads = []
results = {}

def run_bronze_events():
    results['bronze_events'] = simulate_step("bronze", "bronze_events", 0.5, 1000)

def run_bronze_profiles():
    results['bronze_profiles'] = simulate_step("bronze", "bronze_profiles", 0.5, 500)

t1 = threading.Thread(target=run_bronze_events)
t2 = threading.Thread(target=run_bronze_profiles)
t1.start()
t2.start()
t1.join()
t2.join()

group1_time = time.time() - group1_start
logger.info(f"Group 1 completed in {group1_time:.2f}s")

# Group 2: Silver steps run concurrently
logger.info("📦 Executing group 2/3: 2 steps - silver_purchases, silver_customers")
group2_start = time.time()

def run_silver_purchases():
    results['silver_purchases'] = simulate_step("silver", "silver_purchases", 0.8, 350)

def run_silver_customers():
    results['silver_customers'] = simulate_step("silver", "silver_customers", 0.8, 500)

t3 = threading.Thread(target=run_silver_purchases)
t4 = threading.Thread(target=run_silver_customers)
t3.start()
t4.start()
t3.join()
t4.join()

group2_time = time.time() - group2_start
logger.info(f"Group 2 completed in {group2_time:.2f}s")

# Group 3: Gold step (sequential)
logger.info("📦 Executing group 3/3: 1 steps - gold_customer_summary")
group3_start = time.time()
simulate_step("gold", "gold_customer_summary", 0.6, 150)
group3_time = time.time() - group3_start
logger.info(f"Group 3 completed in {group3_time:.2f}s")

par_total = time.time() - par_start
logger.pipeline_end("parallel_pipeline", par_total, True)

# Calculate metrics
speedup = seq_total / par_total
efficiency = (speedup / 4) * 100  # 4 max workers
time_saved = seq_total - par_total
percent_saved = (time_saved / seq_total) * 100

print("\n" + "=" * 80)
print("📊 TIMING COMPARISON")
print("=" * 80)
print(f"Sequential Execution Time: {seq_total:.2f}s")
print(f"Parallel Execution Time:   {par_total:.2f}s")
print(f"Time Saved:                {time_saved:.2f}s ({percent_saved:.1f}%)")
print(f"Speedup Factor:            {speedup:.2f}x")
print(f"Parallel Efficiency:       {efficiency:.1f}%")

print("\n" + "=" * 80)
print("KEY INSIGHTS:")
print("=" * 80)
print("✅ Group 1 (2 bronze steps): ~0.5s parallel vs 1.0s sequential")
print("✅ Group 2 (2 silver steps): ~0.8s parallel vs 1.6s sequential")
print("✅ Group 3 (1 gold step):    ~0.6s (same, no parallelism benefit)")
print(f"\n✅ Total: ~{par_total:.1f}s parallel vs ~{seq_total:.1f}s sequential")
print(f"✅ {percent_saved:.0f}% faster with parallel execution!")
print("\n💡 Real pipelines with more independent steps see even greater speedups!")
print("=" * 80 + "\n")

