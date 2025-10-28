#!/usr/bin/env python3
"""
Dynamic Parallel Execution Example for SparkForge.

This example demonstrates the advanced parallel execution capabilities of SparkForge,
including dynamic worker allocation, intelligent task prioritization, and adaptive optimization.

Key Features Demonstrated:
- Dynamic worker allocation based on workload analysis
- Intelligent task prioritization and dependency management
- Resource-aware execution planning
- Performance monitoring and optimization
- Work-stealing algorithms for optimal resource utilization
"""

import random
import time
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from pipeline_builder import (
    DynamicExecutionStrategy,
    DynamicParallelExecutor,
    PipelineBuilder,
    TaskPriority,
    WorkerAllocationStrategy,
    create_execution_task,
    get_dynamic_executor,
)


def create_sample_data(spark: SparkSession, num_records: int = 10000) -> DataFrame:
    """Create sample data for the example."""
    data = []
    for i in range(num_records):
        data.append(
            {
                "id": i,
                "name": f"user_{i}",
                "age": random.randint(18, 80),
                "salary": random.randint(30000, 150000),
                "department": random.choice(
                    ["Engineering", "Sales", "Marketing", "HR", "Finance"]
                ),
                "location": random.choice(
                    ["New York", "San Francisco", "London", "Tokyo", "Berlin"]
                ),
                "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

    schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("salary", IntegerType(), True),
            StructField("department", StringType(), True),
            StructField("location", StringType(), True),
            StructField("created_at", StringType(), True),
        ]
    )

    return spark.createDataFrame(data, schema)


def complex_data_processing(df: DataFrame) -> DataFrame:
    """Simulate complex data processing with varying execution time."""
    # Simulate processing time based on data size
    processing_time = len(df.columns) * 0.1 + random.uniform(0.5, 2.0)
    time.sleep(processing_time)

    return (
        df.withColumn(
            "salary_category",
            F.when(F.col("salary") < 50000, "Low")
            .when(F.col("salary") < 100000, "Medium")
            .otherwise("High"),
        )
        .withColumn(
            "age_group",
            F.when(F.col("age") < 30, "Young")
            .when(F.col("age") < 50, "Middle")
            .otherwise("Senior"),
        )
        .withColumn("processed_at", F.current_timestamp())
    )


def heavy_aggregation(df: DataFrame) -> DataFrame:
    """Simulate heavy aggregation operations."""
    # Simulate heavy processing
    processing_time = 1.0 + random.uniform(0.5, 3.0)
    time.sleep(processing_time)

    return (
        df.groupBy("department", "location", "salary_category")
        .agg(
            F.count("*").alias("employee_count"),
            F.avg("salary").alias("avg_salary"),
            F.max("salary").alias("max_salary"),
            F.min("salary").alias("min_salary"),
            F.avg("age").alias("avg_age"),
        )
        .withColumn("processed_at", F.current_timestamp())
    )


def lightweight_transformation(df: DataFrame) -> DataFrame:
    """Simulate lightweight transformation operations."""
    # Simulate quick processing
    processing_time = 0.1 + random.uniform(0.1, 0.5)
    time.sleep(processing_time)

    return (
        df.withColumn("name_upper", F.upper(F.col("name")))
        .withColumn("salary_formatted", F.format_number(F.col("salary"), 2))
        .withColumn("processed_at", F.current_timestamp())
    )


def demonstrate_dynamic_parallel_execution():
    """Demonstrate dynamic parallel execution capabilities."""
    print("🚀 SparkForge Dynamic Parallel Execution Example")
    print("=" * 60)

    # Initialize Spark
    spark = (
        SparkSession.builder.appName("SparkForge Dynamic Parallel Execution")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    try:
        # Create sample data
        print("\n📊 Creating sample data...")
        raw_data = create_sample_data(spark, 5000)
        print(f"Created {raw_data.count()} records")

        # Demonstrate basic dynamic parallel execution
        print("\n🔧 Basic Dynamic Parallel Execution")
        print("-" * 40)

        # Create execution tasks with different complexities
        tasks = [
            create_execution_task(
                task_id="complex_processing",
                function=complex_data_processing,
                args=(raw_data,),
                priority=TaskPriority.HIGH,
                estimated_duration=3.0,
                memory_requirement_mb=512.0,
            ),
            create_execution_task(
                task_id="heavy_aggregation",
                function=heavy_aggregation,
                args=(raw_data,),
                priority=TaskPriority.CRITICAL,
                estimated_duration=5.0,
                memory_requirement_mb=1024.0,
            ),
            create_execution_task(
                task_id="lightweight_transform",
                function=lightweight_transformation,
                args=(raw_data,),
                priority=TaskPriority.NORMAL,
                estimated_duration=1.0,
                memory_requirement_mb=256.0,
            ),
        ]

        # Execute tasks with dynamic parallel execution
        executor = get_dynamic_executor()
        start_time = time.time()

        result = executor.execute_parallel(
            tasks=tasks, wait_for_completion=True, timeout=30.0
        )

        execution_time = time.time() - start_time

        print(f"✅ Execution completed in {execution_time:.2f} seconds")
        print(f"📈 Success rate: {result['metrics']['success_rate']:.1f}%")
        print(f"👥 Workers used: {result['metrics']['worker_count']}")
        print(f"⚡ Efficiency: {result['metrics']['average_efficiency']:.2f}")

        # Demonstrate dynamic execution strategy
        print("\n🎯 Dynamic Execution Strategy")
        print("-" * 40)

        # Create a pipeline with dynamic execution strategy
        builder = PipelineBuilder(spark=spark, schema="dynamic_example")

        # Add bronze step
        builder.with_bronze_rules(
            name="raw_employees",
            rules={
                "id": [F.col("id").isNotNull()],
                "name": [F.col("name").isNotNull()],
                "salary": [F.col("salary") > 0],
            },
        )

        # Add silver steps with different complexities
        builder.add_silver_transform(
            name="processed_employees",
            source_bronze="raw_employees",
            transform=complex_data_processing,
            rules={
                "id": [F.col("id").isNotNull()],
                "salary_category": [F.col("salary_category").isNotNull()],
            },
            table_name="processed_employees",
        )

        builder.add_silver_transform(
            name="lightweight_employees",
            source_bronze="raw_employees",
            transform=lightweight_transformation,
            rules={
                "id": [F.col("id").isNotNull()],
                "name_upper": [F.col("name_upper").isNotNull()],
            },
            table_name="lightweight_employees",
        )

        # Add gold step
        builder.add_gold_transform(
            name="department_analytics",
            transform=lambda spark, silvers: heavy_aggregation(
                silvers["processed_employees"]
            ),
            rules={
                "department": [F.col("department").isNotNull()],
                "employee_count": [F.col("employee_count") > 0],
            },
            table_name="department_analytics",
            source_silvers=["processed_employees"],
        )

        # Create pipeline
        pipeline = builder.to_pipeline()

        # Execute with dynamic strategy
        print("Executing pipeline with dynamic execution strategy...")
        start_time = time.time()

        execution_result = pipeline.initial_load(
            bronze_sources={"raw_employees": raw_data}
        )

        execution_time = time.time() - start_time

        print(f"✅ Pipeline executed in {execution_time:.2f} seconds")
        print(f"📊 Steps completed: {execution_result.successful_steps}")
        print(f"❌ Steps failed: {execution_result.failed_steps}")
        print(f"⚡ Parallel efficiency: {execution_result.parallel_efficiency:.2f}")

        # Demonstrate worker allocation strategies
        print("\n🔄 Worker Allocation Strategies")
        print("-" * 40)

        strategies = [
            WorkerAllocationStrategy.STATIC,
            WorkerAllocationStrategy.DYNAMIC,
            WorkerAllocationStrategy.ADAPTIVE,
            WorkerAllocationStrategy.WORK_STEALING,
        ]

        for strategy in strategies:
            print(f"\nTesting {strategy.value} strategy...")

            # Create executor with specific strategy
            strategy_executor = DynamicParallelExecutor()
            strategy_executor.worker_pool.allocation_strategy = strategy

            # Create test tasks
            test_tasks = [
                create_execution_task(
                    task_id=f"task_{i}",
                    function=lambda x, i=i: time.sleep(0.5 + i * 0.1),
                    args=(i,),
                    priority=TaskPriority.NORMAL,
                    estimated_duration=1.0,
                )
                for i in range(5)
            ]

            start_time = time.time()
            result = strategy_executor.execute_parallel(
                test_tasks, wait_for_completion=True
            )
            execution_time = time.time() - start_time

            print(f"  ⏱️  Execution time: {execution_time:.2f}s")
            print(f"  👥 Workers used: {result['metrics']['worker_count']}")
            print(f"  ⚡ Efficiency: {result['metrics']['average_efficiency']:.2f}")

            strategy_executor.shutdown()

        # Demonstrate performance monitoring
        print("\n📊 Performance Monitoring")
        print("-" * 40)

        # Get performance metrics
        metrics = executor.get_performance_metrics()
        print(f"Total tasks executed: {metrics['total_tasks']}")
        print(f"Success rate: {metrics['success_rate']:.1f}%")
        print(f"Average duration: {metrics['average_duration']:.2f}s")
        print(f"Current worker count: {metrics['worker_count']}")
        print(f"Queue size: {metrics['queue_size']}")
        print(f"Running tasks: {metrics['running_tasks']}")

        # Get optimization recommendations
        recommendations = executor.get_optimization_recommendations()
        if recommendations:
            print("\n💡 Optimization Recommendations:")
            for i, rec in enumerate(recommendations, 1):
                print(f"  {i}. {rec}")

        # Demonstrate task prioritization
        print("\n🎯 Task Prioritization")
        print("-" * 40)

        # Create tasks with different priorities
        priority_tasks = [
            create_execution_task(
                task_id="critical_task",
                function=lambda: time.sleep(0.5),
                args=(),
                priority=TaskPriority.CRITICAL,
                estimated_duration=0.5,
            ),
            create_execution_task(
                task_id="high_priority_task",
                function=lambda: time.sleep(0.3),
                args=(),
                priority=TaskPriority.HIGH,
                estimated_duration=0.3,
            ),
            create_execution_task(
                task_id="normal_priority_task",
                function=lambda: time.sleep(0.2),
                args=(),
                priority=TaskPriority.NORMAL,
                estimated_duration=0.2,
            ),
            create_execution_task(
                task_id="low_priority_task",
                function=lambda: time.sleep(0.1),
                args=(),
                priority=TaskPriority.LOW,
                estimated_duration=0.1,
            ),
        ]

        print("Executing tasks with different priorities...")
        start_time = time.time()

        result = executor.execute_parallel(priority_tasks, wait_for_completion=True)

        execution_time = time.time() - start_time
        print(f"✅ Priority execution completed in {execution_time:.2f} seconds")

        # Show task completion order (approximate)
        print("Task execution order (by priority):")
        for task in priority_tasks:
            print(f"  - {task.task_id} ({task.priority.name})")

        print("\n🎉 Dynamic Parallel Execution Example Complete!")
        print("=" * 60)

    finally:
        # Cleanup
        executor.shutdown()
        spark.stop()


def demonstrate_advanced_features():
    """Demonstrate advanced dynamic parallel execution features."""
    print("\n🔬 Advanced Features Demonstration")
    print("=" * 60)

    # Initialize Spark
    spark = SparkSession.builder.appName("SparkForge Advanced Features").getOrCreate()

    try:
        # Create dynamic execution strategy
        strategy = DynamicExecutionStrategy()

        # Create sample steps for analysis
        steps = {
            "bronze_step": {
                "step_type": "bronze",
                "rules": {"id": ["not_null"]},
                "transform": None,
            },
            "complex_silver_step": {
                "step_type": "silver",
                "rules": {
                    "id": ["not_null"],
                    "name": ["not_null"],
                    "value": ["not_null"],
                },
                "transform": complex_data_processing,
            },
            "simple_silver_step": {
                "step_type": "silver",
                "rules": {"id": ["not_null"]},
                "transform": lightweight_transformation,
            },
            "gold_step": {
                "step_type": "gold",
                "rules": {"department": ["not_null"], "count": ["not_null"]},
                "transform": heavy_aggregation,
            },
        }

        # Analyze step complexity
        print("📊 Step Complexity Analysis:")
        analysis = strategy._analyze_steps(steps)

        for step_name, step_analysis in analysis.items():
            print(f"  {step_name}:")
            print(f"    - Complexity Score: {step_analysis.complexity_score:.2f}")
            print(f"    - Priority: {step_analysis.priority.name}")
            print(f"    - Estimated Duration: {step_analysis.estimated_duration:.2f}s")
            print(
                f"    - Memory Requirement: {step_analysis.memory_requirement_mb:.0f}MB"
            )
            print(
                f"    - CPU Requirement: {step_analysis.cpu_requirement_percent:.0f}%"
            )
            print(f"    - Critical Path: {step_analysis.critical_path}")
            print()

        # Create execution plan
        print("📋 Execution Plan:")
        plan = strategy._create_execution_plan(analysis)

        for i, (group, priority) in enumerate(plan, 1):
            print(f"  Group {i} ({priority.name} priority): {', '.join(group)}")

        # Get performance metrics
        print("\n📈 Performance Metrics:")
        metrics = strategy.get_performance_metrics()
        print(f"  Executor Metrics: {metrics['executor_metrics']}")
        print(f"  Execution History Count: {metrics['execution_history_count']}")
        print(f"  Step Analysis Cache Size: {metrics['step_analysis_cache_size']}")
        print(f"  Optimization Enabled: {metrics['optimization_enabled']}")

        # Get optimization recommendations
        recommendations = strategy.get_optimization_recommendations()
        if recommendations:
            print("\n💡 Optimization Recommendations:")
            for i, rec in enumerate(recommendations, 1):
                print(f"  {i}. {rec}")

        strategy.shutdown()

    finally:
        spark.stop()


if __name__ == "__main__":
    # Run the main demonstration
    demonstrate_dynamic_parallel_execution()

    # Run advanced features demonstration
    demonstrate_advanced_features()
