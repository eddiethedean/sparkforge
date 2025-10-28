"""
Step definitions for dependency analysis BDD tests.
"""

from behave import given, then, when

from pipeline_builder.dependencies.analyzer import DependencyAnalyzer
from pipeline_builder.dependencies.graph import DependencyGraph
from pipeline_builder.models import BronzeStep, GoldStep, SilverStep


@given("I have pipeline steps with dependencies")
def step_have_pipeline_steps_with_dependencies(context):
    """Set up pipeline steps with dependencies."""
    context.pipeline_steps = {}
    context.dependency_graph = DependencyGraph()


@given('I have a bronze step named "{step_name}"')
def step_have_bronze_step_named(context, step_name):
    """Create a bronze step with the given name."""
    bronze_step = BronzeStep(
        name=step_name, rules={"id": ["not_null"]}, incremental_col="timestamp"
    )
    context.pipeline_steps[step_name] = bronze_step
    context.dependency_graph.add_node(step_name, "bronze")


@given('I have a silver step named "{step_name}" that depends on "{bronze_name}"')
def step_have_silver_step_that_depends_on(context, step_name, bronze_name):
    """Create a silver step that depends on a bronze step."""
    silver_step = SilverStep(
        name=step_name,
        source_bronze=bronze_name,
        transform=None,
        rules={"id": ["not_null"]},
    )
    context.pipeline_steps[step_name] = silver_step
    context.dependency_graph.add_node(step_name, "silver")
    context.dependency_graph.add_edge(bronze_name, step_name)


@given('I have a gold step named "{step_name}" that depends on "{silver_name}"')
def step_have_gold_step_that_depends_on(context, step_name, silver_name):
    """Create a gold step that depends on a silver step."""
    gold_step = GoldStep(name=step_name, transform=None, rules={"id": ["not_null"]})
    context.pipeline_steps[step_name] = gold_step
    context.dependency_graph.add_node(step_name, "gold")
    context.dependency_graph.add_edge(silver_name, step_name)


@given("I have steps with circular dependencies:")
def step_have_steps_with_circular_dependencies(context):
    """Set up steps with circular dependencies."""
    context.pipeline_steps = {}
    context.dependency_graph = DependencyGraph()

    # Create steps
    for row in context.table:
        step_name = row["step_a"]
        context.pipeline_steps[step_name] = BronzeStep(
            name=step_name, rules={"id": ["not_null"]}, incremental_col="timestamp"
        )
        context.dependency_graph.add_node(step_name, "bronze")

    # Add circular dependencies
    for row in context.table:
        step_a = row["step_a"]
        step_b = row["depends_on"]
        context.dependency_graph.add_edge(step_a, step_b)


@given("I have multiple independent steps")
def step_have_multiple_independent_steps(context):
    """Set up multiple independent steps."""
    context.pipeline_steps = {}
    context.dependency_graph = DependencyGraph()

    # Create independent steps
    for i in range(3):
        step_name = f"independent_step_{i}"
        context.pipeline_steps[step_name] = BronzeStep(
            name=step_name, rules={"id": ["not_null"]}, incremental_col="timestamp"
        )
        context.dependency_graph.add_node(step_name, "bronze")


@given("I have a complex pipeline with multiple layers")
def step_have_complex_pipeline_with_multiple_layers(context):
    """Set up a complex pipeline with multiple layers."""
    context.pipeline_steps = {}
    context.dependency_graph = DependencyGraph()

    # Create bronze steps
    bronze_steps = ["raw_events", "raw_users", "raw_orders"]
    for step_name in bronze_steps:
        context.pipeline_steps[step_name] = BronzeStep(
            name=step_name, rules={"id": ["not_null"]}, incremental_col="timestamp"
        )
        context.dependency_graph.add_node(step_name, "bronze")

    # Create silver steps with dependencies
    silver_steps = [
        ("clean_events", "raw_events"),
        ("clean_users", "raw_users"),
        ("clean_orders", "raw_orders"),
    ]
    for step_name, depends_on in silver_steps:
        context.pipeline_steps[step_name] = SilverStep(
            name=step_name,
            source_bronze=depends_on,
            transform=None,
            rules={"id": ["not_null"]},
        )
        context.dependency_graph.add_node(step_name, "silver")
        context.dependency_graph.add_edge(depends_on, step_name)

    # Create gold steps with multiple dependencies
    gold_steps = [
        ("analytics_events", ["clean_events"]),
        ("user_analytics", ["clean_users", "clean_events"]),
        ("order_analytics", ["clean_orders", "clean_users"]),
    ]
    for step_name, dependencies in gold_steps:
        context.pipeline_steps[step_name] = GoldStep(
            name=step_name, transform=None, rules={"id": ["not_null"]}
        )
        context.dependency_graph.add_node(step_name, "gold")
        for dep in dependencies:
            context.dependency_graph.add_edge(dep, step_name)


@given("I have a step that depends on a non-existent step")
def step_have_step_that_depends_on_nonexistent(context):
    """Set up a step that depends on a non-existent step."""
    context.pipeline_steps = {}
    context.dependency_graph = DependencyGraph()

    # Create a step that depends on a non-existent step
    step_name = "dependent_step"
    context.pipeline_steps[step_name] = SilverStep(
        name=step_name,
        source_bronze="non_existent_step",
        transform=None,
        rules={"id": ["not_null"]},
    )
    context.dependency_graph.add_node(step_name, "silver")
    context.dependency_graph.add_edge("non_existent_step", step_name)


@given("I have steps with different execution times")
def step_have_steps_with_different_execution_times(context):
    """Set up steps with different execution times."""
    context.pipeline_steps = {}
    context.dependency_graph = DependencyGraph()
    context.execution_times = {}

    # Create steps with different execution times
    steps = [("fast_step", 10.0), ("medium_step", 30.0), ("slow_step", 60.0)]

    for step_name, execution_time in steps:
        context.pipeline_steps[step_name] = BronzeStep(
            name=step_name, rules={"id": ["not_null"]}, incremental_col="timestamp"
        )
        context.dependency_graph.add_node(step_name, "bronze")
        context.execution_times[step_name] = execution_time

    # Add dependencies
    context.dependency_graph.add_edge("fast_step", "medium_step")
    context.dependency_graph.add_edge("medium_step", "slow_step")


@when("I analyze the dependencies")
def step_analyze_dependencies(context):
    """Analyze the dependencies."""
    try:
        context.analyzer = DependencyAnalyzer(context.dependency_graph)
        context.analysis_result = context.analyzer.analyze_dependencies()
        context.analysis_success = True
    except Exception as e:
        context.analysis_error = str(e)
        context.analysis_success = False


@then('I should identify that "{step_b}" depends on "{step_a}"')
def step_should_identify_dependency(context, step_a, step_b):
    """Verify that the dependency is identified."""
    assert context.analysis_success, (
        f"Dependency analysis failed: {getattr(context, 'analysis_error', 'Unknown error')}"
    )

    # Check that the dependency is identified
    assert context.analysis_result is not None, "No analysis result"
    assert "dependencies" in context.analysis_result, (
        "No dependencies in analysis result"
    )

    # Check that step_b depends on step_a
    dependencies = context.analysis_result["dependencies"]
    assert step_b in dependencies, f"Step {step_b} not found in dependencies"
    assert step_a in dependencies[step_b], f"Step {step_b} does not depend on {step_a}"


@then('the execution order should be "{step_a}" then "{step_b}"')
def step_execution_order_should_be(context, step_a, step_b):
    """Verify that the execution order is correct."""
    assert context.analysis_success, (
        f"Dependency analysis failed: {getattr(context, 'analysis_error', 'Unknown error')}"
    )

    # Check that the execution order is correct
    assert context.analysis_result is not None, "No analysis result"
    assert "execution_order" in context.analysis_result, (
        "No execution order in analysis result"
    )

    execution_order = context.analysis_result["execution_order"]
    step_a_index = execution_order.index(step_a)
    step_b_index = execution_order.index(step_b)
    assert step_a_index < step_b_index, (
        f"Execution order incorrect: {step_a} should come before {step_b}"
    )


@then("no circular dependencies should be detected")
def step_no_circular_dependencies_should_be_detected(context):
    """Verify that no circular dependencies are detected."""
    assert context.analysis_success, (
        f"Dependency analysis failed: {getattr(context, 'analysis_error', 'Unknown error')}"
    )

    # Check that no circular dependencies are detected
    assert context.analysis_result is not None, "No analysis result"
    assert "circular_dependencies" in context.analysis_result, (
        "No circular dependencies check in analysis result"
    )

    circular_deps = context.analysis_result["circular_dependencies"]
    assert len(circular_deps) == 0, f"Circular dependencies detected: {circular_deps}"


@then("I should detect the circular dependency")
def step_should_detect_circular_dependency(context):
    """Verify that the circular dependency is detected."""
    assert context.analysis_success, (
        f"Dependency analysis failed: {getattr(context, 'analysis_error', 'Unknown error')}"
    )

    # Check that circular dependencies are detected
    assert context.analysis_result is not None, "No analysis result"
    assert "circular_dependencies" in context.analysis_result, (
        "No circular dependencies check in analysis result"
    )

    circular_deps = context.analysis_result["circular_dependencies"]
    assert len(circular_deps) > 0, "No circular dependencies detected"


@then("I should receive a clear error message")
def step_should_receive_clear_error_message(context):
    """Verify that a clear error message is received."""
    assert context.analysis_success, (
        f"Dependency analysis failed: {getattr(context, 'analysis_error', 'Unknown error')}"
    )

    # Check that we have error information
    assert context.analysis_result is not None, "No analysis result"
    assert "errors" in context.analysis_result, "No errors in analysis result"

    errors = context.analysis_result["errors"]
    assert len(errors) > 0, "No error messages provided"


@then("I should get suggestions for resolving the circular dependency")
def step_should_get_suggestions_for_resolving_circular_dependency(context):
    """Verify that suggestions for resolving circular dependencies are provided."""
    assert context.analysis_success, (
        f"Dependency analysis failed: {getattr(context, 'analysis_error', 'Unknown error')}"
    )

    # Check that suggestions are provided
    assert context.analysis_result is not None, "No analysis result"
    assert "suggestions" in context.analysis_result, "No suggestions in analysis result"

    suggestions = context.analysis_result["suggestions"]
    assert len(suggestions) > 0, (
        "No suggestions provided for resolving circular dependencies"
    )


@then("I should identify independent steps that can run in parallel")
def step_should_identify_independent_steps_for_parallel(context):
    """Verify that independent steps are identified for parallel execution."""
    assert context.analysis_success, (
        f"Dependency analysis failed: {getattr(context, 'analysis_error', 'Unknown error')}"
    )

    # Check that independent steps are identified
    assert context.analysis_result is not None, "No analysis result"
    assert "parallel_groups" in context.analysis_result, (
        "No parallel groups in analysis result"
    )

    parallel_groups = context.analysis_result["parallel_groups"]
    assert len(parallel_groups) > 0, "No parallel groups identified"


@then("I should identify the optimal execution order")
def step_should_identify_optimal_execution_order(context):
    """Verify that the optimal execution order is identified."""
    assert context.analysis_success, (
        f"Dependency analysis failed: {getattr(context, 'analysis_error', 'Unknown error')}"
    )

    # Check that optimal execution order is identified
    assert context.analysis_result is not None, "No analysis result"
    assert "execution_order" in context.analysis_result, (
        "No execution order in analysis result"
    )

    execution_order = context.analysis_result["execution_order"]
    assert len(execution_order) > 0, "No execution order identified"


@then("I should receive execution group recommendations")
def step_should_receive_execution_group_recommendations(context):
    """Verify that execution group recommendations are received."""
    assert context.analysis_success, (
        f"Dependency analysis failed: {getattr(context, 'analysis_error', 'Unknown error')}"
    )

    # Check that execution group recommendations are provided
    assert context.analysis_result is not None, "No analysis result"
    assert "execution_groups" in context.analysis_result, (
        "No execution groups in analysis result"
    )

    execution_groups = context.analysis_result["execution_groups"]
    assert len(execution_groups) > 0, "No execution groups recommended"


@then("I should identify all dependencies correctly")
def step_should_identify_all_dependencies_correctly(context):
    """Verify that all dependencies are identified correctly."""
    assert context.analysis_success, (
        f"Dependency analysis failed: {getattr(context, 'analysis_error', 'Unknown error')}"
    )

    # Check that all dependencies are identified
    assert context.analysis_result is not None, "No analysis result"
    assert "dependencies" in context.analysis_result, (
        "No dependencies in analysis result"
    )

    dependencies = context.analysis_result["dependencies"]
    assert len(dependencies) > 0, "No dependencies identified"


@then("I should identify the critical path")
def step_should_identify_critical_path(context):
    """Verify that the critical path is identified."""
    assert context.analysis_success, (
        f"Dependency analysis failed: {getattr(context, 'analysis_error', 'Unknown error')}"
    )

    # Check that critical path is identified
    assert context.analysis_result is not None, "No analysis result"
    assert "critical_path" in context.analysis_result, (
        "No critical path in analysis result"
    )

    critical_path = context.analysis_result["critical_path"]
    assert len(critical_path) > 0, "No critical path identified"


@then("I should identify potential bottlenecks")
def step_should_identify_potential_bottlenecks(context):
    """Verify that potential bottlenecks are identified."""
    assert context.analysis_success, (
        f"Dependency analysis failed: {getattr(context, 'analysis_error', 'Unknown error')}"
    )

    # Check that potential bottlenecks are identified
    assert context.analysis_result is not None, "No analysis result"
    assert "bottlenecks" in context.analysis_result, "No bottlenecks in analysis result"

    bottlenecks = context.analysis_result["bottlenecks"]
    assert len(bottlenecks) > 0, "No potential bottlenecks identified"


@then("I should provide optimization recommendations")
def step_should_provide_optimization_recommendations(context):
    """Verify that optimization recommendations are provided."""
    assert context.analysis_success, (
        f"Dependency analysis failed: {getattr(context, 'analysis_error', 'Unknown error')}"
    )

    # Check that optimization recommendations are provided
    assert context.analysis_result is not None, "No analysis result"
    assert "optimization_recommendations" in context.analysis_result, (
        "No optimization recommendations in analysis result"
    )

    recommendations = context.analysis_result["optimization_recommendations"]
    assert len(recommendations) > 0, "No optimization recommendations provided"


@then("I should detect the missing dependency")
def step_should_detect_missing_dependency(context):
    """Verify that the missing dependency is detected."""
    assert context.analysis_success, (
        f"Dependency analysis failed: {getattr(context, 'analysis_error', 'Unknown error')}"
    )

    # Check that missing dependencies are detected
    assert context.analysis_result is not None, "No analysis result"
    assert "missing_dependencies" in context.analysis_result, (
        "No missing dependencies check in analysis result"
    )

    missing_deps = context.analysis_result["missing_dependencies"]
    assert len(missing_deps) > 0, "No missing dependencies detected"


@then("I should get suggestions for fixing the dependency")
def step_should_get_suggestions_for_fixing_dependency(context):
    """Verify that suggestions for fixing dependencies are provided."""
    assert context.analysis_success, (
        f"Dependency analysis failed: {getattr(context, 'analysis_error', 'Unknown error')}"
    )

    # Check that suggestions are provided
    assert context.analysis_result is not None, "No analysis result"
    assert "suggestions" in context.analysis_result, "No suggestions in analysis result"

    suggestions = context.analysis_result["suggestions"]
    assert len(suggestions) > 0, "No suggestions provided for fixing dependencies"


@then("I should calculate the total execution time")
def step_should_calculate_total_execution_time(context):
    """Verify that the total execution time is calculated."""
    assert context.analysis_success, (
        f"Dependency analysis failed: {getattr(context, 'analysis_error', 'Unknown error')}"
    )

    # Check that total execution time is calculated
    assert context.analysis_result is not None, "No analysis result"
    assert "total_execution_time" in context.analysis_result, (
        "No total execution time in analysis result"
    )

    total_time = context.analysis_result["total_execution_time"]
    assert total_time > 0, "Invalid total execution time"


@then("I should identify the critical path")
def step_should_identify_critical_path_performance(context):
    """Verify that the critical path is identified for performance analysis."""
    assert context.analysis_success, (
        f"Dependency analysis failed: {getattr(context, 'analysis_error', 'Unknown error')}"
    )

    # Check that critical path is identified
    assert context.analysis_result is not None, "No analysis result"
    assert "critical_path" in context.analysis_result, (
        "No critical path in analysis result"
    )

    critical_path = context.analysis_result["critical_path"]
    assert len(critical_path) > 0, "No critical path identified"


@then("I should provide performance optimization suggestions")
def step_should_provide_performance_optimization_suggestions(context):
    """Verify that performance optimization suggestions are provided."""
    assert context.analysis_success, (
        f"Dependency analysis failed: {getattr(context, 'analysis_error', 'Unknown error')}"
    )

    # Check that performance optimization suggestions are provided
    assert context.analysis_result is not None, "No analysis result"
    assert "performance_suggestions" in context.analysis_result, (
        "No performance suggestions in analysis result"
    )

    suggestions = context.analysis_result["performance_suggestions"]
    assert len(suggestions) > 0, "No performance optimization suggestions provided"
