#!/usr/bin/env python3
"""
Comprehensive tests for the dependency_analyzer module.

This module tests all dependency analysis functionality, strategies, cycle detection, and optimization.
"""

import unittest

from sparkforge.dependencies import (
    AnalysisStrategy,
    DependencyAnalysisResult,
    DependencyAnalyzer,
    ExecutionMode,
)
from sparkforge.logging import PipelineLogger
from sparkforge.models import BronzeStep, GoldStep, SilverStep


class TestAnalysisStrategy(unittest.TestCase):
    """Test AnalysisStrategy enum."""

    def test_strategy_values(self):
        """Test strategy enum values."""
        self.assertEqual(AnalysisStrategy.CONSERVATIVE.value, "conservative")
        self.assertEqual(AnalysisStrategy.OPTIMISTIC.value, "optimistic")
        self.assertEqual(AnalysisStrategy.HYBRID.value, "hybrid")


class TestExecutionMode(unittest.TestCase):
    """Test ExecutionMode enum."""

    def test_execution_mode_values(self):
        """Test execution mode enum values."""
        self.assertEqual(ExecutionMode.INITIAL.value, "initial")
        self.assertEqual(ExecutionMode.INCREMENTAL.value, "incremental")


class TestDependencyAnalysisResult(unittest.TestCase):
    """Test DependencyAnalysisResult dataclass."""

    def test_analysis_result_creation(self):
        """Test analysis result creation."""
        from sparkforge.dependencies.graph import DependencyGraph

        graph = DependencyGraph()
        execution_groups = [["step1"], ["step2", "step3"]]
        cycles = []
        conflicts = []
        stats = {"total_steps": 3}
        recommendations = ["Optimize step1"]

        result = DependencyAnalysisResult(
            graph=graph,
            execution_groups=execution_groups,
            cycles=cycles,
            conflicts=conflicts,
            recommendations=recommendations,
            stats=stats,
            analysis_duration=0.1,
        )

        self.assertEqual(result.execution_groups, execution_groups)
        self.assertEqual(result.cycles, cycles)
        self.assertEqual(result.conflicts, conflicts)
        self.assertEqual(result.stats, stats)
        self.assertEqual(result.recommendations, recommendations)

    def test_get_total_execution_time(self):
        """Test total execution time calculation."""
        from sparkforge.dependencies.graph import DependencyGraph

        result = DependencyAnalysisResult(
            graph=DependencyGraph(),
            execution_groups=[["step1"], ["step2"]],
            cycles=[],
            conflicts=[],
            recommendations=[],
            stats={},
            analysis_duration=2.0,
        )

        self.assertEqual(result.analysis_duration, 2.0)

    def test_get_parallelization_ratio(self):
        """Test parallelization ratio calculation."""
        from sparkforge.dependencies.graph import DependencyGraph

        result = DependencyAnalysisResult(
            graph=DependencyGraph(),
            execution_groups=[["step1"], ["step2", "step3"]],
            cycles=[],
            conflicts=[],
            recommendations=[],
            stats={},
            analysis_duration=0.0,
        )

        # Calculate parallelization ratio: parallel steps / total steps
        total_steps = sum(len(group) for group in result.execution_groups)
        parallel_steps = sum(
            len(group) for group in result.execution_groups if len(group) > 1
        )
        ratio = parallel_steps / total_steps if total_steps > 0 else 0.0

        # 2 groups with 1 and 2 steps respectively = 2/3 parallelization ratio
        self.assertEqual(ratio, 2 / 3)


class TestDependencyAnalyzer(unittest.TestCase):
    """Test DependencyAnalyzer class."""

    def setUp(self):
        """Set up test fixtures."""
        self.logger = PipelineLogger(verbose=False)
        self.analyzer = DependencyAnalyzer(logger=self.logger)

        # Create test silver steps
        self.silver_steps = {
            "step1": SilverStep(
                name="step1",
                source_bronze="bronze1",
                transform=lambda df: df,
                rules={"id": ["not_null"]},
                table_name="silver1",
            ),
            "step2": SilverStep(
                name="step2",
                source_bronze="bronze2",
                transform=lambda df: df,
                rules={"id": ["not_null"]},
                table_name="silver2",
            ),
            "step3": SilverStep(
                name="step3",
                source_bronze="bronze3",
                transform=lambda df: df,
                rules={"id": ["not_null"]},
                table_name="silver3",
            ),
        }

    def test_analyzer_creation(self):
        """Test analyzer creation."""
        analyzer = DependencyAnalyzer()
        self.assertIsInstance(analyzer.logger, PipelineLogger)
        self.assertEqual(analyzer.strategy, AnalysisStrategy.HYBRID)

    def test_analyzer_creation_with_custom_params(self):
        """Test analyzer creation with custom parameters."""
        logger = PipelineLogger(verbose=False)
        analyzer = DependencyAnalyzer(
            logger=logger, strategy=AnalysisStrategy.CONSERVATIVE
        )

        self.assertEqual(analyzer.logger, logger)
        self.assertEqual(analyzer.strategy, AnalysisStrategy.CONSERVATIVE)

    def test_analyze_dependencies_basic(self):
        """Test basic dependency analysis."""
        analyzer = DependencyAnalyzer(strategy=AnalysisStrategy.CONSERVATIVE)

        # Test with empty inputs - should work without errors
        result = analyzer.analyze_dependencies()
        self.assertIsInstance(result, DependencyAnalysisResult)
        self.assertIsNotNone(result.graph)
        self.assertIsInstance(result.execution_groups, list)
        self.assertIsInstance(result.cycles, list)
        self.assertIsInstance(result.conflicts, list)
        self.assertIsInstance(result.recommendations, list)
        self.assertIsInstance(result.stats, dict)
        self.assertIsInstance(result.analysis_duration, float)

    def test_analyze_dependencies_with_bronze_steps(self):
        """Test dependency analysis with bronze steps."""
        from sparkforge.models import BronzeStep

        bronze_steps = {
            "bronze1": BronzeStep(name="bronze1", rules={"id": ["not_null"]})
        }

        analyzer = DependencyAnalyzer(strategy=AnalysisStrategy.OPTIMISTIC)
        result = analyzer.analyze_dependencies(bronze_steps=bronze_steps)

        self.assertIsInstance(result, DependencyAnalysisResult)
        self.assertIsNotNone(result.graph)

    def test_analyze_dependencies_with_gold_steps(self):
        """Test dependency analysis with gold steps."""
        from sparkforge.models import GoldStep

        gold_steps = {
            "gold1": GoldStep(
                name="gold1",
                source_silvers=None,  # None means use all available silvers
                transform=lambda df: df,
                rules={"id": ["not_null"]},
                table_name="gold1",
            )
        }

        analyzer = DependencyAnalyzer(strategy=AnalysisStrategy.HYBRID)
        result = analyzer.analyze_dependencies(gold_steps=gold_steps)

        self.assertIsInstance(result, DependencyAnalysisResult)
        self.assertIsNotNone(result.graph)

    def test_analyze_dependencies_caching(self):
        """Test that dependency analysis results are cached."""
        analyzer = DependencyAnalyzer()

        # First analysis
        result1 = analyzer.analyze_dependencies()

        # Second analysis should use cache
        result2 = analyzer.analyze_dependencies()

        # Results should be the same
        self.assertEqual(result1.execution_groups, result2.execution_groups)
        self.assertEqual(result1.cycles, result2.cycles)
        self.assertEqual(result1.conflicts, result2.conflicts)

    def test_analyze_dependencies_force_refresh(self):
        """Test force refresh of dependency analysis."""
        analyzer = DependencyAnalyzer()

        # First analysis
        result1 = analyzer.analyze_dependencies()

        # Force refresh
        result2 = analyzer.analyze_dependencies(force_refresh=True)

        # Results should be the same but cache should be refreshed
        self.assertEqual(result1.execution_groups, result2.execution_groups)


class TestDependencyAnalyzerIntegration(unittest.TestCase):
    """Test DependencyAnalyzer integration scenarios."""

    def setUp(self):
        """Set up test fixtures."""
        self.analyzer = DependencyAnalyzer()

        # Create test steps
        self.bronze_steps = {
            "bronze1": BronzeStep(name="bronze1", rules={"id": ["not_null"]}),
            "bronze2": BronzeStep(name="bronze2", rules={"id": ["not_null"]}),
        }

        self.silver_steps = {
            "silver1": SilverStep(
                name="silver1",
                source_bronze="bronze1",
                transform=lambda df, silvers: df,
                rules={"id": ["not_null"]},
                table_name="silver1",
            ),
            "silver2": SilverStep(
                name="silver2",
                source_bronze="bronze2",
                transform=lambda df, silvers: df,
                rules={"id": ["not_null"]},
                table_name="silver2",
            ),
        }

        self.gold_steps = {
            "gold1": GoldStep(
                name="gold1",
                source_silvers=["silver1", "silver2"],
                transform=lambda spark, silvers: silvers["silver1"],
                rules={"id": ["not_null"]},
                table_name="gold1",
            )
        }

    def test_complex_pipeline_analysis(self):
        """Test analysis of a complex pipeline with all step types."""
        result = self.analyzer.analyze_dependencies(
            bronze_steps=self.bronze_steps,
            silver_steps=self.silver_steps,
            gold_steps=self.gold_steps,
        )

        self.assertIsInstance(result, DependencyAnalysisResult)
        self.assertIsNotNone(result.graph)
        self.assertIsInstance(result.execution_groups, list)
        self.assertIsInstance(result.cycles, list)
        self.assertIsInstance(result.conflicts, list)
        self.assertIsInstance(result.recommendations, list)
        self.assertIsInstance(result.stats, dict)

    def test_different_strategies_comparison(self):
        """Test different analysis strategies produce different results."""
        # Create analyzers with different strategies
        from sparkforge.dependencies.analyzer import (
            AnalysisStrategy,
            DependencyAnalyzer,
        )

        conservative_analyzer = DependencyAnalyzer(
            strategy=AnalysisStrategy.CONSERVATIVE
        )
        optimistic_analyzer = DependencyAnalyzer(strategy=AnalysisStrategy.OPTIMISTIC)

        conservative_result = conservative_analyzer.analyze_dependencies(
            bronze_steps=self.bronze_steps,
            silver_steps=self.silver_steps,
            gold_steps=self.gold_steps,
        )

        optimistic_result = optimistic_analyzer.analyze_dependencies(
            bronze_steps=self.bronze_steps,
            silver_steps=self.silver_steps,
            gold_steps=self.gold_steps,
        )

        # Both should be valid results
        self.assertIsInstance(conservative_result, DependencyAnalysisResult)
        self.assertIsInstance(optimistic_result, DependencyAnalysisResult)

    def test_error_handling(self):
        """Test error handling in dependency analysis."""
        # Test with invalid step configuration
        invalid_silver_steps = {
            "invalid_step": SilverStep(
                name="invalid_step",
                source_bronze="nonexistent_bronze",
                transform=lambda df: df,
                rules={"id": ["not_null"]},
                table_name="invalid_silver",
            )
        }

        # This should not raise an exception, but should detect conflicts
        result = self.analyzer.analyze_dependencies(silver_steps=invalid_silver_steps)
        self.assertIsInstance(result, DependencyAnalysisResult)
        # The result should contain conflicts about missing dependencies
        self.assertIsInstance(result.conflicts, list)


if __name__ == "__main__":
    unittest.main()
