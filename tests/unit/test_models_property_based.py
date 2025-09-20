#!/usr/bin/env python3
"""
Property-based tests for sparkforge/models.py module using Hypothesis.

This module tests data models with generated data to ensure robustness
and catch edge cases that might be missed by traditional unit tests.
"""

from typing import List, Optional

from hypothesis import given, settings
from hypothesis import strategies as st

from sparkforge.models import (
    BronzeStep,
    GoldStep,
    ParallelConfig,
    PipelineConfig,
    SilverStep,
    ValidationThresholds,
)


class TestModelsPropertyBased:
    """Property-based tests for data models."""

    @given(
        bronze=st.floats(min_value=0.0, max_value=100.0),
        silver=st.floats(min_value=0.0, max_value=100.0),
        gold=st.floats(min_value=0.0, max_value=100.0),
    )
    @settings(max_examples=50)
    def test_validation_thresholds_properties(
        self, bronze: float, silver: float, gold: float
    ) -> None:
        """Test ValidationThresholds with generated float values."""
        thresholds = ValidationThresholds(bronze=bronze, silver=silver, gold=gold)

        # Property: All threshold values should be preserved
        assert thresholds.bronze == bronze
        assert thresholds.silver == silver
        assert thresholds.gold == gold

        # Property: Thresholds should be accessible via get_threshold
        from sparkforge.models import PipelinePhase

        assert thresholds.get_threshold(PipelinePhase.BRONZE) == bronze
        assert thresholds.get_threshold(PipelinePhase.SILVER) == silver
        assert thresholds.get_threshold(PipelinePhase.GOLD) == gold

    @given(enabled=st.booleans(), max_workers=st.integers(min_value=1, max_value=100))
    @settings(max_examples=50)
    def test_parallel_config_properties(self, enabled: bool, max_workers: int) -> None:
        """Test ParallelConfig with generated boolean and integer values."""
        parallel = ParallelConfig(enabled=enabled, max_workers=max_workers)

        # Property: All config values should be preserved
        assert parallel.enabled == enabled
        assert parallel.max_workers == max_workers

    @given(
        name=st.text(min_size=1, max_size=50), schema=st.text(min_size=1, max_size=30)
    )
    @settings(max_examples=50)
    def test_pipeline_config_properties(self, name: str, schema: str) -> None:
        """Test PipelineConfig with generated string values."""
        # Filter out strings with special characters that might cause issues
        if all(c.isalnum() or c in "_-" for c in name) and all(
            c.isalnum() or c in "_-" for c in schema
        ):
            config = PipelineConfig.create_default(schema)

            # Property: Schema should be preserved
            assert config.schema == schema

            # Property: Config should have default thresholds and parallel settings
            assert isinstance(config.thresholds, ValidationThresholds)
            assert isinstance(config.parallel, ParallelConfig)

    @given(
        name=st.text(min_size=1, max_size=30),
        incremental_col=st.one_of(st.none(), st.text(min_size=1, max_size=20)),
    )
    @settings(max_examples=50)
    def test_bronze_step_properties(
        self, name: str, incremental_col: Optional[str]
    ) -> None:
        """Test BronzeStep with generated string values."""
        # Filter out strings with special characters
        if all(c.isalnum() or c in "_-" for c in name):
            rules = {"col1": ["col1 > 0"], "col2": ["col2 IS NOT NULL"]}

            step = BronzeStep(name=name, rules=rules, incremental_col=incremental_col)

            # Property: All values should be preserved
            assert step.name == name
            assert step.rules == rules
            assert step.incremental_col == incremental_col

            # Property: Incremental capability should match incremental_col
            assert step.has_incremental_capability == (incremental_col is not None)

    @given(
        name=st.text(min_size=1, max_size=30),
        source_bronze=st.text(min_size=1, max_size=30),
        table_name=st.text(min_size=1, max_size=30),
    )
    @settings(max_examples=50)
    def test_silver_step_properties(
        self, name: str, source_bronze: str, table_name: str
    ) -> None:
        """Test SilverStep with generated string values."""
        # Filter out strings with special characters
        if (
            all(c.isalnum() or c in "_-" for c in name)
            and all(c.isalnum() or c in "_-" for c in source_bronze)
            and all(c.isalnum() or c in "_-" for c in table_name)
        ):

            def mock_transform(spark, bronze_df, prior_silvers):
                return bronze_df

            rules = {"col1": ["col1 > 0"]}

            step = SilverStep(
                name=name,
                source_bronze=source_bronze,
                transform=mock_transform,
                rules=rules,
                table_name=table_name,
            )

            # Property: All values should be preserved
            assert step.name == name
            assert step.source_bronze == source_bronze
            assert step.table_name == table_name
            assert step.rules == rules

    @given(
        name=st.text(min_size=1, max_size=30),
        table_name=st.text(min_size=1, max_size=30),
    )
    @settings(max_examples=50)
    def test_gold_step_properties(self, name: str, table_name: str) -> None:
        """Test GoldStep with generated string values."""
        # Filter out strings with special characters
        if all(c.isalnum() or c in "_-" for c in name) and all(
            c.isalnum() or c in "_-" for c in table_name
        ):

            def mock_transform(spark, silver_dfs):
                return silver_dfs

            rules = {"col1": ["col1 > 0"]}
            source_silvers = ["silver1", "silver2"]

            step = GoldStep(
                name=name,
                table_name=table_name,
                transform=mock_transform,
                rules=rules,
                source_silvers=source_silvers,
            )

            # Property: All values should be preserved
            assert step.name == name
            assert step.table_name == table_name
            assert step.rules == rules
            assert step.source_silvers == source_silvers

    @given(
        bronze1=st.floats(min_value=0.0, max_value=100.0),
        silver1=st.floats(min_value=0.0, max_value=100.0),
        gold1=st.floats(min_value=0.0, max_value=100.0),
        bronze2=st.floats(min_value=0.0, max_value=100.0),
        silver2=st.floats(min_value=0.0, max_value=100.0),
        gold2=st.floats(min_value=0.0, max_value=100.0),
    )
    @settings(max_examples=50)
    def test_validation_thresholds_equality_properties(
        self,
        bronze1: float,
        silver1: float,
        gold1: float,
        bronze2: float,
        silver2: float,
        gold2: float,
    ) -> None:
        """Test ValidationThresholds equality with generated values."""
        thresholds1 = ValidationThresholds(bronze=bronze1, silver=silver1, gold=gold1)
        thresholds2 = ValidationThresholds(bronze=bronze2, silver=silver2, gold=gold2)

        # Property: Equality should be based on all threshold values
        expected_equal = bronze1 == bronze2 and silver1 == silver2 and gold1 == gold2
        assert (thresholds1 == thresholds2) == expected_equal

        # Property: Self-equality should always be true
        assert thresholds1 == thresholds1
        assert thresholds2 == thresholds2

    @given(
        enabled1=st.booleans(),
        max_workers1=st.integers(min_value=1, max_value=100),
        enabled2=st.booleans(),
        max_workers2=st.integers(min_value=1, max_value=100),
    )
    @settings(max_examples=50)
    def test_parallel_config_equality_properties(
        self, enabled1: bool, max_workers1: int, enabled2: bool, max_workers2: int
    ) -> None:
        """Test ParallelConfig equality with generated values."""
        parallel1 = ParallelConfig(enabled=enabled1, max_workers=max_workers1)
        parallel2 = ParallelConfig(enabled=enabled2, max_workers=max_workers2)

        # Property: Equality should be based on both enabled and max_workers
        expected_equal = enabled1 == enabled2 and max_workers1 == max_workers2
        assert (parallel1 == parallel2) == expected_equal

        # Property: Self-equality should always be true
        assert parallel1 == parallel1
        assert parallel2 == parallel2

    @given(
        name=st.text(min_size=1, max_size=30),
        rules_size=st.integers(min_value=1, max_value=5),
    )
    @settings(max_examples=50)
    def test_bronze_step_rules_properties(self, name: str, rules_size: int) -> None:
        """Test BronzeStep with generated rules of varying sizes."""
        # Filter out strings with special characters
        if all(c.isalnum() or c in "_-" for c in name):
            # Generate rules dictionary
            rules = {}
            for i in range(rules_size):
                col_name = f"col{i}"
                rules[col_name] = [f"{col_name} > 0", f"{col_name} IS NOT NULL"]

            step = BronzeStep(name=name, rules=rules, incremental_col=None)

            # Property: Rules should be preserved exactly
            assert step.rules == rules
            assert len(step.rules) == rules_size

            # Property: All rule keys should be accessible
            for col_name in rules:
                assert col_name in step.rules
                assert len(step.rules[col_name]) == 2

    @given(
        threshold_values=st.lists(
            st.floats(min_value=0.0, max_value=100.0), min_size=3, max_size=3
        )
    )
    @settings(max_examples=50)
    def test_validation_thresholds_factory_methods_properties(
        self, threshold_values: List[float]
    ) -> None:
        """Test ValidationThresholds factory methods with generated values."""
        bronze, silver, gold = threshold_values

        # Test create_strict method
        strict_thresholds = ValidationThresholds.create_strict()

        # Property: Strict thresholds should all be high values
        assert strict_thresholds.bronze >= 95.0
        assert strict_thresholds.silver >= 95.0
        assert strict_thresholds.gold >= 95.0

        # Property: All strict thresholds should be <= 100.0
        assert strict_thresholds.bronze <= 100.0
        assert strict_thresholds.silver <= 100.0
        assert strict_thresholds.gold <= 100.0
