"""
Comprehensive tests for pipeline_builder_base steps module.

Tests StepManager and step utilities.
"""

import pytest

from pipeline_builder_base.errors import ValidationError
from pipeline_builder_base.steps.manager import StepManager
from pipeline_builder_base.steps.utils import (
    classify_step_type,
    extract_step_dependencies,
    get_step_target,
    normalize_step_name,
)


class TestStepManager:
    """Test StepManager class."""

    def test_step_manager_initialization(self):
        """Test manager creation."""
        manager = StepManager()
        assert manager.bronze_steps == {}
        assert manager.silver_steps == {}
        assert manager.gold_steps == {}
        assert manager.validator is not None

    def test_add_step(self):
        """Test adding steps."""
        manager = StepManager()

        class MockBronzeStep:
            def __init__(self):
                self.name = "bronze1"
                self.rules = {"user_id": ["not_null"]}

        step = MockBronzeStep()
        manager.add_step(step, "bronze")

        assert "bronze1" in manager.bronze_steps

    def test_add_step_duplicate(self):
        """Test duplicate step handling."""
        manager = StepManager()

        class MockStep:
            def __init__(self):
                self.name = "step1"
                self.rules = {"user_id": ["not_null"]}

        step1 = MockStep()
        step2 = MockStep()

        manager.add_step(step1, "bronze")

        with pytest.raises(ValidationError):
            manager.add_step(step2, "bronze")

    def test_get_step(self):
        """Test step retrieval."""
        manager = StepManager()

        class MockStep:
            def __init__(self):
                self.name = "step1"
                self.rules = {"user_id": ["not_null"]}

        step = MockStep()
        manager.add_step(step, "bronze")

        retrieved = manager.get_step("step1", "bronze")
        assert retrieved == step

    def test_get_step_missing(self):
        """Test missing step handling."""
        manager = StepManager()
        step = manager.get_step("missing", "bronze")
        assert step is None

    def test_get_step_all_types(self):
        """Test getting step without specifying type."""
        manager = StepManager()

        class MockStep:
            def __init__(self):
                self.name = "step1"
                self.rules = {"user_id": ["not_null"]}

        step = MockStep()
        manager.add_step(step, "bronze")

        retrieved = manager.get_step("step1")
        assert retrieved == step

    def test_get_steps_by_type(self):
        """Test filtering by type."""
        manager = StepManager()

        class MockStep:
            def __init__(self, name):
                self.name = name
                self.rules = {"user_id": ["not_null"]}

        manager.add_step(MockStep("bronze1"), "bronze")
        manager.add_step(MockStep("silver1"), "silver")

        bronze_steps = manager.get_steps_by_type("bronze")
        assert "bronze1" in bronze_steps
        assert "silver1" not in bronze_steps

    def test_remove_step(self):
        """Test step removal by directly manipulating dict."""
        manager = StepManager()

        class MockStep:
            def __init__(self):
                self.name = "step1"
                self.rules = {"user_id": ["not_null"]}

        step = MockStep()
        manager.add_step(step, "bronze")

        # StepManager doesn't have remove_step, but we can test direct removal
        del manager.bronze_steps["step1"]
        assert "step1" not in manager.bronze_steps

    def test_clear_steps(self):
        """Test clearing all steps by direct manipulation."""
        manager = StepManager()

        class MockStep:
            def __init__(self, name):
                self.name = name
                self.rules = {"user_id": ["not_null"]}

        manager.add_step(MockStep("bronze1"), "bronze")
        manager.add_step(MockStep("silver1"), "silver")

        # StepManager doesn't have clear_steps, but we can test direct clearing
        manager.bronze_steps.clear()
        manager.silver_steps.clear()
        manager.gold_steps.clear()

        assert len(manager.bronze_steps) == 0
        assert len(manager.silver_steps) == 0

    def test_get_all_steps(self):
        """Test getting all steps."""
        manager = StepManager()

        class MockStep:
            def __init__(self, name):
                self.name = name
                self.rules = {"user_id": ["not_null"]}

        manager.add_step(MockStep("bronze1"), "bronze")
        manager.add_step(MockStep("silver1"), "silver")

        all_steps = manager.get_all_steps()
        assert "bronze" in all_steps
        assert "silver" in all_steps
        assert "gold" in all_steps

    def test_validate_all_steps(self):
        """Test validation of all steps."""
        manager = StepManager()

        class MockStep:
            def __init__(self, name):
                self.name = name
                self.rules = {"user_id": ["not_null"]}

        manager.add_step(MockStep("bronze1"), "bronze")

        errors = manager.validate_all_steps()
        assert isinstance(errors, list)


class TestStepUtils:
    """Test step utility functions."""

    def test_classify_step_type(self):
        """Test step type classification."""

        class BronzeStep:
            pass

        step = BronzeStep()
        result = classify_step_type(step)
        assert result == "bronze"

    def test_classify_step_type_from_attribute(self):
        """Test classification from type attribute."""

        class MockStep:
            def __init__(self):
                self.type = "silver"

        step = MockStep()
        result = classify_step_type(step)
        assert result == "silver"

    def test_extract_step_dependencies(self):
        """Test dependency extraction."""

        class MockSilverStep:
            def __init__(self):
                self.source_bronze = "bronze1"

        step = MockSilverStep()
        deps = extract_step_dependencies(step)
        assert "bronze1" in deps

    def test_extract_step_dependencies_gold(self):
        """Test dependency extraction from gold step."""

        class MockGoldStep:
            def __init__(self):
                self.source_silvers = ["silver1", "silver2"]

        step = MockGoldStep()
        deps = extract_step_dependencies(step)
        assert "silver1" in deps
        assert "silver2" in deps

    def test_get_step_target(self):
        """Test target extraction."""

        class MockStep:
            def __init__(self):
                self.table_name = "target_table"

        step = MockStep()
        target = get_step_target(step)
        assert target == "target_table"

    def test_get_step_target_missing(self):
        """Test target extraction when missing."""

        class MockStep:
            pass

        step = MockStep()
        target = get_step_target(step)
        assert target == ""

    def test_normalize_step_name(self):
        """Test name normalization."""
        assert normalize_step_name("  TestStep  ") == "teststep"
        assert normalize_step_name("TestStep") == "teststep"
        assert normalize_step_name("") == ""

    def test_normalize_step_name_edge_cases(self):
        """Test edge cases in normalization."""
        assert normalize_step_name("   ") == ""
        # normalize_step_name expects str, None would raise TypeError
        # This is expected behavior - function should be called with str
