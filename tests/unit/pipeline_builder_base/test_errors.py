"""
Comprehensive tests for pipeline_builder_base errors module.

Tests error context builders and suggestion generators.
"""

from pipeline_builder_base.errors.context import ErrorContext, SuggestionGenerator
from pipeline_builder_base.errors import (
    build_execution_context,
    build_validation_context,
)


class TestErrorContext:
    """Test ErrorContext class."""

    def test_error_context_creation(self):
        """Test ErrorContext creation."""
        context = ErrorContext(step_name="step1", step_type="bronze")
        assert context.context["step_name"] == "step1"
        assert context.context["step_type"] == "bronze"

    def test_error_context_to_dict(self):
        """Test context serialization."""
        context = ErrorContext(step_name="step1", error="Test error")
        context_dict = context.to_dict()

        assert isinstance(context_dict, dict)
        assert context_dict["step_name"] == "step1"
        assert context_dict["error"] == "Test error"

    def test_error_context_add(self):
        """Test adding context values."""
        context = ErrorContext()
        context.add("key", "value")

        assert context.context["key"] == "value"

    def test_error_context_merge(self):
        """Test context merging."""
        context1 = ErrorContext(step_name="step1")
        context1.add("key1", "value1")

        # Context doesn't have merge method, but we can test adding multiple values
        context1.add("key2", "value2")
        assert len(context1.context) == 3


class TestSuggestionGenerator:
    """Test SuggestionGenerator class."""

    def test_suggestion_generator_initialization(self):
        """Test generator creation."""
        # SuggestionGenerator is a static class, no initialization needed
        suggestions = SuggestionGenerator.suggest_fix_for_missing_dependency(
            "step1", "missing", "bronze"
        )
        assert isinstance(suggestions, list)

    def test_generate_suggestions_validation_error(self):
        """Test suggestions for validation errors."""
        suggestions = SuggestionGenerator.suggest_fix_for_duplicate_name(
            "step1", "bronze"
        )
        assert isinstance(suggestions, list)
        assert len(suggestions) > 0

    def test_generate_suggestions_dependency_error(self):
        """Test suggestions for dependency errors."""
        suggestions = SuggestionGenerator.suggest_fix_for_missing_dependency(
            "silver_step", "missing_bronze", "silver"
        )
        assert isinstance(suggestions, list)
        assert len(suggestions) > 0
        assert any("bronze" in s.lower() for s in suggestions)

    def test_generate_suggestions_config_error(self):
        """Test suggestions for config errors."""
        suggestions = SuggestionGenerator.suggest_fix_for_invalid_schema("")
        assert isinstance(suggestions, list)
        assert len(suggestions) > 0

    def test_generate_suggestions_empty(self):
        """Test empty suggestions."""
        # Some methods may return empty lists in edge cases
        suggestions = SuggestionGenerator.suggest_fix_for_missing_dependency("", "", "")
        assert isinstance(suggestions, list)


class TestErrorContextBuilders:
    """Test error context builder functions."""

    def test_build_execution_context(self):
        """Test execution context building."""

        class MockStep:
            def __init__(self):
                self.name = "step1"
                self.type = "bronze"

        step = MockStep()
        error = ValueError("Test error")
        context = build_execution_context(step, error)

        assert isinstance(context, dict)
        assert context["step_name"] == "step1"
        assert context["step_type"] == "bronze"
        assert context["error_type"] == "ValueError"
        assert context["error_message"] == "Test error"

    def test_build_execution_context_with_metadata(self):
        """Test context with metadata."""

        class MockStep:
            def __init__(self):
                self.name = "step1"

        step = MockStep()
        error = ValueError("Test error")
        context = build_execution_context(step, error)

        assert context["step_name"] == "step1"

    def test_build_validation_context(self):
        """Test validation context building."""

        class MockStep:
            def __init__(self):
                self.name = "step1"
                self.source_bronze = "bronze1"

        step = MockStep()
        context = build_validation_context(step, "silver")

        assert isinstance(context, dict)
        assert context["step_name"] == "step1"
        assert context["step_type"] == "silver"
        assert context["source_bronze"] == "bronze1"

    def test_build_validation_context_gold(self):
        """Test validation context for gold step."""

        class MockStep:
            def __init__(self):
                self.name = "gold1"
                self.source_silvers = ["silver1", "silver2"]

        step = MockStep()
        context = build_validation_context(step, "gold")

        assert context["step_name"] == "gold1"
        assert context["step_type"] == "gold"
        assert context["source_silvers"] == ["silver1", "silver2"]
