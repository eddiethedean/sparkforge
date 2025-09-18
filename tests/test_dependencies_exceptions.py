#!/usr/bin/env python3
"""
Tests for dependency analysis exceptions.

This module tests all exception classes in the dependencies.exceptions module.
"""


from sparkforge.dependencies.exceptions import (
    CircularDependencyError,
    DependencyAnalysisError,
    DependencyConflictError,
    DependencyError,
    InvalidDependencyError,
)


class TestDependencyError:
    """Test cases for DependencyError base class."""

    def test_dependency_error_basic(self):
        """Test basic DependencyError creation."""
        error = DependencyError("Test error message")

        assert str(error) == "Test error message"
        assert error.step_name is None

    def test_dependency_error_with_step_name(self):
        """Test DependencyError with step name."""
        error = DependencyError("Test error message", step_name="test_step")

        assert str(error) == "Test error message"
        assert error.step_name == "test_step"

    def test_dependency_error_inheritance(self):
        """Test that DependencyError inherits from Exception."""
        error = DependencyError("Test error")

        assert isinstance(error, Exception)
        assert isinstance(error, DependencyError)

    def test_dependency_error_with_empty_message(self):
        """Test DependencyError with empty message."""
        error = DependencyError("")

        assert str(error) == ""
        assert error.step_name is None

    def test_dependency_error_with_none_step_name(self):
        """Test DependencyError with None step name."""
        error = DependencyError("Test error", step_name=None)

        assert str(error) == "Test error"
        assert error.step_name is None


class TestDependencyAnalysisError:
    """Test cases for DependencyAnalysisError."""

    def test_dependency_analysis_error_basic(self):
        """Test basic DependencyAnalysisError creation."""
        error = DependencyAnalysisError("Analysis failed")

        assert str(error) == "Analysis failed"
        assert error.step_name is None
        assert error.analysis_step is None

    def test_dependency_analysis_error_with_step_name(self):
        """Test DependencyAnalysisError with step name."""
        error = DependencyAnalysisError("Analysis failed", analysis_step="test_step")

        assert str(error) == "Analysis failed"
        assert error.step_name == "test_step"
        assert error.analysis_step == "test_step"

    def test_dependency_analysis_error_with_analysis_step(self):
        """Test DependencyAnalysisError with analysis step."""
        error = DependencyAnalysisError(
            "Analysis failed", analysis_step="analysis_step"
        )

        assert str(error) == "Analysis failed"
        assert error.step_name == "analysis_step"
        assert error.analysis_step == "analysis_step"

    def test_dependency_analysis_error_with_both_steps(self):
        """Test DependencyAnalysisError with both step names."""
        error = DependencyAnalysisError(
            "Analysis failed", analysis_step="analysis_step"
        )

        assert str(error) == "Analysis failed"
        assert error.step_name == "analysis_step"
        assert error.analysis_step == "analysis_step"

    def test_dependency_analysis_error_inheritance(self):
        """Test that DependencyAnalysisError inherits from DependencyError."""
        error = DependencyAnalysisError("Test error")

        assert isinstance(error, Exception)
        assert isinstance(error, DependencyError)
        assert isinstance(error, DependencyAnalysisError)

    def test_dependency_analysis_error_constructor_parameters(self):
        """Test DependencyAnalysisError constructor parameter handling."""
        # Test with only message
        error1 = DependencyAnalysisError("Message only")
        assert error1.step_name is None
        assert error1.analysis_step is None

        # Test with analysis_step only
        error2 = DependencyAnalysisError("Message", analysis_step="analysis")
        assert error2.step_name == "analysis"
        assert error2.analysis_step == "analysis"


class TestCircularDependencyError:
    """Test cases for CircularDependencyError."""

    def test_circular_dependency_error_basic(self):
        """Test basic CircularDependencyError creation."""
        cycle = ["step1", "step2", "step3", "step1"]
        error = CircularDependencyError("Circular dependency detected", cycle)

        assert str(error) == "Circular dependency detected"
        assert error.step_name is None
        assert error.cycle == cycle

    def test_circular_dependency_error_with_step_name(self):
        """Test CircularDependencyError with step name."""
        cycle = ["step1", "step2", "step1"]
        error = CircularDependencyError("Circular dependency detected", cycle)

        assert str(error) == "Circular dependency detected"
        assert error.step_name is None
        assert error.cycle == cycle

    def test_circular_dependency_error_empty_cycle(self):
        """Test CircularDependencyError with empty cycle."""
        cycle = []
        error = CircularDependencyError("No cycle", cycle)

        assert str(error) == "No cycle"
        assert error.cycle == cycle

    def test_circular_dependency_error_single_step_cycle(self):
        """Test CircularDependencyError with single step cycle."""
        cycle = ["step1", "step1"]
        error = CircularDependencyError("Self dependency", cycle)

        assert str(error) == "Self dependency"
        assert error.cycle == cycle

    def test_circular_dependency_error_inheritance(self):
        """Test that CircularDependencyError inherits from DependencyError."""
        error = CircularDependencyError("Test error", ["step1", "step2"])

        assert isinstance(error, Exception)
        assert isinstance(error, DependencyError)
        assert isinstance(error, CircularDependencyError)

    def test_circular_dependency_error_cycle_immutability(self):
        """Test that cycle list is stored correctly."""
        original_cycle = ["step1", "step2", "step3"]
        error = CircularDependencyError("Test", original_cycle)

        # Modify original list
        original_cycle.append("step4")

        # Error's cycle should be affected since it's a reference
        assert error.cycle == ["step1", "step2", "step3", "step4"]


class TestInvalidDependencyError:
    """Test cases for InvalidDependencyError."""

    def test_invalid_dependency_error_basic(self):
        """Test basic InvalidDependencyError creation."""
        invalid_deps = ["dep1", "dep2", "dep3"]
        error = InvalidDependencyError("Invalid dependencies found", invalid_deps)

        assert str(error) == "Invalid dependencies found"
        assert error.step_name is None
        assert error.invalid_dependencies == invalid_deps

    def test_invalid_dependency_error_with_step_name(self):
        """Test InvalidDependencyError with step name."""
        invalid_deps = ["dep1", "dep2"]
        error = InvalidDependencyError("Invalid dependencies found", invalid_deps)

        assert str(error) == "Invalid dependencies found"
        assert error.step_name is None
        assert error.invalid_dependencies == invalid_deps

    def test_invalid_dependency_error_empty_list(self):
        """Test InvalidDependencyError with empty invalid dependencies list."""
        invalid_deps = []
        error = InvalidDependencyError("No invalid deps", invalid_deps)

        assert str(error) == "No invalid deps"
        assert error.invalid_dependencies == invalid_deps

    def test_invalid_dependency_error_single_dependency(self):
        """Test InvalidDependencyError with single invalid dependency."""
        invalid_deps = ["dep1"]
        error = InvalidDependencyError("Single invalid dep", invalid_deps)

        assert str(error) == "Single invalid dep"
        assert error.invalid_dependencies == invalid_deps

    def test_invalid_dependency_error_inheritance(self):
        """Test that InvalidDependencyError inherits from DependencyError."""
        error = InvalidDependencyError("Test error", ["dep1"])

        assert isinstance(error, Exception)
        assert isinstance(error, DependencyError)
        assert isinstance(error, InvalidDependencyError)

    def test_invalid_dependency_error_list_immutability(self):
        """Test that invalid dependencies list is stored correctly."""
        original_deps = ["dep1", "dep2"]
        error = InvalidDependencyError("Test", original_deps)

        # Modify original list
        original_deps.append("dep3")

        # Error's list should be affected since it's a reference
        assert error.invalid_dependencies == ["dep1", "dep2", "dep3"]


class TestDependencyConflictError:
    """Test cases for DependencyConflictError."""

    def test_dependency_conflict_error_basic(self):
        """Test basic DependencyConflictError creation."""
        conflicting_steps = ["step1", "step2", "step3"]
        error = DependencyConflictError(
            "Dependency conflict detected", conflicting_steps
        )

        assert str(error) == "Dependency conflict detected"
        assert error.step_name is None
        assert error.conflicting_steps == conflicting_steps

    def test_dependency_conflict_error_with_step_name(self):
        """Test DependencyConflictError with step name."""
        conflicting_steps = ["step1", "step2"]
        error = DependencyConflictError(
            "Dependency conflict detected", conflicting_steps
        )

        assert str(error) == "Dependency conflict detected"
        assert error.step_name is None
        assert error.conflicting_steps == conflicting_steps

    def test_dependency_conflict_error_empty_list(self):
        """Test DependencyConflictError with empty conflicting steps list."""
        conflicting_steps = []
        error = DependencyConflictError("No conflicts", conflicting_steps)

        assert str(error) == "No conflicts"
        assert error.conflicting_steps == conflicting_steps

    def test_dependency_conflict_error_single_step(self):
        """Test DependencyConflictError with single conflicting step."""
        conflicting_steps = ["step1"]
        error = DependencyConflictError("Single conflict", conflicting_steps)

        assert str(error) == "Single conflict"
        assert error.conflicting_steps == conflicting_steps

    def test_dependency_conflict_error_inheritance(self):
        """Test that DependencyConflictError inherits from DependencyError."""
        error = DependencyConflictError("Test error", ["step1"])

        assert isinstance(error, Exception)
        assert isinstance(error, DependencyError)
        assert isinstance(error, DependencyConflictError)

    def test_dependency_conflict_error_list_immutability(self):
        """Test that conflicting steps list is stored correctly."""
        original_steps = ["step1", "step2"]
        error = DependencyConflictError("Test", original_steps)

        # Modify original list
        original_steps.append("step3")

        # Error's list should be affected since it's a reference
        assert error.conflicting_steps == ["step1", "step2", "step3"]


class TestExceptionChaining:
    """Test exception chaining and error propagation."""

    def test_dependency_error_chaining(self):
        """Test that DependencyError can be chained with other exceptions."""
        try:
            raise ValueError("Original error")
        except ValueError as e:
            dependency_error = DependencyError("Dependency failed")
            dependency_error.__cause__ = e
            assert dependency_error.__cause__ is e

    def test_circular_dependency_error_chaining(self):
        """Test that CircularDependencyError can be chained."""
        try:
            raise RuntimeError("Original error")
        except RuntimeError as e:
            circular_error = CircularDependencyError(
                "Circular deps", ["step1", "step2"]
            )
            circular_error.__cause__ = e
            assert circular_error.__cause__ is e

    def test_exception_attributes_preserved(self):
        """Test that exception attributes are preserved during chaining."""
        error = CircularDependencyError("Test", ["step1", "step2"])
        error.__cause__ = ValueError("Cause")

        assert error.cycle == ["step1", "step2"]
        assert error.step_name is None
        assert error.__cause__ is not None


class TestExceptionStringRepresentation:
    """Test string representation of exceptions."""

    def test_dependency_error_str(self):
        """Test DependencyError string representation."""
        error = DependencyError("Test message")
        assert str(error) == "Test message"

    def test_dependency_analysis_error_str(self):
        """Test DependencyAnalysisError string representation."""
        error = DependencyAnalysisError("Analysis failed")
        assert str(error) == "Analysis failed"

    def test_circular_dependency_error_str(self):
        """Test CircularDependencyError string representation."""
        error = CircularDependencyError("Circular deps", ["step1", "step2"])
        assert str(error) == "Circular deps"

    def test_invalid_dependency_error_str(self):
        """Test InvalidDependencyError string representation."""
        error = InvalidDependencyError("Invalid deps", ["dep1", "dep2"])
        assert str(error) == "Invalid deps"

    def test_dependency_conflict_error_str(self):
        """Test DependencyConflictError string representation."""
        error = DependencyConflictError("Conflict", ["step1", "step2"])
        assert str(error) == "Conflict"
