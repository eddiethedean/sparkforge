#!/usr/bin/env python3
"""
Tests to ensure enum consistency across the codebase.

This test module would have caught the ExecutionMode and WriteMode enum bugs
by checking that:
1. Enums defined in multiple locations have identical values
2. Related enums (like PipelineMode and ExecutionMode) have matching values
3. All expected enum values exist
4. Enum imports from different locations return the same values
"""

import pytest

# Import ExecutionMode from all possible locations
from pipeline_builder.models.enums import ExecutionMode as ExecutionModeModels
from pipeline_builder_base.models.enums import ExecutionMode as ExecutionModeBase

# Import WriteMode from all possible locations
from pipeline_builder.models.enums import WriteMode as WriteModeModels
from pipeline_builder.writer.models import WriteMode as WriteModeWriter
from pipeline_builder_base.models.enums import WriteMode as WriteModeBaseModels
from pipeline_builder_base.writer.models import WriteMode as WriteModeBaseWriter

# Import PipelineMode to check mapping
from pipeline_builder.pipeline.models import PipelineMode

# Import StepType and StepStatus from all locations
from pipeline_builder.execution import (
    StepType as StepTypeExecution,
    StepStatus as StepStatusExecution,
)
from pipeline_builder.types import (
    StepType as StepTypeTypes,
    StepStatus as StepStatusTypes,
)
from pipeline_builder_base.dependencies.graph import StepType as StepTypeBaseGraph
from pipeline_builder.dependencies.graph import StepType as StepTypeGraph


class TestExecutionModeConsistency:
    """Test that ExecutionMode is consistent across all import locations."""

    def test_execution_mode_has_all_required_values(self):
        """Test that ExecutionMode has all 4 required values."""
        expected_values = {"initial", "incremental", "full_refresh", "validation_only"}

        # Check models version
        models_values = {e.value for e in ExecutionModeModels}
        assert models_values == expected_values, (
            f"ExecutionMode in models/enums.py missing values. "
            f"Expected: {expected_values}, Got: {models_values}"
        )

        # Check base version
        base_values = {e.value for e in ExecutionModeBase}
        assert base_values == expected_values, (
            f"ExecutionMode in pipeline_builder_base/models/enums.py missing values. "
            f"Expected: {expected_values}, Got: {base_values}"
        )

    def test_execution_mode_models_matches_base(self):
        """Test that ExecutionMode from models matches base."""
        models_values = {e.value for e in ExecutionModeModels}
        base_values = {e.value for e in ExecutionModeBase}

        assert models_values == base_values, (
            f"ExecutionMode mismatch between models and base. "
            f"Models: {models_values}, Base: {base_values}"
        )

    def test_execution_mode_matches_pipeline_mode(self):
        """Test that ExecutionMode values match PipelineMode values."""
        execution_modes = {e.value for e in ExecutionModeModels}
        pipeline_modes = {e.value for e in PipelineMode}

        assert execution_modes == pipeline_modes, (
            f"ExecutionMode and PipelineMode mismatch. "
            f"ExecutionMode: {execution_modes}, PipelineMode: {pipeline_modes}"
        )

    def test_execution_mode_enum_values_exist(self):
        """Test that all ExecutionMode enum values can be accessed."""
        # This would have caught the AttributeError: FULL_REFRESH bug
        assert hasattr(ExecutionModeModels, "INITIAL")
        assert hasattr(ExecutionModeModels, "INCREMENTAL")
        assert hasattr(ExecutionModeModels, "FULL_REFRESH")
        assert hasattr(ExecutionModeModels, "VALIDATION_ONLY")

        assert hasattr(ExecutionModeBase, "INITIAL")
        assert hasattr(ExecutionModeBase, "INCREMENTAL")
        assert hasattr(ExecutionModeBase, "FULL_REFRESH")
        assert hasattr(ExecutionModeBase, "VALIDATION_ONLY")


class TestWriteModeConsistency:
    """Test that WriteMode is consistent across all import locations."""

    def test_write_mode_models_has_basic_values(self):
        """Test that WriteMode in models has at least OVERWRITE and APPEND."""
        models_values = {e.value for e in WriteModeModels}
        assert "overwrite" in models_values
        assert "append" in models_values

    def test_write_mode_writer_has_all_values(self):
        """Test that WriteMode in writer has all 4 values."""
        writer_values = {e.value for e in WriteModeWriter}
        expected_values = {"overwrite", "append", "merge", "ignore"}

        assert writer_values == expected_values, (
            f"WriteMode in writer/models.py missing values. "
            f"Expected: {expected_values}, Got: {writer_values}"
        )

    def test_write_mode_consistency_across_locations(self):
        """Test that WriteMode values are consistent across all locations."""
        models_values = {e.value for e in WriteModeModels}
        writer_values = {e.value for e in WriteModeWriter}
        base_models_values = {e.value for e in WriteModeBaseModels}
        base_writer_values = {e.value for e in WriteModeBaseWriter}

        # All should have at least OVERWRITE and APPEND
        basic_values = {"overwrite", "append"}
        assert models_values >= basic_values
        assert writer_values >= basic_values
        assert base_models_values >= basic_values
        assert base_writer_values >= basic_values

        # Writer versions should have MERGE and IGNORE
        extended_values = {"overwrite", "append", "merge", "ignore"}
        assert writer_values == extended_values, (
            f"WriteMode in writer should have all 4 values. Got: {writer_values}"
        )
        assert base_writer_values == extended_values, (
            f"WriteMode in base writer should have all 4 values. Got: {base_writer_values}"
        )

    def test_write_mode_enum_values_exist(self):
        """Test that all WriteMode enum values can be accessed from writer."""
        # Check writer version has all values
        assert hasattr(WriteModeWriter, "OVERWRITE")
        assert hasattr(WriteModeWriter, "APPEND")
        assert hasattr(WriteModeWriter, "MERGE")
        assert hasattr(WriteModeWriter, "IGNORE")

        # Check models version has basic values
        assert hasattr(WriteModeModels, "OVERWRITE")
        assert hasattr(WriteModeModels, "APPEND")


class TestStepTypeConsistency:
    """Test that StepType is consistent across all import locations."""

    def test_step_type_has_all_required_values(self):
        """Test that StepType has all 3 required values."""
        expected_values = {"bronze", "silver", "gold"}

        execution_values = {e.value for e in StepTypeExecution}
        types_values = {e.value for e in StepTypeTypes}
        base_graph_values = {e.value for e in StepTypeBaseGraph}
        graph_values = {e.value for e in StepTypeGraph}

        assert execution_values == expected_values
        assert types_values == expected_values
        assert base_graph_values == expected_values
        assert graph_values == expected_values

    def test_step_type_consistency_across_locations(self):
        """Test that StepType is consistent across all locations."""
        execution_values = {e.value for e in StepTypeExecution}
        types_values = {e.value for e in StepTypeTypes}
        base_graph_values = {e.value for e in StepTypeBaseGraph}
        graph_values = {e.value for e in StepTypeGraph}

        assert execution_values == types_values == base_graph_values == graph_values, (
            f"StepType mismatch: execution={execution_values}, types={types_values}, "
            f"base_graph={base_graph_values}, graph={graph_values}"
        )


class TestStepStatusConsistency:
    """Test that StepStatus is consistent across all import locations."""

    def test_step_status_has_all_required_values(self):
        """Test that StepStatus has all 5 required values."""
        expected_values = {"pending", "running", "completed", "failed", "skipped"}

        execution_values = {e.value for e in StepStatusExecution}
        types_values = {e.value for e in StepStatusTypes}

        assert execution_values == expected_values
        assert types_values == expected_values

    def test_step_status_consistency_across_locations(self):
        """Test that StepStatus is consistent across all locations."""
        execution_values = {e.value for e in StepStatusExecution}
        types_values = {e.value for e in StepStatusTypes}

        assert execution_values == types_values, (
            f"StepStatus mismatch: execution={execution_values}, types={types_values}"
        )


class TestPipelineModeExecutionModeMapping:
    """Test that PipelineMode to ExecutionMode mapping is complete."""

    def test_pipeline_mode_to_execution_mode_mapping(self):
        """Test that all PipelineMode values can be mapped to ExecutionMode."""
        from pipeline_builder.pipeline.runner import SimplePipelineRunner
        from pipeline_builder.models import PipelineConfig
        from unittest.mock import Mock

        # Create a minimal runner to access _convert_mode
        mock_spark = Mock()
        mock_config = PipelineConfig.create_default(schema="test")
        runner = SimplePipelineRunner(mock_spark, mock_config)

        # Test that all PipelineMode values can be converted
        # This would have caught the AttributeError: FULL_REFRESH bug
        for mode in PipelineMode:
            try:
                execution_mode = runner._convert_mode(mode)
                assert execution_mode is not None, (
                    f"PipelineMode.{mode.name} could not be converted to ExecutionMode"
                )
            except AttributeError as e:
                pytest.fail(
                    f"AttributeError when converting PipelineMode.{mode.name} to ExecutionMode: {e}"
                )

    def test_all_pipeline_modes_have_execution_mode_equivalents(self):
        """Test that every PipelineMode has a corresponding ExecutionMode."""
        pipeline_mode_values = {e.value for e in PipelineMode}
        execution_mode_values = {e.value for e in ExecutionModeModels}

        assert pipeline_mode_values == execution_mode_values, (
            f"PipelineMode and ExecutionMode values don't match. "
            f"PipelineMode: {pipeline_mode_values}, ExecutionMode: {execution_mode_values}"
        )


class TestEnumCompleteness:
    """Test that enums have all expected values."""

    def test_execution_mode_completeness(self):
        """Test that ExecutionMode has all expected values."""
        expected = {
            "INITIAL": "initial",
            "INCREMENTAL": "incremental",
            "FULL_REFRESH": "full_refresh",
            "VALIDATION_ONLY": "validation_only",
        }

        for name, value in expected.items():
            assert hasattr(ExecutionModeModels, name), f"ExecutionMode missing {name}"
            assert getattr(ExecutionModeModels, name).value == value, (
                f"ExecutionMode.{name} has wrong value. Expected {value}, "
                f"got {getattr(ExecutionModeModels, name).value}"
            )

    def test_pipeline_mode_completeness(self):
        """Test that PipelineMode has all expected values."""
        expected = {
            "INITIAL": "initial",
            "INCREMENTAL": "incremental",
            "FULL_REFRESH": "full_refresh",
            "VALIDATION_ONLY": "validation_only",
        }

        for name, value in expected.items():
            assert hasattr(PipelineMode, name), f"PipelineMode missing {name}"
            assert getattr(PipelineMode, name).value == value, (
                f"PipelineMode.{name} has wrong value. Expected {value}, "
                f"got {getattr(PipelineMode, name).value}"
            )
