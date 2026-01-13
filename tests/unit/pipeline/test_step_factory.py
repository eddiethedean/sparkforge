"""Tests for StepFactory."""

from pipeline_builder.pipeline.step_factory import StepFactory


class TestStepFactory:
    """Tests for StepFactory."""

    def test_step_factory_initialization(self, spark_session):
        """Test StepFactory can be initialized."""
        factory = StepFactory()
        assert factory.logger is not None
