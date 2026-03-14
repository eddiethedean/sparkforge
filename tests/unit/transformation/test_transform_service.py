"""Tests for TransformService."""

from pipeline_builder.transformation.transform_service import TransformService


class TestTransformService:
    """Tests for TransformService."""

    def test_transform_service_initialization(self, spark):
        """Test TransformService can be initialized."""
        service = TransformService(spark)
        assert service.spark == spark
        assert service.logger is not None
