"""Tests for TransformService."""

from pipeline_builder.transformation.transform_service import TransformService


class TestTransformService:
    """Tests for TransformService."""

    def test_transform_service_initialization(self, spark_session):
        """Test TransformService can be initialized."""
        service = TransformService(spark_session)
        assert service.spark == spark_session
        assert service.logger is not None
