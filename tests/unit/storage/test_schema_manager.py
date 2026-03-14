"""Tests for SchemaManager."""

from pipeline_builder.storage.schema_manager import SchemaManager


class TestSchemaManager:
    """Tests for SchemaManager."""

    def test_schema_manager_initialization(self, spark):
        """Test SchemaManager can be initialized."""
        service = SchemaManager(spark)
        assert service.spark == spark
        assert service.logger is not None
