"""Tests for TableService."""

from pipeline_builder.storage.table_service import TableService


class TestTableService:
    """Tests for TableService."""

    def test_table_service_initialization(self, spark):
        """Test TableService can be initialized."""
        service = TableService(spark)
        assert service.spark == spark
        assert service.logger is not None
