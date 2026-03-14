"""Tests for WriteService."""

from pipeline_builder.storage.table_service import TableService
from pipeline_builder.storage.write_service import WriteService


class TestWriteService:
    """Tests for WriteService."""

    def test_write_service_initialization(self, spark):
        """Test WriteService can be initialized."""
        table_service = TableService(spark)
        service = WriteService(spark, table_service)
        assert service.spark == spark
        assert service.table_service == table_service
        assert service.logger is not None
