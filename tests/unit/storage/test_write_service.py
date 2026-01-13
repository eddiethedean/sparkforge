"""Tests for WriteService."""

from pipeline_builder.storage.table_service import TableService
from pipeline_builder.storage.write_service import WriteService


class TestWriteService:
    """Tests for WriteService."""

    def test_write_service_initialization(self, spark_session):
        """Test WriteService can be initialized."""
        table_service = TableService(spark_session)
        service = WriteService(spark_session, table_service)
        assert service.spark == spark_session
        assert service.table_service == table_service
        assert service.logger is not None
