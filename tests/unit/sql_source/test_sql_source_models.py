"""Unit tests for JdbcSource and SqlAlchemySource model validation."""

import pytest

from pipeline_builder_base.errors import ValidationError

from pipeline_builder.sql_source.models import JdbcSource, SqlAlchemySource


class TestJdbcSource:
    def test_valid_table(self):
        s = JdbcSource(
            url="jdbc:postgresql://host:5432/db",
            table="public.orders",
            properties={"user": "u", "password": "p"},
        )
        assert s.table == "public.orders"
        assert s.query is None

    def test_valid_query(self):
        s = JdbcSource(
            url="jdbc:postgresql://host/db",
            query="(SELECT 1) AS q",
            properties={"user": "u", "password": "p"},
        )
        assert s.query == "(SELECT 1) AS q"
        assert s.table is None

    def test_both_table_and_query_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            JdbcSource(
                url="jdbc:postgresql://host/db",
                table="t",
                query="(SELECT 1) AS q",
                properties={},
            )
        assert "exactly one" in str(exc_info.value).lower()
        assert "table" in str(exc_info.value).lower()

    def test_neither_table_nor_query_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            JdbcSource(
                url="jdbc:postgresql://host/db",
                properties={},
            )
        assert "exactly one" in str(exc_info.value).lower()

    def test_empty_url_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            JdbcSource(
                url="",
                table="t",
                properties={},
            )
        assert "url" in str(exc_info.value).lower()

    def test_properties_not_dict_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            JdbcSource(
                url="jdbc:postgresql://host/db",
                table="t",
                properties="user=u",  # type: ignore[arg-type]
            )
        assert "properties" in str(exc_info.value).lower()

    def test_driver_stored_when_provided(self):
        s = JdbcSource(
            url="jdbc:postgresql://host/db",
            table="t",
            properties={"user": "u"},
            driver="org.postgresql.Driver",
        )
        assert s.driver == "org.postgresql.Driver"

    def test_empty_properties_allowed(self):
        s = JdbcSource(
            url="jdbc:postgresql://host/db",
            table="t",
            properties={},
        )
        assert s.properties == {}


class TestSqlAlchemySource:
    def test_valid_url_table(self):
        s = SqlAlchemySource(url="postgresql://u:p@host/db", table="orders")
        assert s.url == "postgresql://u:p@host/db"
        assert s.table == "orders"
        assert s.query is None

    def test_valid_url_query(self):
        s = SqlAlchemySource(url="sqlite:///file.db", query="SELECT * FROM t")
        assert s.query == "SELECT * FROM t"
        assert s.table is None

    def test_both_table_and_query_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            SqlAlchemySource(
                url="sqlite:///x.db",
                table="t",
                query="SELECT 1",
            )
        assert "exactly one" in str(exc_info.value).lower()

    def test_both_url_and_engine_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            SqlAlchemySource(
                url="sqlite:///x.db",
                engine=object(),
                table="t",
            )
        assert "url" in str(exc_info.value).lower() and "engine" in str(exc_info.value).lower()

    def test_neither_url_nor_engine_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            SqlAlchemySource(table="t")
        assert "url" in str(exc_info.value).lower() or "engine" in str(exc_info.value).lower()

    def test_schema_stored(self):
        s = SqlAlchemySource(
            url="postgresql://localhost/db",
            table="t",
            schema="public",
        )
        assert s.schema == "public"

    def test_empty_table_string_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            SqlAlchemySource(
                url="sqlite:///x.db",
                table="",
                query=None,
            )
        assert "exactly one" in str(exc_info.value).lower()

    def test_empty_query_string_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            SqlAlchemySource(
                url="sqlite:///x.db",
                table=None,
                query="   ",
            )
        assert "exactly one" in str(exc_info.value).lower()

    def test_engine_only_no_url_stored(self):
        engine = object()
        s = SqlAlchemySource(engine=engine, table="t")
        assert s.engine is engine
        assert s.url is None
