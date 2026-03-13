"""Unit tests for SQLAlchemy write functionality (mocked).

Tests cover:
- SqlAlchemyWriteConfig validation
- write_sqlalchemy via pandas.DataFrame.to_sql
- Error handling and edge cases
"""

import builtins
from unittest.mock import MagicMock, Mock, patch

import pytest

from pipeline_builder.sql_source import (
    SqlAlchemyWriteConfig,
    write_sql_target,
    write_sqlalchemy,
)
from pipeline_builder_base.errors import ValidationError


class TestSqlAlchemyWriteConfigValidation:
    """Test SqlAlchemyWriteConfig validation."""

    def test_valid_config_with_url(self):
        """Test creating a valid config with URL."""
        config = SqlAlchemyWriteConfig(
            url="postgresql://user:pass@localhost/db",
            table="orders",
            schema="public",
            if_exists="append",
        )

        assert config.url == "postgresql://user:pass@localhost/db"
        assert config.table == "orders"
        assert config.schema == "public"
        assert config.if_exists == "append"
        assert config.index is False

    def test_valid_config_with_engine(self):
        """Test creating a valid config with engine."""
        mock_engine = Mock()
        config = SqlAlchemyWriteConfig(
            engine=mock_engine,
            table="orders",
        )

        assert config.engine is mock_engine
        assert config.table == "orders"

    def test_config_requires_table(self):
        """Test that config requires a table."""
        with pytest.raises(ValidationError, match="non-empty 'table'"):
            SqlAlchemyWriteConfig(
                url="postgresql://localhost/db",
                table="",
            )

    def test_config_requires_url_or_engine(self):
        """Test that config requires url or engine."""
        with pytest.raises(ValidationError, match="url.*engine"):
            SqlAlchemyWriteConfig(table="orders")

    def test_config_rejects_both_url_and_engine(self):
        """Test that config rejects both url and engine."""
        mock_engine = Mock()
        with pytest.raises(ValidationError, match="url.*engine"):
            SqlAlchemyWriteConfig(
                url="postgresql://localhost/db",
                engine=mock_engine,
                table="orders",
            )

    def test_config_requires_valid_if_exists(self):
        """Test that config requires valid if_exists value."""
        with pytest.raises(ValidationError, match="if_exists"):
            SqlAlchemyWriteConfig(
                url="postgresql://localhost/db",
                table="orders",
                if_exists="invalid",  # type: ignore
            )

    def test_config_valid_if_exists_values(self):
        """Test all valid if_exists values."""
        for if_exists in ["fail", "replace", "append"]:
            config = SqlAlchemyWriteConfig(
                url="postgresql://localhost/db",
                table="orders",
                if_exists=if_exists,  # type: ignore
            )
            assert config.if_exists == if_exists

    def test_config_default_if_exists_is_append(self):
        """Test that default if_exists is append."""
        config = SqlAlchemyWriteConfig(
            url="postgresql://localhost/db",
            table="orders",
        )
        assert config.if_exists == "append"

    def test_config_default_index_is_false(self):
        """Test that default index is False."""
        config = SqlAlchemyWriteConfig(
            url="postgresql://localhost/db",
            table="orders",
        )
        assert config.index is False

    def test_config_with_index_true(self):
        """Test config with index=True."""
        config = SqlAlchemyWriteConfig(
            url="postgresql://localhost/db",
            table="orders",
            index=True,
        )
        assert config.index is True


class TestWriteSqlAlchemy:
    """Test write_sqlalchemy functionality."""

    def _create_mock_spark_df(self, row_count: int = 10):
        """Create a mock Spark DataFrame that converts to pandas."""
        mock_df = Mock()
        mock_pdf = MagicMock()
        mock_pdf.__len__ = Mock(return_value=row_count)
        mock_df.toPandas.return_value = mock_pdf
        return mock_df, mock_pdf

    def test_write_sqlalchemy_with_url(self):
        """Test writing via SQLAlchemy with URL."""
        pd = pytest.importorskip("pandas")
        pytest.importorskip("sqlalchemy")

        mock_df, mock_pdf = self._create_mock_spark_df(row_count=25)

        config = SqlAlchemyWriteConfig(
            url="postgresql://user:pass@localhost/db",
            table="orders",
            schema="public",
            if_exists="append",
        )

        with patch("sqlalchemy.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            rows_written = write_sqlalchemy(mock_df, config)

            mock_create_engine.assert_called_once_with("postgresql://user:pass@localhost/db")
            mock_pdf.to_sql.assert_called_once_with(
                name="orders",
                con=mock_engine,
                schema="public",
                if_exists="append",
                index=False,
            )
            assert rows_written == 25

    def test_write_sqlalchemy_with_engine(self):
        """Test writing via SQLAlchemy with pre-created engine."""
        pd = pytest.importorskip("pandas")
        pytest.importorskip("sqlalchemy")

        mock_df, mock_pdf = self._create_mock_spark_df(row_count=15)
        mock_engine = Mock()

        config = SqlAlchemyWriteConfig(
            engine=mock_engine,
            table="orders",
            if_exists="replace",
        )

        with patch("sqlalchemy.create_engine") as mock_create_engine:
            rows_written = write_sqlalchemy(mock_df, config)

            mock_create_engine.assert_not_called()
            mock_pdf.to_sql.assert_called_once_with(
                name="orders",
                con=mock_engine,
                schema=None,
                if_exists="replace",
                index=False,
            )
            assert rows_written == 15

    def test_write_sqlalchemy_with_index(self):
        """Test writing with index=True."""
        pd = pytest.importorskip("pandas")
        pytest.importorskip("sqlalchemy")

        mock_df, mock_pdf = self._create_mock_spark_df()

        config = SqlAlchemyWriteConfig(
            url="postgresql://localhost/db",
            table="orders",
            index=True,
        )

        with patch("sqlalchemy.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            write_sqlalchemy(mock_df, config)

            call_kwargs = mock_pdf.to_sql.call_args[1]
            assert call_kwargs["index"] is True

    def test_write_sqlalchemy_fail_mode(self):
        """Test writing with if_exists='fail'."""
        pd = pytest.importorskip("pandas")
        pytest.importorskip("sqlalchemy")

        mock_df, mock_pdf = self._create_mock_spark_df()

        config = SqlAlchemyWriteConfig(
            url="postgresql://localhost/db",
            table="orders",
            if_exists="fail",
        )

        with patch("sqlalchemy.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            write_sqlalchemy(mock_df, config)

            call_kwargs = mock_pdf.to_sql.call_args[1]
            assert call_kwargs["if_exists"] == "fail"

    def test_write_sqlalchemy_missing_pandas_raises(self):
        """Test that missing pandas raises RuntimeError."""
        mock_df = Mock()

        config = SqlAlchemyWriteConfig(
            url="postgresql://localhost/db",
            table="orders",
        )

        real_import = getattr(builtins, "__import__", __import__)

        def block_pandas(name, *args, **kwargs):
            if name == "pandas":
                raise ImportError("No module named 'pandas'")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=block_pandas):
            with pytest.raises(RuntimeError, match="pandas"):
                write_sqlalchemy(mock_df, config)

    def test_write_sqlalchemy_missing_sqlalchemy_raises(self):
        """Test that missing sqlalchemy raises RuntimeError."""
        pytest.importorskip("pandas")
        mock_df = Mock()
        mock_df.toPandas.return_value = MagicMock()

        config = SqlAlchemyWriteConfig(
            url="postgresql://localhost/db",
            table="orders",
        )

        real_import = getattr(builtins, "__import__", __import__)

        def block_sqlalchemy(name, *args, **kwargs):
            if name == "sqlalchemy":
                raise ImportError("No module named 'sqlalchemy'")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=block_sqlalchemy):
            with pytest.raises(RuntimeError, match="sqlalchemy"):
                write_sqlalchemy(mock_df, config)


class TestWriteSqlTargetDispatch:
    """Test write_sql_target dispatch for SQLAlchemy."""

    def test_dispatch_to_sqlalchemy(self):
        """Test that SqlAlchemyWriteConfig dispatches to write_sqlalchemy."""
        pd = pytest.importorskip("pandas")
        pytest.importorskip("sqlalchemy")

        mock_df = Mock()
        mock_pdf = MagicMock()
        mock_pdf.__len__ = Mock(return_value=5)
        mock_df.toPandas.return_value = mock_pdf

        config = SqlAlchemyWriteConfig(
            url="postgresql://localhost/db",
            table="orders",
        )

        with patch("sqlalchemy.create_engine") as mock_create_engine:
            mock_engine = Mock()
            mock_create_engine.return_value = mock_engine

            rows = write_sql_target(mock_df, config)

            assert rows == 5
            mock_pdf.to_sql.assert_called_once()
