"""
Unit tests for `_create_dataframe_writer` to ensure write modes and formats
are preserved for both Delta and parquet paths.
"""

from pipeline_builder.execution import _create_dataframe_writer


class DummyWriter:
    def __init__(self):
        self.format_value = None
        self.mode_value = None
        self.options = {}

    def format(self, fmt):
        self.format_value = fmt
        return self

    def mode(self, mode):
        self.mode_value = mode
        return self

    def option(self, key, value):
        self.options[key] = value
        return self


class DummyDataFrame:
    def __init__(self):
        self.write = DummyWriter()


def test_delta_writer_preserves_overwrite_mode(monkeypatch):
    """Delta path should avoid truncate by using append+overwriteSchema for overwrite."""
    df = DummyDataFrame()
    monkeypatch.setattr(
        "pipeline_builder.execution._is_delta_lake_available_execution", lambda spark: True
    )

    writer = _create_dataframe_writer(df, spark="dummy", mode="overwrite")

    assert writer.format_value == "delta"
    # overwrite is converted to append to avoid truncate on Delta V2 tables
    assert writer.mode_value == "append"
    # overwriteSchema is injected by default
    assert writer.options.get("overwriteSchema") == "true"


def test_parquet_writer_preserves_mode(monkeypatch):
    """Parquet fallback should also keep requested mode and options."""
    df = DummyDataFrame()
    monkeypatch.setattr(
        "pipeline_builder.execution._is_delta_lake_available_execution", lambda spark: False
    )

    writer = _create_dataframe_writer(df, spark="dummy", mode="append", extra="yes")

    assert writer.format_value == "parquet"
    assert writer.mode_value == "append"
    assert writer.options.get("extra") == "yes"


def test_delta_writer_respects_explicit_overwrite_schema(monkeypatch):
    """Caller-provided overwriteSchema should be preserved."""
    df = DummyDataFrame()
    monkeypatch.setattr(
        "pipeline_builder.execution._is_delta_lake_available_execution", lambda spark: True
    )

    writer = _create_dataframe_writer(
        df, spark="dummy", mode="overwrite", overwriteSchema="false"
    )

    assert writer.format_value == "delta"
    assert writer.mode_value == "append"
    # Caller override remains
    assert writer.options.get("overwriteSchema") == "false"

