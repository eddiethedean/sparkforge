# -----------------------------------------------------------------------------
# writer.py
# Minimal, strongly-typed log writer for PipelineBuilder reports.
# - Flattens a single PipelineBuilder report into compact log rows
# - Uses real TimestampType columns for all datetimes
# - Strict schema + typed helpers
# - Resilient to Silver entries that may not have a "transform" block
#   (e.g., when using with_silver_rules on an existing table)
# -----------------------------------------------------------------------------

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Literal, Optional, TypedDict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# If you have TypedDicts for the builder's report, you can import them.
# To keep this log writer self-contained, we type the incoming report
# loosely as Dict[str, Any] and validate keys at access time.
ReportDict = Dict[str, Any]


# ---------- Minimal log row (Typed) ----------

class MinimalLogRow(TypedDict):
    run_mode: Literal["initial", "incremental"]
    run_started_at: datetime | None
    run_ended_at: datetime | None

    phase: Literal["bronze", "silver", "gold"]
    step: str

    start_time: datetime | None
    end_time: datetime | None

    table_fqn: str | None
    write_mode: Literal["overwrite", "append"] | None

    input_rows: int | None
    output_rows: int | None
    rows_written: int | None

    valid_rows: int
    invalid_rows: int
    validation_rate: float

    # Kept for forward-compatibility with earlier reports; set to None
    # in the current builder which does not include watermark in the log row.
    previous_watermark: datetime | None
    filtered_rows: int | None


# ---------- Strict Spark schema for the minimal logs table ----------

MIN_LOG_SCHEMA = StructType([
    StructField("run_mode", StringType(), False),
    StructField("run_started_at", TimestampType(), True),
    StructField("run_ended_at", TimestampType(), True),

    StructField("phase", StringType(), False),
    StructField("step", StringType(), False),

    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),

    StructField("table_fqn", StringType(), True),
    StructField("write_mode", StringType(), True),

    StructField("input_rows", IntegerType(), True),
    StructField("output_rows", IntegerType(), True),
    StructField("rows_written", IntegerType(), True),

    StructField("valid_rows", IntegerType(), False),
    StructField("invalid_rows", IntegerType(), False),
    StructField("validation_rate", FloatType(), False),

    StructField("previous_watermark", TimestampType(), True),
    StructField("filtered_rows", IntegerType(), True),
])


# ---------- Flatten helpers (Typed) ----------

def _min_row_base(report: ReportDict, phase: Literal["bronze", "silver", "gold"], step: str) -> MinimalLogRow:
    run = report.get("run", {})
    return {
        "run_mode": run.get("mode", "initial"),  # type: ignore[assignment]
        "run_started_at": run.get("started_at"),
        "run_ended_at": run.get("ended_at"),

        "phase": phase,
        "step": step,

        "start_time": None,
        "end_time": None,

        "table_fqn": None,
        "write_mode": None,

        "input_rows": None,
        "output_rows": None,
        "rows_written": None,

        "valid_rows": 0,
        "invalid_rows": 0,
        "validation_rate": 100.0,

        "previous_watermark": None,
        "filtered_rows": None,
    }


def _row_from_bronze(phase_name: str, record: dict[str, Any], report: ReportDict) -> MinimalLogRow:
    row = _min_row_base(report, "bronze", phase_name)
    v = record.get("validation", {})
    row["start_time"] = v.get("start_at")
    row["end_time"] = v.get("end_at")
    row["valid_rows"] = int(v.get("valid_rows", 0))
    row["invalid_rows"] = int(v.get("invalid_rows", 0))
    row["validation_rate"] = float(v.get("validation_rate", 100.0))
    return row


def _row_from_silver(phase_name: str, record: dict[str, Any], report: ReportDict) -> MinimalLogRow:
    row = _min_row_base(report, "silver", phase_name)
    t = record.get("transform")  # may be None for with_silver_rules
    w = record.get("write", {})
    v = record.get("validation", {})

    # start/end: prefer transform window; else validation; else write
    row["start_time"] = (t or {}).get("start_at") or v.get("start_at") or w.get("start_at")
    row["end_time"]   = (t or {}).get("end_at")   or v.get("end_at")   or w.get("end_at")

    row["table_fqn"]  = record.get("table_fqn")
    row["write_mode"] = w.get("mode")

    row["input_rows"]   = int((t or {}).get("input_rows", 0)) if t else None
    row["output_rows"]  = int((t or {}).get("output_rows", 0)) if t else None
    row["rows_written"] = int(w.get("rows_written", 0))

    row["valid_rows"] = int(v.get("valid_rows", 0))
    row["invalid_rows"] = int(v.get("invalid_rows", 0))
    row["validation_rate"] = float(v.get("validation_rate", 100.0))

    # Current builder log row does not include watermark context
    row["previous_watermark"] = None
    row["filtered_rows"] = None
    return row


def _row_from_gold(phase_name: str, record: dict[str, Any], report: ReportDict) -> MinimalLogRow:
    row = _min_row_base(report, "gold", phase_name)
    t = record.get("transform", {})
    w = record.get("write", {})
    v = record.get("validation", {})

    row["start_time"] = t.get("start_at") or v.get("start_at") or w.get("start_at")
    row["end_time"]   = t.get("end_at")   or v.get("end_at")   or w.get("end_at")

    row["table_fqn"]  = record.get("table_fqn")
    row["write_mode"] = w.get("mode")

    row["input_rows"]   = int(t.get("input_rows", 0))
    row["output_rows"]  = int(t.get("output_rows", 0))
    row["rows_written"] = int(w.get("rows_written", 0))

    row["valid_rows"] = int(v.get("valid_rows", 0))
    row["invalid_rows"] = int(v.get("invalid_rows", 0))
    row["validation_rate"] = float(v.get("validation_rate", 100.0))

    row["previous_watermark"] = None
    row["filtered_rows"] = None
    return row


def flatten_report_min(report: ReportDict) -> list[MinimalLogRow]:
    rows: list[MinimalLogRow] = []

    for bname, brec in report.get("bronze", {}).items():
        rows.append(_row_from_bronze(bname, brec, report))

    for sname, srec in report.get("silver", {}).items():
        rows.append(_row_from_silver(sname, srec, report))

    for gname, grec in report.get("gold", {}).items():
        rows.append(_row_from_gold(gname, grec, report))

    return rows


# ---------- LogWriter (class) ----------

class LogWriter:
    """
    Minimal log writer around the schema-stable flattened logs.

    Methods
    -------
    - create_table(initial_report)
      Overwrites/creates the Delta table with flattened rows.
    - append(report)
      Appends flattened rows to the existing Delta table.
    - show(n=None)
      Convenience display helper.
    """

    def __init__(
        self,
        spark: SparkSession,
        write_schema: str,
        logs_table_name: str
    ) -> None:
        self.spark = spark
        self.write_schema = write_schema
        self.logs_table_name = logs_table_name
        self.schema = MIN_LOG_SCHEMA

    def create_table(self, initial_report: ReportDict) -> DataFrame:
        rows = flatten_report_min(initial_report)
        df = self.spark.createDataFrame(rows, schema=self.schema)
        (df.write
           .format("delta")
           .mode("overwrite")
           .option("overwriteSchema", "true")
           .saveAsTable(f"{self.write_schema}.{self.logs_table_name}"))
        return df

    def append(self, report: ReportDict) -> DataFrame:
        rows = flatten_report_min(report)
        df = self.spark.createDataFrame(rows, schema=self.schema)
        (df.write
           .format("delta")
           .mode("append")
           .saveAsTable(f"{self.write_schema}.{self.logs_table_name}"))
        return df

    def show(self, n: int | None = None) -> None:
        self.spark.table(f"{self.write_schema}.{self.logs_table_name}").show(n)
