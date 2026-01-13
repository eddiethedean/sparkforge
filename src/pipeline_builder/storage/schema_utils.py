"""
Schema utility functions.

This module provides utility functions for schema operations that are used
by both execution and storage modules.
"""

from __future__ import annotations

from typing import Any, Optional


def get_existing_schema_safe(spark: Any, table_name: str) -> Optional[Any]:
    """
    Safely get the schema of an existing table.

    Tries multiple methods to get the schema:
    1. Direct schema from spark.table()
    2. If empty schema (catalog sync issue), try DESCRIBE TABLE
    3. If still empty, try reading a sample of data to infer schema

    Args:
        spark: Spark session
        table_name: Fully qualified table name

    Returns:
        StructType schema if table exists and schema is readable (may be empty struct<>), None if table doesn't exist or schema can't be read
    """
    try:
        table_df = spark.table(table_name)
        schema = table_df.schema

        # If schema is empty (catalog sync issue), try DESCRIBE TABLE as fallback
        if not schema.fields or len(schema.fields) == 0:
            try:
                # Try DESCRIBE TABLE to get schema information
                describe_df = spark.sql(f"DESCRIBE TABLE {table_name}")
                describe_rows = describe_df.collect()

                # If DESCRIBE returns rows with column info, try to read schema from data
                if describe_rows and len(describe_rows) > 0:
                    # Try reading a sample row to infer schema
                    try:
                        sample_df = spark.sql(f"SELECT * FROM {table_name} LIMIT 1")
                        inferred_schema = sample_df.schema
                        if inferred_schema.fields and len(inferred_schema.fields) > 0:
                            return inferred_schema
                    except Exception:
                        pass
            except Exception:
                pass

        # Return schema even if empty (struct<>) - caller will handle empty schemas specially
        return schema
    except Exception:
        pass
    return None


def schemas_match(existing_schema: Any, output_schema: Any) -> tuple[bool, list[str]]:
    """
    Compare two schemas and determine if they match exactly.

    Args:
        existing_schema: Schema of the existing table
        output_schema: Schema of the output DataFrame

    Returns:
        Tuple of (matches: bool, differences: list[str])
        differences contains descriptions of any mismatches
    """
    differences = []

    # Extract field dictionaries
    existing_fields = (
        {f.name: f for f in existing_schema.fields} if existing_schema.fields else {}
    )
    output_fields = (
        {f.name: f for f in output_schema.fields} if output_schema.fields else {}
    )

    existing_columns = set(existing_fields.keys())
    output_columns = set(output_fields.keys())

    # Check for missing columns in output
    missing_in_output = existing_columns - output_columns
    if missing_in_output:
        differences.append(f"Missing columns in output: {sorted(missing_in_output)}")

    # Check for new columns in output
    new_in_output = output_columns - existing_columns
    if new_in_output:
        differences.append(
            f"New columns in output (not in existing table): {sorted(new_in_output)}"
        )

    # Check for type mismatches and nullable changes in common columns
    common_columns = existing_columns & output_columns
    type_mismatches = []
    nullable_changes = []
    for col in common_columns:
        existing_field = existing_fields[col]
        output_field = output_fields[col]

        # Check type mismatch
        if existing_field.dataType != output_field.dataType:
            type_mismatches.append(
                f"{col}: existing={existing_field.dataType}, "
                f"output={output_field.dataType}"
            )

        # Check nullable changes (nullable -> non-nullable is stricter, non-nullable -> nullable is more lenient)
        existing_nullable = getattr(existing_field, "nullable", True)
        output_nullable = getattr(output_field, "nullable", True)
        if existing_nullable != output_nullable:
            if not existing_nullable and output_nullable:
                # Existing is non-nullable, output is nullable - this is usually OK (more lenient)
                nullable_changes.append(
                    f"{col}: nullable changed from False to True (more lenient - usually OK)"
                )
            else:
                # Existing is nullable, output is non-nullable - this is stricter and may cause issues
                nullable_changes.append(
                    f"{col}: nullable changed from True to False (stricter - may cause issues if data has nulls)"
                )

    if type_mismatches:
        differences.append(f"Type mismatches: {', '.join(type_mismatches)}")

    if nullable_changes:
        # Note nullable changes but don't fail validation for them (Delta Lake handles this)
        differences.append(
            f"Nullable changes (informational): {', '.join(nullable_changes)}"
        )

    # Check for column order differences (informational only - order doesn't affect functionality)
    existing_order = list(existing_fields.keys())
    output_order = list(output_fields.keys())
    if (
        existing_order != output_order
        and common_columns == existing_columns == output_columns
    ):
        # All columns match, just order is different
        differences.append(
            f"Column order differs (informational - order doesn't affect functionality): "
            f"existing={existing_order}, output={output_order}"
        )

    return len(
        [d for d in differences if "informational" not in d.lower()]
    ) == 0, differences
