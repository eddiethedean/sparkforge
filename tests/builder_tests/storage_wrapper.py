"""
Storage wrapper utility for SparkSession.

This module provides a storage wrapper that adds convenient methods
for schema and table operations to SparkSession objects.
Works with both mock-spark and real PySpark.
"""

from typing import Any, List, Optional, Union

from pipeline_builder.compat import DataFrame, SparkSession, types


class StorageWrapper:
    """
    Storage wrapper that provides convenient methods for schema and table operations.

    This wrapper works with both mock-spark and real PySpark sessions,
    providing a consistent API for test code.
    """

    def __init__(self, spark: SparkSession):  # type: ignore[valid-type]
        """
        Initialize the storage wrapper.

        Args:
            spark: SparkSession instance (mock or real)
        """
        self.spark = spark

    def create_schema(self, schema_name: str) -> None:
        """
        Create database/schema if it doesn't exist.

        Works with both mock-spark (uses SCHEMA) and PySpark (uses DATABASE).

        Args:
            schema_name: Name of the schema/database to create
        """
        try:
            # Try CREATE SCHEMA first (works for mock-spark)
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")  # type: ignore[attr-defined]
        except Exception:
            # If that fails, try CREATE DATABASE (works for PySpark)
            try:
                self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema_name}")  # type: ignore[attr-defined]
            except Exception:
                # If both fail, try without IF NOT EXISTS
                try:
                    self.spark.sql(f"CREATE SCHEMA {schema_name}")  # type: ignore[attr-defined]
                except Exception:
                    self.spark.sql(f"CREATE DATABASE {schema_name}")  # type: ignore[attr-defined]

    def schema_exists(self, schema_name: str) -> bool:
        """
        Check if schema/database exists.

        Args:
            schema_name: Name of the schema/database to check

        Returns:
            True if schema exists, False otherwise
        """
        try:
            databases = [db.name for db in self.spark.catalog.listDatabases()]  # type: ignore[attr-defined]
            return schema_name in databases
        except Exception:
            return False

    def create_table(
        self,
        schema: str,
        table: str,
        fields: Optional[List[types.StructField]] = None,
    ) -> None:
        """
        Create table with optional schema fields.

        Args:
            schema: Schema/database name
            table: Table name
            fields: Optional list of StructField objects defining the table schema
        """
        table_fqn = f"{schema}.{table}"

        if fields:
            # Convert StructField objects to SQL DDL
            # For mock-spark compatibility, we'll create the table using DataFrame
            # instead of SQL DDL which has syntax differences
            try:
                # Try creating an empty DataFrame with the schema and save it as a table
                empty_data = []
                df = self.spark.createDataFrame(empty_data, types.StructType(fields))  # type: ignore[attr-defined]
                df.write.mode("overwrite").saveAsTable(table_fqn)  # type: ignore[attr-defined]
            except Exception:
                # If that fails, try SQL DDL (for real Spark)
                field_defs = []
                for field in fields:
                    # Get data type as string
                    data_type_str = self._field_type_to_sql(field.dataType)
                    # For SQL DDL, only add NOT NULL if field is not nullable
                    # (NULL is default, so we don't need to specify it)
                    if field.nullable:
                        field_defs.append(f"{field.name} {data_type_str}")
                    else:
                        field_defs.append(f"{field.name} {data_type_str} NOT NULL")

                ddl = (
                    f"CREATE TABLE IF NOT EXISTS {table_fqn} ({', '.join(field_defs)})"
                )
                self.spark.sql(ddl)  # type: ignore[attr-defined]
        else:
            # Create empty table with a dummy column (tests may populate later)
            try:
                # Try using DataFrame approach first (works better with mock-spark)
                from pipeline_builder.compat import IntegerType, StructField, StructType

                schema_obj = StructType([StructField("dummy", IntegerType(), True)])
                empty_df = self.spark.createDataFrame([], schema_obj)  # type: ignore[attr-defined]
                empty_df.write.mode("overwrite").saveAsTable(table_fqn)  # type: ignore[attr-defined]
            except Exception:
                # Fall back to SQL
                self.spark.sql(  # type: ignore[attr-defined]
                    f"CREATE TABLE IF NOT EXISTS {table_fqn} (dummy INT)"
                )

    def _field_type_to_sql(self, data_type: Any) -> str:
        """
        Convert Spark data type to SQL string.

        Args:
            data_type: Spark data type object

        Returns:
            SQL type string
        """
        type_name = str(data_type).lower()

        # Map common types
        type_mapping = {
            "stringtype": "STRING",
            "integertype": "INT",
            "longtype": "BIGINT",
            "doubletype": "DOUBLE",
            "floattype": "FLOAT",
            "booleantype": "BOOLEAN",
            "datetype": "DATE",
            "timestamptype": "TIMESTAMP",
            "decimaltype": "DECIMAL",
        }

        for spark_type, sql_type in type_mapping.items():
            if spark_type in type_name:
                return sql_type

        # Default to STRING if unknown
        return "STRING"

    def table_exists(self, schema: str, table: str) -> bool:
        """
        Check if table exists.

        Args:
            schema: Schema/database name
            table: Table name

        Returns:
            True if table exists, False otherwise
        """
        try:
            tables = [t.name for t in self.spark.catalog.listTables(schema)]  # type: ignore[attr-defined]
            return table in tables
        except Exception:
            # If listTables fails, try querying the table directly
            try:
                self.spark.table(f"{schema}.{table}")  # type: ignore[attr-defined]
                return True
            except Exception:
                return False

    def query_table(self, schema: str, table: str) -> List[dict]:
        """
        Query table and return list of dictionaries.

        This method returns a list of dicts to be compatible with tests
        that expect len() to work on the result.

        Args:
            schema: Schema/database name
            table: Table name

        Returns:
            List of dictionaries containing table data
        """
        table_fqn = f"{schema}.{table}"
        df = self.spark.table(table_fqn)  # type: ignore[attr-defined]
        # Convert to list of dicts for compatibility with tests that use len()
        return [row.asDict() for row in df.collect()]  # type: ignore[attr-defined]

    def insert_data(
        self,
        schema: str,
        table: str,
        data: Union[List[dict], DataFrame],  # type: ignore[valid-type]
    ) -> None:
        """
        Insert data into table.

        Args:
            schema: Schema/database name
            table: Table name
            data: Data to insert - can be list of dicts or DataFrame
        """
        table_fqn = f"{schema}.{table}"

        if isinstance(data, list):
            # Convert list of dicts to DataFrame
            df = self.spark.createDataFrame(data)  # type: ignore[attr-defined]
        else:
            # Assume DataFrame
            df = data

        # Write to table using append mode
        try:
            df.write.mode("append").saveAsTable(table_fqn)  # type: ignore[attr-defined]
        except Exception:
            # If saveAsTable fails, try using SQL INSERT
            try:
                # Register as temp view
                temp_view = f"temp_{table}_{id(df)}"
                df.createOrReplaceTempView(temp_view)  # type: ignore[attr-defined]
                # Insert using SQL
                self.spark.sql(  # type: ignore[attr-defined]
                    f"INSERT INTO {table_fqn} SELECT * FROM {temp_view}"
                )
            except Exception as e:
                raise RuntimeError(
                    f"Failed to insert data into {table_fqn}: {e}"
                ) from e
