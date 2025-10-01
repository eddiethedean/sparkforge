"""
In-memory storage backend for Mock Spark.

This module provides an in-memory storage system using SQLite for fast,
SQL-compatible operations that can handle all DataFrame operations.
"""

import sqlite3
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from contextlib import contextmanager
from dataclasses import dataclass

from .types import MockStructType, MockStructField


@dataclass
class TableMetadata:
    """Metadata for a table."""
    
    schema: str
    table: str
    columns: List[MockStructField]
    created_at: float
    row_count: int = 0
    
    @property
    def fqn(self) -> str:
        """Fully qualified name."""
        return f"{self.schema}.{self.table}"


class MockTable:
    """Mock table for storing data."""
    
    def __init__(self, schema_name: str, table_name: str, schema: MockStructType):
        """Initialize MockTable."""
        self.schema_name = schema_name
        self.table_name = table_name
        self.schema = schema
        self.data: List[Dict[str, Any]] = []
        self.fqn = f"{schema_name}.{table_name}"
    
    def insert_data(self, data: List[Dict[str, Any]], mode: str = "append") -> None:
        """Insert data into table."""
        if mode == "append":
            self.data.extend(data)
        elif mode == "overwrite":
            self.data = data
        else:
            raise ValueError(f"Unsupported mode: {mode}")
    
    def query_data(self, filter_expr: Optional[str] = None) -> List[Dict[str, Any]]:
        """Query data from table."""
        if filter_expr is None:
            return self.data.copy()
        
        # Simple filter implementation
        # In a real implementation, this would parse and evaluate the filter expression
        return self.data.copy()  # Simplified for now
    
    def get_schema(self) -> MockStructType:
        """Get table schema."""
        return self.schema
    
    def get_metadata(self) -> Dict[str, Any]:
        """Get table metadata."""
        return {
            "schema_name": self.schema_name,
            "table_name": self.table_name,
            "fqn": self.fqn,
            "row_count": len(self.data),
            "column_count": len(self.schema)
        }


class MockStorageManager:
    """In-memory storage manager using SQLite."""
    
    def __init__(self):
        # Each schema gets its own in-memory SQLite database
        self._databases: Dict[str, sqlite3.Connection] = {}
        self._table_metadata: Dict[str, TableMetadata] = {}
        self._temp_tables: Set[str] = set()
    
    def get_database(self, schema: str) -> sqlite3.Connection:
        """Get or create database for schema."""
        if schema not in self._databases:
            conn = sqlite3.connect(":memory:")
            conn.row_factory = sqlite3.Row  # Enable dict-like access
            self._databases[schema] = conn
        return self._databases[schema]
    
    def create_schema(self, schema: str) -> None:
        """Create a schema (database)."""
        if schema not in self._databases:
            conn = sqlite3.connect(":memory:")
            conn.row_factory = sqlite3.Row
            self._databases[schema] = conn
    
    def schema_exists(self, schema: str) -> bool:
        """Check if schema exists."""
        return schema in self._databases
    
    def drop_schema(self, schema: str) -> None:
        """Drop a schema."""
        if schema in self._databases:
            # Close the connection
            self._databases[schema].close()
            del self._databases[schema]
    
    def list_schemas(self) -> List[str]:
        """List all schemas."""
        return list(self._databases.keys())
    
    def get_table(self, schema: str, table: str) -> Optional[MockTable]:
        """Get table object."""
        if not self.table_exists(schema, table):
            return None
        
        # Create a MockTable from the SQLite data
        columns = self.get_table_schema(schema, table)
        if not columns:
            return None
        
        from .types import MockStructType
        struct_type = MockStructType(columns)
        mock_table = MockTable(schema, table, struct_type)
        
        # Load data from SQLite
        data = self.query_table(schema, table)
        mock_table.data = data
        
        return mock_table
    
    def insert_data(self, schema: str, table: str, data: List[Dict[str, Any]], mode: str = "append") -> int:
        """Insert data into table."""
        if not self.table_exists(schema, table):
            raise ValueError(f"Table {schema}.{table} does not exist")
        
        conn = self.get_database(schema)
        
        if mode == "overwrite":
            # Clear existing data
            conn.execute(f"DELETE FROM {table}")
        
        # Insert new data
        if data:
            columns = list(data[0].keys())
            placeholders = ", ".join("?" for _ in columns)
            insert_sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
            
            for row in data:
                values = [row.get(col) for col in columns]
                conn.execute(insert_sql, values)
            
            conn.commit()
        
        return len(data)
    
    def query_table(self, schema: str, table: str, filter_expr: str = None) -> List[Dict[str, Any]]:
        """Query table data."""
        if not self.table_exists(schema, table):
            return []
        
        conn = self.get_database(schema)
        
        if filter_expr:
            # Simple filter implementation - in real implementation would parse SQL
            sql = f"SELECT * FROM {table}"
        else:
            sql = f"SELECT * FROM {table}"
        
        cursor = conn.execute(sql)
        rows = cursor.fetchall()
        
        # Convert to list of dicts
        return [dict(row) for row in rows]
    
    def get_table_schema(self, schema: str, table: str) -> Optional[List[MockStructField]]:
        """Get table schema."""
        if not self.table_exists(schema, table):
            return None
        
        conn = self.get_database(schema)
        cursor = conn.execute(f"PRAGMA table_info({table})")
        columns = cursor.fetchall()
        
        fields = []
        for col in columns:
            col_name = col[1]  # Column name
            col_type = col[2]  # Column type
            
            # Convert SQLite type to MockStructField
            if "INT" in col_type.upper():
                field_type = IntegerType()
            elif "TEXT" in col_type.upper() or "VARCHAR" in col_type.upper():
                field_type = StringType()
            else:
                field_type = StringType()  # Default
            
            fields.append(MockStructField(col_name, field_type))
        
        return fields
    
    def get_table_metadata(self, schema: str, table: str) -> Dict[str, Any]:
        """Get table metadata."""
        if not self.table_exists(schema, table):
            return {}
        
        conn = self.get_database(schema)
        cursor = conn.execute(f"SELECT COUNT(*) as count FROM {table}")
        row_count = cursor.fetchone()[0]
        
        columns = self.get_table_schema(schema, table)
        column_count = len(columns) if columns else 0
        
        return {
            "schema_name": schema,
            "table_name": table,
            "fqn": f"{schema}.{table}",
            "row_count": row_count,
            "column_count": column_count
        }
    
    def list_tables(self, schema: str) -> List[str]:
        """List tables in schema."""
        if schema not in self._databases:
            return []
        
        conn = self.get_database(schema)
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        return tables
    
    def clear_all(self) -> None:
        """Clear all data."""
        self._databases.clear()
        self._table_metadata.clear()
        self._temp_tables.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        total_schemas = len(self._databases)
        total_tables = sum(len(self.list_tables(schema)) for schema in self._databases)
        total_rows = 0
        
        for schema in self._databases:
            for table in self.list_tables(schema):
                metadata = self.get_table_metadata(schema, table)
                total_rows += metadata.get("row_count", 0)
        
        return {
            "total_schemas": total_schemas,
            "total_tables": total_tables,
            "total_rows": total_rows
        }
    
    def create_table(self, schema: str, table: str, columns: List[MockStructField]) -> None:
        """Create table with schema."""
        conn = self.get_database(schema)
        
        # Convert MockStructField to SQLite schema
        sqlite_columns = []
        for field in columns:
            sqlite_type = self._convert_mock_type_to_sqlite(field.dataType)
            nullable = "NULL" if field.nullable else "NOT NULL"
            sqlite_columns.append(f"{field.name} {sqlite_type} {nullable}")
        
        sqlite_schema = ", ".join(sqlite_columns)
        conn.execute(f"CREATE TABLE {table} ({sqlite_schema})")
        
        # Store metadata
        fqn = f"{schema}.{table}"
        self._table_metadata[fqn] = TableMetadata(
            schema=schema,
            table=table,
            columns=columns,
            created_at=time.time()
        )
    
    def insert_data(self, schema: str, table: str, data: List[Dict[str, Any]]) -> int:
        """Insert data into table."""
        conn = self.get_database(schema)
        if not data:
            return 0
        
        # Get column names from first row
        columns = list(data[0].keys())
        
        # Check if table exists and get current schema
        table_info = self.get_table(schema, table)
        if table_info:
            existing_columns = [field.name for field in table_info.schema.fields]
            # Add missing columns to table
            missing_columns = [col for col in columns if col not in existing_columns]
            if missing_columns:
                for col in missing_columns:
                    # Add column to table (simplified - just add as TEXT)
                    try:
                        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} TEXT")
                    except sqlite3.OperationalError:
                        # Column might already exist, ignore
                        pass
        
        placeholders = ", ".join("?" * len(columns))
        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        
        # Convert data to tuples, handling various types
        values = []
        for row in data:
            row_values = []
            for col in columns:
                value = row[col]
                if hasattr(value, 'name'):  # MockColumn object
                    row_values.append(str(value))  # Convert to string
                elif isinstance(value, datetime):
                    row_values.append(value.isoformat())  # Convert datetime to string
                elif value is None:
                    row_values.append(None)
                elif isinstance(value, (int, float, str, bool)):
                    row_values.append(value)
                else:
                    row_values.append(str(value))  # Convert other types to string
            values.append(tuple(row_values))
        
        conn.executemany(query, values)
        conn.commit()
        
        # Update metadata
        fqn = f"{schema}.{table}"
        if fqn in self._table_metadata:
            self._table_metadata[fqn].row_count += len(data)
        
        return len(data)
    
    def query_table(self, schema: str, table: str, sql_query: str = None) -> List[Dict[str, Any]]:
        """Query table data."""
        conn = self.get_database(schema)
        if sql_query:
            cursor = conn.execute(sql_query)
        else:
            cursor = conn.execute(f"SELECT * FROM {table}")
        
        return [dict(row) for row in cursor.fetchall()]
    
    def table_exists(self, schema: str, table: str) -> bool:
        """Check if table exists."""
        try:
            conn = self.get_database(schema)
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?", 
                (table,)
            )
            return cursor.fetchone() is not None
        except Exception:
            return False
    
    def drop_table(self, schema: str, table: str) -> bool:
        """Drop table if it exists."""
        try:
            conn = self.get_database(schema)
            conn.execute(f"DROP TABLE IF EXISTS {table}")
            conn.commit()
            
            # Remove from metadata
            fqn = f"{schema}.{table}"
            if fqn in self._table_metadata:
                del self._table_metadata[fqn]
            
            return True
        except Exception:
            return False
    
    def truncate_table(self, schema: str, table: str) -> None:
        """Truncate table (remove all data)."""
        conn = self.get_database(schema)
        conn.execute(f"DELETE FROM {table}")
        conn.commit()
        
        # Update metadata
        fqn = f"{schema}.{table}"
        if fqn in self._table_metadata:
            self._table_metadata[fqn].row_count = 0
    
    def get_table_metadata(self, fqn: str) -> Optional[TableMetadata]:
        """Get table metadata."""
        return self._table_metadata.get(fqn)
    
    def list_tables(self, schema: str) -> List[str]:
        """List tables in schema."""
        try:
            conn = self.get_database(schema)
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
            return [row[0] for row in cursor.fetchall()]
        except Exception:
            return []
    
    def get_table_schema(self, schema: str, table: str) -> Optional[List[MockStructField]]:
        """Get table schema."""
        fqn = f"{schema}.{table}"
        metadata = self._table_metadata.get(fqn)
        return metadata.columns if metadata else None
    
    @contextmanager
    def transaction(self, schema: str):
        """Provide transaction support for ACID properties."""
        conn = self.get_database(schema)
        try:
            conn.execute("BEGIN TRANSACTION")
            yield conn
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    
    def _convert_mock_type_to_sqlite(self, mock_type) -> str:
        """Convert MockDataType to SQLite type."""
        type_mapping = {
            "StringType": "TEXT",
            "IntegerType": "INTEGER",
            "LongType": "INTEGER",
            "DoubleType": "REAL",
            "BooleanType": "INTEGER",  # SQLite uses INTEGER for boolean
            "DateType": "TEXT",
            "TimestampType": "TEXT",
        }
        
        type_name = mock_type.__class__.__name__
        return type_mapping.get(type_name, "TEXT")
    
    def create_index(self, schema: str, table: str, column: str) -> None:
        """Create index on column for performance."""
        conn = self.get_database(schema)
        index_name = f"idx_{table}_{column}"
        conn.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table} ({column})")
        conn.commit()
    
    def get_table_stats(self, schema: str, table: str) -> Dict[str, Any]:
        """Get table statistics."""
        fqn = f"{schema}.{table}"
        metadata = self._table_metadata.get(fqn)
        
        if not metadata:
            return {"row_count": 0, "column_count": 0}
        
        return {
            "row_count": metadata.row_count,
            "column_count": len(metadata.columns),
            "created_at": metadata.created_at,
            "schema": metadata.schema,
            "table": metadata.table
        }
