"""
Monkey patch for mock-spark 0.3.1 DataFrame.select method to fix tuple row access issues.
"""

from mock_spark import MockDataFrame, MockStructType


def _patched_select(self, *columns):
    """Patched select method that handles tuple row access correctly."""
    if not columns:
        return self

    # Handle simple column selection
    col_names = []
    for col in columns:
        if isinstance(col, str):
            if col == "*":
                col_names.extend([field.name for field in self.schema.fields])
            else:
                col_names.append(col)
        else:
            col_names.append(str(col))

    # Validate columns exist
    available_columns = [field.name for field in self.schema.fields]
    for col_name in col_names:
        if col_name not in available_columns and col_name != "*":
            from mock_spark.errors import ColumnNotFoundException

            raise ColumnNotFoundException(col_name)

    # Filter data to selected columns
    filtered_data = []
    for row in self.data:
        filtered_row = []
        for col_name in col_names:
            if col_name == "*":
                filtered_row.extend(row)
            else:
                # Get column index
                col_index = available_columns.index(col_name)
                # Handle both tuple and dict-like row access
                if isinstance(row, (tuple, list)):
                    filtered_row.append(row[col_index])
                else:
                    # For dict-like rows, access by column name
                    filtered_row.append(row.get(col_name))
        filtered_data.append(tuple(filtered_row))

    # Create new schema with selected fields
    selected_fields = []
    for col_name in col_names:
        if col_name == "*":
            selected_fields.extend(self.schema.fields)
        else:
            for field in self.schema.fields:
                if field.name == col_name:
                    selected_fields.append(field)
                    break

    new_schema = MockStructType(selected_fields)
    return MockDataFrame(filtered_data, new_schema, self.storage)


def apply_mock_spark_patches():
    """Apply all necessary patches to mock-spark 0.3.1."""
    MockDataFrame.select = _patched_select
