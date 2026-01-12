"""
Shared fixtures for pipeline_builder_base tests.
"""

import pytest

from pipeline_builder_base.config import (
    create_development_config,
    create_production_config,
    create_test_config,
)
from pipeline_builder_base.logging import PipelineLogger
from pipeline_builder_base.models import (
    PipelineConfig,
    ValidationThresholds,
)


@pytest.fixture
def pipeline_config():
    """Standard PipelineConfig fixture."""
    return PipelineConfig(
        schema="test_schema",
        thresholds=ValidationThresholds(bronze=80.0, silver=85.0, gold=90.0),
        verbose=False,
    )


@pytest.fixture
def development_config():
    """Development config fixture."""
    return create_development_config("dev_schema")


@pytest.fixture
def production_config():
    """Production config fixture."""
    return create_production_config("prod_schema")


@pytest.fixture
def test_config():
    """Test config fixture."""
    return create_test_config("test_schema")


@pytest.fixture
def logger():
    """PipelineLogger fixture."""
    return PipelineLogger(verbose=False, name="TestLogger")


@pytest.fixture
def mock_step():
    """Mock step object fixture."""

    class MockStep:
        def __init__(
            self,
            name,
            step_type="bronze",
            source_bronze=None,
            source_silvers=None,
            rules=None,
        ):
            self.name = name
            self.type = step_type
            self.source_bronze = source_bronze
            self.source_silvers = source_silvers or []
            self.rules = rules or {}
            self.table_name = f"{name}_table"

    return MockStep


@pytest.fixture
def bronze_step_dict():
    """Bronze step dictionary fixture."""
    return {
        "name": "test_bronze",
        "rules": {"user_id": ["not_null"]},
        "incremental_col": "timestamp",
    }


@pytest.fixture
def silver_step_dict():
    """Silver step dictionary fixture."""
    return {
        "name": "test_silver",
        "source_bronze": "test_bronze",
        "transform": lambda x: x,
        "rules": {"user_id": ["not_null"]},
        "table_name": "test_silver_table",
    }


@pytest.fixture
def gold_step_dict():
    """Gold step dictionary fixture."""
    return {
        "name": "test_gold",
        "transform": lambda x: x,
        "rules": {"metric": ["not_null"]},
        "table_name": "test_gold_table",
        "source_silvers": ["test_silver"],
    }
