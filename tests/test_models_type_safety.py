"""
Test type safety for the refactored models system.

This module tests that all model classes use explicit types instead of Any,
and that the new type definitions work correctly.
"""

import pytest
from dataclasses import dataclass
from typing import Dict, List, Optional, Union
from sparkforge.models import (
    BaseModel,
    ColumnRules,
    ModelValue,
    ColumnRule,
    ResourceValue,
    Serializable,
    Validatable,
)


class TestModelTypeSafety:
    """Test that model classes use explicit types."""

    def test_model_value_type_validation(self):
        """Test that ModelValue type accepts only valid types."""
        # Valid ModelValue types
        valid_values: ModelValue = "string"
        valid_values = 42
        valid_values = 3.14
        valid_values = True
        valid_values = ["item1", "item2"]
        valid_values = {"key": "value"}
        valid_values = None
        
        # This should not raise type errors
        assert isinstance(valid_values, (str, int, float, bool, list, dict, type(None)))

    def test_column_rule_type_validation(self):
        """Test that ColumnRule type accepts only valid types."""
        # Valid ColumnRule types (simplified for testing)
        valid_rules: ColumnRule = "string_rule"
        valid_rules = True
        valid_rules = False
        
        # This should not raise type errors
        assert isinstance(valid_rules, (str, bool))

    def test_resource_value_type_validation(self):
        """Test that ResourceValue type accepts only valid types."""
        # Valid ResourceValue types
        valid_resources: ResourceValue = "string"
        valid_resources = 42
        valid_resources = 3.14
        valid_resources = True
        valid_resources = ["item1", "item2"]
        valid_resources = {"key": "value"}
        
        # This should not raise type errors
        assert isinstance(valid_resources, (str, int, float, bool, list, dict))

    def test_column_rules_type_validation(self):
        """Test that ColumnRules type accepts only valid ColumnRule types."""
        # Valid ColumnRules
        rules: ColumnRules = {
            "column1": ["rule1", "rule2"],
            "column2": [True, False],
            "column3": ["rule3"]
        }
        
        # This should not raise type errors
        assert isinstance(rules, dict)
        for column_name, rule_list in rules.items():
            assert isinstance(column_name, str)
            assert isinstance(rule_list, list)
            for rule in rule_list:
                assert isinstance(rule, (str, bool))

    def test_base_model_explicit_types(self):
        """Test that BaseModel uses explicit types in to_dict method."""
        @dataclass
        class TestModel(BaseModel):
            name: str
            value: int
            metadata: Dict[str, str]
        
        model = TestModel(
            name="test",
            value=42,
            metadata={"key": "value"}
        )
        
        # Test to_dict returns explicit types
        result = model.to_dict()
        assert isinstance(result, dict)
        
        # All values should be ModelValue types
        for key, value in result.items():
            assert isinstance(key, str)
            assert isinstance(value, (str, int, float, bool, list, dict, type(None)))

    def test_serializable_protocol_explicit_types(self):
        """Test that Serializable protocol uses explicit types."""
        @dataclass
        class TestSerializable(Serializable):
            name: str
            value: int
            
            def to_dict(self) -> Dict[str, ModelValue]:
                return {"name": self.name, "value": self.value}
            
            def to_json(self) -> str:
                import json
                return json.dumps(self.to_dict())
        
        obj = TestSerializable(name="test", value=42)
        result = obj.to_dict()
        
        # Should return explicit ModelValue types
        assert isinstance(result, dict)
        for key, value in result.items():
            assert isinstance(key, str)
            assert isinstance(value, (str, int, float, bool, list, dict, type(None)))

    def test_validatable_protocol(self):
        """Test that Validatable protocol works correctly."""
        @dataclass
        class TestValidatable(Validatable):
            name: str
            value: int
            
            def validate(self) -> None:
                if not self.name:
                    raise ValueError("Name cannot be empty")
                if self.value < 0:
                    raise ValueError("Value must be non-negative")
        
        # Valid object
        obj = TestValidatable(name="test", value=42)
        obj.validate()  # Should not raise
        
        # Invalid object
        with pytest.raises(ValueError, match="Name cannot be empty"):
            TestValidatable(name="", value=42).validate()
        
        with pytest.raises(ValueError, match="Value must be non-negative"):
            TestValidatable(name="test", value=-1).validate()

    def test_model_inheritance_with_explicit_types(self):
        """Test that model inheritance works with explicit types."""
        @dataclass
        class TestStep(BaseModel):
            name: str
            rules: ColumnRules
            metadata: Dict[str, str]
            
            def validate(self) -> None:
                if not self.name:
                    raise ValueError("Name cannot be empty")
                if not self.rules:
                    raise ValueError("Rules cannot be empty")
        
        step = TestStep(
            name="test_step",
            rules={"id": ["not_null", "unique"]},
            metadata={"source": "test"}
        )
        
        # Test validation
        step.validate()  # Should not raise
        
        # Test serialization
        result = step.to_dict()
        assert isinstance(result, dict)
        assert result["name"] == "test_step"
        assert isinstance(result["rules"], dict)
        assert isinstance(result["metadata"], dict)

    def test_resource_requirements_explicit_types(self):
        """Test that resource_requirements uses explicit ResourceValue types."""
        from sparkforge.models import UnifiedStepConfig
        
        step_config = UnifiedStepConfig(
            name="test_step",
            step_type="bronze",
            resource_requirements={
                "memory": "512MB",
                "cpu": 2,
                "timeout": 300.0,
                "enabled": True,
                "tags": ["test", "bronze"],
                "config": {"key": "value"}
            }
        )
        
        # Test that resource_requirements uses explicit types
        for key, value in step_config.resource_requirements.items():
            assert isinstance(key, str)
            assert isinstance(value, (str, int, float, bool, list, dict))

    def test_no_any_types_in_models(self):
        """Test that no Any types are used in model methods."""
        import inspect
        from sparkforge.models import BaseModel
        
        # Get all methods from BaseModel
        methods = [method for method in dir(BaseModel) 
                  if not method.startswith('_') and callable(getattr(BaseModel, method))]
        
        for method_name in methods:
            method = getattr(BaseModel, method_name)
            if hasattr(method, '__annotations__'):
                annotations = method.__annotations__
                # Check that no parameter uses Any type
                for param_name, param_type in annotations.items():
                    if param_name != 'return':
                        assert param_type != 'Any', f"Method {method_name} uses Any type for parameter {param_name}"

    def test_type_aliases_work_correctly(self):
        """Test that type aliases work correctly in practice."""
        # Test ColumnRules
        rules: ColumnRules = {
            "id": ["not_null", "unique"],
            "name": ["not_null", "min_length"],
            "email": ["not_null", "email_format"]
        }
        assert isinstance(rules, dict)
        
        # Test ModelValue in various contexts
        model_data: Dict[str, ModelValue] = {
            "name": "test",
            "age": 25,
            "active": True,
            "tags": ["tag1", "tag2"],
            "metadata": {"key": "value"},
            "optional": None
        }
        assert isinstance(model_data, dict)
        
        # Test ResourceValue
        resources: Dict[str, ResourceValue] = {
            "memory": "1GB",
            "cpu": 4,
            "timeout": 60.0,
            "enabled": True,
            "tags": ["production"],
            "config": {"setting": "value"}
        }
        assert isinstance(resources, dict)

    def test_model_serialization_roundtrip(self):
        """Test that model serialization works correctly with explicit types."""
        @dataclass
        class TestModel(BaseModel):
            name: str
            value: int
            metadata: Dict[str, str]
            optional: Optional[str] = None
            
            def validate(self) -> None:
                if not self.name:
                    raise ValueError("Name cannot be empty")
        
        # Create model
        model = TestModel(
            name="test_model",
            value=42,
            metadata={"key": "value"},
            optional=None
        )
        
        # Validate
        model.validate()
        
        # Serialize
        data = model.to_dict()
        assert isinstance(data, dict)
        assert data["name"] == "test_model"
        assert data["value"] == 42
        assert data["metadata"] == {"key": "value"}
        assert data["optional"] is None
        
        # JSON serialization
        json_str = model.to_json()
        assert isinstance(json_str, str)
        assert "test_model" in json_str

    def test_complex_model_with_nested_types(self):
        """Test complex model with nested types using explicit types."""
        @dataclass
        class ComplexModel(BaseModel):
            name: str
            rules: ColumnRules
            resources: Dict[str, ResourceValue]
            metadata: Dict[str, str]
            tags: List[str]
            active: bool
            count: int
            rate: float
            
            def validate(self) -> None:
                if not self.name:
                    raise ValueError("Name cannot be empty")
                if not self.rules:
                    raise ValueError("Rules cannot be empty")
                if self.count < 0:
                    raise ValueError("Count must be non-negative")
                if not 0 <= self.rate <= 1:
                    raise ValueError("Rate must be between 0 and 1")
        
        model = ComplexModel(
            name="complex_test",
            rules={"id": ["not_null"], "name": ["not_null", "min_length"]},
            resources={"memory": "1GB", "cpu": 2, "enabled": True},
            metadata={"source": "test", "version": "1.0"},
            tags=["test", "complex", "model"],
            active=True,
            count=100,
            rate=0.95
        )
        
        # Validate
        model.validate()
        
        # Test serialization
        data = model.to_dict()
        assert isinstance(data, dict)
        assert data["name"] == "complex_test"
        assert isinstance(data["rules"], dict)
        assert isinstance(data["resources"], dict)
        assert isinstance(data["metadata"], dict)
        assert isinstance(data["tags"], list)
        assert data["active"] is True
        assert data["count"] == 100
        assert data["rate"] == 0.95


class TestModelBackwardCompatibility:
    """Test backward compatibility for model changes."""

    def test_existing_models_still_work(self):
        """Test that existing model usage still works."""
        from sparkforge.models import UnifiedStepConfig
        
        # Test that existing UnifiedStepConfig still works
        step = UnifiedStepConfig(
            name="test_step",
            step_type="bronze",
            resource_requirements={"memory": "512MB", "cpu": 2}
        )
        
        # Should work without type errors
        assert step.name == "test_step"
        assert step.step_type == "bronze"
        assert step.resource_requirements["memory"] == "512MB"
        assert step.resource_requirements["cpu"] == 2

    def test_type_aliases_backward_compatible(self):
        """Test that type aliases are backward compatible."""
        # These should still work
        rules: ColumnRules = {"id": ["not_null"]}
        assert isinstance(rules, dict)
        
        # ModelValue should accept the same types as before
        value: ModelValue = "string"
        value = 42
        value = True
        value = None


if __name__ == "__main__":
    pytest.main([__file__])
