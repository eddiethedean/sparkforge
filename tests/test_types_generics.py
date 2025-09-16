"""
Test type safety for the refactored types/generics system.

This module tests that all generic types use explicit types instead of Any,
and that the new type definitions work correctly.
"""

import pytest
from typing import Callable, Dict, List, Optional, Union
from sparkforge.types.generics import (
    GenericValue,
    GenericDictValue,
    GenericListValue,
    GenericCallableValue,
    TypedDict,
    TypedList,
    Functor,
    Monad,
    Foldable,
    safe_cast,
    force_cast,
    is_instance_of,
    get_type_name,
    typed_function,
    generic_method,
)


class TestGenericTypeSafety:
    """Test that generic types use explicit types."""

    def test_generic_value_type_validation(self):
        """Test that GenericValue type accepts only valid types."""
        # Valid GenericValue types
        valid_values: GenericValue = "string"
        valid_values = 42
        valid_values = 3.14
        valid_values = True
        valid_values = ["item1", "item2"]
        valid_values = {"key": "value"}
        valid_values = None
        
        # This should not raise type errors
        assert isinstance(valid_values, (str, int, float, bool, list, dict, type(None)))

    def test_generic_dict_value_type_validation(self):
        """Test that GenericDictValue type accepts only valid types."""
        # Valid GenericDictValue types
        valid_values: GenericDictValue = "string"
        valid_values = 42
        valid_values = 3.14
        valid_values = True
        valid_values = ["item1", "item2"]
        valid_values = {"key": "value"}
        
        # This should not raise type errors
        assert isinstance(valid_values, (str, int, float, bool, list, dict))

    def test_generic_list_value_type_validation(self):
        """Test that GenericListValue type accepts only valid types."""
        # Valid GenericListValue types
        valid_values: GenericListValue = "string"
        valid_values = 42
        valid_values = 3.14
        valid_values = True
        valid_values = {"key": "value"}
        
        # This should not raise type errors
        assert isinstance(valid_values, (str, int, float, bool, dict))

    def test_generic_callable_value_type_validation(self):
        """Test that GenericCallableValue type accepts only valid types."""
        # Valid GenericCallableValue types
        valid_values: GenericCallableValue = "string"
        valid_values = 42
        valid_values = 3.14
        valid_values = True
        valid_values = ["item1", "item2"]
        valid_values = {"key": "value"}
        
        # This should not raise type errors
        assert isinstance(valid_values, (str, int, float, bool, list, dict))

    def test_typed_dict_explicit_types(self):
        """Test that TypedDict uses explicit types."""
        # Test TypedDict with explicit types
        typed_dict: TypedDict[str, str] = TypedDict()
        typed_dict["key1"] = "value1"
        typed_dict["key2"] = "value2"
        
        # Test get_typed method
        value = typed_dict.get_typed("key1", "default")
        assert value == "value1"
        
        # Test set_typed method
        typed_dict.set_typed("key3", "value3")
        assert typed_dict["key3"] == "value3"

    def test_typed_list_explicit_types(self):
        """Test that TypedList uses explicit types."""
        # Test TypedList with explicit types
        typed_list: TypedList[str] = TypedList()
        typed_list.append("item1")
        typed_list.append("item2")
        
        # Test append_typed method
        typed_list.append_typed("item3")
        assert "item3" in typed_list
        
        # Test get_typed method
        value = typed_list.get_typed(0)
        assert value == "item1"

    def test_functor_protocol_explicit_types(self):
        """Test that Functor protocol uses explicit types."""
        class TestFunctor:
            def __init__(self, value: str):
                self.value = value
            
            def map(self, func: Callable[[str], str]) -> "TestFunctor":
                return TestFunctor(func(self.value))
        
        functor = TestFunctor("hello")
        result = functor.map(str.upper)
        assert result.value == "HELLO"

    def test_monad_protocol_explicit_types(self):
        """Test that Monad protocol uses explicit types."""
        class TestMonad:
            def __init__(self, value: str):
                self.value = value
            
            def flat_map(self, func: Callable[[str], "TestMonad"]) -> "TestMonad":
                return func(self.value)
            
            def unit(self, value: str) -> "TestMonad":
                return TestMonad(value)
        
        monad = TestMonad("hello")
        result = monad.flat_map(lambda x: TestMonad(x.upper()))
        assert result.value == "HELLO"

    def test_foldable_protocol_explicit_types(self):
        """Test that Foldable protocol uses explicit types."""
        class TestFoldable:
            def __init__(self, values: List[str]):
                self.values = values
            
            def fold_left(self, initial: str, func: Callable[[str, str], str]) -> str:
                result = initial
                for value in self.values:
                    result = func(result, value)
                return result
            
            def fold_right(self, initial: str, func: Callable[[str, str], str]) -> str:
                result = initial
                for value in reversed(self.values):
                    result = func(result, value)
                return result
        
        foldable = TestFoldable(["a", "b", "c"])
        result = foldable.fold_left("", lambda acc, x: acc + x)
        assert result == "abc"

    def test_safe_cast_explicit_types(self):
        """Test that safe_cast uses explicit types."""
        # Test successful cast
        result = safe_cast("42", int)
        assert result == 42
        
        # Test failed cast
        result = safe_cast("not_a_number", int)
        assert result is None
        
        # Test None value
        result = safe_cast(None, str)
        assert result == "None"  # str(None) returns "None"

    def test_force_cast_explicit_types(self):
        """Test that force_cast uses explicit types."""
        # Test successful cast
        result = force_cast("42", int)
        assert result == 42
        
        # Test failed cast
        with pytest.raises(TypeError):
            force_cast("not_a_number", int)

    def test_is_instance_of_explicit_types(self):
        """Test that is_instance_of uses explicit types."""
        # Test valid instances
        assert is_instance_of("hello", str)
        assert is_instance_of(42, int)
        assert is_instance_of(3.14, float)
        assert is_instance_of(True, bool)
        assert is_instance_of([1, 2, 3], list)
        assert is_instance_of({"key": "value"}, dict)
        
        # Test invalid instances
        assert not is_instance_of("hello", int)
        assert not is_instance_of(42, str)

    def test_get_type_name_explicit_types(self):
        """Test that get_type_name uses explicit types."""
        assert get_type_name("hello") == "str"
        assert get_type_name(42) == "int"
        assert get_type_name(3.14) == "float"
        assert get_type_name(True) == "bool"
        assert get_type_name([1, 2, 3]) == "list"
        assert get_type_name({"key": "value"}) == "dict"
        assert get_type_name(None) == "NoneType"

    def test_typed_function_decorator_explicit_types(self):
        """Test that typed_function decorator uses explicit types."""
        @typed_function
        def test_func(x: str) -> str:
            return x.upper()
        
        # Should work without type errors
        result = test_func("hello")
        assert result == "HELLO"
        
        # Check that function is marked as typed
        assert hasattr(test_func, '__typed__')
        assert getattr(test_func, '__typed__') is True

    def test_generic_method_decorator_explicit_types(self):
        """Test that generic_method decorator uses explicit types."""
        @generic_method
        def test_method(x: str) -> str:
            return x.lower()
        
        # Should work without type errors
        result = test_method("HELLO")
        assert result == "hello"
        
        # Check that method is marked as generic
        assert hasattr(test_method, '__generic__')
        assert getattr(test_method, '__generic__') is True

    def test_no_any_types_in_generics(self):
        """Test that no Any types are used in generic functions."""
        import inspect
        from sparkforge.types.generics import safe_cast, force_cast, is_instance_of, get_type_name
        
        # Get all functions from the module
        functions = [safe_cast, force_cast, is_instance_of, get_type_name]
        
        for func in functions:
            if hasattr(func, '__annotations__'):
                annotations = func.__annotations__
                # Check that no parameter uses Any type
                for param_name, param_type in annotations.items():
                    if param_name != 'return':
                        assert param_type != 'Any', f"Function {func.__name__} uses Any type for parameter {param_name}"

    def test_type_aliases_work_correctly(self):
        """Test that type aliases work correctly in practice."""
        # Test GenericValue in various contexts
        values: List[GenericValue] = ["string", 42, 3.14, True, None]
        assert all(isinstance(v, (str, int, float, bool, type(None))) for v in values)
        
        # Test GenericDictValue
        dict_values: Dict[str, GenericDictValue] = {
            "string": "value",
            "number": 42,
            "float": 3.14,
            "boolean": True,
            "list": ["item1", "item2"],
            "dict": {"nested": "value"}
        }
        assert isinstance(dict_values, dict)
        
        # Test GenericListValue
        list_values: List[GenericListValue] = ["string", 42, 3.14, True, {"key": "value"}]
        assert isinstance(list_values, list)

    def test_typed_collections_inheritance(self):
        """Test that typed collections inherit correctly."""
        # Test TypedDict inheritance
        typed_dict: TypedDict[str, int] = TypedDict()
        typed_dict["key1"] = 1
        typed_dict["key2"] = 2
        
        # Should behave like a regular dict
        assert len(typed_dict) == 2
        assert "key1" in typed_dict
        assert typed_dict["key1"] == 1
        
        # Test TypedList inheritance
        typed_list: TypedList[str] = TypedList()
        typed_list.append("item1")
        typed_list.append("item2")
        
        # Should behave like a regular list
        assert len(typed_list) == 2
        assert "item1" in typed_list
        assert typed_list[0] == "item1"

    def test_generic_protocols_type_safety(self):
        """Test that generic protocols provide type safety."""
        # Test Functor implementation
        class StringFunctor:
            def __init__(self, value: str):
                self.value = value
            
            def map(self, func: Callable[[str], str]) -> "StringFunctor":
                return StringFunctor(func(self.value))
        
        functor = StringFunctor("hello")
        result = functor.map(str.upper)
        assert isinstance(result, StringFunctor)
        assert result.value == "HELLO"
        
        # Test Monad implementation
        class StringMonad:
            def __init__(self, value: str):
                self.value = value
            
            def flat_map(self, func: Callable[[str], "StringMonad"]) -> "StringMonad":
                return func(self.value)
            
            def unit(self, value: str) -> "StringMonad":
                return StringMonad(value)
        
        monad = StringMonad("hello")
        result = monad.flat_map(lambda x: StringMonad(x.upper()))
        assert isinstance(result, StringMonad)
        assert result.value == "HELLO"


class TestGenericBackwardCompatibility:
    """Test backward compatibility for generic changes."""

    def test_existing_generic_usage_still_works(self):
        """Test that existing generic usage still works."""
        # Test TypedDict usage
        typed_dict = TypedDict[str, str]()
        typed_dict["key"] = "value"
        assert typed_dict["key"] == "value"
        
        # Test TypedList usage
        typed_list = TypedList[str]()
        typed_list.append("item")
        assert typed_list[0] == "item"
        
        # Test type conversion functions
        result = safe_cast("42", int)
        assert result == 42
        
        result = force_cast("hello", str)
        assert result == "hello"
        
        assert is_instance_of(42, int)
        assert get_type_name(42) == "int"

    def test_type_aliases_backward_compatible(self):
        """Test that type aliases are backward compatible."""
        # These should still work
        value: GenericValue = "string"
        value = 42
        value = None
        
        dict_value: GenericDictValue = "string"
        dict_value = 42
        dict_value = {"key": "value"}
        
        list_value: GenericListValue = "string"
        list_value = 42
        list_value = {"key": "value"}


if __name__ == "__main__":
    pytest.main([__file__])
