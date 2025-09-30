"""
Simple test for Dict type annotation issues without Spark dependencies.

This test focuses specifically on the Dict vs dict issue in Python 3.8
without requiring Spark or other heavy dependencies.
"""

import sys
import unittest
from pathlib import Path
from typing import Dict, Union, get_type_hints, TypedDict


class SimpleDictCheckTest(unittest.TestCase):
    """Simple test for Dict type annotation issues."""

    def test_python_version(self):
        """Test that we're running on Python 3.8+."""
        self.assertGreaterEqual(
            sys.version_info[:2],
            (3, 8),
            f"Expected Python 3.8+, got {sys.version_info[:2]}",
        )

    def test_dict_vs_Dict_equivalence(self):
        """Test that dict and Dict are equivalent in Python 3.8+."""
        # Test function with Dict annotation (works in Python 3.8)
        def func_with_Dict() -> Dict[str, int]:
            return {"test": 1}

        # Test function with dict annotation (doesn't work in Python 3.8)
        # This will fail with TypeError: 'type' object is not subscriptable
        with self.assertRaises(TypeError):

            def func_with_dict() -> (
                dict[str, int]
            ):  # This syntax doesn't work in Python 3.8
                return {"test": 1}

        # Dict annotation should work
        self.assertEqual(func_with_Dict(), {"test": 1})

        # Test type hints
        hints_Dict = get_type_hints(func_with_Dict)
        self.assertIn("return", hints_Dict)

    def test_typeddict_compatibility(self):
        """Test TypedDict compatibility."""
        class TestDict(TypedDict):
            name: str
            value: int

        # Should work without issues
        test_dict: TestDict = {"name": "test", "value": 1}
        self.assertEqual(test_dict["name"], "test")
        self.assertEqual(test_dict["value"], 1)

    def test_dict_instantiation(self):
        """Test that dict can be instantiated but Dict cannot."""
        # This should work
        d1 = dict()
        d2 = dict(a=1, b=2)
        self.assertEqual(d1, {})
        self.assertEqual(d2, {"a": 1, "b": 2})

        # This should fail in Python 3.8
        with self.assertRaises(TypeError):
            d3 = Dict()  # This should raise TypeError

    def test_dict_type_annotation_works(self):
        """Test that dict type annotations work in Python 3.8+."""
        def func_with_dict_annotation(data: Dict[str, int]) -> Dict[str, str]:
            return {k: str(v) for k, v in data.items()}

        result = func_with_dict_annotation({"a": 1, "b": 2})
        self.assertEqual(result, {"a": "1", "b": "2"})

    def test_union_with_dict_works(self):
        """Test that Union with dict works."""
        def func_with_union_dict(
            value: Union[Dict[str, int], str]
        ) -> Union[Dict[str, int], str]:
            return value

        # Should work without issues
        self.assertEqual(func_with_union_dict({"test": 1}), {"test": 1})
        self.assertEqual(func_with_union_dict("test"), "test")


if __name__ == "__main__":
    # Run tests with verbose output
    unittest.main(verbosity=2)
