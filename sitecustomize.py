"""Project-wide interpreter tweaks for compatibility.

Ensures typing.TypeAlias is usable under Python 3.8 when third-party packages
expect the Python 3.10 behaviour.
"""

from __future__ import annotations

import importlib
import importlib.util
import sys
import typing
from collections import abc as collections_abc


class _CompatTypeAlias:
    """Minimal stand-in that supports subscription in type annotations."""

    def __getitem__(self, item):  # pragma: no cover - trivial
        return item


_TYPE_ALIAS_SENTINEL = _CompatTypeAlias()


def _patch_module(module: object) -> None:
    if hasattr(module, "TypeAlias"):
        try:
            module.TypeAlias["T"]  # type: ignore[index]
            return
        except Exception:
            pass
    module.TypeAlias = _TYPE_ALIAS_SENTINEL


_patch_module(typing)

# Ensure typing_extensions also exposes the sentinel before other imports bind names.
try:
    spec = importlib.util.find_spec("typing_extensions")
except ImportError:  # pragma: no cover - dependency missing
    spec = None

if spec and spec.loader:
    real_module = importlib.util.module_from_spec(spec)
    loader = spec.loader
    assert loader is not None
    loader.exec_module(real_module)  # type: ignore[attr-defined]

    patched_module = type(sys)("typing_extensions")
    for attr in dir(real_module):
        setattr(patched_module, attr, getattr(real_module, attr))

    patched_module.TypeAlias = _TYPE_ALIAS_SENTINEL
    sys.modules["typing_extensions"] = patched_module
else:
    try:
        import typing_extensions as typing_extensions_module  # type: ignore  # noqa: F401
    except Exception:  # pragma: no cover
        typing_extensions_module = None  # type: ignore[assignment]
    if typing_extensions_module is not None:
        _patch_module(typing_extensions_module)


def _make_generic_getitem(name: str):
    typing_generic = getattr(typing, name)

    def __class_getitem__(cls, params):  # pragma: no cover - simple delegation
        return typing_generic[params]  # type: ignore[index]

    return classmethod(__class_getitem__)


for _generic_name in ("Mapping", "MutableMapping", "Sequence", "Iterable"):
    _cls = getattr(collections_abc, _generic_name)
    if not hasattr(_cls, "__class_getitem__"):
        _cls.__class_getitem__ = _make_generic_getitem(_generic_name)

# Provide a stub duckdb backend package so optional imports don't crash on Python 3.8.
import types  # noqa: E402

if "mock_spark.backend.duckdb" not in sys.modules:
    duckdb_stub = types.ModuleType("mock_spark.backend.duckdb")
    duckdb_stub.__path__ = []  # type: ignore[attr-defined]
    sys.modules["mock_spark.backend.duckdb"] = duckdb_stub
