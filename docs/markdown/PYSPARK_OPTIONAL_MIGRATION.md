# PySpark Optional Dependency Migration Guide

## Overview

Starting with version 0.9.0, SparkForge has been refactored to make PySpark an **optional dependency**. This allows you to use SparkForge with either PySpark or mock-spark (or any other compatible package).

## What Changed?

### Before (v0.8.x and earlier)

PySpark was a hard dependency:
```bash
pip install sparkforge
# Automatically installed pyspark==3.2.4 and delta-spark
```

### After (v0.9.0+)

PySpark is now optional:
```bash
# Install with PySpark (for production)
pip install sparkforge[pyspark]

# Install with mock-spark (for testing/development)
pip install sparkforge[mock]

# Install base package only
pip install sparkforge
```

## Migration Steps

### For Production Users

If you're using SparkForge in production with PySpark:

1. **Update your installation:**
   ```bash
   pip install sparkforge[pyspark]
   ```

2. **No code changes required!** Your existing code will continue to work exactly as before.

### For Development/Testing Users

If you want to use mock-spark for faster, lighter testing:

1. **Install with mock-spark:**
   ```bash
   pip install sparkforge[mock]
   ```

2. **Set the engine (optional):**
   ```bash
   # Via environment variable
   export SPARKFORGE_ENGINE=mock
   
   # Or in your code
   import os
   os.environ["SPARKFORGE_ENGINE"] = "mock"
   ```

### For CI/CD Pipelines

Update your CI/CD configuration:

```yaml
# Example GitHub Actions
- name: Install SparkForge with PySpark
  run: pip install sparkforge[pyspark]

# Or for faster tests
- name: Install SparkForge with mock-spark
  run: pip install sparkforge[mock]
```

## Engine Selection

SparkForge automatically selects the appropriate engine based on availability:

1. **Explicit selection** via `SPARKFORGE_ENGINE` environment variable:
   - `SPARKFORGE_ENGINE=pyspark` - Use PySpark
   - `SPARKFORGE_ENGINE=mock` - Use mock-spark
   - `SPARKFORGE_ENGINE=auto` - Auto-detect (default)

2. **Auto-detection** (when `SPARKFORGE_ENGINE=auto` or not set):
   - First tries PySpark if available
   - Falls back to mock-spark if PySpark not found
   - Raises error if neither is available

### Example Usage

```python
from sparkforge import PipelineBuilder

# Works with either PySpark or mock-spark
builder = PipelineBuilder(spark=spark, schema="analytics")

# Your code works the same regardless of engine
builder.with_bronze_rules(
    name="events",
    rules={"user_id": ["not_null"]},
    incremental_col="timestamp"
)

pipeline = builder.to_pipeline()
result = pipeline.run_initial_load(bronze_sources={"events": df})
```

## Feature Compatibility

### Full Compatibility

These features work with both PySpark and mock-spark:
- ✅ Pipeline building and execution
- ✅ Validation rules and data quality checks
- ✅ Step-by-step execution
- ✅ Error handling and reporting
- ✅ Multi-schema support
- ✅ Performance monitoring

### PySpark-Only Features

These features require PySpark:
- ⚠️ Delta Lake operations (OPTIMIZE, VACUUM, time travel)
- ⚠️ Advanced DataFrame operations
- ⚠️ Real distributed processing

When using mock-spark, Delta Lake operations will be skipped with a warning message.

## Testing Strategy

### Unit Tests (mock-spark)

```bash
# Fast, lightweight tests
pip install sparkforge[mock]
pytest tests/unit/
```

### Integration Tests (PySpark)

```bash
# Full integration tests with real Spark
pip install sparkforge[compat-test]
SPARKFORGE_ENGINE=pyspark pytest tests/integration/
```

### System Tests (PySpark)

```bash
# End-to-end tests with Delta Lake
pip install sparkforge[compat-test]
SPARKFORGE_ENGINE=pyspark pytest tests/system/
```

## Breaking Changes

**None!** If you install with `sparkforge[pyspark]`, your code will work exactly as before. The changes are purely internal to how SparkForge imports and uses PySpark.

## Troubleshooting

### ImportError: Neither pyspark nor mock-spark could be imported

**Solution:** Install either PySpark or mock-spark:
```bash
pip install sparkforge[pyspark]
# or
pip install sparkforge[mock]
```

### Delta Lake operations skipped

**Cause:** You're using mock-spark which doesn't support Delta Lake operations.

**Solution:** Either:
1. Use PySpark: `pip install sparkforge[pyspark]`
2. Accept that Delta Lake operations will be skipped in mock mode

### Type checking errors with mypy

**Solution:** The mypy configuration already handles optional imports. If you see errors, ensure you're using the latest version of the project.

## Performance Considerations

| Scenario | Engine | Speed | Memory | Use Case |
|----------|--------|-------|--------|----------|
| Unit tests | mock-spark | ⚡ Fast | 💾 Low | Development |
| Integration tests | PySpark | 🐢 Slower | 💾💾 Medium | CI/CD |
| Production | PySpark | 🚀 Distributed | 💾💾💾 High | Production |

## FAQ

**Q: Can I switch engines at runtime?**  
A: Yes, set the `SPARKFORGE_ENGINE` environment variable before importing sparkforge.

**Q: Will my existing code break?**  
A: No, if you install with `sparkforge[pyspark]`, everything works exactly as before.

**Q: Which engine should I use?**  
A: Use PySpark for production, mock-spark for development/testing.

**Q: Can I use both engines in the same project?**  
A: Yes, but you'll need to install both and switch via environment variable.

**Q: What about Delta Lake?**  
A: Delta Lake operations require PySpark. In mock mode, they'll be skipped with warnings.

## Support

If you encounter any issues during migration:

1. Check the [troubleshooting section](#troubleshooting) above
2. Review the [changelog](CHANGELOG.md) for detailed changes
3. Open an issue on GitHub with:
   - Your installation command
   - The error message
   - Your Python and package versions

## Summary

- ✅ **No breaking changes** for users installing with `[pyspark]`
- ✅ **Faster development** with mock-spark option
- ✅ **Flexible testing** with engine selection
- ✅ **Backward compatible** with existing code

Happy coding! 🚀

