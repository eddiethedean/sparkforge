# SparkForge Refactoring Guide

This guide explains the code organization improvements made to SparkForge to reduce complexity and improve maintainability.

## 🎯 **What Changed**

### **1. Unified Execution System**

**Before:** Two separate execution engines
- `ExecutionEngine` - Basic execution with limited features
- `UnifiedExecutionEngine` - Advanced execution with cross-layer support

**After:** Single unified execution system
- `sparkforge.execution.Engine` - Consolidated execution engine
- Pluggable execution strategies (Sequential, Parallel, Adaptive)
- Better error handling and resource management

### **2. Unified Dependency Analysis**

**Before:** Two separate dependency analyzers
- `DependencyAnalyzer` - Silver step dependencies only
- `UnifiedDependencyAnalyzer` - Cross-layer dependencies

**After:** Single unified dependency analyzer
- `sparkforge.dependencies.DependencyAnalyzer` - Handles all step types
- Cleaner graph representation
- Better cycle detection and resolution

### **3. Improved Error Handling**

**Before:** Inconsistent error handling patterns
- Different exception types across modules
- Generic error messages

**After:** Standardized exception hierarchy
- `ExecutionError` - Base execution errors
- `StepExecutionError` - Step-specific errors
- `DependencyError` - Dependency-related errors
- Better error context and debugging information

## 🔄 **Migration Guide**

### **For Execution Engines**

**Old Code:**
```python
from sparkforge.execution_engine import ExecutionEngine, ExecutionConfig
from sparkforge.unified_execution_engine import UnifiedExecutionEngine

# Old way - two different engines
engine = ExecutionEngine(spark, logger, thresholds, schema)
unified_engine = UnifiedExecutionEngine(spark, config, logger)
```

**New Code:**
```python
from sparkforge import UnifiedExecutionEngine, ExecutionConfig, ExecutionMode

# New way - single unified engine
config = ExecutionConfig(
    mode=ExecutionMode.ADAPTIVE,
    max_workers=4,
    timeout_seconds=300
)
engine = UnifiedExecutionEngine(spark, logger, config)
```

### **For Dependency Analysis**

**Old Code:**
```python
from sparkforge.dependency_analyzer import DependencyAnalyzer
from sparkforge.unified_dependency_analyzer import UnifiedDependencyAnalyzer

# Old way - two different analyzers
analyzer = DependencyAnalyzer(strategy, logger)
unified_analyzer = UnifiedDependencyAnalyzer(logger)
```

**New Code:**
```python
from sparkforge import UnifiedDependencyAnalyzer, AnalysisStrategy

# New way - single unified analyzer
analyzer = UnifiedDependencyAnalyzer(
    strategy=AnalysisStrategy.HYBRID,
    logger=logger
)
```

### **For Error Handling**

**Old Code:**
```python
# Old way - inconsistent error handling
try:
    result = pipeline.run()
except Exception as e:
    print(f"Error: {e}")
```

**New Code:**
```python
from sparkforge.execution import ExecutionError, StepExecutionError

# New way - specific error handling
try:
    result = pipeline.run()
except StepExecutionError as e:
    print(f"Step {e.step_name} failed: {e}")
    print(f"Error code: {e.error_code}")
except ExecutionError as e:
    print(f"Execution failed: {e}")
```

## 📁 **New Module Structure**

```
sparkforge/
├── execution/              # Unified execution system
│   ├── __init__.py
│   ├── engine.py          # Main execution engine
│   ├── strategies.py      # Execution strategies
│   ├── results.py         # Result classes
│   └── exceptions.py      # Execution exceptions
├── dependencies/          # Unified dependency analysis
│   ├── __init__.py
│   ├── analyzer.py        # Main dependency analyzer
│   ├── graph.py          # Dependency graph
│   └── exceptions.py     # Dependency exceptions
└── ... (other modules)
```

## 🚀 **Benefits of the Refactoring**

### **1. Reduced Complexity**
- **Before:** 2 execution engines + 2 dependency analyzers = 4 complex modules
- **After:** 1 execution engine + 1 dependency analyzer = 2 focused modules
- **Result:** 50% reduction in core complexity

### **2. Better Maintainability**
- Single source of truth for each concern
- Consistent APIs across all functionality
- Easier to add new features and fix bugs

### **3. Improved Performance**
- Eliminated duplicate code paths
- Better resource management
- More efficient execution strategies

### **4. Enhanced Developer Experience**
- Clearer module organization
- Better error messages and debugging
- Consistent patterns across the codebase

## 🔧 **Backward Compatibility**

The refactoring maintains backward compatibility by:

1. **Keeping old imports working** - Old imports still work but show deprecation warnings
2. **Preserving existing APIs** - Core functionality remains the same
3. **Gradual migration** - Users can migrate at their own pace

## 📋 **Migration Checklist**

- [ ] Update imports to use new unified modules
- [ ] Replace old execution engines with `UnifiedExecutionEngine`
- [ ] Replace old dependency analyzers with `UnifiedDependencyAnalyzer`
- [ ] Update error handling to use new exception hierarchy
- [ ] Test pipeline execution with new modules
- [ ] Update documentation and examples

## 🆘 **Getting Help**

If you encounter issues during migration:

1. **Check the examples** - Updated examples show the new patterns
2. **Review the API docs** - New modules have comprehensive documentation
3. **Use the troubleshooting guide** - Common issues and solutions
4. **Open an issue** - Report bugs or ask questions

## 🎉 **What's Next**

This refactoring is the first step in a series of improvements:

1. ✅ **Code Organization** - Unified execution and dependency systems
2. 🔄 **Pipeline Builder Refactoring** - Break down the massive PipelineBuilder class
3. 🔄 **Error Handling Standardization** - Complete error handling improvements
4. 🔄 **Type Safety** - Add comprehensive type annotations
5. 🔄 **Performance Optimization** - Memory management and caching improvements

---

**Ready to migrate?** Start with the [5-Minute Quick Start](QUICK_START_5_MIN.md) to see the new patterns in action!
