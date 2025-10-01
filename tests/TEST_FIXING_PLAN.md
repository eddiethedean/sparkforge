# 🛠️ Test Fixing Plan - Comprehensive Test Suite Repair

## 📊 **Current Status Analysis**

### **✅ Working Tests (60/87 - 69% pass rate)**
- **Validation tests** (`test_validation_standalone.py`) - 30/30 passed ✅
- **Basic functionality** - Core SparkForge features working

### **❌ Failing Tests (27/87 - 31% failure rate)**
- **Pipeline builder tests** - 48 errors (API mismatches, missing arguments)
- **Execution engine tests** - 9 failures (enum value mismatches, missing config)
- **Writer core tests** - 9 failures (enum value mismatches, API changes)

## 🎯 **Fix Strategy Overview**

### **Core Principle: Mock-First Approach**
> **If a test passes with real Spark but fails with mock Spark, the mock is wrong, not the test.**

### **Phase 1: Mock Implementation Fixes (Priority: HIGH)**
Fix mock objects to behave exactly like real Spark objects.

### **Phase 2: API Alignment (Priority: HIGH)**
Fix API mismatches between test expectations and actual SparkForge implementation.

### **Phase 3: Enum Value Corrections (Priority: HIGH)**
Update enum value assertions to match actual implementation.

### **Phase 4: Configuration Fixes (Priority: MEDIUM)**
Fix missing required arguments and configuration issues.

### **Phase 5: Validation & Testing (Priority: LOW)**
Verify all fixes work in both modes.

## 🔧 **Detailed Fix Plan**

### **1. Mock Implementation Analysis**

#### **Test Results Analysis:**
- **Mock Spark**: 60/87 passed (69% pass rate)
- **Real Spark**: 60/87 passed (69% pass rate)
- **Key Insight**: Both modes have identical failure patterns, suggesting the issues are in the **test code itself**, not the mocks

#### **Mock vs Real Spark Comparison:**
- If tests fail in both modes → **Test code issues** (API mismatches, wrong parameters)
- If tests pass in real Spark but fail in mock → **Mock implementation issues**
- If tests pass in mock but fail in real Spark → **Real Spark compatibility issues**

### **2. Pipeline Builder Tests (`test_pipeline_builder_basic.py`)**

#### **Issues Identified:**
- 48 errors related to API mismatches
- Missing required arguments in constructors
- Incorrect method signatures
- Enum value mismatches

#### **Fix Actions:**
1. **Constructor Fixes:**
   - ✅ `PipelineBuilder` constructor: `PipelineBuilder(spark, schema, min_bronze_rate=95.0, min_silver_rate=98.0, min_gold_rate=99.0, verbose=True, functions=None)`
   - ✅ `ExecutionEngine` constructor: `ExecutionEngine(spark, config, logger=None)` - **REQUIRES config**
   - ✅ `LogWriter` constructor: `LogWriter(spark, config, logger=None)` - **REQUIRES config**

2. **Method Signature Updates:**
   - Update method calls to match actual API signatures
   - Fix parameter names and types
   - Add missing required parameters

3. **Enum Value Corrections:**
   - ✅ `StepStatus.PENDING` = `"pending"` (not `"PENDING"`)
   - ✅ `StepType.BRONZE` = `"bronze"` (not `"BRONZE"`)
   - ✅ `WriteMode.APPEND` = `"append"` (not `"APPEND"`)
   - ✅ `LogLevel.DEBUG` = `"DEBUG"` (correct case)

4. **API Mapping:**
   - Map test expectations to actual SparkForge API
   - Update method names and signatures
   - Fix property access patterns

### **3. Execution Engine Tests (`test_execution_engine_simple.py`)**

#### **Issues Identified:**
- 9 failures related to enum value mismatches
- Missing required `config` argument
- Incorrect enum member access

#### **Fix Actions:**
1. **Constructor Fixes:**
   - ✅ Add required `PipelineConfig` argument to `ExecutionEngine` constructor
   - ✅ Fix invalid constructor calls: `ExecutionEngine(spark, config)` not `ExecutionEngine(spark)`

2. **Enum Value Corrections:**
   - ✅ `StepStatus.PENDING` = `"pending"` (not `"PENDING"`)
   - ✅ `StepType.BRONZE` = `"bronze"` (not `"BRONZE"`)
   - ✅ `ExecutionMode.SEQUENTIAL` = `"sequential"` (not `"SEQUENTIAL"`)

3. **Method Updates:**
   - Update method calls to match actual API
   - Fix parameter types and names

### **4. Writer Core Tests (`test_writer_core_simple.py`)**

#### **Issues Identified:**
- 9 failures related to enum value mismatches
- Missing required `config` argument
- Incorrect parameter names (`schema_name` vs `table_schema`)

#### **Fix Actions:**
1. **Constructor Fixes:**
   - ✅ Add required `WriterConfig` argument to `LogWriter` constructor
   - ✅ Fix invalid constructor calls: `LogWriter(spark, config)` not `LogWriter(spark)`

2. **Parameter Name Corrections:**
   - ✅ Change `schema_name` to `table_schema` in `WriterConfig` calls
   - ✅ Update other parameter names to match actual API

3. **Enum Value Corrections:**
   - ✅ `WriteMode.APPEND` = `"append"` (not `"APPEND"`)
   - ✅ `LogLevel.DEBUG` = `"DEBUG"` (correct case)

4. **Method Updates:**
   - Update method calls to match actual API
   - Fix return value assertions

### **5. Real Spark Compatibility**

#### **Issues Identified:**
- Delta Lake configuration failures
- Java gateway process issues
- Environment-specific test failures

#### **Fix Actions:**
1. **Environment Detection:**
   - Add proper environment detection in tests
   - Skip Delta Lake tests when not available
   - Use appropriate Spark configuration for each mode

2. **Mock/Real Spark Abstraction:**
   - Ensure tests work with both mock and real Spark
   - Use appropriate fixtures for each mode
   - Handle mode-specific differences gracefully

3. **Configuration Management:**
   - Use environment variables for configuration
   - Provide fallback configurations
   - Handle missing dependencies gracefully

## 🎯 **Mock-First Debugging Strategy**

### **When to Fix Mocks vs Tests:**

1. **Fix Mocks When:**
   - Test passes with real Spark but fails with mock Spark
   - Mock behavior doesn't match real Spark behavior
   - Mock is missing required methods or properties
   - Mock returns wrong data types or values

2. **Fix Tests When:**
   - Test fails in both mock and real Spark modes
   - Test has incorrect API usage or parameters
   - Test has wrong assertions or expectations
   - Test is testing non-existent functionality

### **Debugging Process:**
1. **Run test with real Spark** - Does it pass?
2. **Run test with mock Spark** - Does it pass?
3. **If real passes but mock fails** → Fix the mock
4. **If both fail** → Fix the test
5. **If mock passes but real fails** → Fix real Spark compatibility

## 📋 **Implementation Steps**

### **Step 1: Fix Pipeline Builder Tests**
1. Update constructor calls to include required arguments
2. Fix method signatures and parameter names
3. Correct enum value assertions
4. Update API mappings

### **Step 2: Fix Execution Engine Tests**
1. Add required `PipelineConfig` arguments
2. Fix enum value assertions
3. Update method calls to match API
4. Fix constructor calls

### **Step 3: Fix Writer Core Tests**
1. Add required `WriterConfig` arguments
2. Fix parameter names (`schema_name` → `table_schema`)
3. Correct enum value assertions
4. Update method calls

### **Step 4: Ensure Real Spark Compatibility**
1. Add environment detection
2. Handle Delta Lake configuration issues
3. Provide fallback configurations
4. Test in both modes

### **Step 5: Validation & Testing**
1. Run tests in mock Spark mode
2. Run tests in real Spark mode
3. Verify all tests pass
4. Update documentation

## 🔧 **Specific Code Fixes**

### **1. Execution Engine Constructor Fixes**
```python
# ❌ Current (failing)
engine = ExecutionEngine(spark=mock_spark_session)

# ✅ Fixed
from sparkforge.models import PipelineConfig, ValidationThresholds, ParallelConfig
config = PipelineConfig(
    schema="test_schema",
    thresholds=ValidationThresholds(bronze=95.0, silver=98.0, gold=99.0),
    parallel=ParallelConfig(enabled=False, max_workers=1)
)
engine = ExecutionEngine(spark=mock_spark_session, config=config)
```

### **2. LogWriter Constructor Fixes**
```python
# ❌ Current (failing)
writer = LogWriter(spark=mock_spark_session)

# ✅ Fixed
from sparkforge.writer.models import WriterConfig, WriteMode, LogLevel
config = WriterConfig(
    table_schema="test_schema",  # Not schema_name
    table_name="test_logs",
    write_mode=WriteMode.APPEND,
    log_level=LogLevel.INFO
)
writer = LogWriter(spark=mock_spark_session, config=config)
```

### **3. Enum Value Fixes**
```python
# ❌ Current (failing)
assert StepStatus.PENDING == "PENDING"
assert StepType.BRONZE == "BRONZE"
assert WriteMode.APPEND == "APPEND"

# ✅ Fixed
assert StepStatus.PENDING == "pending"
assert StepType.BRONZE == "bronze"
assert WriteMode.APPEND == "append"
assert LogLevel.DEBUG == "DEBUG"  # This one is correct
```

### **4. WriterConfig Parameter Fixes**
```python
# ❌ Current (failing)
config = WriterConfig(
    table_name="test_logs",
    schema_name="test_schema",  # Wrong parameter name
    write_mode=WriteMode.APPEND,
    log_level=LogLevel.INFO
)

# ✅ Fixed
config = WriterConfig(
    table_schema="test_schema",  # Correct parameter name
    table_name="test_logs",
    write_mode=WriteMode.APPEND,
    log_level=LogLevel.INFO
)
```

## 🎯 **Success Criteria**

### **Mock Spark Mode:**
- ✅ All 87 tests pass (100% pass rate)
- ✅ No errors or failures
- ✅ Fast execution (< 1 minute)

### **Real Spark Mode:**
- ✅ All 87 tests pass (100% pass rate)
- ✅ No errors or failures
- ✅ Proper Delta Lake integration

### **Overall:**
- ✅ 100% test pass rate in both modes
- ✅ No breaking changes to existing functionality
- ✅ Comprehensive test coverage
- ✅ Production-ready test suite

## 📊 **Expected Timeline**

- **Phase 1 (API Alignment)**: 2-3 hours
- **Phase 2 (Enum Corrections)**: 1-2 hours
- **Phase 3 (Configuration Fixes)**: 1-2 hours
- **Phase 4 (Real Spark Compatibility)**: 1-2 hours
- **Phase 5 (Validation & Testing)**: 1 hour

**Total Estimated Time**: 6-10 hours

## 🚀 **Next Steps**

1. **Start with Phase 1** - Fix pipeline builder tests (highest impact)
2. **Move to Phase 2** - Fix execution engine tests
3. **Continue with Phase 3** - Fix writer core tests
4. **Address Phase 4** - Ensure real Spark compatibility
5. **Complete Phase 5** - Validate and test everything

## 📝 **Files to Update**

### **Test Files:**
- `unit/test_pipeline_builder_basic.py` - 48 errors (API mismatches, missing arguments)
- `unit/test_execution_engine_simple.py` - 9 failures (enum values, missing config)
- `unit/test_writer_core_simple.py` - 9 failures (enum values, parameter names)

### **Configuration Files:**
- `conftest.py` (if needed for real Spark compatibility)
- `pytest.ini` (if needed for test markers)

### **Documentation:**
- `TEST_FIXING_PLAN.md` (this file)
- `README_SPARK_MODES.md` (update with fixes)

## 🚨 **Critical Issues Summary**

### **High Priority (Must Fix)**
1. **Missing Required Arguments:**
   - `ExecutionEngine` requires `config` parameter
   - `LogWriter` requires `config` parameter

2. **Enum Value Mismatches:**
   - `StepStatus.PENDING` = `"pending"` (not `"PENDING"`)
   - `StepType.BRONZE` = `"bronze"` (not `"BRONZE"`)
   - `WriteMode.APPEND` = `"append"` (not `"APPEND"`)

3. **Parameter Name Errors:**
   - `WriterConfig` uses `table_schema` (not `schema_name`)

### **Medium Priority (Should Fix)**
1. **Method Signature Updates:**
   - Update method calls to match actual API
   - Fix parameter types and names

2. **Real Spark Compatibility:**
   - Handle Delta Lake configuration issues
   - Add environment detection

### **Low Priority (Nice to Have)**
1. **Test Organization:**
   - Improve test structure
   - Add better error messages
   - Enhance test coverage

---

**🎯 Goal: Achieve 100% test pass rate in both mock and real Spark modes**
