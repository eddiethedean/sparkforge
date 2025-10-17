# # Copyright (c) 2024 Odos Matthews
# #
# # Permission is hereby granted, free of charge, to any person obtaining a copy
# # of this software and associated documentation files (the "Software"), to deal
# # in the Software without restriction, including without limitation the rights
# # to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# # copies of the Software, and to permit persons to whom the Software is
# # furnished to do so, subject to the following conditions:
# #
# # The above copyright notice and this permission notice shall be included in all
# # copies or substantial portions of the Software.
# #
# # THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# # IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# # FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# # AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# # LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# # OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# # SOFTWARE.

# Changelog

All notable changes to SparkForge will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.0] - 2025-10-17

### 📊 Enhanced Logging & Monitoring

#### Added
- **Unified logging format** with consistent timestamps and formatting across all messages
- **Rich metrics logging** including:
  - Rows processed and rows written
  - Invalid row counts
  - Validation rates (percentage)
  - Execution durations
- **Visual indicators** with emojis for better readability:
  - 🚀 Starting steps
  - ✅ Completed steps
  - ❌ Failed steps
  - 📦 Execution groups
- **Smart formatting** that differentiates:
  - Bronze steps: Show "rows processed" (validation only, no writes)
  - Silver/Gold steps: Show "rows written" and "rows processed" separately
- **Parallel execution visibility** in logs:
  - Group-based execution logging
  - Interleaved logs from concurrent steps
  - Parallel efficiency metrics
  
#### Logging Methods
- `logger.step_start(step_type, step_name)` - Consistent step start logging
- `logger.step_complete(step_type, step_name, duration, rows_processed, rows_written, invalid_rows, validation_rate)` - Detailed completion logging
- `logger.step_failed(step_type, step_name, error, duration)` - Standardized error logging

### ⚡ Parallel Execution Enhancements

#### Improved
- **Real-time parallel logging** - See concurrent step execution with interleaved log messages
- **Enhanced execution reporting** with:
  - `execution_groups_count` - Number of parallel execution groups
  - `max_group_size` - Maximum concurrent steps in any group
  - `parallel_efficiency` - Percentage efficiency of parallelization
- **Better dependency analysis** logging and visibility

### 📈 Pipeline Report Improvements

#### Added
- **Detailed step results by layer**:
  - `bronze_results` - Dictionary of bronze step results
  - `silver_results` - Dictionary of silver step results
  - `gold_results` - Dictionary of gold step results
- **Per-step information** including:
  - Status, duration, rows processed
  - Output table names
  - Start and end timestamps
  - Error messages (if failed)

### 🧪 Testing & Quality

#### Added
- **157 new tests** bringing total to **1,441 tests** (was 1,284)
- Enhanced test coverage for logging functionality
- Updated test assertions for new logging format
- Fixed test compatibility issues

#### Improved
- All tests passing (1,441 passed, 3 skipped)
- 100% mypy type checking compliance
- Zero ruff linter warnings (only config deprecation notice)
- Enhanced mock compatibility

### 🎨 Demo Scripts

#### Added
- `scripts/demo_logging.py` - Showcases new uniform logging format
- `scripts/demo_parallel_timing.py` - Demonstrates concurrent vs sequential execution with real timing
- `scripts/demo_logwriter.py` - Shows LogWriter creating and appending to log tables

### 📝 Documentation

#### Updated
- README.md with v1.2.0 features and enhanced logging examples
- Badge updates: version 1.2.0, 1,441 tests
- Performance comparison table with logging improvements
- Added "What's New in v1.2.0" section

### 🔧 Bug Fixes
- Fixed import paths in security and unit tests
- Resolved merge conflicts from parallel development streams
- Fixed AnalysisException handling in table operations tests
- Improved performance test mocking compatibility

### Breaking Changes
None - fully backward compatible with v1.1.0

### Migration Notes
No migration needed - all existing code continues to work. New logging features are automatically applied.

---

## [0.8.0] - 2025-09-30

### Added
- Comprehensive trap detection and fixing system
- 10 critical code quality traps identified and resolved
- Enhanced error handling with specific exception types
- Improved debugging capabilities with detailed error messages
- Better validation with explicit parameter checking
- Comprehensive test coverage for all trap scenarios

### Fixed
- **Trap 1**: Silent exception handling with generic fallbacks
- **Trap 2**: Missing object creation (silent failure)
- **Trap 3**: Hardcoded fallback values in LogRow creation
- **Trap 4**: Broad exception catching in writer components
- **Trap 5**: Default schema fallbacks masking validation issues
- **Trap 6**: Hasattr checks hiding missing functionality
- **Trap 7**: Silent fallback in test configuration
- **Trap 8**: Generic error handling in performance monitoring
- **Trap 9**: Default value fallbacks in configuration
- **Trap 10**: Silent skip in test parsing

### Improved
- Error messages now provide clear context and actionable guidance
- Exception handling is more robust with proper error chaining
- Code reliability significantly enhanced
- Maintainability improved with better error handling patterns
- Test coverage expanded with 76 new trap-specific tests

### Changed
- Replaced silent failures with explicit error raising
- Enhanced validation with proper parameter checking
- Improved logging with better error context
- Better object lifecycle management

## [Unreleased]

No unreleased changes.

## [1.0.0] - 2025-10-15

### Added
- **Smart Parallel Execution**: Automatic dependency-aware parallel execution of pipeline steps
  - **Automatic dependency analysis**: Builds dependency graph to determine execution order
  - **Concurrent execution**: Independent steps run in parallel using ThreadPoolExecutor
  - **Thread-safe**: Built-in locks prevent race conditions in shared context
  - **Performance metrics**: Tracks parallel efficiency, execution groups, and max parallelism
  - **Zero configuration**: Enabled by default with 4 workers
  - **Highly configurable**: Supports 1-16+ workers for different performance needs
  - **3-5x faster**: For pipelines with independent steps
  - **Backward compatible**: Works seamlessly with existing code
  - New metrics in `ExecutionResult`:
    - `parallel_efficiency`: Percentage of ideal parallelization achieved
    - `execution_groups_count`: Number of sequential execution phases
    - `max_group_size`: Maximum concurrent steps executed
  - New properties in `PipelineReport`:
    - `execution_groups_count`: Direct access to execution group count
    - `max_group_size`: Direct access to max parallelism achieved
  - Configuration options via `PipelineConfig.parallel`:
    - `enabled`: Enable/disable parallel execution (default: True)
    - `max_workers`: Maximum concurrent workers (default: 4)
    - `timeout_secs`: Timeout for operations (default: 300)
  - Presets available:
    - `PipelineConfig.create_default()`: 4 workers (balanced)
    - `PipelineConfig.create_high_performance()`: 16 workers (maximum throughput)
    - `PipelineConfig.create_conservative()`: 1 worker (sequential)

- **Robust Validation System**: Early validation with comprehensive error detection
  - **BronzeStep**: Must have non-empty validation rules
  - **SilverStep**: Must have non-empty validation rules, valid transform function, and valid source_bronze (except for existing tables)
  - **GoldStep**: Must have non-empty validation rules and valid transform function
  - Clear error messages for invalid configurations
  - 100% test coverage with 702+ comprehensive tests
- **Column Filtering Control**: Explicit control over which columns are preserved after validation
  - `filter_columns_by_rules` parameter added to `apply_column_rules()` function
  - `filter_columns_by_rules=True` (default): Only keep columns with validation rules
  - `filter_columns_by_rules=False`: Preserve all original columns for downstream steps
  - Comprehensive test coverage with 9 new tests
  - Updated documentation across all guides and examples

### Changed
- **Pipeline execution now runs in parallel by default**: Independent steps execute concurrently for 3-5x performance improvement
- **PipelineBuilder now uses parallel configuration by default**: Changed from `ParallelConfig.create_sequential()` to `ParallelConfig.create_default()` (4 workers)
- **Automatic schema assignment**: Silver and Gold steps now automatically use builder's schema if not explicitly provided
- **Dependency graph topological sort**: Fixed to correctly handle dependency ordering for proper parallel execution
- Enhanced validation system with early error detection during step construction
- Improved error handling with detailed validation messages
- Enhanced `apply_column_rules()` function with explicit column filtering behavior
- Updated all internal calls to include the new parameter
- Improved user experience by making column filtering behavior explicit and controllable

### Fixed
- Fixed critical bug in column filtering logic where `invalid_proj` was not being returned
- Updated mock functions in tests to include the new parameter
- Fixed all test fixtures to use valid validation rules
- Resolved import issues in test files

### Removed
- **Test Suite Cleanup**: Removed 58 redundant tests to improve maintainability
  - Removed `test_pipeline_builder_fixed.py` (exact duplicate of `test_pipeline_builder.py`)
  - Removed `test_models_basic.py` (41 out of 42 tests duplicated in `test_models_simple.py`)
  - Reduced test count from 760 to 702 tests (7.6% reduction)
  - Maintained 100% test coverage and functionality

## [0.4.3] - 2024-12-19

### Added
- **Multi-Schema Support**: Cross-schema data flows for multi-tenant applications
  - `schema` parameter added to `with_bronze_rules()`, `with_silver_rules()`, `add_silver_transform()`, and `add_gold_transform()`
  - Schema validation with helpful error messages
  - Optional schema creation functionality
  - Full backward compatibility (no schema = use default)
- **Cross-Schema Pipeline Example**: Complete example demonstrating multi-schema data flows
- **Comprehensive Multi-Schema Tests**: Full test coverage for all schema functionality
- **Updated Documentation**: README, API Reference, and examples updated with multi-schema features

### Changed
- Enhanced PipelineBuilder methods to support schema parameters
- Updated BronzeStep, SilverStep, and GoldStep models to include schema field
- Improved error messages for schema validation failures

### Fixed
- Schema validation integration with step creation
- Error handling for schema access issues

## [0.4.2] - 2024-12-19

### Added
- **Comprehensive Documentation Updates**: All documentation now includes new user experience features
- **Enhanced API Reference**: Complete documentation for all new methods and auto-inference features
- **Updated Quick Reference**: Side-by-side comparison of traditional vs simplified API
- **Improved README**: Showcases all new features with practical examples

### Changed
- Documentation structure optimized for better developer experience
- Examples updated to demonstrate new features
- API signatures updated to reflect auto-inference capabilities

### Fixed
- All test failures resolved (483/483 tests passing)
- Test performance improved by 17x through better isolation
- Documentation consistency across all files

## [0.4.1] - 2024-12-19

### Added
- **Auto-Inference of Source Bronze**: `add_silver_transform` now automatically infers `source_bronze` from the most recent `with_bronze_rules` call
- **Auto-Inference of Source Silvers**: `add_gold_transform` now automatically infers `source_silvers` from all available silver steps
- **Preset Configurations**: New class methods `for_development()`, `for_production()`, and `for_testing()` for quick setup
- **Validation Helper Methods**: Static methods `not_null_rules()`, `positive_number_rules()`, `string_not_empty_rules()`, `timestamp_rules()` for common validation patterns
- **Timestamp Column Detection**: `detect_timestamp_columns()` method to automatically identify timestamp columns for watermarking
- **Simplified API**: Significantly reduced boilerplate code across all pipeline building
- **Comprehensive Test Coverage**: Added 20+ test cases for all new user experience features
- **Example Documentation**: Added comprehensive example demonstrating all new features

### Changed
- `add_silver_transform` method signature: `source_bronze` parameter is now optional
- `add_gold_transform` method signature: `source_silvers` parameter is now optional
- Enhanced developer experience with intuitive default behavior and helper methods
- Improved error messages with helpful suggestions and better validation

### Fixed
- Fixed `SilverTransformFunction` type signature to include `SparkSession` parameter
- Enhanced backward compatibility - explicit parameters still work
- Improved test isolation and performance (17x faster test execution)
- Fixed all test failures to achieve 100% pass rate (483/483 tests passing)

## [0.4.0] - 2024-12-19

### Added
- **Enterprise Security Features**
  - Input validation with configurable rules
  - SQL injection protection
  - Role-based access control
  - Comprehensive audit logging
  - SecurityManager class for advanced security management

- **Performance Optimization**
  - Intelligent caching with TTL and LRU eviction
  - Automatic memory management
  - Performance monitoring and metrics
  - PerformanceCache class for advanced caching

- **Advanced Parallel Execution**
  - Dynamic worker allocation based on workload
  - Task prioritization (Critical, High, Normal, Low, Background)
  - Work-stealing algorithms for optimal resource utilization
  - Real-time resource monitoring
  - DynamicParallelExecutor for complex workloads

- **Enhanced Documentation**
  - Complete API reference for all new features
  - Comprehensive user guide with enterprise features
  - Advanced examples and use cases
  - Professional documentation structure

### Changed
- Security features are now enabled automatically
- Performance optimization is enabled by default
- Enhanced validation with security checks
- Improved parallel execution with dynamic allocation

### Fixed
- API compatibility issues in dynamic parallel execution
- Simplified and more reliable parallel execution system
- Enhanced error handling and recovery

## [0.3.5] - 2024-12-18

### Added
- Initial release of SparkForge
- Fluent pipeline building API
- Bronze-Silver-Gold architecture support
- Concurrent execution of independent steps
- Comprehensive data validation framework
- Delta Lake integration
- Performance monitoring and metrics
- Error handling and retry mechanisms
- Comprehensive logging system
- Real Spark integration (no mocks)
- Extensive test suite (282+ tests)
- PyPI package structure

### Features
- `PipelineBuilder` - Fluent API for building data pipelines
- `PipelineRunner` - Execute pipelines with various modes
- `ValidationThresholds` - Configurable data quality thresholds
- `ParallelConfig` - Concurrent execution configuration
- `LogWriter` - Comprehensive pipeline logging
- Support for Bronze, Silver, and Gold data layers
- Incremental and full refresh execution modes
- Schema evolution support
- Watermark-based processing
- ACID transaction support

## [0.1.0] - 2024-01-11

### Added
- Initial release
- Core pipeline building functionality
- Bronze-Silver-Gold architecture
- Data validation framework
- Concurrent execution
- Delta Lake integration
- Comprehensive test suite
- Documentation and examples

---

For more details, see the [GitHub repository](https://github.com/yourusername/sparkforge).
