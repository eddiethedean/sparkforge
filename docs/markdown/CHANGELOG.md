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

## [Unreleased]

### Added
- **Column Filtering Control**: Explicit control over which columns are preserved after validation
  - `filter_columns_by_rules` parameter added to `apply_column_rules()` function
  - `filter_columns_by_rules=True` (default): Only keep columns with validation rules
  - `filter_columns_by_rules=False`: Preserve all original columns for downstream steps
  - Comprehensive test coverage with 9 new tests
  - Updated documentation across all guides and examples

### Changed
- Enhanced `apply_column_rules()` function with explicit column filtering behavior
- Updated all internal calls to include the new parameter
- Improved user experience by making column filtering behavior explicit and controllable

### Fixed
- Fixed critical bug in column filtering logic where `invalid_proj` was not being returned
- Updated mock functions in tests to include the new parameter

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
