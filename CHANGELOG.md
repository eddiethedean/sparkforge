# Changelog

All notable changes to SparkForge will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
