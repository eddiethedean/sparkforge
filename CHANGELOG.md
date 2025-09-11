# Changelog

All notable changes to SparkForge will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
