Troubleshooting
==============

This section provides solutions to common issues you may encounter when using SparkForge.

Common Issues
-------------

Pipeline Initialization Errors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Problem**: PipelineBuilder fails to initialize

**Solutions**:
- Ensure SparkSession is properly configured and active
- Verify schema name is valid and accessible
- Check that quality thresholds are between 0 and 100

Validation Failures
~~~~~~~~~~~~~~~~~~

**Problem**: Data validation fails with low quality scores

**Solutions**:
- Review validation rules for correctness
- Check data source quality
- Adjust quality thresholds if appropriate
- Investigate specific validation failures

Execution Timeouts
~~~~~~~~~~~~~~~~~

**Problem**: Pipeline steps timeout during execution

**Solutions**:
- Increase timeout settings in ExecutionConfig
- Optimize transformation functions
- Consider using parallel execution mode
- Check resource availability

Memory Issues
~~~~~~~~~~~~

**Problem**: OutOfMemoryError during execution

**Solutions**:
- Increase Spark driver and executor memory
- Optimize DataFrame operations
- Use caching strategically
- Consider data partitioning

Getting Help
------------

If you encounter issues not covered here:

1. Check the `Quick Start Guide <quick_start_5_min.html>`_ for basic setup
2. Review the `User Guide <user_guide.html>`_ for detailed usage patterns
3. Look at `Examples <examples/index.html>`_ for working code samples
4. Consult the `API Reference <api_reference.html>`_ for method documentation

For additional troubleshooting information, see the complete guide: `TROUBLESHOOTING.md <../TROUBLESHOOTING.md>`_
