# Databricks Deployment Examples

This directory contains practical examples for deploying PipelineBuilder pipelines on Databricks.

## Examples Overview

### Single Notebook Pipeline (`single_notebook_pipeline.py`)

A complete, self-contained pipeline that runs everything in one job. Perfect for:
- Simple workflows
- Quick prototypes
- Pipeline with minimal steps
- Single cluster configurations

**Features:**
- Complete Bronze → Silver → Gold flow
- LogWriter integration
- Error handling and reporting
- Databricks widget parameterization
- Sample data for quick testing

**Usage:**
```bash
# Upload to Databricks and run as a notebook
# Or test locally:
python examples/databricks/single_notebook_pipeline.py
```

### Multi-Task Job (`multi_task_job/`)

A production-ready deployment pattern with task dependencies. Perfect for:
- Complex pipelines with multiple steps
- Different resource requirements per task
- Fine-grained control over execution
- Independent scaling of tasks

**Structure:**
```
multi_task_job/
├── bronze_ingestion.py   # Task 1: Load and validate raw data
├── silver_processing.py  # Task 2: Clean and enrich data
├── gold_analytics.py     # Task 3: Create business metrics
├── log_execution.py      # Task 4: Log results (optional)
└── job_config.json       # Databricks job configuration
```

**Task Flow:**
1. **Bronze Ingestion**: Validates raw data, checks quality
2. **Silver Processing**: Cleans data, applies business rules
3. **Gold Analytics**: Creates aggregations and metrics
4. **Log Results**: Stores execution metadata

**Usage:**

**Option 1: Deploy via Databricks UI**
1. Upload notebooks to Databricks workspace
2. Create new job in Databricks
3. Add tasks for each notebook
4. Configure dependencies between tasks
5. Schedule or run on-demand

**Option 2: Deploy via Databricks CLI**
```bash
# Install Databricks CLI
pip install databricks-cli

# Configure CLI
databricks configure --token

# Deploy notebooks
databricks workspace import \
  examples/databricks/multi_task_job/bronze_ingestion.py \
  /Workspace/pipeline_builder/bronze_ingestion \
  --language PYTHON

# Create job from config
databricks jobs create --json-file examples/databricks/multi_task_job/job_config.json
```

**Option 3: Deploy via Databricks REST API**
```python
import requests

# Create job using REST API
with open('examples/databricks/multi_task_job/job_config.json') as f:
    job_config = json.load(f)

response = requests.post(
    f"{DATABRICKS_URL}/api/2.1/jobs/create",
    headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
    json=job_config
)

job_id = response.json()['job_id']
print(f"Job created with ID: {job_id}")

# Run job
requests.post(
    f"{DATABRICKS_URL}/api/2.1/jobs/run-now",
    headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"},
    json={"job_id": job_id}
)
```

## Configuration

### Job Configuration

The `job_config.json` file contains a complete Databricks job configuration:

```json
{
  "name": "PipelineBuilder ETL Pipeline",
  "job_clusters": [...],
  "tasks": [...],
  "schedule": {...}
}
```

**Key Settings:**
- **Spark Version**: 13.3.x-scala2.12 (latest stable)
- **Node Type**: i3.xlarge for main tasks, i3.large for logging
- **Auto-scaling**: 2-8 workers based on workload
- **Adaptive Query Execution**: Enabled for better performance
- **Delta Optimizations**: Enabled by default

### Widgets and Parameters

Both patterns use Databricks widgets for parameterization:

```python
# Define widgets
dbutils.widgets.text("source_path", "/mnt/raw/user_events/")
dbutils.widgets.text("target_schema", "analytics")

# Get values
source_path = dbutils.widgets.get("source_path")
target_schema = dbutils.widgets.get("target_schema")
```

When running jobs, parameters can be overridden:
```python
# Programmatic run
jobs.run_now(job_id, 
    notebook_params={"source_path": "/custom/path", "run_date": "2024-01-01"})
```

## Monitoring and Logging

### LogWriter Integration

Both examples integrate LogWriter for comprehensive monitoring:

```python
from pipeline_builder import LogWriter

writer = LogWriter(
    spark=spark,
    schema="monitoring",
    table_name="pipeline_logs"
)

# Log execution results
writer.append(result)

# Query logs
writer.show_logs(limit=20)

# Analyze trends
quality_trends = writer.analyze_quality_trends(days=30)
anomalies = writer.detect_quality_anomalies()
```

### Alerting

Set up alerts in the job configuration:

```json
{
  "email_notifications": {
    "on_failure": ["team@company.com"],
    "on_start": [],
    "on_success": []
  }
}
```

Or use Databricks Alerts API for custom alerting:
```python
# Create alert on pipeline failure
alerts.create(
    query_id=dashboard_query_id,
    condition={"error_count": "> 0"},
    destinations=[slack_webhook]
)
```

## Best Practices

### 1. Cluster Selection

**For Bronze/Silver/Gold Tasks:**
- Use larger nodes (i3.xlarge+) for data-intensive work
- Enable auto-scaling for variable workloads
- Configure Delta Lake optimizations

**For Logging Tasks:**
- Use smaller nodes (i3.large) for lightweight operations
- Fixed size (no auto-scaling needed)
- Minimal Spark configuration

### 2. Error Handling

Always implement proper error handling:

```python
try:
    result = pipeline.run_initial_load(bronze_sources={"events": df})
    if result.status != "completed":
        raise Exception(f"Pipeline failed: {result.errors}")
except Exception as e:
    # Log error
    logger.error(f"Pipeline execution failed: {e}")
    
    # Send alert
    dbutils.notebook.exit(f"Error: {e}")
    
    # Re-raise for job to mark as failed
    raise
```

### 3. Resource Management

Configure appropriate timeouts and retries:

```json
{
  "tasks": [{
    "task_key": "bronze_ingestion",
    "timeout_seconds": 1800,
    "max_retries": 2,
    "retry_on_timeout": true
  }]
}
```

### 4. Data Partitioning

For large datasets, partition your Delta tables:

```python
# In Silver/Gold transforms
.option("partitionBy", "event_date")

# Or configure in table properties
spark.sql("ALTER TABLE my_table SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')")
```

### 5. Monitoring Dashboards

Create dashboards to monitor pipeline health:

```sql
-- Query pipeline logs
SELECT 
  DATE(start_time) as run_date,
  phase,
  COUNT(*) as total_runs,
  SUM(CASE WHEN success = true THEN 1 ELSE 0 END) as successful_runs,
  AVG(duration_secs) as avg_duration,
  AVG(validation_rate) as avg_quality
FROM monitoring.pipeline_logs
WHERE run_date >= CURRENT_DATE - INTERVAL 30 DAYS
GROUP BY run_date, phase
ORDER BY run_date DESC, phase
```

## Testing

Test your Databricks deployment locally:

```bash
# Install test dependencies
pip install pipeline_builder[pyspark]

# Run examples
python examples/databricks/single_notebook_pipeline.py
python examples/databricks/multi_task_job/bronze_ingestion.py
```

## Troubleshooting

See the [Comprehensive Troubleshooting Guide](../../docs/COMPREHENSIVE_TROUBLESHOOTING_GUIDE.md) for:
- Common errors and solutions
- Performance optimization
- Debugging tips
- Log analysis

## Additional Resources

- [Comprehensive User Guide](../../docs/COMPREHENSIVE_USER_GUIDE.md)
- [LogWriter User Guide](../../docs/writer_user_guide.md)
- [Production Deployment Guide](../../docs/PRODUCTION_DEPLOYMENT_GUIDE.md)
- [Performance Tuning Guide](../../docs/PERFORMANCE_TUNING_GUIDE.md)
- [API Reference](../../docs/ENHANCED_API_REFERENCE.md)

## Support

For issues or questions:
- Check [Troubleshooting Guide](../../docs/COMPREHENSIVE_TROUBLESHOOTING_GUIDE.md)
- Review [Examples](../README.md)
- File an issue on GitHub

---

**Happy Deploying! 🚀**

