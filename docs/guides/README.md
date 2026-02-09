# SparkForge Guides

This directory is the **self-contained guide set** for SparkForge Pipeline Builder. Every link in these guides points only to other files in this directory. You can learn, build, run, and troubleshoot pipelines without leaving `docs/guides/`.

## First steps

New to SparkForge? Start here:

- **[Getting Started](./GETTING_STARTED_GUIDE.md)** — Install, configure the engine, and run your first pipeline.

## All guides

| Guide | Description |
|-------|-------------|
| [Getting Started](./GETTING_STARTED_GUIDE.md) | Installation, engine configuration, and first pipeline (fully inlined). |
| [Building Pipelines](./BUILDING_PIPELINES_GUIDE.md) | Medallion architecture, bronze/silver/gold steps, transforms, and `to_pipeline()`. |
| [SQL Source Steps](./SQL_SOURCE_STEPS_GUIDE.md) | Bronze, silver, and gold steps that read from JDBC or SQLAlchemy databases. |
| [Stepwise Execution](./STEPWISE_EXECUTION_GUIDE.md) | Run individual steps, override parameters, and debug without full pipeline runs. |
| [Validation-Only Steps](./VALIDATION_ONLY_STEPS_GUIDE.md) | Validate existing silver/gold tables and use them in downstream transforms. |
| [Execution Modes](./EXECUTION_MODES_GUIDE.md) | Initial load, incremental, full refresh, and validation-only runs; result object. |
| [Validation Rules](./VALIDATION_RULES_GUIDE.md) | Rule syntax, string rules, helper methods, and validation thresholds. |
| [Logging and Monitoring](./LOGGING_AND_MONITORING_GUIDE.md) | LogWriter setup, logging runs, querying logs, and analytics. |
| [Common Workflows](./COMMON_WORKFLOWS_GUIDE.md) | Multi-source pipelines, initial+incremental pattern, mixed steps, edge cases. |
| [Deployment](./DEPLOYMENT_GUIDE.md) | Prerequisites, local and production setup, cloud and CI/CD. |
| [Performance](./PERFORMANCE_GUIDE.md) | Metrics, tuning, caching, and resource configuration. |
| [Error Handling](./ERROR_HANDLING_GUIDE.md) | Exception hierarchy, common errors, and what to do. |
| [Troubleshooting](./TROUBLESHOOTING_GUIDE.md) | Installation, engine, validation, and execution issues and fixes. |
| [Use Cases Index](./USE_CASES_INDEX.md) | Index of BI, e-commerce, and IoT use-case guides. |
| [Use Case: Business Intelligence](./USE_CASE_BI_GUIDE.md) | KPIs, dashboards, and analytics pipelines. |
| [Use Case: E-commerce](./USE_CASE_ECOMMERCE_GUIDE.md) | Orders, products, and funnel analytics. |
| [Use Case: IoT and Events](./USE_CASE_IOT_GUIDE.md) | Sensors, time-series, and event pipelines. |

## By task

| I want to… | Go to |
|------------|--------|
| Run my first pipeline | [Getting Started](./GETTING_STARTED_GUIDE.md) |
| Understand how to define bronze, silver, and gold steps | [Building Pipelines](./BUILDING_PIPELINES_GUIDE.md) |
| Read data from a SQL database (Postgres, MySQL, etc.) | [SQL Source Steps](./SQL_SOURCE_STEPS_GUIDE.md) |
| Run only one step or debug step-by-step | [Stepwise Execution](./STEPWISE_EXECUTION_GUIDE.md) |
| Validate an existing table without a transform | [Validation-Only Steps](./VALIDATION_ONLY_STEPS_GUIDE.md) |
| Run incremental or full refresh instead of initial load | [Execution Modes](./EXECUTION_MODES_GUIDE.md) |
| Write validation rules (syntax, string rules, helpers) | [Validation Rules](./VALIDATION_RULES_GUIDE.md) |
| Log pipeline runs and query execution history | [Logging and Monitoring](./LOGGING_AND_MONITORING_GUIDE.md) |
| Build a multi-source or initial+incremental workflow | [Common Workflows](./COMMON_WORKFLOWS_GUIDE.md) |
| Deploy to production or cloud | [Deployment](./DEPLOYMENT_GUIDE.md) |
| Improve pipeline speed and resource usage | [Performance](./PERFORMANCE_GUIDE.md) |
| Understand and fix pipeline errors | [Error Handling](./ERROR_HANDLING_GUIDE.md) |
| Fix installation, engine, or execution issues | [Troubleshooting](./TROUBLESHOOTING_GUIDE.md) |
| See BI, e-commerce, or IoT examples | [Use Cases Index](./USE_CASES_INDEX.md) |
