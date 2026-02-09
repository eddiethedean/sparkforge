# Use Cases Index

**Version**: 2.8.0  
**Last Updated**: February 2025

This index points to full use-case guides in this directory. Each guide walks through building a complete pipeline for a specific domain using the Medallion architecture (Bronze → Silver → Gold).

## Use Case Guides

| Guide | When to use | What you get |
|-------|-------------|--------------|
| [Business Intelligence](./USE_CASE_BI_GUIDE.md) | KPIs, dashboards, sales and customer analytics, executive metrics | Bronze/silver/gold for sales, customers, employees; gold aggregates for reporting |
| [E-commerce](./USE_CASE_ECOMMERCE_GUIDE.md) | Order processing, customer profiles, revenue and product analytics | Order ingestion, enrichment, customer profiles, daily and regional gold tables |
| [IoT and Events](./USE_CASE_IOT_GUIDE.md) | Sensor data, time-series, anomaly detection, device health | Sensor ingestion, anomaly flags, zone-level and device health gold aggregates |

## Prerequisites

For all use-case guides:

- Python 3.8+ with SparkForge installed (see [Getting Started](./GETTING_STARTED_GUIDE.md))
- Engine configured before building pipelines (`configure_engine(spark=spark)`)

## Next steps

- [Getting Started](./GETTING_STARTED_GUIDE.md) — Install and run your first pipeline
- [Building Pipelines](./BUILDING_PIPELINES_GUIDE.md) — Bronze, silver, and gold steps in depth
- [Execution Modes](./EXECUTION_MODES_GUIDE.md) — Initial load, incremental, and result handling
