## What Is Pipeline Builder?

### High-Level Definition

- **Pipeline Builder** is a **Python framework** that helps you build **Spark + Delta Lake pipelines** in a structured way.
- It is designed around the **Medallion architecture**:
  - **Bronze** – raw data ingestion and basic validation.
  - **Silver** – cleaned and enriched data.
  - **Gold** – business‑ready tables and metrics.
- Instead of writing one big notebook per layer, you:
  - Describe your steps in **Pipeline Builder**.
  - Let the framework figure out **order, dependencies, and validation**.

---

### Core Concepts: Bronze, Silver, Gold

- **Bronze layer**:
  - Where raw data first lands.
  - Focus on **ingestion** and **basic checks** (for example: “no null IDs”).
- **Silver layer**:
  - Where data is cleaned, standardized, and enriched.
  - Ready to be joined across datasets and reused.
- **Gold layer**:
  - Business‑facing tables and metrics (daily sales, KPIs, dashboards).
  - Designed for reporting, BI tools, and downstream analytics.

Pipeline Builder makes these layers **first‑class citizens**:

- You define Bronze/Silver/Gold steps explicitly.
- The framework understands how they are related.

---

### Two Main Pieces: Builder & Runner

- **PipelineBuilder**:
  - Where you **define** steps:
    - Which Bronze tables you ingest and validate.
    - How you transform data into Silver and Gold.
    - What validation rules apply at each step.
  - Think of it as writing a **checklist** or **recipe** for your pipeline.

- **PipelineRunner**:
  - Takes the plan from `PipelineBuilder` and **executes it**:
    - Runs steps in the **right dependency order**.
    - Supports **initial loads**, **incremental runs**, and **validation‑only** runs.
    - Produces a **report** with status, row counts, durations, and validation metrics.

Together:

- You define **what** should happen with PipelineBuilder.
- The system handles **how and in what order** it happens with PipelineRunner.

