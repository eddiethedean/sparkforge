## How Pipeline Builder Fits into Databricks

### Where It Runs

- **Databricks notebooks**:
  - You can import and use Pipeline Builder directly in a notebook.
  - Great for **exploration**, **prototyping**, and **developer workflows**.
- **Databricks jobs**:
  - Move your pipeline definition into a Python file or notebook.
  - Schedule it as a **job** to run on a cluster (for example: hourly, daily).

In both cases, you are still using:

- The same **Spark DataFrames** you know.
- The same **Delta tables** and **schemas** you already work with.

---

### Basic Usage Flow (Conceptual)

1. **Start a Spark session** (Databricks does this for you in a notebook).
2. **Configure the engine once**:
   - Call the provided configuration function so Pipeline Builder knows how to talk to Spark on Databricks.
3. **Create a `PipelineBuilder`**:
   - Point it at the Spark session and the schema where you want tables.
4. **Define Bronze steps**:
   - Tell it about your raw sources and validation rules.
5. **Define Silver and Gold steps**:
   - Add transformations as Python functions that work on DataFrames.
   - Specify dependencies between steps (which Bronze/Silver tables they need).
6. **Build the pipeline**:
   - Turn your builder definition into a runnable pipeline object.
7. **Run with `PipelineRunner`**:
   - Execute an **initial load**, **incremental run**, or **validation‑only run**.
   - Review the resulting report and any logs.

---

### Typical Databricks Workflow

- **During development**:
  - Work in a notebook.
  - Experiment with small datasets.
  - Adjust steps and validation rules until the pipeline behaves as expected.

- **For production**:
  - Move the code into a Python module or production notebook.
  - Create a Databricks job that calls your pipeline run.
  - Use Delta tables for storage and integrate with Unity Catalog if needed.

