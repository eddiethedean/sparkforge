## Limitations, Considerations, and Next Steps

### Limitations and Things to Know

- **Opinionated around Bronze/Silver/Gold**:
  - Works best when you structure pipelines using the Medallion pattern.
  - If your process does not follow this at all, you may need to adjust your design.

- **Python‑first, not a visual designer**:
  - There is no drag‑and‑drop UI.
  - Pipelines are defined in Python code (cleanly, but still code).

- **Not a replacement for good data modeling**:
  - You still need to design schemas and tables carefully.
  - Pipeline Builder helps with structure and execution, not business semantics.

- **Best with disciplined development practices**:
  - Benefits grow when you:
    - Keep pipelines in version control.
    - Use tests.
    - Treat notebooks as a UI over well‑structured code.

---

### Next Steps and Resources

- **Start small**:
  - Pick one existing Databricks pipeline.
  - Re‑implement it using Pipeline Builder to see the differences.

- **Explore examples**:
  - Look at the “hello world”, basic pipeline, and real‑world use cases.
  - Adapt one to your own data domain (e.g., ecommerce, IoT, BI).

- **Integrate with Databricks jobs**:
  - Wrap your pipeline run in a Python script or notebook.
  - Schedule it as a job for regular execution (daily/hourly).

- **Plan adoption**:
  - Decide where Pipeline Builder will add the most value:
    - Replacing fragile legacy notebooks, or
    - Being the standard for all new pipelines.

