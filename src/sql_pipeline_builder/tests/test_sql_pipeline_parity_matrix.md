This is a lightweight reference for SQL test parity vs the PySpark pipeline builder.

Priority areas (SQLite-only):

- Validation:
  - Moltres DataFrame validation with multiple rules
  - Core Select / compound selects
  - Bad rule objects and error messages
- Execution:
  - Initial vs incremental (append/overwrite) semantics with Moltres transforms
  - Silver context type (Moltres DF) after write and re-read
  - Gold overwrites every run
- Writer/table ops:
  - Core Select writes (already covered)
  - Ambiguous/duplicate columns handling (covered by robustness join test)
  - Rollback behavior (already covered for ORM query; add for Core Select)
- Error handling:
  - Missing sources (covered)
  - Bad transform return types (covered)
  - to_sqlalchemy failures

