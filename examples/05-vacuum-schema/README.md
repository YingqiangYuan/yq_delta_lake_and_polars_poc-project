# 05 - Vacuum and Schema Evolution

Maintenance operations for long-lived Delta tables.

## Scripts

### s01_vacuum.py

Clean up old data files that are no longer referenced by any version.

- Write a table, then overwrite it several times to create old Parquet files
- List S3 files before vacuum (``s3dir.iterdir()``) — observe multiple Parquet files
- ``dt.vacuum(retention_hours=0, enforce_retention_duration=False)``
  (use 0 hours only for demo — production should use 168+ hours)
- List S3 files after vacuum — only the latest version's files remain
- Verify that time travel to old versions now fails (files are gone)

### s02_schema_evolution.py

Add new columns to an existing Delta table without rewriting old data.

- Write version 0 with schema: ``(id: i64, name: str)``
- Write version 1 with ``schema_mode="merge"`` and new schema:
  ``(id: i64, name: str, email: str)``
- Read the full table — old rows have ``email = null``, new rows have email values
- Verify the merged schema contains all three columns
