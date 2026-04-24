# 04 - Delta Lake Time Travel

Version-based historical reads — the foundation for OCC audit trail requirements.

## Scripts

### s01_version_read.py

Read specific historical versions of a Delta table.

- Write version 0: 3 rows
- Overwrite with version 1: 5 rows (different data)
- Overwrite with version 2: 2 rows
- Read each version explicitly:
  - ``pl.read_delta(delta_uri, version=0, ...)`` — verify 3 rows
  - ``pl.read_delta(delta_uri, version=1, ...)`` — verify 5 rows
  - ``pl.read_delta(delta_uri, ...)`` (latest) — verify 2 rows
- Print each version side by side

### s02_history.py

Inspect the commit log of a Delta table.

- Reuse the table from s01 (or create a fresh one with multiple writes)
- ``dt = DeltaTable(delta_uri, storage_options=...)``
- ``dt.history()`` — returns a list of commit entries
- Print each entry: version number, timestamp, operation type, parameters
- Show how to find "who wrote what and when" from the log
