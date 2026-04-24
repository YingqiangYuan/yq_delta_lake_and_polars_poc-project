# 01 - Delta Lake Read and Write

Fundamental Delta Lake I/O operations on S3.

## Scripts

### s01_overwrite.py

Write a DataFrame to a Delta table using ``mode="overwrite"``, then read it back
with ``pl.read_delta()``. This is the simplest possible Delta Lake round-trip.

- Create a small DataFrame (3-5 rows)
- ``df.write_delta(delta_uri, mode="overwrite", storage_options=...)``
- ``pl.read_delta(delta_uri, storage_options=...)``
- Print and verify the result matches the original

### s02_append.py

Write an initial batch, then append a second batch using ``mode="append"``.

- Write batch 1 (3 rows) with ``mode="overwrite"``
- Write batch 2 (2 rows) with ``mode="append"``
- Read back and verify total row count is 5
- Demonstrate that append does NOT deduplicate — if batch 2 contains duplicates,
  they will appear twice

### s03_error_mode.py

Demonstrate ``mode="error"`` which raises an exception if the table already exists.

- Write a table once — succeeds
- Write again with ``mode="error"`` — raises ``DeltaError``
- Catch the error, print the message, confirm the original data is untouched
