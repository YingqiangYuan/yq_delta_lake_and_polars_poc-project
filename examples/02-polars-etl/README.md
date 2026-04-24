# 02 - Polars ETL Operations

Core Polars data processing capabilities, independent of Delta Lake.
All operations are in-memory — no S3 reads or writes in this section.

## Scripts

### s01_column_ops.py

Basic column manipulation and row filtering.

- ``with_columns``: add / rename / transform columns
- ``drop``: remove columns
- ``cast``: type conversion (e.g. str to int, int to float)
- ``filter``: row filtering with ``pl.col("amount") > 0``
- ``select``: pick specific columns

### s02_dedup_agg.py

Deduplication, aggregation, and joins.

- ``unique(subset=["id"])``: deduplicate by key column
- ``group_by(...).agg()``: grouped aggregation with ``pl.count()``,
  ``pl.col("amount").sum()``, ``pl.col("amount").mean()``
- ``join(other, on="key", how="left")``: left join two DataFrames
- Show the difference between ``inner``, ``left``, and ``outer`` joins

### s03_transform.py

Custom transformations for PII handling and conditional logic.

- ``map_elements(lambda v: hashlib.sha256(...), return_dtype=pl.Utf8)``:
  SHA-256 tokenization on a ``card_id`` column (simulating PII masking)
- ``pl.when(condition).then(...).otherwise(...).alias("col")``:
  conditional column creation (e.g. flag transactions above a threshold)
- Chain multiple transformations into a single ``with_columns`` call
