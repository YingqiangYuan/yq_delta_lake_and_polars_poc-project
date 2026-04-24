# 06 - Full ETL Simulation

End-to-end Bronze-to-Silver ETL pipeline combining all techniques from 01-05.
Uses minimal mock data stored in the ``data/`` directory.

## Data

``data/batch_1.json`` — 10 mock transaction records (initial load):

- Fields: transaction_id, card_id, amount, currency, transaction_ts, auth_status
- Contains 1 deliberate duplicate transaction_id

``data/batch_2.json`` — 5 mock transaction records (incremental load):

- 3 brand new transactions
- 2 transactions with transaction_id overlapping batch_1 (to test merge dedup)

## Run order

These scripts form a pipeline and must be run sequentially: ``s01 → s02 → s03``.
Each script depends on the Delta tables created by the previous one.

## Scripts

### s01_bronze_write.py

Simulate raw data landing in the Bronze layer.

- Read ``data/batch_1.json`` with ``pl.read_ndjson()``
- Write to S3 Delta table (Bronze) using ``mode="overwrite"``
- Print row count and schema

### s02_silver_etl.py

Bronze-to-Silver ETL: clean, deduplicate, and mask PII.

- Read the Bronze Delta table
- Polars pipeline:
  - ``unique(subset=["transaction_id"])`` — deduplicate
  - ``map_elements`` — SHA-256 tokenize ``card_id``
  - ``filter`` — drop rows where ``auth_status == "DECLINED"``
  - ``with_columns`` — add ``etl_ts`` column with current timestamp
- Write to a separate S3 Delta table (Silver) using ``mode="overwrite"``
- Print before/after row counts

### s03_incremental.py

Incremental load: append new batch, merge into Silver, verify with time travel.

- Read ``data/batch_2.json``, write to Bronze with ``mode="append"``
- Re-read full Bronze table, apply the same ETL pipeline as s02
- Merge into Silver table using ``transaction_id`` as the merge key
- Read Silver and verify:
  - No duplicate transaction_ids
  - card_id is tokenized
  - Total row count is correct
- Time travel: read Silver version 0 vs version 1, print both
