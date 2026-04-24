# Quiz Q&A — Delta Lake & Polars POC

## Index

- [Category 1: AWS S3 Fundamentals](#category-1-aws-s3-fundamentals)
- [Category 2: Polars DataFrame Operations](#category-2-polars-dataframe-operations)
- [Category 3: Delta Lake Core Concepts](#category-3-delta-lake-core-concepts)
- [Category 4: Merge / Upsert](#category-4-merge--upsert)
- [Category 5: Time Travel, Vacuum & Schema Evolution](#category-5-time-travel-vacuum--schema-evolution)
- [Category 6: ETL Pipeline Design](#category-6-etl-pipeline-design)

---

## Category 1: AWS S3 Fundamentals

### Q1. S3 has no real directories — so what happens when you see a "folder" in the AWS Console?

The AWS Console renders the `/` characters in object keys as a folder tree for visual convenience, but S3 itself has no directory concept. Every "path" like `bronze/transactions/2025-01-15.json` is a single flat key string. When code "lists files in a folder", it is actually performing a prefix scan — asking S3 for all keys that start with a given prefix. See `examples/00-minimal-poc/README.md` Section 1 for a detailed explanation.

### Q2. What are the only four operations you can perform on S3 objects?

PUT (upload), GET (download), LIST (find by prefix), and DELETE (remove). There is no rename or move — to "rename" a file, you copy it to a new key and delete the old one. This constraint is by design: it keeps S3 simple and massively scalable. See `examples/00-minimal-poc/README.md` Section 1.

### Q3. In the POC scripts, what does `s3dir_example.delete(bsm=bsm)` do and why is it called at the top of every script?

It deletes all objects under the script's S3 path prefix, giving each run a clean slate. This makes scripts idempotent — you can run them repeatedly and always get the same result. Every script that touches S3 uses a unique path suffix (e.g., `example-delta-01-01`) so scripts don't interfere with each other. See `examples/00-minimal-poc/s01_minimal_poc.py:25` and `examples/01-delta-read-write/s01_overwrite.py:17`.

### Q4. Why does the project use `s3dir_example.uri` to get the Delta table path instead of `str(s3dir_example)`?

The `.uri` property returns the proper `s3://bucket/prefix/` URI format that Delta Lake and Polars expect. Using `str()` would return a different string representation that may not be a valid S3 URI for these libraries. This is a convention established in `examples/00-minimal-poc/s01_minimal_poc.py:28`.

### Q5. What is `storage_options` and why does every `read_delta` / `write_delta` call need it?

`storage_options` is a dictionary containing AWS credentials (access key, secret key, region, etc.) that Polars and Delta Lake need to authenticate with S3. It is obtained from `one.polars_storage_options` which wraps the project's AWS credential management. Without it, the libraries cannot read from or write to S3. See the setup block in any example script, e.g., `examples/01-delta-read-write/s01_overwrite.py:12-13`.

---

## Category 2: Polars DataFrame Operations

### Q6. What is the difference between `with_columns` and `select` in Polars?

`with_columns` adds or transforms columns while keeping all existing columns — the output has all original columns plus any new ones. `select` picks only the specified columns, dropping everything else. See `examples/02-polars-etl/s01_column_ops.py` where both are demonstrated: `with_columns` adds `name_upper` and `amount_with_tax` (lines 24-27), while `select` picks only `id`, `name`, `amount` (lines 55-58).

### Q7. How does Polars `unique(subset=["id"], keep="first")` differ from SQL's `DISTINCT`?

SQL `DISTINCT` deduplicates on all columns — two rows must be identical in every column to be considered duplicates. Polars `unique(subset=["id"])` deduplicates based only on the specified columns, and `keep="first"` or `keep="last"` controls which row survives when duplicates are found. See `examples/02-polars-etl/s02_dedup_agg.py:25-28`.

### Q8. Explain what `map_elements(lambda v: hashlib.sha256(v.encode()).hexdigest()[:16], return_dtype=pl.Utf8)` does and when you would use it.

It applies a Python function element-by-element to a column. In this case, it takes each `card_id` string, hashes it with SHA-256, and keeps the first 16 hex characters as a token. This is used for PII masking — replacing sensitive card numbers with irreversible hashes so the data can be used for analytics without exposing real card numbers. The `return_dtype=pl.Utf8` tells Polars the output type is a string. See `examples/02-polars-etl/s03_transform.py:27-32`.

### Q9. How does `pl.when().then().otherwise()` work? Give an example from the codebase.

It is Polars' equivalent of SQL's `CASE WHEN`. You chain conditions: `pl.when(condition1).then(value1).when(condition2).then(value2).otherwise(default_value)`. In `examples/02-polars-etl/s03_transform.py:36-41`, it classifies transactions by risk level: amount >= 500 → "high", amount >= 100 → "medium", otherwise → "low".

### Q10. What is the difference between `inner`, `left`, and `full` joins? Use the example from s02_dedup_agg.py.

In that script, `df_agg` (4 customers: Alice, Bob, Cathy, David) is joined with `df_customers` (4 customers: Alice, Bob, Cathy, Frank). **Inner join** returns only matching rows (3 rows — David and Frank are dropped). **Left join** keeps all rows from the left table, filling nulls for non-matches (4 rows — David gets `tier=null`, Frank is dropped). **Full outer join** keeps all rows from both sides (5 rows — David has `tier=null`, Frank has aggregation columns as `null`). See `examples/02-polars-etl/s02_dedup_agg.py:47-72`.

---

## Category 3: Delta Lake Core Concepts

### Q11. What is the `_delta_log` directory and why is it the "magic" behind Delta Lake?

The `_delta_log` directory contains a sequence of JSON files (one per commit) that record every change to the table: which Parquet files were added, which were removed, and what metadata changed. To read the current table state, Delta Lake replays this log from the beginning. This is the same principle as database write-ahead logs or Git's commit history. It enables ACID transactions, time travel, and merge operations. See `examples/00-minimal-poc/README.md` Section 3.

### Q12. What is the difference between `mode="overwrite"`, `mode="append"`, and `mode="error"` when writing a Delta table?

**Overwrite** replaces the entire table — it logically removes all existing Parquet files and writes new ones (a new commit in the log). **Append** adds new Parquet files without touching existing data. **Error** (the default) raises an exception if the table already exists, preventing accidental overwrites. These are demonstrated in `examples/01-delta-read-write/s01_overwrite.py`, `s02_append.py`, and `s03_error_mode.py` respectively.

### Q13. After three `mode="overwrite"` writes to the same Delta table, how many Parquet files exist on S3?

All three versions' Parquet files still exist on S3 (typically 3+ files), even though only the latest version's files are "active". The `_delta_log` knows which files belong to which version. The old files remain until a `vacuum` operation removes them. This is demonstrated in `examples/05-vacuum-schema/s01_vacuum.py:35-42` where 4 writes produce 4 Parquet files.

### Q14. Why does a Delta Lake table store data as Parquet files instead of CSV or JSON?

Parquet is a columnar format optimized for analytics. It stores data column-by-column, so operations like "sum of the amount column" only read that one column, skipping everything else. It also compresses efficiently — a 1 GB CSV might shrink to 100-200 MB as Parquet. But Parquet alone has no concept of transactions, updates, or history — Delta Lake adds those capabilities on top. See `examples/00-minimal-poc/README.md` Section 3 (The Parquet Interlude).

### Q15. What does `DeltaTable(delta_uri, storage_options=storage_options)` give you that `pl.read_delta()` does not?

`pl.read_delta()` returns a Polars DataFrame — it reads the data. `DeltaTable()` gives you the Delta Lake table object itself, which provides table-level operations: `dt.merge()` for upserts, `dt.vacuum()` for cleanup, `dt.history()` for commit log, `dt.version()` for current version, and `dt.file_uris()` for listing active Parquet files. You need the `DeltaTable` object whenever you want to do more than just read data. See `examples/04-time-travel/s02_history.py:50-65`.

---

## Category 4: Merge / Upsert

### Q16. Explain the Delta Lake merge API: what do `predicate`, `source_alias`, and `target_alias` mean?

The `predicate` is a SQL-like condition that determines how source and target rows are matched (e.g., `"target.id = source.id"`). `source_alias` and `target_alias` are the names used in the predicate to refer to each side. `when_matched_update_all()` says "if a source row matches a target row, update all columns". `when_not_matched_insert_all()` says "if a source row has no match, insert it as a new row". See `examples/03-merge-upsert/s01_basic_upsert.py:47-58`.

### Q17. In s01_basic_upsert.py, the initial table has Alice(1) and Bob(2), and the upsert source has Bella(2) and Cathy(3). What happens to each row?

Alice(1): no matching row in source → **unchanged**. Bob(2): matches source row Bella(2) on `id=2` → **updated** to Bella. Cathy(3): no matching row in target → **inserted** as new row. Final result: Alice, Bella, Cathy (3 rows). This demonstrates the core upsert pattern: update existing + insert new. See `examples/03-merge-upsert/s01_basic_upsert.py`.

### Q18. In s02_credit_profile.py, why is the final row count 220 and not 250 after merging 50 rows into 200?

The 50-row merge source contains 30 updates to existing customers and 20 new customers. The 30 updated rows don't add new rows — they modify existing ones in place. So: 200 original rows + 20 new rows = 220 total. The 30 updates change credit_score values but don't change the row count. See `examples/03-merge-upsert/s02_credit_profile.py` and the verification assertions at the end.

### Q19. What is the difference between `keep="first"` and `keep="last"` in `unique()`, and when does it matter?

`keep="first"` retains the first occurrence of each duplicate key; `keep="last"` retains the last. It matters when duplicate rows have different values in non-key columns. In `examples/06-full-etl/s03_incremental.py:53-55`, `keep="last"` is used so that when batch_2 has updated versions of transactions from batch_1 (e.g., TXN002 with a new amount), the newer version wins.

### Q20. Why does the merge in s03_incremental.py use `when_matched_update_all` instead of just overwriting the Silver table?

A full overwrite would rewrite all rows, even unchanged ones. The merge only touches rows that match the predicate — unchanged rows are left in their original Parquet files. For large tables (millions of rows) where only a small fraction changes, merge is dramatically more efficient. It also creates a new version in the Delta log, enabling time travel to see before/after states. See `examples/06-full-etl/s03_incremental.py:72-83`.

---

## Category 5: Time Travel, Vacuum & Schema Evolution

### Q21. How do you read a specific historical version of a Delta table with Polars?

Pass the `version` parameter: `pl.read_delta(delta_uri, version=0, storage_options=storage_options)`. Each write operation creates a new version (0, 1, 2, ...). Omitting `version` reads the latest. See `examples/04-time-travel/s01_version_read.py:63-72` where versions 0, 1, and latest are read separately.

### Q22. What does `dt.history()` return and what information is in each entry?

It returns a list of commit entries, one per version, ordered newest-first. Each entry contains: `version` (integer), `timestamp` (epoch milliseconds), `operation` (e.g., "WRITE"), and `operationParameters` (e.g., `{'mode': 'Overwrite'}` or `{'mode': 'Append'}`). This is the audit trail showing who wrote what and when. See `examples/04-time-travel/s02_history.py:55-61`.

### Q23. What does `dt.vacuum(retention_hours=0, enforce_retention_duration=False)` do, and why should you never use `retention_hours=0` in production?

Vacuum deletes Parquet files that are no longer referenced by any version within the retention window. `retention_hours=0` means "delete everything not referenced by the latest version immediately" — this is dangerous in production because concurrent readers might still be reading old versions, and those files would be deleted from under them. Production should use 168+ hours (7 days). The `enforce_retention_duration=False` flag is required to allow values below the safety default. See `examples/05-vacuum-schema/s01_vacuum.py:46-53`.

### Q24. After vacuum, what happens when you try to time-travel to an old version?

The read fails because the Parquet files for that version have been physically deleted from S3. The `_delta_log` still has the commit record (it knows version 0 existed), but the actual data files are gone. This is the trade-off: vacuum saves storage but destroys time-travel capability for old versions. See `examples/05-vacuum-schema/s01_vacuum.py`.

### Q25. Explain how schema evolution works with `schema_mode="merge"`. What happens to old rows?

When you write data with a new column using `delta_write_options={"schema_mode": "merge"}`, Delta Lake updates the table's schema to include all columns from both old and new data. Old Parquet files are NOT rewritten — they simply don't have the new column. When you read the full table, old rows get `null` for the new column. This avoids expensive rewrites of historical data. See `examples/05-vacuum-schema/s02_schema_evolution.py:39-44` where `email` is added and old rows show `email=null`.

---

## Category 6: ETL Pipeline Design

### Q26. What are the Bronze and Silver layers in a data lake, and how does this project use them?

**Bronze** is the raw data landing zone — data is ingested as-is with no transformation. **Silver** is the cleaned, validated layer — data is deduplicated, PII is masked, invalid records are filtered out, and metadata (like ETL timestamps) is added. In this project, `s01_bronze_write.py` loads raw JSON into Bronze, and `s02_silver_etl.py` applies a four-step pipeline (dedup → hash PII → filter declined → add etl_ts) to produce Silver. See `examples/06-full-etl/`.

### Q27. Why are the three scripts in 06-full-etl NOT independently idempotent, unlike all other examples?

They form a pipeline: s01 writes Bronze, s02 reads Bronze and writes Silver, s03 appends to Bronze and merges into Silver. Each script depends on the Delta tables created by the previous one. Making each independently idempotent would require each script to rebuild all upstream tables, which would defeat the purpose of demonstrating incremental pipeline behavior. See `examples/06-full-etl/README.md` (Run Order section).

### Q28. In s02_silver_etl.py, the pipeline goes from 10 Bronze rows to 7 Silver rows. Account for each row lost.

Bronze has 10 rows with 1 duplicate TXN003 and 2 DECLINED transactions (TXN004, TXN008). Step 1: `unique(subset=["transaction_id"])` removes 1 duplicate → 9 rows. Step 2: SHA-256 hashes card_id (no row change). Step 3: `filter(auth_status != "DECLINED")` removes TXN004 and TXN008 → 7 rows. Step 4: adds `etl_ts` column (no row change). Final: 7 rows. See `examples/06-full-etl/s02_silver_etl.py`.

### Q29. In s03_incremental.py, why is `keep="last"` used instead of `keep="first"` during deduplication?

Batch 2 contains updated versions of existing transactions (TXN002 with amount 155 vs original 150, TXN005 with amount 1250 vs original 1200). Since batch 2 is appended after batch 1, using `keep="last"` ensures the newer version of each transaction wins during deduplication. If `keep="first"` were used, the stale batch 1 values would be retained instead. See `examples/06-full-etl/s03_incremental.py:53-55`.

### Q30. After running all three scripts in 06-full-etl, the Silver table has 2 versions. What does each version contain and why?

**Version 0** (from s02): 7 rows — the initial ETL result from batch 1 only (10 raw → 9 deduped → 7 after filtering DECLINED). **Version 1** (from s03): 10 rows — the merged result after adding batch 2's 3 new transactions and updating 2 existing ones. Time travel lets you compare these versions to audit what changed in the incremental load. See `examples/06-full-etl/s03_incremental.py:96-108`.
