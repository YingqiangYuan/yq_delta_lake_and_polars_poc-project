# Examples — POC Scripts

This directory contains a series of proof-of-concept scripts that explore
**Delta Lake** and **Polars** capabilities on S3. Each numbered folder focuses
on a single topic, progressing from simple to complex.

**Design principles:**

- Most scripts are **idempotent** — each one creates its own isolated S3 folder,
  deletes it at the top, and rebuilds from scratch every run.
- No real data is used. All datasets are either inline or stored as small mock
  files under each folder's `data/` directory.
- Scripts are meant to be run individually (e.g. `python examples/01-delta-read-write/s01_overwrite.py`).
  The one exception is `06-full-etl`, which is a pipeline that must be run in order.

## Index

### [00-minimal-poc](./00-minimal-poc/) — Golden reference

The authoritative example that defines coding conventions for all other scripts.
Start with [README.md](./00-minimal-poc/README.md) for a beginner-friendly
primer on the three core technologies (S3, Polars, Delta Lake) used throughout
this repository.

- [README.md](./00-minimal-poc/README.md) — Background concepts: S3, Polars, and Delta Lake explained from first principles
- [s01_minimal_poc.py](./00-minimal-poc/s01_minimal_poc.py) — Minimal Polars + Delta Lake + S3 round-trip with upsert

### [01-delta-read-write](./01-delta-read-write/) — Basic Delta Lake I/O

- [s01_overwrite.py](./01-delta-read-write/s01_overwrite.py) — Write with `mode="overwrite"`, read back, verify round-trip
- [s02_append.py](./01-delta-read-write/s02_append.py) — Append rows to an existing Delta table
- [s03_error_mode.py](./01-delta-read-write/s03_error_mode.py) — Write with `mode="error"` to prevent accidental overwrites

### [02-polars-etl](./02-polars-etl/) — Core Polars data processing

Pure in-memory operations, no S3.

- [s01_column_ops.py](./02-polars-etl/s01_column_ops.py) — `with_columns`, `drop`, `cast`, `filter`, `select`
- [s02_dedup_agg.py](./02-polars-etl/s02_dedup_agg.py) — `unique`, `group_by`+`agg`, inner/left/full outer joins
- [s03_transform.py](./02-polars-etl/s03_transform.py) — SHA-256 PII masking, `when/then/otherwise`, chained transforms

### [03-merge-upsert](./03-merge-upsert/) — Delta Lake merge / upsert

- [s01_basic_upsert.py](./03-merge-upsert/s01_basic_upsert.py) — Minimal merge: update matched, insert unmatched
- [s02_credit_profile.py](./03-merge-upsert/s02_credit_profile.py) — 200-row baseline + 50-row incremental merge with verification

### [04-time-travel](./04-time-travel/) — Version-based historical reads

- [s01_version_read.py](./04-time-travel/s01_version_read.py) — Read specific historical versions of a Delta table
- [s02_history.py](./04-time-travel/s02_history.py) — Inspect the commit log: version, timestamp, operation

### [05-vacuum-schema](./05-vacuum-schema/) — Maintenance operations

- [s01_vacuum.py](./05-vacuum-schema/s01_vacuum.py) — Clean up old Parquet files with `dt.vacuum()`
- [s02_schema_evolution.py](./05-vacuum-schema/s02_schema_evolution.py) — Add columns with `schema_mode="merge"`, old rows get nulls

### [06-full-etl](./06-full-etl/) — End-to-end Bronze-to-Silver pipeline

**Run in order:** `s01 -> s02 -> s03`. Each script depends on the previous one.

- [s01_bronze_write.py](./06-full-etl/s01_bronze_write.py) — Load raw JSON into Bronze Delta table
- [s02_silver_etl.py](./06-full-etl/s02_silver_etl.py) — Deduplicate, mask PII, filter, write to Silver
- [s03_incremental.py](./06-full-etl/s03_incremental.py) — Append new batch, merge into Silver, verify with time travel
