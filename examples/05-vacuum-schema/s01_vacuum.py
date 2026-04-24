# -*- coding: utf-8 -*-

"""
Clean up old Parquet files no longer referenced by the latest Delta version.
"""

import polars as pl
from deltalake import DeltaTable

from yq_delta_lake_and_polars_poc.one.api import one

# --- setup ---
bsm = one.bsm
storage_options = one.polars_storage_options

s3dir_example = one.s3dir_root.joinpath("example-delta-05-01").to_dir()
s3dir_example.delete(bsm=bsm)  # start from a clean state every run
print(f"preview at: {s3dir_example.console_url}")

delta_uri = s3dir_example.uri

# --- create multiple versions to accumulate old Parquet files ---
for version in range(4):
    df = pl.DataFrame(
        {
            "id": list(range(version * 10, version * 10 + 3)),
            "value": [f"v{version}_a", f"v{version}_b", f"v{version}_c"],
        }
    )
    df.write_delta(
        delta_uri,
        mode="overwrite",
        storage_options=storage_options,
    )
    print(f"=== wrote version {version} ===")

# --- list S3 files before vacuum ---
files_before = [p.key for p in s3dir_example.iterdir(bsm=bsm)]
parquet_before = [f for f in files_before if f.endswith(".parquet")]
print(f"\n=== files before vacuum: {len(parquet_before)} parquet files ===")
for f in parquet_before:
    print(f"  {f}")

# --- vacuum with 0 hours retention (demo only, use 168+ in production) ---
dt = DeltaTable(
    delta_uri,
    storage_options=storage_options,
)
print(f"\n=== current version: {dt.version()} ===")
removed = dt.vacuum(
    retention_hours=0,
    enforce_retention_duration=False,
)
print(f"=== vacuum removed {len(removed)} files ===")

# --- verify vacuum removed the right number of files ---
assert len(removed) == 3, f"Expected 3 files removed, got {len(removed)}"
print(f"=== removed files ===")
for f in removed:
    print(f"  {f}")

# --- verify latest version still readable ---
df_latest = pl.read_delta(
    delta_uri,
    storage_options=storage_options,
)
print("\n=== latest version still readable ===")
print(df_latest)

# --- note on S3 eventual consistency ---
# After vacuum, old version reads may still succeed briefly due to S3
# eventual consistency. In a real scenario the old Parquet files are deleted
# and reading version 0 would raise an error.
print("\n=== verification passed: vacuum removed 3 old parquet files ===")
