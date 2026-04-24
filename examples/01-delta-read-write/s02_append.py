# -*- coding: utf-8 -*-

"""
Append POC: write an initial batch, then append a second batch using
``mode="append"``. Demonstrates that append does NOT deduplicate.
"""

import polars as pl

from yq_delta_lake_and_polars_poc.one.api import one

# --- setup ---
bsm = one.bsm
storage_options = one.polars_storage_options

s3dir_example = one.s3dir_root.joinpath("example-delta-01-02").to_dir()
s3dir_example.delete(bsm=bsm)  # start from a clean state every run
print(f"preview at: {s3dir_example.console_url}")

delta_uri = s3dir_example.uri

# --- batch 1: initial write with mode="overwrite" ---
df_batch1 = pl.DataFrame(
    {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Cathy"],
    }
)
print("=== batch 1 (overwrite) ===")
print(df_batch1)

df_batch1.write_delta(
    delta_uri,
    mode="overwrite",
    storage_options=storage_options,
)

# --- batch 2: append (contains a duplicate id=3 on purpose) ---
df_batch2 = pl.DataFrame(
    {
        "id": [3, 4],
        "name": ["Cathy-dup", "David"],
    }
)
print("=== batch 2 (append) ===")
print(df_batch2)

df_batch2.write_delta(
    delta_uri,
    mode="append",
    storage_options=storage_options,
)

# --- read back and verify ---
df_all = pl.read_delta(
    delta_uri,
    storage_options=storage_options,
)
print("=== read back after append ===")
print(df_all.sort("id"))

# --- verify total row count is 5 (3 + 2, no dedup) ---
assert df_all.shape[0] == 5, f"Expected 5 rows, got {df_all.shape[0]}"
print("=== verification passed: 5 rows total (append does NOT deduplicate) ===")
