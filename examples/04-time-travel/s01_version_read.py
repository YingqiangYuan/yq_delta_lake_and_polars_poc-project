# -*- coding: utf-8 -*-

"""
Read specific historical versions of a Delta table using time travel.
"""

import polars as pl

from yq_delta_lake_and_polars_poc.one.api import one

# --- setup ---
bsm = one.bsm
storage_options = one.polars_storage_options

s3dir_example = one.s3dir_root.joinpath("example-delta-04-01").to_dir()
s3dir_example.delete(bsm=bsm)  # start from a clean state every run
print(f"preview at: {s3dir_example.console_url}")

delta_uri = s3dir_example.uri

# --- version 0: write 3 rows ---
df_v0 = pl.DataFrame(
    {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Cathy"],
    }
)
print("=== writing version 0 (3 rows) ===")
print(df_v0)

df_v0.write_delta(
    delta_uri,
    mode="overwrite",
    storage_options=storage_options,
)

# --- version 1: overwrite with 5 rows ---
df_v1 = pl.DataFrame(
    {
        "id": [10, 20, 30, 40, 50],
        "name": ["Victor", "Wendy", "Xander", "Yara", "Zack"],
    }
)
print("=== writing version 1 (5 rows) ===")
print(df_v1)

df_v1.write_delta(
    delta_uri,
    mode="overwrite",
    storage_options=storage_options,
)

# --- version 2: overwrite with 2 rows ---
df_v2 = pl.DataFrame(
    {
        "id": [100, 200],
        "name": ["Final_A", "Final_B"],
    }
)
print("=== writing version 2 (2 rows) ===")
print(df_v2)

df_v2.write_delta(
    delta_uri,
    mode="overwrite",
    storage_options=storage_options,
)

# --- read each version explicitly ---
df_read_v0 = pl.read_delta(
    delta_uri,
    version=0,
    storage_options=storage_options,
)
print("=== read version 0 (expect 3 rows) ===")
print(df_read_v0)
assert df_read_v0.shape[0] == 3, f"v0: expected 3 rows, got {df_read_v0.shape[0]}"

df_read_v1 = pl.read_delta(
    delta_uri,
    version=1,
    storage_options=storage_options,
)
print("=== read version 1 (expect 5 rows) ===")
print(df_read_v1)
assert df_read_v1.shape[0] == 5, f"v1: expected 5 rows, got {df_read_v1.shape[0]}"

df_read_latest = pl.read_delta(
    delta_uri,
    storage_options=storage_options,
)
print("=== read latest / version 2 (expect 2 rows) ===")
print(df_read_latest)
assert df_read_latest.shape[0] == 2, f"latest: expected 2 rows, got {df_read_latest.shape[0]}"

print("=== verification passed: all versions read correctly ===")
