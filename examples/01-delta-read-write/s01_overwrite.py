# -*- coding: utf-8 -*-

"""
Overwrite POC: write a DataFrame to a Delta table with ``mode="overwrite"``,
then read it back. This is the simplest possible Delta Lake round-trip.
"""

import polars as pl

from yq_delta_lake_and_polars_poc.one.api import one

# --- setup ---
bsm = one.bsm
storage_options = one.polars_storage_options

s3dir_example = one.s3dir_root.joinpath("example-delta-01-01").to_dir()
s3dir_example.delete(bsm=bsm)  # start from a clean state every run
print(f"preview at: {s3dir_example.console_url}")

delta_uri = s3dir_example.uri

# --- write with mode="overwrite" ---
df_original = pl.DataFrame(
    {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Cathy", "David", "Eve"],
        "score": [90, 85, 92, 78, 95],
    }
)
print("=== original DataFrame ===")
print(df_original)

df_original.write_delta(
    delta_uri,
    mode="overwrite",
    storage_options=storage_options,
)

# --- read back after write ---
df_readback = pl.read_delta(
    delta_uri,
    storage_options=storage_options,
)
print("=== read back after overwrite ===")
print(df_readback)

# --- verify round-trip ---
assert df_readback.sort("id").equals(df_original.sort("id")), "Round-trip mismatch!"
print("=== verification passed: read-back matches original ===")
