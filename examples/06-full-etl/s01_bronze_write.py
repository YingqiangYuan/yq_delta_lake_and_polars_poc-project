# -*- coding: utf-8 -*-

"""
Simulate raw data landing in the Bronze layer from batch_1.json.
"""

from pathlib import Path

import polars as pl

from yq_delta_lake_and_polars_poc.one.api import one

# --- setup ---
bsm = one.bsm
storage_options = one.polars_storage_options

s3dir_example = one.s3dir_root.joinpath("example-delta-06-01").to_dir()
s3dir_example.delete(bsm=bsm)  # start from a clean state every run
print(f"preview at: {s3dir_example.console_url}")

delta_uri = s3dir_example.uri

# --- read raw JSON data ---
data_dir = Path(__file__).parent / "data"
df_batch1 = pl.read_ndjson(data_dir / "batch_1.json")

print(f"=== batch_1.json: {df_batch1.shape[0]} rows ===")
print(df_batch1)

# --- write to Bronze Delta table ---
df_batch1.write_delta(
    delta_uri,
    mode="overwrite",
    storage_options=storage_options,
)

# --- read back and verify ---
df_bronze = pl.read_delta(
    delta_uri,
    storage_options=storage_options,
)
print(f"\n=== Bronze table: {df_bronze.shape[0]} rows ===")
print(f"schema: {df_bronze.schema}")
print(df_bronze)
