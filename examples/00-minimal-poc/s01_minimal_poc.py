# -*- coding: utf-8 -*-

"""
Minimal POC: Polars + Delta Lake + S3

Naming conventions:
- ``s3dir_`` prefix: S3Path pointing to a directory
- ``s3path_`` prefix: S3Path pointing to a file
- ``df_`` prefix: Polars DataFrame
- ``s3dir_example`` is the root of this test; deleting it cleans everything up

All function calls use multi-line style, no single-line chained calls.
"""

import polars as pl
from deltalake import DeltaTable

from yq_delta_lake_and_polars_poc.one.api import one

# --- setup ---
bsm = one.bsm
storage_options = one.polars_storage_options

s3dir_example = one.s3dir_root.joinpath("example-delta-01").to_dir()
s3dir_example.delete(bsm=bsm)  # start from a clean state every run
print(f"preview at: {s3dir_example.console_url}")

delta_uri = s3dir_example.uri

# --- initial write ---
df_init = pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
print("=== initial write ===")
print(df_init)
df_init.write_delta(
    delta_uri,
    mode="overwrite",
    storage_options=storage_options,
)

# --- read back after write ---
df_after_write = pl.read_delta(
    delta_uri,
    storage_options=storage_options,
)
print("=== read after initial write ===")
print(df_after_write)

# --- upsert: update id=2 to Bella, insert id=3 Cathy ---
df_upsert = pl.DataFrame({"id": [2, 3], "name": ["Bella", "Cathy"]})
print("=== upsert source ===")
print(df_upsert)

dt = DeltaTable(delta_uri, storage_options=storage_options)
(
    dt.merge(
        source=df_upsert,
        predicate="target.id = source.id",
        source_alias="source",
        target_alias="target",
    )
    .when_matched_update_all()
    .when_not_matched_insert_all()
    .execute()
)

# --- final read ---
df_after_upsert = pl.read_delta(
    delta_uri,
    storage_options=storage_options,
)
print("=== read after upsert (expect: Alice/Bella/Cathy) ===")
print(df_after_upsert.sort("id"))
