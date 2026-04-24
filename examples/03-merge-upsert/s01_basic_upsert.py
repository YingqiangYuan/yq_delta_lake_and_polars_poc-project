# -*- coding: utf-8 -*-

"""
Minimal merge/upsert: update existing rows and insert new ones.
"""

import polars as pl
from deltalake import DeltaTable

from yq_delta_lake_and_polars_poc.one.api import one

# --- setup ---
bsm = one.bsm
storage_options = one.polars_storage_options

s3dir_example = one.s3dir_root.joinpath("example-delta-03-01").to_dir()
s3dir_example.delete(bsm=bsm)  # start from a clean state every run
print(f"preview at: {s3dir_example.console_url}")

delta_uri = s3dir_example.uri

# --- initial write ---
df_init = pl.DataFrame(
    {
        "id": [1, 2],
        "name": ["Alice", "Bob"],
    }
)
print("=== initial table ===")
print(df_init)

df_init.write_delta(
    delta_uri,
    mode="overwrite",
    storage_options=storage_options,
)

# --- prepare upsert source ---
df_upsert = pl.DataFrame(
    {
        "id": [2, 3],
        "name": ["Bella", "Cathy"],
    }
)
print("=== upsert source ===")
print(df_upsert)

# --- merge: update matched, insert unmatched ---
dt = DeltaTable(
    delta_uri,
    storage_options=storage_options,
)
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

# --- read back and verify ---
df_after_upsert = pl.read_delta(
    delta_uri,
    storage_options=storage_options,
)
print("=== after upsert (expect: Alice, Bella, Cathy) ===")
print(df_after_upsert.sort("id"))

assert df_after_upsert.shape[0] == 3, f"Expected 3 rows, got {df_after_upsert.shape[0]}"
assert set(df_after_upsert["name"].to_list()) == {"Alice", "Bella", "Cathy"}
print("=== verification passed ===")
