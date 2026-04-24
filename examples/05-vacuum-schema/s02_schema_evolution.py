# -*- coding: utf-8 -*-

"""
Add new columns to an existing Delta table without rewriting old data.
"""

import polars as pl

from yq_delta_lake_and_polars_poc.one.api import one

# --- setup ---
bsm = one.bsm
storage_options = one.polars_storage_options

s3dir_example = one.s3dir_root.joinpath("example-delta-05-02").to_dir()
s3dir_example.delete(bsm=bsm)  # start from a clean state every run
print(f"preview at: {s3dir_example.console_url}")

delta_uri = s3dir_example.uri

# --- version 0: schema (id: i64, name: str) ---
df_v0 = pl.DataFrame(
    {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Cathy"],
    }
)
print("=== version 0 schema: id, name ===")
print(df_v0)

df_v0.write_delta(
    delta_uri,
    mode="overwrite",
    storage_options=storage_options,
)

# --- version 1: add email column with schema_mode="merge" ---
df_v1 = pl.DataFrame(
    {
        "id": [4, 5],
        "name": ["David", "Eve"],
        "email": ["david@example.com", "eve@example.com"],
    }
)
print("\n=== version 1 schema: id, name, email ===")
print(df_v1)

df_v1.write_delta(
    delta_uri,
    mode="append",
    delta_write_options={
        "schema_mode": "merge",
    },
    storage_options=storage_options,
)

# --- read the full table ---
df_merged = pl.read_delta(
    delta_uri,
    storage_options=storage_options,
)
print("\n=== full table after schema evolution ===")
print(df_merged.sort("id"))

# --- verify merged schema contains all three columns ---
assert set(df_merged.columns) == {"id", "name", "email"}, (
    f"Expected columns {{id, name, email}}, got {set(df_merged.columns)}"
)

# --- verify old rows have email = null ---
df_old_rows = df_merged.filter(
    pl.col("id").is_in([1, 2, 3]),
).sort("id")
assert df_old_rows["email"].null_count() == 3, (
    "Old rows should have null email"
)
print("\n=== old rows (email = null as expected) ===")
print(df_old_rows)

# --- verify new rows have email values ---
df_new_rows = df_merged.filter(
    pl.col("id").is_in([4, 5]),
).sort("id")
assert df_new_rows["email"].null_count() == 0, (
    "New rows should have email values"
)
print("\n=== new rows (email populated) ===")
print(df_new_rows)

print("\n=== verification passed: schema evolution works correctly ===")
