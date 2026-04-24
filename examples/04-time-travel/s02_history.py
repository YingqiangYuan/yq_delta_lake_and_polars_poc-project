# -*- coding: utf-8 -*-

"""
Inspect the commit log of a Delta table to see who wrote what and when.
"""

import polars as pl
from deltalake import DeltaTable

from yq_delta_lake_and_polars_poc.one.api import one

# --- setup ---
bsm = one.bsm
storage_options = one.polars_storage_options

s3dir_example = one.s3dir_root.joinpath("example-delta-04-02").to_dir()
s3dir_example.delete(bsm=bsm)  # start from a clean state every run
print(f"preview at: {s3dir_example.console_url}")

delta_uri = s3dir_example.uri

# --- create a table with multiple writes to build history ---
df_v0 = pl.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
df_v0.write_delta(
    delta_uri,
    mode="overwrite",
    storage_options=storage_options,
)
print("=== wrote version 0 (3 rows, overwrite) ===")

df_v1 = pl.DataFrame({"id": [4, 5], "value": ["d", "e"]})
df_v1.write_delta(
    delta_uri,
    mode="append",
    storage_options=storage_options,
)
print("=== wrote version 1 (2 rows, append) ===")

df_v2 = pl.DataFrame({"id": [10, 20], "value": ["x", "y"]})
df_v2.write_delta(
    delta_uri,
    mode="overwrite",
    storage_options=storage_options,
)
print("=== wrote version 2 (2 rows, overwrite) ===")

# --- inspect commit history ---
dt = DeltaTable(
    delta_uri,
    storage_options=storage_options,
)
history = dt.history()

print(f"\n=== commit history ({len(history)} entries) ===")
for entry in history:
    version = entry.get("version", "?")
    timestamp = entry.get("timestamp", "?")
    operation = entry.get("operation", "?")
    parameters = entry.get("operationParameters", {})
    print(f"  version={version}  timestamp={timestamp}  operation={operation}")
    print(f"    parameters={parameters}")

print("\n=== latest version ===")
print(f"  version: {dt.version()}")
print(f"  files: {dt.file_uris()}")
