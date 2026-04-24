# -*- coding: utf-8 -*-

"""
Error-mode POC: demonstrate ``mode="error"`` which raises an exception if the
Delta table already exists. Verifies that original data is untouched after failure.
"""

import polars as pl
from deltalake.exceptions import DeltaError

from yq_delta_lake_and_polars_poc.one.api import one

# --- setup ---
bsm = one.bsm
storage_options = one.polars_storage_options

s3dir_example = one.s3dir_root.joinpath("example-delta-01-03").to_dir()
s3dir_example.delete(bsm=bsm)  # start from a clean state every run
print(f"preview at: {s3dir_example.console_url}")

delta_uri = s3dir_example.uri

# --- first write: succeeds ---
df_first = pl.DataFrame(
    {
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Cathy"],
    }
)
print("=== first write with mode='error' ===")
print(df_first)

df_first.write_delta(
    delta_uri,
    mode="error",
    storage_options=storage_options,
)
print("=== first write succeeded ===")

# --- second write: raises DeltaError ---
df_second = pl.DataFrame(
    {
        "id": [4, 5],
        "name": ["David", "Eve"],
    }
)
print("=== second write with mode='error' (expect failure) ===")

try:
    df_second.write_delta(
        delta_uri,
        mode="error",
        storage_options=storage_options,
    )
except DeltaError as e:
    print(f"=== caught expected DeltaError: {e} ===")

# --- confirm original data is untouched ---
df_after = pl.read_delta(
    delta_uri,
    storage_options=storage_options,
)
print("=== read back: original data untouched ===")
print(df_after.sort("id"))

assert df_after.sort("id").equals(df_first.sort("id")), "Data was modified!"
print("=== verification passed: original data intact after failed error-mode write ===")
