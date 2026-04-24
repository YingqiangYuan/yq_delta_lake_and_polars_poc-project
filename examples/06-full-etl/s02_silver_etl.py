# -*- coding: utf-8 -*-

"""
Bronze-to-Silver ETL: deduplicate, mask PII, filter declined, add ETL timestamp.
"""

import hashlib
from datetime import datetime
from datetime import timezone

import polars as pl

from yq_delta_lake_and_polars_poc.one.api import one

# --- setup ---
bsm = one.bsm
storage_options = one.polars_storage_options

# Bronze table (written by s01)
s3dir_bronze = one.s3dir_root.joinpath("example-delta-06-01").to_dir()
bronze_uri = s3dir_bronze.uri

# Silver table (new)
s3dir_silver = one.s3dir_root.joinpath("example-delta-06-02").to_dir()
s3dir_silver.delete(bsm=bsm)  # start from a clean state every run
print(f"silver preview at: {s3dir_silver.console_url}")

silver_uri = s3dir_silver.uri

# --- read Bronze ---
df_bronze = pl.read_delta(
    bronze_uri,
    storage_options=storage_options,
)
print(f"=== Bronze: {df_bronze.shape[0]} rows ===")

# --- ETL pipeline ---
etl_ts = datetime.now(timezone.utc).isoformat()

df_silver = (
    df_bronze
    # deduplicate by transaction_id
    .unique(
        subset=["transaction_id"],
        keep="first",
    )
    # SHA-256 tokenize card_id
    .with_columns(
        pl.col("card_id")
        .map_elements(
            lambda v: hashlib.sha256(v.encode()).hexdigest()[:16],
            return_dtype=pl.Utf8,
        )
        .alias("card_id"),
    )
    # filter out declined transactions
    .filter(
        pl.col("auth_status") != "DECLINED",
    )
    # add ETL timestamp
    .with_columns(
        pl.lit(etl_ts).alias("etl_ts"),
    )
)

print(f"=== Silver after ETL: {df_silver.shape[0]} rows ===")
print(f"  deduped: removed {df_bronze.shape[0] - df_bronze.unique(subset=['transaction_id']).shape[0]} duplicates")
print(f"  filtered: removed declined transactions")
print(f"  PII: card_id tokenized")
print(df_silver.sort("transaction_id"))

# --- write to Silver Delta table ---
df_silver.write_delta(
    silver_uri,
    mode="overwrite",
    storage_options=storage_options,
)

# --- verify ---
df_readback = pl.read_delta(
    silver_uri,
    storage_options=storage_options,
)
print(f"\n=== Silver readback: {df_readback.shape[0]} rows ===")
assert df_readback["transaction_id"].n_unique() == df_readback.shape[0], "Duplicates found!"
assert "etl_ts" in df_readback.columns, "etl_ts column missing!"
print("=== verification passed ===")
