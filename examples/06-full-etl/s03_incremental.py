# -*- coding: utf-8 -*-

"""
Incremental load: append batch_2 to Bronze, merge into Silver, verify with time travel.
"""

import hashlib
from datetime import datetime
from datetime import timezone
from pathlib import Path

import polars as pl
from deltalake import DeltaTable

from yq_delta_lake_and_polars_poc.one.api import one

# --- setup ---
bsm = one.bsm
storage_options = one.polars_storage_options

s3dir_bronze = one.s3dir_root.joinpath("example-delta-06-01").to_dir()
bronze_uri = s3dir_bronze.uri

s3dir_silver = one.s3dir_root.joinpath("example-delta-06-02").to_dir()
silver_uri = s3dir_silver.uri

print(f"bronze preview at: {s3dir_bronze.console_url}")
print(f"silver preview at: {s3dir_silver.console_url}")

# --- append batch_2.json to Bronze ---
data_dir = Path(__file__).parent / "data"
df_batch2 = pl.read_ndjson(data_dir / "batch_2.json")

print(f"\n=== batch_2.json: {df_batch2.shape[0]} rows ===")
print(df_batch2)

df_batch2.write_delta(
    bronze_uri,
    mode="append",
    storage_options=storage_options,
)

# --- re-read full Bronze table ---
df_bronze_full = pl.read_delta(
    bronze_uri,
    storage_options=storage_options,
)
print(f"\n=== Bronze after append: {df_bronze_full.shape[0]} rows ===")

# --- apply same ETL pipeline as s02 ---
etl_ts = datetime.now(timezone.utc).isoformat()

df_silver_new = (
    df_bronze_full
    .unique(
        subset=["transaction_id"],
        keep="last",
    )
    .with_columns(
        pl.col("card_id")
        .map_elements(
            lambda v: hashlib.sha256(v.encode()).hexdigest()[:16],
            return_dtype=pl.Utf8,
        )
        .alias("card_id"),
    )
    .filter(
        pl.col("auth_status") != "DECLINED",
    )
    .with_columns(
        pl.lit(etl_ts).alias("etl_ts"),
    )
)

print(f"=== ETL output: {df_silver_new.shape[0]} rows ===")

# --- merge into Silver using transaction_id as key ---
dt_silver = DeltaTable(
    silver_uri,
    storage_options=storage_options,
)
(
    dt_silver.merge(
        source=df_silver_new,
        predicate="target.transaction_id = source.transaction_id",
        source_alias="source",
        target_alias="target",
    )
    .when_matched_update_all()
    .when_not_matched_insert_all()
    .execute()
)

# --- read Silver and verify ---
df_silver_final = pl.read_delta(
    silver_uri,
    storage_options=storage_options,
)
print(f"\n=== Silver after merge: {df_silver_final.shape[0]} rows ===")
print(df_silver_final.sort("transaction_id"))

# no duplicate transaction_ids
assert df_silver_final["transaction_id"].n_unique() == df_silver_final.shape[0], (
    "Duplicate transaction_ids found in Silver!"
)

# card_id is tokenized (should be hex strings, not card numbers)
for card in df_silver_final["card_id"].to_list():
    assert "-" not in card, f"card_id not tokenized: {card}"

print(f"\n=== verified: {df_silver_final.shape[0]} unique transactions, all card_ids tokenized ===")

# --- time travel: compare Silver version 0 vs version 1 ---
df_silver_v0 = pl.read_delta(
    silver_uri,
    version=0,
    storage_options=storage_options,
)
df_silver_v1 = pl.read_delta(
    silver_uri,
    version=1,
    storage_options=storage_options,
)

print(f"\n=== Silver version 0: {df_silver_v0.shape[0]} rows ===")
print(df_silver_v0.sort("transaction_id"))

print(f"\n=== Silver version 1 (after merge): {df_silver_v1.shape[0]} rows ===")
print(df_silver_v1.sort("transaction_id"))

print(f"\n=== time travel verified: v0={df_silver_v0.shape[0]} rows, v1={df_silver_v1.shape[0]} rows ===")
