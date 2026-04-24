# -*- coding: utf-8 -*-

"""
Simulate a credit profile incremental update with 200 baseline records,
30 updates, and 20 new inserts via Delta Lake merge.
"""

import random
from datetime import datetime
from datetime import timedelta

import polars as pl
from deltalake import DeltaTable

from yq_delta_lake_and_polars_poc.one.api import one

# --- setup ---
bsm = one.bsm
storage_options = one.polars_storage_options

s3dir_example = one.s3dir_root.joinpath("example-delta-03-02").to_dir()
s3dir_example.delete(bsm=bsm)  # start from a clean state every run
print(f"preview at: {s3dir_example.console_url}")

delta_uri = s3dir_example.uri

# --- generate 200 baseline customer records ---
random.seed(42)
base_date = datetime(2025, 1, 1)

df_baseline = pl.DataFrame(
    {
        "customer_id": list(range(1, 201)),
        "name": [f"Customer_{i:03d}" for i in range(1, 201)],
        "credit_score": [random.randint(300, 850) for _ in range(200)],
        "updated_at": [
            (base_date + timedelta(days=random.randint(0, 30))).isoformat()
            for _ in range(200)
        ],
    }
)
print(f"=== baseline: {df_baseline.shape[0]} rows ===")
print(df_baseline.head(5))

df_baseline.write_delta(
    delta_uri,
    mode="overwrite",
    storage_options=storage_options,
)

# --- prepare 50-row merge source ---
# 30 existing customers with updated credit_score and updated_at
update_ids = random.sample(range(1, 201), 30)
# 20 brand new customers
new_ids = list(range(201, 221))

merge_date = datetime(2025, 6, 1)

df_merge_source = pl.DataFrame(
    {
        "customer_id": update_ids + new_ids,
        "name": [f"Customer_{i:03d}" for i in update_ids]
        + [f"Customer_{i:03d}" for i in new_ids],
        "credit_score": [random.randint(300, 850) for _ in range(50)],
        "updated_at": [merge_date.isoformat() for _ in range(50)],
    }
)
print(f"=== merge source: {df_merge_source.shape[0]} rows (30 updates + 20 new) ===")
print(df_merge_source.head(5))

# --- save update_ids for verification later ---
update_scores = dict(
    zip(
        df_merge_source.filter(pl.col("customer_id").is_in(update_ids))["customer_id"].to_list(),
        df_merge_source.filter(pl.col("customer_id").is_in(update_ids))["credit_score"].to_list(),
    )
)

# --- merge on customer_id ---
dt = DeltaTable(
    delta_uri,
    storage_options=storage_options,
)
(
    dt.merge(
        source=df_merge_source,
        predicate="target.customer_id = source.customer_id",
        source_alias="source",
        target_alias="target",
    )
    .when_matched_update_all()
    .when_not_matched_insert_all()
    .execute()
)

# --- read back and verify ---
df_after_merge = pl.read_delta(
    delta_uri,
    storage_options=storage_options,
)
print(f"=== after merge: {df_after_merge.shape[0]} rows (expect 220) ===")
print(df_after_merge.sort("customer_id").tail(5))

# verify total row count
assert df_after_merge.shape[0] == 220, (
    f"Expected 220 rows, got {df_after_merge.shape[0]}"
)

# verify the 30 updated customers have new credit_score values
df_updated = df_after_merge.filter(
    pl.col("customer_id").is_in(update_ids),
).sort("customer_id")
for row in df_updated.iter_rows(named=True):
    expected_score = update_scores[row["customer_id"]]
    assert row["credit_score"] == expected_score, (
        f"Customer {row['customer_id']}: expected score {expected_score}, got {row['credit_score']}"
    )

# verify the 20 new customers are present
df_new = df_after_merge.filter(
    pl.col("customer_id").is_in(new_ids),
)
assert df_new.shape[0] == 20, f"Expected 20 new customers, got {df_new.shape[0]}"

print("=== verification passed: 200 original + 20 new = 220, 30 scores updated ===")
