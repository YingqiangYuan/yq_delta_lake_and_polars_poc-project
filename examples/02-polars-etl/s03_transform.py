# -*- coding: utf-8 -*-

"""
Custom transformations: PII hashing and conditional column creation.
"""

import hashlib

import polars as pl

# --- sample transaction data ---
df_transactions = pl.DataFrame(
    {
        "txn_id": [1, 2, 3, 4, 5],
        "card_id": [
            "4111-1111-1111-1111",
            "5500-0000-0000-0004",
            "4111-1111-1111-1111",
            "3400-0000-0000-009",
            "5500-0000-0000-0004",
        ],
        "amount": [25.0, 150.0, 500.0, 75.0, 1200.0],
        "merchant": ["Coffee Shop", "Electronics", "Jewelry", "Gas Station", "Travel"],
    }
)
print("=== raw transactions ===")
print(df_transactions)

# --- SHA-256 tokenization on card_id (PII masking) ---
df_hashed = df_transactions.with_columns(
    pl.col("card_id")
    .map_elements(
        lambda v: hashlib.sha256(v.encode()).hexdigest()[:16],
        return_dtype=pl.Utf8,
    )
    .alias("card_id_hash"),
)
print("=== after SHA-256 tokenization (first 16 chars) ===")
print(df_hashed)

# --- conditional column with when/then/otherwise ---
df_flagged = df_transactions.with_columns(
    pl.when(pl.col("amount") >= 500.0)
    .then(pl.lit("high"))
    .when(pl.col("amount") >= 100.0)
    .then(pl.lit("medium"))
    .otherwise(pl.lit("low"))
    .alias("risk_level"),
)
print("=== after conditional risk_level column ===")
print(df_flagged)

# --- chain multiple transformations in a single with_columns ---
df_final = df_transactions.with_columns(
    # PII masking
    pl.col("card_id")
    .map_elements(
        lambda v: hashlib.sha256(v.encode()).hexdigest()[:16],
        return_dtype=pl.Utf8,
    )
    .alias("card_id_hash"),
    # risk classification
    pl.when(pl.col("amount") >= 500.0)
    .then(pl.lit("high"))
    .when(pl.col("amount") >= 100.0)
    .then(pl.lit("medium"))
    .otherwise(pl.lit("low"))
    .alias("risk_level"),
    # amount in cents
    (pl.col("amount") * 100).cast(pl.Int64).alias("amount_cents"),
).drop("card_id")  # drop original PII column
print("=== final: chained transformations, PII removed ===")
print(df_final)
