# -*- coding: utf-8 -*-

"""
Basic column manipulation and row filtering with Polars.
"""

import polars as pl

# --- sample data ---
df_raw = pl.DataFrame(
    {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Cathy", "David", "Eve"],
        "amount": [100, -50, 200, 0, 150],
        "status": ["active", "inactive", "active", "active", "inactive"],
    }
)
print("=== raw data ===")
print(df_raw)

# --- with_columns: add / rename / transform ---
df_transformed = df_raw.with_columns(
    pl.col("name").str.to_uppercase().alias("name_upper"),
    (pl.col("amount") * 1.1).alias("amount_with_tax"),
)
print("=== after with_columns (add name_upper, amount_with_tax) ===")
print(df_transformed)

# --- drop: remove columns ---
df_dropped = df_transformed.drop("name_upper")
print("=== after drop name_upper ===")
print(df_dropped)

# --- cast: type conversion ---
df_casted = df_raw.with_columns(
    pl.col("amount").cast(pl.Float64).alias("amount_float"),
    pl.col("id").cast(pl.Utf8).alias("id_str"),
)
print("=== after cast (amount -> Float64, id -> Utf8) ===")
print(df_casted)

# --- filter: row filtering ---
df_positive = df_raw.filter(
    pl.col("amount") > 0,
)
print("=== filter: amount > 0 ===")
print(df_positive)

df_active_positive = df_raw.filter(
    (pl.col("amount") > 0) & (pl.col("status") == "active"),
)
print("=== filter: amount > 0 AND status == active ===")
print(df_active_positive)

# --- select: pick specific columns ---
df_selected = df_raw.select(
    "id",
    "name",
    "amount",
)
print("=== select: id, name, amount ===")
print(df_selected)
