# -*- coding: utf-8 -*-

"""
Deduplication, aggregation, and joins with Polars.
"""

import polars as pl

# --- sample data with duplicates ---
df_orders = pl.DataFrame(
    {
        "id": [1, 2, 3, 2, 4, 3],
        "customer": ["Alice", "Bob", "Cathy", "Bob", "David", "Cathy"],
        "amount": [100, 200, 150, 200, 300, 180],
    }
)
print("=== orders (with duplicates) ===")
print(df_orders)

# --- unique: deduplicate by key column ---
df_deduped = df_orders.unique(
    subset=["id"],
    keep="first",
)
print("=== after unique(subset=['id'], keep='first') ===")
print(df_deduped.sort("id"))

# --- group_by + agg ---
df_agg = df_orders.group_by("customer").agg(
    pl.len().alias("order_count"),
    pl.col("amount").sum().alias("total_amount"),
    pl.col("amount").mean().alias("avg_amount"),
)
print("=== group_by customer -> count, sum, mean ===")
print(df_agg.sort("customer"))

# --- join: prepare a second DataFrame ---
df_customers = pl.DataFrame(
    {
        "customer": ["Alice", "Bob", "Cathy", "Frank"],
        "tier": ["gold", "silver", "gold", "bronze"],
    }
)
print("=== customers reference table ===")
print(df_customers)

# --- inner join ---
df_inner = df_agg.join(
    df_customers,
    on="customer",
    how="inner",
)
print("=== inner join (only matching rows) ===")
print(df_inner.sort("customer"))

# --- left join ---
df_left = df_agg.join(
    df_customers,
    on="customer",
    how="left",
)
print("=== left join (all from orders, null if no match) ===")
print(df_left.sort("customer"))

# --- full outer join ---
df_outer = df_agg.join(
    df_customers,
    on="customer",
    how="full",
    coalesce=True,
)
print("=== full outer join (all from both sides) ===")
print(df_outer.sort("customer"))
