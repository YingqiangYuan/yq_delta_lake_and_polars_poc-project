# 03 - Delta Lake Merge / Upsert

The core business operation for incremental data updates on S3.

## Scripts

### s01_basic_upsert.py

Minimal merge/upsert example.

- Write an initial table: ``(1, "Alice"), (2, "Bob")``
- Prepare upsert source: ``(2, "Bella"), (3, "Cathy")``
- ``DeltaTable.merge(source, predicate="target.id = source.id")``
  - ``.when_matched_update_all()`` — id=2 updates from Bob to Bella
  - ``.when_not_matched_insert_all()`` — id=3 Cathy is inserted
- Read back and verify: Alice, Bella, Cathy

### s02_credit_profile.py

Simulate a credit profile incremental update — closer to real project usage.

- Write 200 baseline customer records (customer_id, name, credit_score, updated_at)
- Prepare 50-row merge source:
  - 30 existing customers with updated credit_score and updated_at
  - 20 brand new customers
- Merge with predicate on ``customer_id``
- Read back and verify:
  - Total rows = 220 (200 - 30 updated + 30 updated + 20 new)
  - The 30 updated customers have new credit_score values
  - The 20 new customers are present
