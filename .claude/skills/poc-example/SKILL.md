---
name: poc-example
description: Conventions and structure for writing POC example scripts under examples/. Use when creating or modifying POC scripts, adding new example folders, or reviewing existing examples for compliance.
argument-hint: "[task description or folder name]"
disable-model-invocation: true
---

# POC Example Conventions

Follow these rules when writing scripts under `examples/`.

## Directory structure

```
examples/
├── 00-minimal-poc/              # Golden reference — defines the coding conventions
│   └── s01_minimal_poc.py
├── {NN}-{topic-name}/           # Each topic gets a numbered folder
│   ├── README.md                # Describes what this POC covers
│   ├── data/                    # (optional) mock data files used by scripts
│   │   └── *.json / *.csv
│   ├── s01_{name}.py
│   ├── s02_{name}.py
│   └── ...
```

- Folder names: `{NN}-{kebab-case-topic}` (e.g. `01-delta-read-write`, `02-polars-etl`)
- Script names: `s{NN}_{snake_case_name}.py` (e.g. `s01_overwrite.py`, `s02_append.py`)
- Scripts within a folder are ordered from simple to complex

## Golden reference

Read `examples/00-minimal-poc/s01_minimal_poc.py` before writing any new script. It is the authoritative example for all conventions below.

## Script conventions

### 1. File header

Every script starts with:

```python
# -*- coding: utf-8 -*-

"""
Brief description of what this POC tests.
"""
```

The docstring should be 1-3 sentences. No naming-convention boilerplate — that lives in `00-minimal-poc` only.

### 2. Setup block — isolated S3 workspace

Every script that touches S3 must have an independent `s3dir_example` so deleting it resets everything:

```python
from yq_delta_lake_and_polars_poc.one.api import one

# --- setup ---
bsm = one.bsm
storage_options = one.polars_storage_options

s3dir_example = one.s3dir_root.joinpath("example-delta-{NN}-{SS}").to_dir()
s3dir_example.delete(bsm=bsm)  # start from a clean state every run
print(f"preview at: {s3dir_example.console_url}")

delta_uri = s3dir_example.uri
```

Key rules:
- Each script gets a **unique** S3 path suffix like `example-delta-01-01`, `example-delta-01-02`, etc. (folder number + script number)
- Always call `s3dir_example.delete(bsm=bsm)` at the top for idempotency
- Always print `s3dir_example.console_url` so the user can click to open the S3 console
- Get the URI via `s3dir_example.uri`, not `str(s3dir_example)`

### 3. Naming prefixes

| Prefix | Type | Example |
|--------|------|---------|
| `s3dir_` | S3Path pointing to a directory | `s3dir_example`, `s3dir_silver` |
| `s3path_` | S3Path pointing to a file | `s3path_config` |
| `df_` | Polars DataFrame | `df_init`, `df_after_write`, `df_upsert` |
| `delta_uri` | String URI for Delta table | `delta_uri = s3dir_example.uri` |

### 4. Function call style

All function calls with 2+ arguments use **multi-line** format:

```python
# good
df_init.write_delta(
    delta_uri,
    mode="overwrite",
    storage_options=storage_options,
)

# bad
df_init.write_delta(delta_uri, mode="overwrite", storage_options=storage_options)
```

### 5. Inline comments and print statements

- Use `# --- section name ---` separators between logical blocks
- Add inline comments near S3 operations explaining what they do
- Print a label before printing a DataFrame:

```python
print("=== read after initial write ===")
print(df_after_write)
```

- No logging framework — just `print()`
- Comments and docstrings in **English only**

### 6. Data

- Mock data files go in `examples/{folder-name}/data/`
- Keep datasets minimal (5-20 rows) — just enough to verify the behavior
- Prefer inline `pl.DataFrame(...)` for trivial data; use `data/*.json` for anything more structured

## README.md

Every folder must have a `README.md` that describes:
- What this POC covers (one-liner)
- A section per script explaining what it does and what to verify

## Task

$ARGUMENTS
