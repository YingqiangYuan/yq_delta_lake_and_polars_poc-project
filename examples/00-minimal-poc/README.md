# 00 — Minimal POC: Background Concepts

Before you open `s01_minimal_poc.py`, this document walks you through the three
technologies that make it work: **AWS S3**, **Polars**, and **Delta Lake**. If
you already know all three, skip straight to the code. If any of them sound
unfamiliar, read on — we will build up from first principles, with a bit of
history along the way.

This project lives in the **fintech / financial data engineering** space. The
examples simulate things like credit-card transaction streams, credit profiles,
and fraud alerts flowing through a data lake. You do not need a finance
background to follow along, but knowing the domain helps explain *why* certain
technology choices were made.

---

## 1. AWS S3 — The World's Biggest Hard Drive

### A brief origin story

In the early 2000s, Amazon was solving its own problem: how do you store
billions of product images, logs, and backups without buying ever-larger file
servers? The answer, launched publicly in March 2006, was **Amazon Simple
Storage Service** — S3 for short. It was one of the very first AWS services
(along with SQS), and it quietly became the foundation under most of the
modern data industry.

Today, S3 stores **over 350 trillion objects**. Netflix streams from it. Airbnb
stores its data lake on it. NASA puts satellite imagery in it. And in fintech,
almost every data lake — from transaction logs to credit bureau files — sits on
S3, because it is cheap, durable (99.999999999% — eleven nines), and
practically infinite in capacity.

### What S3 actually is

Think of S3 as a **network hard drive** — but one that works nothing like the
hard drive in your laptop.

Your laptop has a **file system**: directories inside directories, paths like
`/Users/alice/Documents/report.pdf`. You can rename folders, move files around,
and list everything inside a directory instantly.

S3 has none of that. It has exactly two concepts:

| Concept    | What it is                                                     | Example                              |
|------------|----------------------------------------------------------------|--------------------------------------|
| **Bucket** | A globally unique container. Think of it as a top-level drive. | `my-company-data-lake`               |
| **Object** | A file stored inside a bucket, identified by a **key**.        | `bronze/transactions/2025-01-15.json`|

That is it. There are no directories, no folders, no subdirectories. When you
see something like `bronze/transactions/2025-01-15.json` in the AWS Console
and it *looks* like a folder structure — that is an illusion. The entire string
`bronze/transactions/2025-01-15.json` is just the object's **key** (its name).
The `/` characters have no special meaning to S3 itself; the Console simply
renders them as a folder tree for human convenience.

Why does this matter? Because when your code "lists all files in the
`bronze/transactions/` folder", it is actually asking S3: *give me all objects
whose key starts with the prefix `bronze/transactions/`*. That is a string
prefix scan, not a directory listing. Understanding this helps you debug
performance issues and write efficient data pipelines later.

### The four things you do with S3

In practice, you only ever do four operations:

1. **PUT** — upload an object (write a file)
2. **GET** — download an object (read a file)
3. **LIST** — find objects by key prefix (like "give me all keys starting with `bronze/`")
4. **DELETE** — remove an object

There is no "rename" and no "move". To rename a file, you copy it to a new key
and delete the old one. This is a deliberate design choice — it keeps the
system simple and massively scalable.

### S3 in this project

In our fintech data lake, S3 is the **storage layer**. Every piece of data —
raw transaction JSON from card processors, cleaned credit profiles, aggregated
fraud reports — ultimately lives as objects in an S3 bucket. The bucket is
organized by key prefixes that represent logical layers:

```
s3://axiomcard-datalake/
    bronze/transactions/...      <- raw data, as-received
    silver/transactions/...      <- cleaned, deduplicated, PII-masked
    gold/daily-summary/...       <- aggregated business metrics
```

But remember: those are not folders. They are just naming conventions — strings
that happen to contain `/`.

---

## 2. Polars — The DataFrame Library That Took the Crown

### The Pandas era

If you have ever touched data in Python, you have probably used **Pandas**.
Released in 2008 by Wes McKinney (who was working in finance at the time — the
connection to fintech runs deep), Pandas gave Python its first real
"spreadsheet-in-code" experience. You could load a CSV, filter rows, join
tables, compute aggregates — all in a few lines. It was revolutionary, and for
over a decade it was *the* tool.

But Pandas has a problem. It was designed in an era when datasets fit in memory
on a single machine, and it was written in Python with NumPy under the hood.
As datasets grew from megabytes to gigabytes, Pandas started to struggle:

- It uses a **single CPU core** for most operations. Your 8-core machine sits
  mostly idle.
- Its memory usage is **inefficient** — a 1 GB CSV can easily consume 3-5 GB
  of RAM after loading, because of how Pandas represents data internally.
- The API has **inconsistencies** that trip up even experienced users (the
  infamous `SettingWithCopyWarning` anyone?).

For small datasets, none of this matters. For the kind of data we deal with in
fintech — millions of transactions per day, each with dozens of fields — it
becomes a real bottleneck.

### Enter Polars

In 2020, a Dutch developer named Ritchie Vink started building **Polars** — a
DataFrame library written in **Rust** (a systems programming language known for
speed and memory safety) with a Python API on top. The pitch was simple: *what
if DataFrames were fast by default?*

Polars is:

- **Multi-threaded** — it automatically uses all your CPU cores. A `group_by`
  on 10 million rows that takes 8 seconds in Pandas might take 0.8 seconds in
  Polars.
- **Memory-efficient** — it uses Apache Arrow's columnar memory format under
  the hood, which means less copying, less waste, and better cache locality.
- **Lazy-evaluation capable** — you can build up a chain of operations and
  Polars will optimize the entire plan before executing it (similar to how SQL
  databases optimize queries).
- **Consistent API** — no copy-vs-view confusion, no chained indexing warnings.

Benchmarks vary, but Polars is commonly **5-10x faster** than Pandas for
typical ETL workloads, and sometimes 20-50x faster for operations that benefit
from parallelism.

### What is a DataFrame, anyway?

If you are completely new to this: a **DataFrame** is just a **two-dimensional
table** — rows and columns, like a spreadsheet or a database table.

```
┌────┬─────────┬────────┬──────────┐
│ id │ name    │ amount │ currency │
├────┼─────────┼────────┼──────────┤
│  1 │ Alice   │  42.50 │ USD      │
│  2 │ Bob     │ 137.00 │ EUR      │
│  3 │ Cathy   │  89.99 │ USD      │
└────┴─────────┴────────┴──────────┘
```

Each column has a **data type** (integer, string, float, date, etc.), and you
can perform operations on entire columns at once — filter all rows where
`amount > 100`, compute the sum of `amount` grouped by `currency`, join this
table with another table on the `id` column, and so on.

Pandas and Polars both work with DataFrames. The difference is that Polars does
it much, much faster.

### Polars in this project

In our fintech data pipeline, Polars is the **compute engine**. Every time we
need to transform data — deduplicate transactions, mask credit card numbers
with SHA-256 hashes, filter out invalid records, aggregate daily summaries — we
do it with Polars.

We chose Polars over Pandas because our ETL runs inside **AWS Lambda** (a
serverless compute service with strict memory limits). When you are processing
a gigabyte of transaction data and your Lambda function only has 4 GB of RAM,
you need a library that is stingy with memory and fast enough to finish within
the 15-minute execution limit. Polars fits that constraint; Pandas often does
not.

The heavyweight alternative would be **Apache Spark** — the distributed
processing engine that dominated big data for most of the 2010s. Spark is
enormously powerful, but it requires a cluster of machines to run, which means
operational complexity and cost. For datasets in the low-to-mid gigabyte range
(which covers the vast majority of fintech ETL workloads), a single Polars
process on a beefy Lambda is simpler, cheaper, and fast enough.

---

## 3. Delta Lake — The Transaction Log That Changed Everything

### The problem with plain files

Here is a story that has played out at hundreds of companies.

You start with a simple data lake: dump raw JSON or CSV files into S3, organized
by date. Life is good. Then someone says: *"We need to update customer records
when their credit score changes, not just append new ones."* So you try to
overwrite the files — but halfway through the write, your process crashes. Now
you have half-old, half-new data. No one knows what state the table is in.

Someone else says: *"The regulator wants to see what the data looked like last
Tuesday."* But you overwrote last Tuesday's data on Wednesday. It is gone.

A third person asks: *"Can we add a new column to the table without rewriting
every historical file?"* And you realize that your simple pile-of-files
architecture cannot do any of this reliably.

This is the **data lake reliability problem**, and by the mid-2010s it was so
common that it had a name: the **data swamp**. Companies would build data lakes
that quickly became unusable — data quality issues, no transactional guarantees,
no ability to go back in time.

### The Parquet interlude

Before we get to the solution, a quick word about **Parquet**. When people store
structured data on S3, they rarely use CSV or JSON (those formats are slow to
read and waste space). Instead, they use **Apache Parquet** — a columnar file
format designed for analytics.

Parquet stores data column-by-column rather than row-by-row, which makes
operations like "sum of the `amount` column" extremely fast because the engine
only reads that one column, skipping everything else. It also compresses very
well — a 1 GB CSV file might shrink to 100-200 MB as Parquet.

But Parquet is just a **file format**. It has no concept of transactions, no
way to update individual rows, no history. It is a building block, not a
complete solution.

### The table format revolution (2017-2020)

Three projects emerged almost simultaneously to solve the data swamp problem:

| Project        | Origin       | Year | Creator               |
|----------------|--------------|------|-----------------------|
| **Delta Lake** | Databricks   | 2017 | Originally internal, open-sourced 2019 |
| **Apache Hudi**| Uber         | 2017 | Built for Uber's ride data pipeline    |
| **Apache Iceberg** | Netflix  | 2018 | Built for Netflix's analytics platform |

All three solve the same fundamental problem: *how do you make a collection of
Parquet files on S3 behave like a proper database table?* They use different
internal designs, but the capabilities are strikingly similar: ACID
transactions, time travel, schema evolution, and efficient upserts.

We use **Delta Lake** in this project. The reason is primarily ecosystem fit —
Delta Lake has excellent Python support through the `deltalake` package
(powered by `delta-rs`, a Rust implementation), it integrates seamlessly with
Polars, and it does not require a Spark cluster to operate. Hudi and Iceberg
are both excellent; if you encounter them in other projects, the core concepts
transfer directly.

### How Delta Lake works (the 60-second version)

A Delta Lake table is just a directory on S3 containing:

```
s3://bucket/my-table/
    _delta_log/                   <- the magic
        00000000000000000000.json  <- commit #0
        00000000000000000001.json  <- commit #1
        00000000000000000002.json  <- commit #2
    part-00000-xxxx.parquet        <- data file
    part-00001-xxxx.parquet        <- data file
    part-00002-xxxx.parquet        <- data file (added in commit #2)
```

The **data files** are just regular Parquet files — nothing special about them.

The **`_delta_log`** directory is where the magic lives. Each JSON file in
this directory is a **commit** — a record of what changed. A commit might say:

- *"I added file `part-00000-xxxx.parquet` with 1000 rows."* (an append)
- *"I removed file `part-00000-xxxx.parquet` and added
  `part-00002-xxxx.parquet`."* (an update — the old file is logically replaced)

To read the current state of the table, Delta Lake replays the log: start from
commit #0, apply each subsequent commit, and you know exactly which Parquet
files are "active". This is the same idea behind database write-ahead logs and
Git's commit history.

### Why upsert (merge) is the killer feature

In fintech, data does not just arrive once and sit there forever. Customer
credit scores change monthly. Transaction statuses get updated (authorized →
settled → disputed). Fraud alerts get resolved.

This means you constantly need to **upsert**: update rows that already exist,
and insert rows that are new. With plain Parquet files, this is a nightmare —
you would have to read the entire table, merge the changes in memory, and write
the whole thing back. For a table with 100 million rows where you are updating
10,000, that is absurdly wasteful.

Delta Lake solves this with its **merge** operation:

```
"For each incoming row:
   - If a row with the same transaction_id already exists → update it
   - If no matching row exists → insert it"
```

Under the hood, Delta Lake only rewrites the Parquet files that contain
affected rows. The transaction log records which files were removed and which
were added. Other files remain untouched. This makes upsert practical even on
very large tables.

### The other superpowers

Beyond upsert, Delta Lake gives you:

- **ACID Transactions** — writes either fully succeed or fully fail. No more
  half-written tables after a crash.
- **Time Travel** — every commit is preserved in the log. You can read the
  table as it existed at any historical version. This is critical for financial
  auditing: *"Show me what the customer's credit profile looked like on
  January 15th."*
- **Schema Evolution** — you can add new columns to the table without rewriting
  historical data. Old rows simply get `null` for the new columns.
- **Vacuum** — periodically clean up old Parquet files that are no longer
  referenced by any recent commit, reclaiming storage space.

### Delta Lake in this project

In our fintech data lake:

- The **Silver layer** (cleaned, deduplicated transaction data) is stored as
  Delta Lake tables. Every ETL run writes a new commit — either appending new
  transactions or merging updates.
- The **Gold layer** (aggregated business metrics) is also Delta Lake, rebuilt
  daily from Silver.
- **Time travel** enables audit queries that regulators require: *"What did the
  data look like at the time of this compliance review?"*
- **Merge/upsert** handles the daily credit profile refresh: 200,000 customer
  records where only 5,000 changed — Delta Lake updates just those 5,000
  without touching the rest.

---

## 4. How the Three Fit Together

Now you can see the full picture:

```
                     ┌─────────────────────────────────┐
                     │           AWS S3                 │
                     │     (storage — the hard drive)   │
                     │                                  │
                     │   Parquet files + _delta_log     │
                     │   organized by key prefixes      │
                     │   (bronze/, silver/, gold/)       │
                     └──────────┬──────────┬────────────┘
                                │          │
                          write │          │ read
                                │          │
┌───────────────┐       ┌───────▼──────────▼───────┐
│    Raw Data   │       │        Polars            │
│  (JSON, CSV)  │──────►│  (compute — the engine)  │
│               │ read  │                          │
└───────────────┘       │  filter, deduplicate,    │
                        │  mask PII, aggregate     │
                        └───────────┬──────────────┘
                                    │
                                    │ DataFrame
                                    ▼
                        ┌───────────────────────┐
                        │     Delta Lake        │
                        │  (table format —      │
                        │   the rules)          │
                        │                       │
                        │  ACID, time travel,   │
                        │  merge/upsert,        │
                        │  schema evolution     │
                        └───────────────────────┘
```

- **S3** is where data physically lives — it is the storage.
- **Polars** is how you transform data — it is the compute.
- **Delta Lake** is the contract that keeps your data reliable — it is the
  table format.

Polars reads raw data, processes it, and hands the result to Delta Lake. Delta
Lake writes Parquet files to S3 and maintains a transaction log so that
everything stays consistent. When you need to read data back, Delta Lake
figures out which Parquet files are current, and Polars loads them into a
DataFrame.

In `s01_minimal_poc.py`, you will see this three-way collaboration in about 70
lines of code:

1. **Create** a Polars DataFrame (two rows: Alice and Bob)
2. **Write** it to S3 as a Delta Lake table (`df.write_delta(s3_uri)`)
3. **Read** it back (`pl.read_delta(s3_uri)`)
4. **Upsert** new data (update Bob → Bella, insert Cathy) using Delta Lake's
   merge API
5. **Read** again to verify the merge worked

That is the entire loop. Storage, compute, and reliable table format — working
together.

---

## Where to Go from Here

Now that you have the conceptual background, open
[s01_minimal_poc.py](./s01_minimal_poc.py) and read through the code. Every
operation maps directly to a concept described above.

After that, the remaining examples in this repository explore each capability
in depth:

| Example | What you will learn |
|---------|---------------------|
| [01-delta-read-write](../01-delta-read-write/) | The three write modes: overwrite, append, error |
| [02-polars-etl](../02-polars-etl/) | Column operations, deduplication, PII masking |
| [03-merge-upsert](../03-merge-upsert/) | The merge API in detail, with realistic data volumes |
| [04-time-travel](../04-time-travel/) | Reading historical versions, inspecting commit history |
| [05-vacuum-schema](../05-vacuum-schema/) | Cleaning up old files, adding columns safely |
| [06-full-etl](../06-full-etl/) | A complete Bronze → Silver pipeline tying it all together |
