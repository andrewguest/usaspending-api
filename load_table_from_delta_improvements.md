# load_table_from_delta Improvements

## Overview

The `load_table_from_delta` management command currently performs a **full table reload** every time it runs. For `transaction_search`, this means exporting every row from the Delta table and re-importing them all into Postgres, which is slow and resource-intensive on EMR.

The goal is to use Delta Lake's **Change Data Feed (CDF)** to perform incremental updates instead — only inserting, updating, or deleting the rows that have actually changed.

## Current Behavior of load_table_from_delta

Key files:
- **Main command**: `usaspending_api/etl/management/commands/load_table_from_delta.py`
- **CSV-to-Postgres helper**: `usaspending_api/common/csv_stream_s3_to_pg.py`
- **TABLE_SPEC for transaction_search**: `usaspending_api/etl/management/commands/load_query_to_delta.py` (line ~246)

### Step-by-step flow

1. Checks if the temp destination table (`temp.transaction_search_temp`) exists in Postgres
2. If `--recreate` flag is set and table exists, DROP it
3. If table doesn't exist, CREATE it (schema copied from `rpt.transaction_search`)
4. Read the **entire** Delta table (`rpt.transaction_search`) into a Spark DataFrame via `spark.table(delta_table)`
5. If table already existed, TRUNCATE it
6. Write the DataFrame as gzipped CSV files to S3
7. Spark executors each open a psycopg2 connection to Postgres, download their assigned CSV files from S3, and stream them in via SQL `COPY` commands (concurrency calibrated to Postgres `max_parallel_workers`)
8. Optionally reset Postgres sequences
9. Data lands in `temp.transaction_search_temp`, then a separate `copy_table_metadata` command handles swapping it into `rpt.transaction_search` to minimize downtime

### Table name resolution for transaction_search

- **Delta source**: `rpt.transaction_search`
- **Postgres reference table** (for schema): `rpt.transaction_search`
- **Postgres temp destination**: `temp.transaction_search_temp`

## Date Column Differences in transaction_search

The `transaction_search` table is a **denormalized** table that joins data from `transaction_normalized`, `awards`, `transaction_fpds`, `transaction_fabs`, and other sources. This is important for understanding the date fields.

### last_modified_date

- **Source**: FPDS `last_modified` / FABS `modified_at`
- **Meaning**: When the **source system** (FPDS or FABS) last changed the record
- **Set by**: External systems, not USAspending ETL

### update_date

- **Source**: `transaction_normalized.update_date` (Django `auto_now=True` field)
- **Meaning**: When the USAspending system last saved that specific **transaction** record
- **Set by**: Django ORM automatically on save

### etl_update_date

- **Source**: `GREATEST(transaction_normalized.update_date, awards.update_date)`
- **Meaning**: The most recent timestamp between when the **transaction** or its **parent Award** was last saved
- **Set by**: Calculated in the Spark query that builds `transaction_search`
- **Code location**: `usaspending_api/search/delta_models/dataframes/transaction_search.py` (lines 102-105)

### Why etl_update_date matters

Because `transaction_search` is denormalized, it contains flattened award-level fields (`award_category`, `total_obligation`, `fain`, `piid`, `date_signed`, etc.). When an Award is updated — e.g., a new transaction is added and `total_obligation` changes — every `transaction_search` row for that Award has **stale denormalized data**, even though the underlying transactions themselves weren't re-saved.

`etl_update_date` uses `GREATEST()` to capture this: if only the Award changed, all transactions on that Award get the same `etl_update_date` (the Award's `update_date`), correctly flagging them as needing an update.

This is why the Elasticsearch indexer and monthly delta file generation both use `etl_update_date` rather than plain `update_date`.

## Proposed Improvement: CDF-based Incremental Updates

Instead of a full reload, use Delta Lake's Change Data Feed to only replay the changes.

### Approach

1. Read CDF parquet files from the `_change_data/` folder in S3
2. Each CDF row has a `_change_type` column: `insert`, `update_preimage`, `update_postimage`, `delete`
3. Translate those into corresponding SQL operations against `rpt.transaction_search` in Postgres
4. Track the last-processed `_commit_version` in a Postgres table to avoid replaying changes

### Why DuckDB instead of PySpark

- Avoids the overhead of spinning up an EMR/Spark cluster just to read CDF parquet files
- DuckDB can read parquet from S3 directly and is much lighter weight
- DuckDB has an OpenSearch community extension, so the same CDF files could be used to update both Postgres and OpenSearch in one pass

### DuckDB Delta Extension Limitation

DuckDB's delta extension does **not** natively support `table_changes()` or CDF reading. That is a Spark SQL function. Two workarounds exist:

1. **Raw parquet read**: `read_parquet('s3://bucket/path/_change_data/*.parquet')` — but `_commit_version` and `_commit_timestamp` may not be present as columns. **Important caveat**: the number in the CDF filename (e.g., `cdc-00036-{uuid}.snappy.parquet`) is the **Spark partition ID**, not the commit version. Multiple files can share the same partition number but belong to different commits (written on different dates by different ETL runs). The actual commit version is stored in the Delta transaction log (`_delta_log/` JSON files), so using this option would require parsing `_delta_log/` to map CDF files to their commit versions — essentially reimplementing part of what `delta-rs` does.
2. **Use `delta-rs` Python library to read CDF, then hand to DuckDB** (recommended)

## Action Items

### Read CDF with delta-rs and query with DuckDB

Use the `delta-rs` Python library to read the Change Data Feed (which provides `_commit_version` and `_commit_timestamp` columns), then use DuckDB's query engine for processing and filtering:

```python
import duckdb
from deltalake import DeltaTable

dt = DeltaTable("s3://your-bucket/path/to/delta_table")
cdf_arrow = dt.load_cdf(
    starting_version=5,
    ending_version=10
)

con = duckdb.connect()
# DuckDB can query Arrow tables directly with zero-copy
con.sql("SELECT * FROM cdf_arrow WHERE _change_type = 'update_postimage'")
```

This approach:
- Gives `_commit_version` and `_commit_timestamp` columns for free (delta-rs is Delta-aware)
- Uses DuckDB's query engine for processing/filtering
- Can potentially leverage DuckDB's OpenSearch extension to update OpenSearch in the same pass

### Replay CDF changes to Postgres via DuckDB

Use DuckDB's `postgres` extension to replay the CDF inserts, updates, and deletes directly against the Postgres `transaction_search` table. Updates are handled as a delete-then-insert since `transaction_search` has many columns and building a full `UPDATE SET` clause would be unwieldy.

```python
import duckdb
from deltalake import DeltaTable

# 1. Load CDF from delta-rs
dt = DeltaTable("s3://your-bucket/path/to/delta_table")
cdf_arrow = dt.load_cdf(
    starting_version=last_processed_version + 1,
    ending_version=latest_version
)

con = duckdb.connect()

# 2. Attach Postgres via DuckDB's postgres extension
con.install_extension("postgres")
con.load_extension("postgres")
con.sql("""
    ATTACH 'dbname=usaspending host=localhost user=your_user password=your_pass'
    AS pg (TYPE POSTGRES)
""")

# 3. Register the Arrow table so DuckDB can query it
con.register("cdf", cdf_arrow)

# 4. Delete rows that were updated or deleted
#    (update_preimage/update_postimage both indicate the row changed;
#     we delete by the preimage/delete keys, then re-insert postimages)
con.sql("""
    DELETE FROM pg.rpt.transaction_search
    WHERE transaction_id IN (
        SELECT transaction_id FROM cdf
        WHERE _change_type IN ('update_preimage', 'delete')
    )
""")

# 5. Insert new rows and updated rows (postimage)
#    Exclude CDF metadata columns that don't belong in the target table
con.sql("""
    INSERT INTO pg.rpt.transaction_search BY NAME
    SELECT * EXCLUDE (merge_hash_key, _change_type, _commit_version, _commit_timestamp)
    FROM cdf
    WHERE _change_type IN ('insert', 'update_postimage')
""")

# 6. Update the tracking table with the latest processed version
con.sql(f"""
    UPDATE pg.public.cdf_tracking
    SET last_processed_version = {latest_version},
        processed_at = NOW()
    WHERE table_name = 'transaction_search'
""")
```

Notes:
- `transaction_id` is the primary key for `transaction_search` (via `transaction` column which is a OneToOneField to `TransactionNormalized`)
- The delete-then-insert pattern for updates avoids having to enumerate every column in an `UPDATE SET` clause
- The `EXCLUDE` clause strips the CDF metadata columns so the `INSERT` schema matches the Postgres table
- This should be wrapped in a transaction so the deletes and inserts are atomic
- The same CDF Arrow table (`cdf`) can also be used with DuckDB's OpenSearch extension in the same session
