# Incremental Load from Delta via Change Data Feed

## Overview

The existing `load_table_from_delta` management command performs a full reload of the target Postgres table every time it runs. For a table like `rpt.transaction_search` (253M+ rows), most nights change fewer than 500K rows, so exporting and re-importing the entire table is wasteful in terms of Spark compute, S3 I/O, and Postgres write amplification.

This document describes the new **incremental** flow that replays only the rows that changed, using Delta Lake's Change Data Feed (CDF) as the source of truth.

The full-reload command is preserved unchanged. The incremental flow is introduced as a separate, parallel code path so operators can continue to use full reloads when necessary (schema changes, recovery, backfills) while defaulting to incremental for nightly runs.

## Motivation Recap

Full reload problems:

- Reads every row from the Delta table on S3 into Spark every run, regardless of how many actually changed.
- Writes every row to gzipped CSV files back to S3, then bulk-COPYs them into Postgres.
- Requires an EMR/Spark cluster to run.
- Produces enormous WAL volume in Postgres on the swap + write path.

Incremental goals:

- Read only the Delta commits that landed since the last run.
- Write only the affected rows to Postgres.
- Eliminate the Spark dependency on the read path (delta-rs reads Delta directly from Python).
- Keep the full-reload command fully functional and untouched for the cases that need it.

## High-Level Architecture

The incremental flow is composed of three new pieces plus a new tracking table:

| Piece | Location | Purpose |
|---|---|---|
| `CDFVersionTracking` model | `usaspending_api/etl/models.py` | Stores the last-processed Delta CDF version per logical table |
| `cdf_reader` module | `usaspending_api/etl/cdf_reader.py` | Reads CDF from Delta via `delta-rs`, splits change types |
| `cdf_apply` module | `usaspending_api/etl/cdf_apply.py` | Converts PyArrow data into Postgres COPY-compatible CSV buffers |
| `load_table_from_delta_incremental` command | `usaspending_api/etl/management/commands/load_table_from_delta_incremental.py` | Orchestrates the end-to-end flow |

The read path uses the `deltalake` Python package (delta-rs). It does not require Spark. The command uses Django's database connection so the data-apply step and the tracking-update step share a single atomic transaction.

## Tracking: `cdf_version_tracking`

A new table tracks the last-processed CDF version per logical table:

| Column | Type | Purpose |
|---|---|---|
| `table_name` | TEXT PRIMARY KEY | Logical table name, matches a `TABLE_SPEC` key (e.g. `"transaction_search"`) |
| `last_processed_version` | BIGINT | Delta commit version last successfully applied |
| `last_commit_timestamp` | TIMESTAMP | `_commit_timestamp` of the last commit in the processed range, for auditing |
| `updated_at` | TIMESTAMP | Auto-updated when the row is written |

Why a dedicated tracking table (rather than reusing `external_data_load_date`): CDF is naturally versioned by integer commit number, not by datetime. Storing the commit version directly avoids lossy datetime-to-version conversions and keeps semantics clean for future tables using the same incremental pattern.

The tracking row is **not seeded automatically**. An operator must seed it once after the first full reload so subsequent incremental runs know where to pick up. The incremental command exits with a warning if no tracking row exists.

## Flow Overview

```
┌─────────────────────────────────────────────────────────────────┐
│ load_table_from_delta_incremental --delta-table transaction_search
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
        ┌───────────────────────────────────────────────┐
        │ 1. Look up TABLE_SPEC[delta_table]            │
        │    - primary_key_column, destination_database │
        │    - swap_schema/table, column_names          │
        └───────────────────────────────────────────────┘
                                │
                                ▼
        ┌───────────────────────────────────────────────┐
        │ 2. get_last_processed_version(delta_table)    │
        │    - exit with warning if None (first run)    │
        └───────────────────────────────────────────────┘
                                │
                                ▼
        ┌───────────────────────────────────────────────┐
        │ 3. Build s3:// URI from CONFIG + spec         │
        │    read_cdf_changes(uri, starting_version)    │
        │    - exit cleanly if nothing new              │
        └───────────────────────────────────────────────┘
                                │
                                ▼
        ┌───────────────────────────────────────────────┐
        │ 4. split_cdf_by_change_type(cdf, pk)          │
        │    → (deleted_ids, upsert_rows)               │
        └───────────────────────────────────────────────┘
                                │
                                ▼
        ┌───────────────────────────────────────────────┐
        │ ATOMIC TRANSACTION:                           │
        │  5. Drop + recreate staging tables            │
        │     - temp.{t}_cdf_deletes (pk col only)      │
        │     - temp.{t}_cdf_upserts (LIKE live table)  │
        │  6. COPY deleted_ids  → deletes staging       │
        │  7. COPY upsert_rows  → upserts staging       │
        │  8. DELETE FROM live USING deletes staging    │
        │  9. INSERT INTO live SELECT FROM upserts stg. │
        │ 10. update_last_processed_version(...)        │
        └───────────────────────────────────────────────┘
                                │
                                ▼
        ┌───────────────────────────────────────────────┐
        │ 11. (optional) --cleanup-staging              │
        │     → DROP staging tables                     │
        └───────────────────────────────────────────────┘
```

## Step-by-Step Detail

### 1. Spec Resolution

The command accepts `--delta-table` (the key into `TABLE_SPEC`) and resolves:

- `primary_key_column` — new required field for incremental (e.g. `"transaction_id"`). If missing, the command fails fast with a clear error.
- `destination_database` + `delta-table` key — used to build the `s3://` URI for the Delta table.
- `swap_schema` + `swap_table` — identify the live Postgres table (`rpt.transaction_search`).
- `column_names` — canonical column order used for CSV writing and the `INSERT ... SELECT`.

### 2. Last Processed Version Lookup

`get_last_processed_version(table_name)` reads `CDFVersionTracking` and returns the stored integer, or `None` if no record exists. On `None`, the command logs a warning and exits — **the incremental flow never seeds itself**.

### 3. CDF Read (delta-rs, not Spark)

The S3 URI is built via `build_delta_table_s3_uri(destination_database, destination_table)`, which reuses `CONFIG.SPARK_S3_BUCKET` and `CONFIG.DELTA_LAKE_S3_PATH`. The same config drives the full-reload flow, so both paths stay environment-consistent.

`read_cdf_changes(uri, starting_version)`:

- Opens a `DeltaTable` with the right `storage_options` (AWS default credential chain when `USE_AWS=True`, else explicit keys + endpoint for local/sandbox).
- Queries `dt.version()` to find the latest committed version.
- If `starting_version >= latest_version`, returns `None` (nothing new).
- Calls `dt.load_cdf(starting_version=last+1, ending_version=latest).read_all()` to get a PyArrow `Table`.
- Returns a `CDFChangeSet` with the Arrow table, `latest_version`, and `max(_commit_timestamp)`.

### 4. Split by Change Type

`split_cdf_by_change_type(cdf, pk_column)` returns `(deleted_ids, upsert_rows)`:

- **`deleted_ids`**: distinct PKs across every row in the CDF, regardless of change type. Rationale: we use a delete-then-insert pattern for updates, and we want a retry after a partial failure to be idempotent. Deleting a PK that doesn't exist in Postgres is a no-op.
- **`upsert_rows`**: rows where the **latest** (by `_commit_version`) non-preimage change type is `insert` or `update_postimage`. Implementation:
  1. Drop rows with `_change_type == update_preimage` (redundant — paired with postimage).
  2. Per PK, keep the row with the largest `_commit_version`. Handled via a small pandas round-trip because PyArrow does not have a clean "keep-last-per-group" primitive.
  3. Drop anything whose final change type is `delete` (handles the insert-then-delete-within-window case).
  4. Strip the CDF metadata columns (`_change_type`, `_commit_version`, `_commit_timestamp`) so the Arrow schema matches the target Postgres schema.

### 5. Staging Tables

Two persistent tables in the `temp` schema:

- `temp.{swap_table}_cdf_deletes` — created with `CREATE TABLE ... AS SELECT {pk_column} FROM {live_table} WITH NO DATA`. Single-column staging table, type inferred from the live table's PK column.
- `temp.{swap_table}_cdf_upserts` — created with `CREATE TABLE ... (LIKE {live_table} INCLUDING DEFAULTS)`. Matches the live schema without inheriting partitioning, identity, or generated-column definitions.

Both tables are dropped and recreated at the **start** of each run. That guarantees a clean slate and naturally picks up any schema changes on the live table. They are left populated after a successful apply for auditability — operators can inspect `temp.transaction_search_cdf_deletes` and `temp.transaction_search_cdf_upserts` to see what was just applied. The `--cleanup-staging` flag drops them after success if the operator prefers.

### 6–7. COPY from In-Memory Buffers

The `cdf_apply` module turns Python/Arrow data into CSV buffers the Postgres COPY command can consume:

- `ids_to_csv_buffer(ids)` writes a single-column CSV for the deletes staging table.
- `arrow_to_pg_csv_buffer(table, column_order)` reorders the Arrow table to the canonical column order, renders list columns as Postgres array literals (`{"a","b"}`) with proper escaping and NULL handling, and writes CSV via pandas with `na_rep=""` and `QUOTE_MINIMAL`.

Data goes directly from memory into Postgres via `cursor.copy_expert(...)`. No S3 round-trip, no filesystem temp files, no Spark cluster.

### 8–9. Apply

```sql
DELETE FROM {live_table} AS tgt
USING {deletes_staging} AS stg
WHERE tgt.{pk_column} = stg.{pk_column};

INSERT INTO {live_table} ({cols})
SELECT {cols} FROM {upserts_staging};
```

The `DELETE ... USING` form lets Postgres plan a hash join against the staging table, which scales far better than `WHERE pk IN (...)` for tens of thousands of IDs.

### 10. Tracking Update (Same Transaction)

`update_last_processed_version(table_name, version, commit_timestamp)` is an `update_or_create` via the Django ORM. Because it runs inside the same `transaction.atomic()` block that wrapped the DELETE/INSERT, the tracking update commits only if the data apply succeeds. If anything raises, the entire transaction rolls back — live table untouched, tracking unchanged, safe to retry.

## Atomicity & Failure Modes

Everything from staging-table DDL through the tracking update runs inside a single `transaction.atomic()` block on Django's database connection. Postgres supports DDL inside transactions, so a mid-flow crash rolls back cleanly, including the staging `CREATE TABLE` statements themselves.

Failure scenarios:

- **CDF read fails**: no transaction has started yet; nothing to roll back.
- **CSV construction fails**: same — we haven't touched Postgres.
- **Staging COPY fails**: transaction rolls back; live table untouched; tracking unchanged. Retry is safe.
- **Apply DELETE/INSERT fails**: rolls back; same as above.
- **Tracking update fails** (unlikely, since it's a single row upsert): rolls back the data apply.

Because every retry drops and recreates the staging tables, there is no risk of stale staging data contaminating a subsequent run.

## Deduplication Semantics — Why This Is Correct

The CDF window we pull can span many commits, and the same PK can appear multiple times with different change types. The splitter's rules guarantee correct final state:

| Scenario | CDF entries | deleted_ids | upsert_rows | Final DB state |
|---|---|---|---|---|
| Pure insert | `insert X` | `{X}` | `X` | X present (inserted) |
| Pure delete | `delete X` | `{X}` | — | X absent (deleted) |
| Single update | `preimage X`, `postimage X'` | `{X}` | `X'` | X present with new values |
| Insert then update | `insert X`, `preimage X`, `postimage X'` | `{X}` | `X'` | X present with final values |
| Insert then delete | `insert X`, `delete X` | `{X}` | — | X absent |
| Deleted then re-inserted | `delete X`, `insert X''` | `{X}` | `X''` | X present with new values |
| Updated then deleted | `preimage X`, `postimage X'`, `delete X` | `{X}` | — | X absent |

The `deleted_ids` list deliberately includes every PK present in the CDF (including rows that also appear in `upsert_rows`). This preserves the simple DELETE-then-INSERT pattern without needing ON CONFLICT clauses or enumerated UPDATE SET statements, and makes retries idempotent.

## First-Run & Seeding

Because the incremental command refuses to run without a `CDFVersionTracking` row, the first-run protocol is:

1. Run a full reload via the existing `load_table_from_delta` command to get the Postgres table aligned with the Delta table.
2. Identify the Delta commit version that was current at the time of that full reload (e.g., `DeltaTable(uri).version()`).
3. Insert a `CDFVersionTracking` row with `table_name`, that `last_processed_version`, and a sensible `last_commit_timestamp`.
4. From that point forward, scheduled incremental runs will pick up where the full reload left off.

If the tracking row is ever lost or falls too far behind, the fallback is always a full reload via `load_table_from_delta` followed by re-seeding.

## Non-Disruption Guarantees

- `load_table_from_delta` is unchanged. No shared mutable state.
- No changes to `copy_table_metadata`, `csv_stream_s3_to_pg`, or Spark helpers.
- The only TABLE_SPEC change is an additive optional field: `primary_key_column`. Tables without it are unaffected.
- The `CDFVersionTracking` model is a new table in a new Django app migration (`etl/0006_create_cdf_version_tracking.py`); nothing existing was mutated.

## Dependencies

- `deltalake==1.5.x` added to `pyproject.toml` (via `uv add deltalake`). This is independent of `delta-spark`, which stays in place for the full-reload command.
- `pandas` is already a transitive dependency of the project.

## Operational Notes

- The incremental command uses Django's database connection. It does not spin up Spark, so it can run on a much smaller instance than the full-reload command.
- For very large CDF windows (catching up after an outage), memory is the main concern — the Arrow table is materialized in memory. If a window is too large, run multiple incremental passes with narrower version ranges, or fall back to a full reload.
- Staging tables in the `temp` schema persist across runs for auditability. They can be inspected directly (`SELECT * FROM temp.transaction_search_cdf_upserts LIMIT ...`) or dropped ad hoc.

## Future Work

- Enable the incremental flow for other tables in `TABLE_SPEC` by adding `primary_key_column` entries.
- Consider a pure-Arrow (or DuckDB-backed) dedup implementation to avoid the pandas round-trip if memory becomes a concern.
- Integrate with the OpenSearch indexer so the same CDF Arrow table can drive both Postgres and OpenSearch updates in one pass.
- Optional `--end-version` argument to process CDF in chunks for catch-up scenarios.
- Metrics/alerts on version lag (`latest_version - last_processed_version`) so a stuck incremental is visible.
