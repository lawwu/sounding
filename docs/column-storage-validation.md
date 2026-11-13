# Column storage and inventory validation

Measured on 2026-11-13 using copies of the available local catalog. The source was
52,965,376 bytes, with 2,122 historical table rows and 509,713 historical column rows.
The larger three-tenant catalog mentioned in the proposal was not available here;
these figures must not be extrapolated to it without running the same scripts there.
No source catalog was migrated, compacted, pruned or replaced.

## Storage benchmark

All variants use the same source. Compacted sizes are directly comparable because
each was copied with DuckDB `COPY FROM DATABASE`.

| Layout | Compacted bytes | Migration | Simple column searches |
| --- | ---: | ---: | ---: |
| Original composite column key | 49,819,648 | — | 0.80 ms |
| Integer table snapshot key only | 39,333,888 | 0.23 s | 1.21 ms |
| Shared flattened definitions + sparse measurements | 5,255,168 | 3.89 s | 1.61 ms |

Shared storage is **89.5% smaller** than the compacted original. The integer-key-only
layout is 21.1% smaller. Neither figure is a promise about other catalogs.
The un-compacted shared file was 54,276,096 bytes: migration alone does not reclaim
all allocated blocks. No automatic compaction is introduced.

## Public reads and import writes

Compared v0.7.0 against the new catalog implementation on separate copies:

| Check | v0.7.0 | Shared columns |
| --- | ---: | ---: |
| Public search, mean of 20 calls | 5.45 ms | 5.79 ms |
| Offline metadata replay, 499 tables / 126,573 columns | 0.47 s | 0.52 s |
| Flattened definitions stored by replay | 126,573 | 1,106 |

Replay measures the local metadata-publishing portion of a harvest, not network
latency or end-to-end Google Cloud performance. These are single-machine samples,
not statistical performance guarantees. They show a small absolute write/search
cost in exchange for substantially lower duplicated storage.

Public `describe`, `search`, `browse`, and `stats` output was identical before and
after migration. Both output hashes were:

```text
689150c599be496492418f54dce91785465073a9d693ee16824fe2df9822371d
```

## Design and safeguards

Schema 7 assigns integer IDs to table snapshots and distinct flattened schemas.
Definitions preserve array order, descriptions, modes and policy tags (including
null versus empty lists). They have a database primary key on `(column_schema_id,
path)`. Sparse, table-specific costs have a primary key on `(table_snapshot_id,
path)`. Costs never participate in schema sharing. The read-only `columns` view
reconstructs per-snapshot records; writes use the underlying shared tables.

Migration runs transactionally, reads at most one table's schema at a time, rejects
orphaned columns, and preserves snapshots, raw schema metadata and row previews.
Tests cover rollback after a completed migration body, reopening, duplicate-path
rejection, column ordering, independent costs, and historic field equality.

Inventory is opt-in and requests only table identity/type/storage/timestamps using
partial `tables.get` responses. It skips schema/view SQL and auxiliary collection;
explicit hydration uses free metadata reads and checks for a concurrent dataset
refresh before saving. It does not execute SQL or read table rows. Missing fields
remain unknown. API design references: [tables.get](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/tables/get)
and [hidden dataset discovery](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/datasets/list).

Refresh tests cover persistent checkpoints, skipping completed work, resuming
failed work, temporary-error retries, include/exclude filters, explicit hidden
dataset inclusion, and bounded project concurrency. API behavior is tested offline
with mocked responses; no live multi-project harvest was run for this change.

## Reproduce

Close catalog writers first. Both commands refuse to reuse an output directory.
The benchmark copies its input under Sounding's catalog lock and refuses an active
WAL. The validation command should receive that scratch copy, not the live catalog.

```bash
uv run python scripts/benchmark_column_storage.py /path/catalog.duckdb /tmp/column-benchmark
uv run python scripts/validate_column_storage.py /tmp/column-benchmark/original.duckdb /tmp/column-validation
```

The benchmark expects a schema 3–6 catalog with the old physical `columns` table;
it compares candidate layouts without changing its schema version. The validation
script reads the checked-in `v0.7.0` catalog implementation from Git and upgrades
separate copies. It writes `measurements.json` in each output directory.
