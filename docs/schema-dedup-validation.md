# Schema deduplication validation

Tested against a scratch copy of the available local schema-5 catalog. The original
catalog was copied under its advisory lock and was never opened by the new code.
This is a smaller catalog than the 5 GB example in the request.

| Measurement | Before | Migrated | Migrated and compacted copy |
| --- | ---: | ---: | ---: |
| Physical file bytes | 52,965,376 | 52,965,376 | 50,343,936 |
| Per-table resource JSON bytes | 1,024,345 | 1,014,809 | 1,014,809 |
| Inline schema JSON bytes | 8,476 | 0 | 0 |
| Shared schema JSON bytes | 0 | 4,238 | 4,238 |
| Table rows (including history) | 2,122 | 2,122 | 2,122 |
| Distinct stored schemas | — | 7 | 7 |

The concatenated public table descriptions have the same SHA-256 before migration,
after migration and after compaction:
`dcadb9ba3e7582f7053cc9aa9ad9da40e7e174597de8c9966f311fd781fad649`.
Older snapshots in this local catalog mostly predate full-schema retention, which
limits its schema savings. No schemas were invented for those rows.

Compaction used DuckDB's `COPY FROM DATABASE` into a fresh scratch file; physical
savings also reflect previously unused space and are not solely schema deduplication.
No snapshot pruning or preview deletion occurred.

The only production reader that consumed `resource["schema"]` was
`catalog.describe`. It now resolves `schema_hash` before constructing field metadata
and removes the internal hash from its response. CLI, MCP and HTTP/UI delegate to
that reader and keep their existing public output. The Arrow table write schema now
includes the hash; schemas and table rows are committed together. `bq.table_record`
still supplies the full schema in memory, and the catalog writer separates it before
storage. Other raw-resource fields are unchanged.

Regression fixtures cover byte-identical public read results, all historical table
fields (including saved previews), canonical object-key ordering, preserved column
order, distinct schemas, missing/null/empty schemas, bounded migration batches,
reopening/rerunning, and transactional rollback on failure.
