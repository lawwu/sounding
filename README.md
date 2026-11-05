# Sounding

Sailors took a sounding to measure the water's depth before going in. Sounding does the same
for BigQuery: it shows what data exists, how big and fresh it is, how tables are shaped, and
what a query may cost, all without running a paid query.

It harvests metadata into a local DuckDB catalog using only BigQuery's free calls. People
read the catalog from the CLI and agents read it over MCP.

## The cost ladder

Sounding answers each question from the cheapest source that can.

| Source | What you get | Cost |
| --- | --- | --- |
| Local catalog | Everything already harvested, plus snapshot history | $0, no BigQuery call |
| REST `tables.list` / `tables.get` | Schema (nested), descriptions, row and byte counts, last modified, partitioning, clustering, view SQL | $0 (API quota) |
| `tabledata.list` | Sample rows (tables only, not views) | $0 |
| Dry run | Estimated bytes, reported referenced tables, and estimate limitations | $0 |
| INFORMATION_SCHEMA | Partitions, storage, job history | 10 MB minimum for on-demand queries; capacity queries consume slots; never cached: **not harvested** (the cost guard can still dry-run it for free) |
| Sampled profiles | `TABLESAMPLE` + `APPROX_*` | Billed: **not used yet** (planned as opt-in, capped, approved) |

Two free tricks come from dry runs:
- **Per-column cost:** a dry run of `SELECT col FROM t` gives the bytes that column scans.
- **View lineage:** a dry run of a view lists the base tables it reads.

Dry runs themselves are free; a zero-byte or incomplete dry run does **not** prove
the eventual query would be free. Sounding reports unknown estimates as `null`,
including scripts, dynamic SQL, zero/missing bytes, unknown/lower-bound estimates,
and referenced routines. It never falls back to executing SQL. A SELECT that reports
0 bytes is explained rather than priced: it may read nothing, or it may read external
or row-level-secured tables a dry run can't cost. If BigQuery's response ever lacks the
estimate-accuracy field, every estimate turns unknown and says why. SQL that queries
INFORMATION_SCHEMA is dry-run like any other SQL; the harvester itself never queries it.

Prices cover scan costs at the **US on-demand list rate of $6.25/TiB**, before the
monthly free tier and discounts; they do not cover other services or regional rates.
They include MB rounding and the 10 MB minimum per referenced table. Multiple-table
queries return a price range because dry runs do not expose per-table billable bytes;
an upper-bound byte estimate produces only an upper price bound. If the table list
may be incomplete, there is no price quote. Estimates are not spending caps.

Google documents [free dry runs and their limitations](https://docs.cloud.google.com/bigquery/docs/running-queries#dry_run),
[script dry-run limitations](https://docs.cloud.google.com/bigquery/docs/multi-statement-queries#dry-run_a_multi-statement_query),
[free previews](https://docs.cloud.google.com/bigquery/docs/best-practices-costs#avoid_running_queries_to_explore_table_data),
[the restriction on previewing logical views](https://docs.cloud.google.com/bigquery/docs/views-intro#logical_views_limitations),
and [on-demand pricing](https://cloud.google.com/bigquery/pricing#on_demand_pricing).

## Use

```bash
uv sync
gcloud auth application-default login         # once
export SOUNDING_JOB_PROJECT=<your-project>    # optional; dry runs are attributed here, never billed

uv run sounding harvest bigquery-public-data.thelook_ecommerce bigquery-public-data.noaa_gsod
uv run sounding show order                    # search the catalog
uv run sounding describe bigquery-public-data.thelook_ecommerce.orders
uv run sounding column-costs bigquery-public-data.thelook_ecommerce.order_items
uv run sounding estimate 'SELECT * FROM `bigquery-public-data.stackoverflow.posts_questions`'
uv run sounding preview bigquery-public-data.thelook_ecommerce.orders
```

The catalog lives at `~/.sounding/catalog.duckdb`. Set `SOUNDING_CATALOG` to put it elsewhere.

Each dataset publishes a complete snapshot in one transaction. A failed dataset
crawl leaves its previous snapshot current; earlier datasets in the same command
remain committed. Dataset aliases are normalized to `project.dataset`. Snapshot IDs
are unique and allocated during the write transaction. Timestamps are stored as
absolute instants and returned in UTC with an explicit offset.

All Sounding processes coordinate short catalog operations through a `.lock`
sidecar file. Harvests and column-cost calculations release the catalog before
network calls, so UI and MCP reads can continue during those calls. Operations may
wait up to 10 seconds for another local operation, then return a retryable error.
Keep the catalog on a local filesystem; external DuckDB programs do not participate
in this coordination and must close their connections before Sounding opens it.

### Catalog upgrades

This version uses catalog schema 2. The catalog is a cache that a harvest can rebuild,
and older schemas can't be converted reliably (timezone-less timestamps, alias-keyed
snapshots). So the first time any Sounding command opens an older catalog, it renames
it next to itself (for example `catalog.v1-20261105T082205.duckdb`), starts an empty
catalog at the same path, and prints a notice. Re-run `sounding harvest` for your
datasets. Nothing is deleted, and no setting needs to change. A catalog from a *newer*
Sounding is refused and left untouched.

### Search UI

```bash
uv run sounding ui --open      # http://127.0.0.1:8765, localhost only
```

The page has live search over table names, column paths and descriptions, and reports which
columns matched. Pick a table to see its size, freshness, full-scan cost, and nested schema.
Each table page has three free actions:
- **Column costs:** one dry run per column, drawn as bars.
- **Preview:** 10 rows via `tabledata.list`.
- **Cost guard:** paste SQL and see the bytes and dollars it would scan, without running it.

Search and table pages never call BigQuery.

The UI validates Host and Origin, rejects cross-site requests, and requires a random
per-server token on API requests. All three BigQuery actions are JSON-only POSTs,
including preview. Search pages contain at most 100 tables, with a bounded sample
and total count of matched column paths; **Load more** retrieves the next page.
Search treats `%`, `_`, and backslashes literally. Table responses and actions are
discarded when the selection changes, and catalog text is escaped before display.

### Agents (MCP)

```json
{ "mcpServers": { "sounding": { "command": "uv", "args": ["run", "--directory", "/path/to/sounding", "sounding", "mcp"] } } }
```

| Tool | Calls BigQuery? |
| --- | --- |
| `search_catalog`, `describe_table` | Never |
| `preview_rows`, `estimate_query_cost` | Yes, free calls only |

`search_catalog(q="", limit=50, offset=0)` returns `{items, next_offset}`; pass
`next_offset` as `offset` to continue. Limits are 1–100 tables and 20 matching paths
per table; `matched_column_count` retains the full count. Search descriptions are
excerpts; `describe_table` returns the full schema and descriptions. Paging reflects
the latest catalog at each request, so a concurrent harvest can change page contents.
`preview_rows` accepts 1–50 rows and rejects views/external tables without querying
them. Tool failures use MCP protocol errors, with credential and filesystem details
removed. For estimates, agents must inspect `estimate_status`, `warnings`,
`usd_on_demand_range`, and `referenced_tables_complete`, rather than interpreting
unknown values as zero.

## What the first run showed (2026-11-04)

- Five public datasets (thelook_ecommerce, google_analytics_sample, stackoverflow,
  github_repos, noaa_gsod) came to 499 tables and 126k column paths. The harvest took
  **23 s wall time with 5.7 s of CPU**, almost all of it network wait.
- A row-at-a-time DuckDB `executemany` first took 133 s. Bulk Arrow inserts fixed it.
- After dozens of dry runs, the job project listed no jobs.
- Example costs: one `github_repos.commits` aggregate is a 97 GB dry run (about $0.55 on
  demand), and a `SELECT *` on `stackoverflow.posts_questions` is 40 GB.

**Why Python:** the harvester is I/O-bound. Google ships a first-party Python BigQuery client,
and the Python MCP SDK is the reference implementation.

## Roadmap

1. ✅ Free-only harvester and catalog-backed MCP server
2. Snapshot diffs: row-count growth, schema drift, late tables
3. Table families: collapse date shards (`ga_sessions_*`, `gsod*`)
4. Join inference from column name, type and value overlap
5. ✅ Search UI (`sounding ui`) with column costs, preview and a dry-run cost guard; next: an overview page
6. Opt-in billed profiling behind a byte cap and approval, with results cached in the catalog

## Development

```bash
uv run pytest
```

The suite is offline and fails on unmocked BigQuery calls. Install Node.js 22+ for
the JavaScript security, pagination and stale-response tests (run by pytest;
skipped with an explicit reason if Node is missing). CI installs Node and runs the
full suite. Tests also cover process/thread locks, rollback, Arrow field mapping,
legacy catalogs, UTC round trips, price minimums, HTTP security, and MCP over stdio.
