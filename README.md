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
| REST `tables.list` / `tables.get` | Schema (nested), descriptions, row and byte counts, last modified, partitioning, clustering, view SQL, plus everything else in the [table resource](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/tables) (see below) | $0 (API quota) |
| REST `datasets.get` | Location, description, labels, default expirations, storage billing model, time-travel window | $0 (API quota) |
| REST `routines.list` / `routines.get` | Functions and procedures: type, language, signature, return type, description, body | $0 (API quota) |
| `tabledata.list` | Sample rows (tables only, not views) | $0 |
| Dry run | Estimated bytes, reported referenced tables, and estimate limitations | $0 |
| REST `jobs.list` | Jobs this identity ran in the job project, with what BigQuery billed (`sounding cost-report`) | $0 (API quota) |
| INFORMATION_SCHEMA | Per-partition details, job history | 10 MB minimum for on-demand queries; capacity queries consume slots; never cached: **not harvested** (the cost guard can still dry-run it for free) |
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
uv run sounding cost-report --hours 24        # what this identity was billed, with evidence
```

The catalog lives at `~/.sounding/catalog.duckdb`. Set `SOUNDING_CATALOG` to put it elsewhere.

Harvest crawls up to 4 datasets at once, each fetching up to 16 tables or routines
at once, and column costs run up to 16 dry runs at once. In a terminal both show a
progress bar on stderr; its total grows as list calls reveal how many tables there
are. Piped output gets plain per-dataset lines.

Each dataset publishes a complete snapshot in one transaction. A failed dataset
crawl leaves its previous snapshot current; the other datasets in the same command
still finish and stay committed, and the command then exits with the first error. Dataset aliases are normalized to `project.dataset`. Snapshot IDs
are unique and allocated during the write transaction. Timestamps are stored as
absolute instants and returned in UTC with an explicit offset.

All Sounding processes coordinate short catalog operations through a `.lock`
sidecar file. Harvests and column-cost calculations release the catalog before
network calls, so UI and MCP reads can continue during those calls. Operations may
wait up to 10 seconds for another local operation, then return a retryable error.
Keep the catalog on a local filesystem; external DuckDB programs do not participate
in this coordination and must close their connections before Sounding opens it.

### Cost receipts

Every action that calls BigQuery returns a `receipt` showing it cost nothing, and
`sounding cost-report` shows the jobs this identity ran in the job project, with what was billed.

- **Dry runs** (estimate, cost guard, column costs): the receipt copies three fields
  from BigQuery's own response. `dry_run_confirmed` is `configuration.dryRun`,
  `billed_bytes` is `statistics.query.totalBytesBilled` (0), and `job_id` is null
  because no job is created. If any of them says otherwise, `free` is false and the
  estimate carries a warning.
- **Metadata calls and previews** (harvest, `tabledata.list`): these are not jobs, and
  BigQuery returns no billing fields for them. Their receipt names the calls made and
  points to [Google's list of free operations](https://cloud.google.com/bigquery/pricing#free).
- **`sounding cost-report`** lists the jobs this identity ran in the job project over
  a window: 24 hours by default, up to the 180 days BigQuery keeps. It uses the free
  `jobs.list` call and shows each job's billed bytes and list price.
  - None of Sounding's calls creates a job, so a window in which you only used
    Sounding shows **0 jobs**. That is evidence nothing in it could have been billed
    to that project.
  - A query run by any other tool in that project appears with its own billed bytes.
  - It does not show jobs billed to other projects or by other identities, and it
    prices at list rate, so your Cloud Billing report remains the final word.

### What a harvest keeps

Each `tables.get` response is kept whole (minus the schema, which is stored per column),
so nothing BigQuery returns is thrown away. These fields are also pulled out for
search, `describe` and the UI, at no extra BigQuery cost:

- **Declared keys:** primary and foreign keys. BigQuery does not enforce them, so they
  are join hints, not proof.
- **Partition guidance:** whether a partition filter is required, partition expiry,
  and the partition count.
- **Storage:** active and long-term bytes, logical and physical, and time-travel bytes.
- **Governance and lifecycle:** labels, column policy tags, expiry time, the encryption
  key, and the source table of a snapshot or clone.
- **External and materialized tables:** the external source settings, and a
  materialized view's query, refresh settings and status.

A missing field is stored as unknown (`null`), never as zero or false. `describe`
adds plain-language `notes` for things to know before querying, such as a required
partition filter or policy-tagged columns.

Each harvest also calls `datasets.get` once and harvests the dataset's routines. The
dataset's `access` list (who can read it, by email) is never stored. If routines
can't be listed, the tables are still saved and the dataset shows `routines_status: denied`.

```bash
uv run sounding dataset bigquery-public-data.persistent_udfs   # settings and routine signatures
uv run sounding routines bignumber                             # search functions and procedures
uv run sounding routine bigquery-public-data.persistent_udfs.bignumber_add
```

### Beyond BigQuery: builders and discovery (prototype)

Two more free sources, read over Google's admin APIs. Neither is a BigQuery job or
reads table data.

```bash
sounding harvest-builders my-project      # scheduled queries, transfers, Dataform
sounding builders orders                  # search them by name, target, dependency or SQL
sounding discover projects/my-project     # Cloud Asset Inventory (also folders/N, organizations/N)
sounding assets --type bucket
```

- **Builders** answer "how is this table made?". From the BigQuery Data Transfer API,
  each scheduled query's SQL and destination, and each other transfer's data source
  (Google Ads, S3 and so on). When a scheduled query has no destination setting, the
  table its `CREATE TABLE`, `INSERT` or `MERGE` writes is taken from the SQL, and
  `target_from` says so. From Dataform, the newest compilation result of each
  repository: every table, view, incremental table, operation and assertion, with its
  SQL, dependencies, description and column descriptions. `sounding describe` and the
  MCP `describe_table` then include `built_by` (with the SQL) and `read_by`.
- **Discovery** lists the resources that hold or move data across a project, folder
  or organization: buckets, Spanner, Cloud SQL, AlloyDB, Bigtable, Firestore, Pub/Sub
  topics, Vertex AI datasets and models, Dataform, Composer and Looker. Only names,
  types, locations, labels, descriptions and timestamps are requested; IAM policies and
  `additionalAttributes` are never read. `--type` overrides the type list.
- Owner emails on transfer configs (`ownerInfo`, `userId`) are dropped before
  anything is stored, the same rule as dataset access lists.
- Each source and location is saved as its own run and replaces only its previous run,
  so a disabled API (each needs enabling: `bigquerydatatransfer.googleapis.com`,
  `dataform.googleapis.com`, `cloudasset.googleapis.com`) skips that source and the
  rest still land.
- Not yet: the UI, scheduled queries' *input* tables (a free dry run of their SQL would
  give them), and Dataform release configurations (production may run an older
  compilation than the newest one).

### Catalog upgrades

This version uses catalog schema 4 (schema 3 plus the builders and discovery tables).
A schema 2 or 3 catalog is upgraded in place, one step at a time, the first
time any Sounding command opens it: the new fields are added, existing snapshots show
them as unknown until the next harvest, and nothing needs re-harvesting. The upgrade
runs in one transaction, so a failure leaves the catalog as it was.

Schema 1 catalogs (Sounding before 0.1.0) can't be converted reliably (timezone-less
timestamps, alias-keyed snapshots). The first time Sounding opens one, it renames it
next to itself (for example `catalog.v1-20261105T082205.duckdb`), starts an empty
catalog at the same path, and prints a notice. Re-run `sounding harvest` for your
datasets. Nothing is deleted. A catalog from a *newer* Sounding is refused and left
untouched.

### Search UI

```bash
uv run sounding ui --open      # http://127.0.0.1:8765, localhost only
```

The page has live search over table names, column paths and descriptions, and reports which
columns matched. Pick a table to see its size, freshness, full-scan cost, and nested schema,
plus notes, labels, declared keys, storage, and which columns have policy tags.
The **Cost report** button lists jobs run in the job project over a chosen window, with their billed bytes. Each table page has three free actions, and each one shows its receipt:
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
| `search_catalog`, `describe_table`, `describe_dataset`, `search_routines`, `describe_routine` | Never |
| `preview_rows`, `estimate_query_cost`, `cost_report` | Yes, free calls only; each result carries a receipt or job list |

`search_catalog(q="", limit=50, offset=0)` returns `{items, next_offset}`; pass
`next_offset` as `offset` to continue. Limits are 1–100 tables and 20 matching paths
per table; `matched_column_count` retains the full count. Search descriptions are
excerpts; `describe_table` returns the full schema and descriptions. Paging reflects
the latest catalog at each request, so a concurrent harvest can change page contents.
`search_routines` pages the same way and returns 1000-character body excerpts;
`describe_routine` returns the whole body, and `describe_dataset` lists signatures only.
`preview_rows` returns `{rows, receipt}`, accepts 1–50 rows and rejects views/external tables without querying
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
7. ✅ Keep the whole table resource (keys, partition guidance, storage, labels, policy tags), dataset settings and routines; next: BigQuery ML models, row-access policy metadata, and opt-in job history
8. Prototype: builders (scheduled queries, transfers, Dataform) and Cloud Asset discovery; next: Looker LookML

## Stability

Sounding follows [semantic versioning](https://semver.org). While the version is 0.x,
any minor release may break things, and its release notes say so when it does. From
1.0.0, these are the public interface, and breaking any of them needs a major version:

- **CLI:** command names, flags, and the JSON that commands print. New fields may be
  added to that JSON at any time, so parse it by key rather than by position.
- **MCP tools:** tool names, their inputs, and their result shapes, with the same
  allowance for added fields.
- **The catalog file:** every 1.x release opens and upgrades every 1.x catalog in place.
  Query it through Sounding; its table layout is not part of the interface.
- **The cost contract:** Sounding makes only free BigQuery calls, every call that reaches
  BigQuery reports a receipt, and an unknown cost is reported as unknown, never as zero.

**The Python modules are internal.** `sounding.bq`, `sounding.catalog`, `sounding.ui`,
`sounding.mcp_server` and the rest may change in any release, including patch releases,
without notice. Use the CLI or the MCP server instead. If you need to drive Sounding from
Python, open an issue: a small public API can be designed for that.

## Development

```bash
uv run pytest
```

The suite is offline and fails on unmocked BigQuery calls. Install Node.js 24+ for
the JavaScript security, pagination and stale-response tests (run by pytest;
skipped with an explicit reason if Node is missing). CI installs Node and runs the
full suite. Tests also cover process/thread locks, rollback, Arrow field mapping,
legacy catalogs, UTC round trips, price minimums, HTTP security, and MCP over stdio.

## License

MIT. See [LICENSE](LICENSE).
