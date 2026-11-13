# Sounding

Sailors took a sounding to measure the water's depth before going in. Sounding does the same
for BigQuery: it shows what data exists, how big and fresh it is, how tables are shaped, and
what a query may cost, all without running a paid query. An optional workload scan
also identifies costly query patterns worth investigating, under an explicit byte cap.

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
| REST `models.list` / `models.get` | Model type, features, labels, timestamps and remote model settings | $0 (API quota) |
| REST `rowAccessPolicies.list` | Policy names and filter predicates, without IAM membership lists | $0 (API quota) |
| Recommender `recommendations.list` / `insights.list` | Partition/cluster suggestions and table workload insights; cached by project/location for 24 hours | $0 ([read quotas](https://cloud.google.com/recommender/pricing)) |
| `tabledata.list` | Sample rows (tables only, not views) | $0 |
| Dry run | Estimated bytes, reported referenced tables, and estimate limitations | $0 |
| REST `jobs.list` / `jobs.get` | Cost receipts and individual query execution details (`cost-report`, `inspect-job`) | $0 (API quota) |
| INFORMATION_SCHEMA | Regional job history grouped into optimization candidates (`query-opportunities`) | **Opt-in only**, byte-capped; 10 MB minimum on demand, capacity queries consume slots, never cached. The metadata harvester does not use it. |
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

Tables that require a partition filter get one in every column-cost dry run
(`WHERE <partition column> IS NOT NULL`), and the partition column's own bytes are
subtracted from each other column, so the costs stay per column. Rows in the NULL
partition, if any, are not counted, and the command says so.

Each dataset publishes its metadata snapshot in one transaction. Optional row samples, when requested, are attached afterward so metadata is browseable sooner. A table or routine deleted between the list call and its get call is skipped
with a log line. A failed dataset
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

Free BigQuery actions return a `receipt` showing the evidence that they cost nothing, and
`sounding cost-report` shows the jobs this identity ran in the job project, with what was billed.
Opt-in workload scans return their actual job ID, billed bytes and slot time instead.

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
  - Sounding's metadata, preview and dry-run calls create no jobs. A window using
    only these operations shows **0 jobs**. Opt-in workload scans do create jobs
    and appear in this report.
  - A query run by any other tool in that project appears with its own billed bytes.
  - It does not show jobs billed to other projects or by other identities, and it
    prices at list rate, so your Cloud Billing report remains the final word.

### Find queries worth optimizing

`query-opportunities` is available through the CLI and MCP. It scans job metadata,
groups repeated executions by BigQuery's normalized query hash, and ranks the
groups by **total billed bytes** or **total slot time**. This catches a smaller query
run thousands of times as well as a single large query. Each candidate includes up
to three representative job IDs and evidence-based investigation suggestions.

```bash
# Free preview: shows the fixed metadata SQL, scope, estimate and cap.
uv run sounding query-opportunities --location US --hours 168

# Execute one capped metadata scan and save its report locally.
uv run sounding query-opportunities --location US --hours 168 --allow-billed

# Project-wide scope; slot-time ranking is useful for capacity workloads.
uv run sounding query-opportunities --location EU --project my-project \
  --scope project --rank-by slot_ms --allow-billed

# Reopen the latest saved report (or pass a report_id), with no BigQuery call.
uv run sounding query-report

# Free inspection of a representative job; SQL text is explicitly opt-in.
uv run sounding inspect-job JOB_ID --location US --include-sql
```

The default scope is the current identity's jobs in `SOUNDING_JOB_PROJECT` (or the
credentials' default project). `--project` selects the project whose history is
read; the monitoring query itself still runs in the job project. Project scope
requires `bigquery.jobs.listAll`, commonly granted by `roles/bigquery.resourceViewer`,
and execution requires `bigquery.jobs.create` in the job project. Location is
required: `US`, `EU` and individual regions are distinct scopes.

Scans preview first and execute only with `--allow-billed`. The default
`--max-bytes-billed` is **104857600 (100 MiB)**, with an allowed range of 10 MiB to
1 GiB. BigQuery enforces it on execution; a preview over the cap is blocked.
Unknown dry-run estimates stay unknown and still require explicit opt-in and the
execution cap. The cap limits on-demand scan bytes, **not capacity charges**. A
60-second best-effort job timeout is also requested. Sounding does not submit a
replacement job after execution failure or widen the cap automatically.

Candidates qualify when a group's known total reaches `--min-billed-bytes`
(default 1 GiB), `--min-slot-ms` (default 60000 slot milliseconds), or has a BigQuery
resource/performance warning. The default is the top 20 groups (`--limit`, maximum
100), over seven days (`--hours`, maximum 4320). Grouping happens before the limit;
`total_candidates` and `truncated` describe the result cutoff.

The report flags resource warnings, slot contention, insufficient shuffle capacity,
high-cardinality joins and partition skew. `inspect-job` adds stage-level shuffle
spill, large row expansion, labels (including dbt attribution when present), and
referenced tables. Use the harvested schemas and free dry runs to evaluate possible
rewrites. These are investigation leads, not proof that a query is wasteful or a
promise of savings. Slot contention may call for workload scheduling or capacity
changes rather than a SQL rewrite.

Only successful, completed, non-cached query jobs created in the window count.
Script parents are excluded to avoid counting their children twice, and monitoring
jobs are excluded by label. Jobs without a normalized hash stay separate. Missing
metrics remain `null`; `known_*` totals are partial when corresponding
`missing_*_jobs` counts are positive. Dollar figures are **US on-demand scan
equivalents**, not capacity charges or invoices; model training is left unpriced.
An empty result does not establish zero usage: check scope, region and thresholds.

Reports are stored in the local catalog with their scope, timestamps and receipts;
`query-report` never refreshes them. The latest report can belong to a different
project or region, so use its `report_id` for an exact lookup. Business SQL and user
emails are not selected or persisted by the scan. `inspect-job --include-sql` prints
SQL without saving it; SQL and labels may contain sensitive values. A failed scan
returns `status: failed` with available billing evidence; a blocked scan returns
`status: blocked`. The CLI exits with code 2 for either, retaining the JSON response.

The design draws on Google's [BigQuery observability guidance](https://github.com/google/skills/tree/main/skills/cloud/bigquery-observability)
and [optimization guidance](https://github.com/google/skills/tree/main/skills/cloud/bigquery-optimization),
and the recurring-query and materialization-candidate reports in
[dbt-bigquery-monitoring](https://github.com/bqbooster/dbt-bigquery-monitoring).
Sounding uses these patterns directly without requiring a dbt deployment or billing
export. See Google's [JOBS schema](https://docs.cloud.google.com/bigquery/docs/information-schema-jobs)
for scope, permissions, hash behavior and metric limitations.

### What a harvest keeps

Each `tables.get` response retains its metadata, including the original schema.
Transport identifiers such as etags and self-links are omitted; view SQL is stored
separately. Schema paths are also flattened for search. New harvests preserve field
defaults, precision/scale, maximum length, collation, rounding, range element types
and other field metadata; older snapshots need a refresh to populate these details.
These fields are also pulled out for search, `describe` and the UI, at no extra BigQuery cost:

- **Declared keys:** primary and foreign keys. BigQuery does not enforce them, so they
  are join hints, not proof.
- **Partition guidance:** whether a partition filter is required, partition expiry,
  partition count and integer-range bounds. Schema columns are marked with their
  partition role, clustering order and declared key roles; ingestion-time pseudo
  columns are explained separately.
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

### Catalog upgrades

This version uses catalog schema 5. Schema 2, 3 and 4 catalogs are upgraded in place
the first time any Sounding command opens them. Schema 3 adds richer metadata;
schema 4 adds saved workload reports; schema 5 adds models, row policies, recommendation caches and optional samples. Existing snapshots are preserved, and older
metadata fields stay unknown until the next harvest. Each migration runs in one
transaction, so a failed step rolls back without changing its starting schema.

Schema 1 catalogs (Sounding before 0.1.0) can't be converted reliably (timezone-less
timestamps, alias-keyed snapshots). The first time Sounding opens one, it renames it
next to itself (for example `catalog.v1-20261105T082205.duckdb`), starts an empty
catalog at the same path, and prints a notice. Re-run `sounding harvest` for your
datasets. Nothing is deleted. A catalog from a *newer* Sounding is refused and left
untouched.

### Catalog UI

```bash
uv run sounding ui --open      # http://127.0.0.1:8765, localhost only
```

The UI opens to a **project → dataset → table** browser over your saved local catalog.
No sign-in is needed to browse. Choose a project or dataset in the sidebar to scope the
table list and search; **All projects** searches everything. Project pages list their
datasets, and dataset pages show saved settings, labels, freshness and routine signatures.
Empty datasets remain visible, and identically named datasets in different projects stay
separate. **Reload catalog** reloads the local catalog after a CLI harvest; it does not contact
Google or sync data. Project and dataset pages have bookmarkable URLs.

The datastore already keys snapshots by the full `project.dataset` identity and stores
dataset settings separately from tables. The browser groups those latest snapshots by
project, so existing catalogs work immediately without another harvest or a migration.
Enter a project ID under **Open a Google Cloud project** and choose **Open & import** to discover and import its datasets immediately. The catalog updates as metadata is saved; empty projects discovered this way are remembered too. Uses existing credentials, with no sign-in flow.

The page has live search over table names, column paths and descriptions, and reports which
columns matched. Pick a table to see its size, freshness, full-scan cost, and nested schema,
plus notes, labels, declared keys, storage, and which columns have policy tags.
The menu separates **Catalog**, **Cost report**, **Refresh metadata**, and **Docs & API**.
Documentation includes CLI commands, MCP setup/tools and the internal local HTTP API.
The Cost report tab lists jobs run in the job project over a chosen window, with their billed bytes.
Table pages provide these free actions, with receipts:
- **Column costs:** one dry run per column, drawn as bars.
- **Preview:** 10 rows via `tabledata.list`.
- **Cost guard:** paste SQL and see the bytes and dollars it would scan, without running it.
- **Top queries:** choose 1–180 days (default **30**), a job project, user/project
  visibility, and rank individual query jobs by billed bytes or slot time. Load
  history explicitly; **Show SQL & details** uses `jobs.get` and never saves SQL.

Browsing project, dataset, search and table pages never calls BigQuery. Live actions
use existing Google credentials; the UI does not sign in or change credentials.

#### Refresh datasets or whole projects

Select one or more projects or datasets under **Refresh metadata**, or start from a
project/dataset page. Whole-project refresh paginates `datasets.list`, discovering
accessible non-hidden datasets, including new ones. Overlapping selections are
deduplicated. Dataset snapshots publish independently; failures keep the previous
snapshot and other datasets continue. Previously saved datasets no longer visible
in a project are retained, not treated as deleted. Unreachable locations and access
failures are reported. No query jobs are created.

The progress display shows completed datasets and discovered metadata items; item
totals grow during discovery. It stays visible across tabs and browser reloads.
Status belongs to the current UI/MCP server and is not retained across server
restarts; completed catalog snapshots persist. A catalog-wide refresh lock prevents
overlapping refresh jobs, while catalog reads remain available. The original
`harvest` command retains its existing behavior. Metadata refresh does not recompute
column dry-run estimates, which belong to their original snapshots.

Models and row-access policies are included in refresh. Dataset pages list models with a detail view; table pages show policy names and expandable filters. Models require `bigquery.models.list` / `bigquery.models.getMetadata`; policies require `bigquery.rowAccessPolicies.list`. Missing permission or an unavailable API is reported as unknown coverage, not “zero models/policies.” Successful metadata is saved and the overall refresh is marked partial.

Partition/cluster recommendations and StatsInsight metadata appear on project, dataset and table pages. Their project/location responses are cached for 24 hours (failed attempts too, to conserve quota). Failed refreshes retain the last good response and mark it stale. Reads require Recommender viewer permissions such as `roles/recommender.bigqueryPartitionClusterViewer` and an enabled Recommender API; Sounding does not enable APIs or apply suggestions. `sounding model project.dataset.model` and `sounding recommendations project` read this saved metadata, as do MCP `describe_model` and `project_recommendations`.

Row samples are **not included by default**. To opt in:

```sh
uv run sounding harvest my-project.analytics --include-preview-rows
uv run sounding refresh --project my-project --include-preview-rows
```

**Warning: including preview rows can add a material delay**, and saves actual data locally. Both commands print this warning before starting. One measurement on seven public tables took 0.64 seconds for `tables.get` versus 19.86 seconds for their sample reads (70 rows, about 21 KB); this is illustrative, not a general timing guarantee. Up to 10 rows / 256 KiB of serialized JSON are saved per native table or snapshot using free `tabledata.list`, with no SQL fallback. The storage cap does not bound network response size. Samples have arbitrary ordering and a saved timestamp; denied reads are explicit. A metadata-only refresh does not copy samples into its new snapshot. Older snapshots can still contain previously saved data. The table UI's **Load 10 rows / Refresh sample** explicitly reads and saves one table's sample on demand.

```bash
uv run sounding refresh my-project.analytics other-project.sales
uv run sounding refresh --project my-project --project other-project
uv run sounding browse
uv run sounding show orders --dataset my-project.analytics
uv run sounding table-queries my-project.analytics.orders --days 30 --project job-project
```

`refresh` prints final JSON to stdout and terminal progress to stderr; partial or
failed refreshes exit with code 2. A request accepts up to 25 projects and 1000
explicit datasets; whole projects can discover more datasets.

Table query history uses free `jobs.list` and scans at most the **newest 1000
completed top-level jobs** in the selected job project. It matches reported table
or logical-view references, excludes failed jobs and script parents, and does not
search script child jobs. Missing references, redacted metadata, other job projects,
unreachable regions and truncation can leave gaps. Project-wide full metadata
requires `bigquery.jobs.listAll`; ordinary job-list access can yield redacted jobs.
Results are individual executions, with whole-job metrics rather than costs
attributable to this table. Empty results do not prove no usage; inspect warnings.
Unlike the opt-in workload scan, this does not query INFORMATION_SCHEMA or group
by normalized SQL hashes. See [Google's jobs.list reference](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/jobs/list).

The UI validates Host and Origin, rejects cross-site requests, and requires a random
per-server token on API requests. All BigQuery actions are JSON-only POSTs,
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
| `browse_catalog`, `search_catalog`, `describe_table`, `describe_dataset`, `search_routines`, `describe_routine`, `describe_model`, `project_recommendations` | Never |
| `refresh_metadata` | Starts a background metadata-only refresh of `datasets` and/or `projects` |
| `refresh_status` | Never; polls the current/latest refresh on this MCP server |
| `table_query_history` | Free `jobs.list`; defaults to 30 days and current-identity scope |
| `preview_rows`, `estimate_query_cost`, `cost_report` | Yes, free calls only; each result carries a receipt or job list |
| `inspect_query_job` | Yes, free `jobs.get`; SQL is opt-in |
| `query_opportunities` | Free preview by default; `allow_billed=true` executes one byte-capped metadata query |
| `query_opportunity_report` | Never; reads a saved report by ID, or the latest across all scopes |

`search_catalog(q="", limit=50, offset=0)` returns `{items, next_offset}`; pass
`next_offset` as `offset` to continue. Limits are 1–100 tables and 20 matching paths
per table; `matched_column_count` retains the full count. Search descriptions are
excerpts; `describe_table` returns the full schema and descriptions. Paging reflects
the latest catalog at each request, so a concurrent harvest can change page contents.
Optional `project` and `dataset` parameters scope search by exact identity.
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
5. ✅ Catalog UI (`sounding ui`) with project and dataset overviews, scoped search, column costs, preview and a dry-run cost guard
6. Opt-in billed profiling behind a byte cap and approval, with results cached in the catalog
7. ✅ Keep table metadata, dataset settings, routines, BigQuery ML models, row-access policies and cached partition/cluster recommendations
8. ✅ Opt-in query opportunity scans, saved reports and free job inspection; next: workload trends and a UI for reports

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
- **The cost contract:** Sounding uses free calls by default. Executing fixed workload
  monitoring SQL requires explicit opt-in and a byte cap; user-supplied SQL remains
  dry-run only. Results carry cost evidence, and unknown cost remains unknown, never zero.

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

The GitHub Actions workflows are locked: `.github/workflows/actions.lock` records the
exact commit of every action, and CI fails if a workflow uses one it doesn't record or
a tag has moved. After changing a workflow, relock with `gh actions-lock` (a GitHub CLI
extension: `gh extension install github/gh-actions-lock`) and lint with
`uvx zizmor .github/`. Keep comments off `uses:` lines; relocking drops them.
Dependabot proposes Actions and `uv.lock` updates weekly, each at least 7 days old;
patch and minor updates merge themselves once the required checks pass.

## License

MIT. See [LICENSE](LICENSE).
