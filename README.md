# Sounding

Sailors took a sounding to measure the water's depth before going in. Sounding does the same
for BigQuery: it shows what data exists, how big and fresh it is, how tables are shaped, and
what a query will cost, all without running a paid query.

It harvests metadata into a local DuckDB catalog using only BigQuery's free calls. People
read the catalog from the CLI and agents read it over MCP.

## The cost ladder

Sounding answers each question from the cheapest source that can.

| Source | What you get | Cost |
| --- | --- | --- |
| Local catalog | Everything already harvested, plus snapshot history | $0, no BigQuery call |
| REST `tables.list` / `tables.get` | Schema (nested), descriptions, row and byte counts, last modified, partitioning, clustering, view SQL | $0 (API quota) |
| `tabledata.list` | Sample rows (tables only, not views) | $0 |
| Dry run | Bytes a query would scan, referenced tables | $0 |
| INFORMATION_SCHEMA | Partitions, storage, job history | 10 MB minimum per query, never cached: **not used yet** |
| Sampled profiles | `TABLESAMPLE` + `APPROX_*` | Billed: **not used yet** (planned as opt-in, capped, approved) |

Two free tricks come from dry runs:
- **Per-column cost:** a dry run of `SELECT col FROM t` gives the bytes that column scans.
- **View lineage:** a dry run of a view lists the base tables it reads.

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

### Agents (MCP)

```json
{ "mcpServers": { "sounding": { "command": "uv", "args": ["run", "--directory", "/path/to/sounding", "sounding", "mcp"] } } }
```

| Tool | Calls BigQuery? |
| --- | --- |
| `search_catalog`, `describe_table` | Never |
| `preview_rows`, `estimate_query_cost` | Yes, free calls only |

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
5. Web UI: an overview and table pages with a dry-run cost guard
6. Opt-in billed profiling behind a byte cap and approval, with results cached in the catalog

## Development

```bash
uv run pytest
```
