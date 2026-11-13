# Free BigQuery metadata APIs and Google skills review

Research date: **2026-11-12**. Sources: Google product documentation and the
[Google cloud skills directory](https://github.com/google/skills/tree/main/skills/cloud).
This is a read-only research report; no skills were installed and no cloud resources were changed.

## Findings

The best additions to Sounding are **partition/clustering recommendations through
the Recommender REST API**, **model metadata**, and **row access policy metadata**.
There is also useful information already available in existing table, routine and
job responses. The Google skills reinforce Sounding's approach: inspect an
individual job through REST, and reserve SQL telemetry for explicitly requested
aggregate analysis. Their setup and execution instructions are not a reason to
enable services, install companion skills, or execute queries automatically.

### Existing coverage and accompanying changes

The reviewed `README.md`, `src/sounding/bq.py` and `src/sounding/catalog.py` already
cover table/dataset/routine metadata, previews, dry runs, job inspection and an
opt-in INFORMATION_SCHEMA workload scan. Partitioning, clustering, declared keys,
storage metrics and materialized-view settings already come from `tables.get`.
They do not require additional metadata SQL.

The accompanying implementation adds refresh progress, column layout/key
badges and bounded table-related job discovery. It also preserves the full
schema in the existing table-resource JSON and exposing additional column
attributes. This avoids a catalog schema migration; older snapshots need a fresh
harvest to acquire those attributes. Subsequent implementation in schema 5 adds models,
row-access policies, and daily cached Recommender recommendations/insights, with explicit
coverage statuses and local UI, CLI and MCP access. Refresh remains metadata-only by
default; the CLI's `--include-preview-rows` opt-in warns about material delay.

## Additional free metadata reads, in priority order

“Free metadata” here means a metadata read with no BigQuery query execution. Google
explicitly identifies metadata operations as free in its
[cost-optimization guidance](https://cloud.google.com/blog/products/data-analytics/cost-optimization-best-practices-for-bigquery).
The current [pricing page](https://cloud.google.com/bigquery/pricing) should remain
the billing authority; it separately prices query processing, storage and streaming
reads. API quotas and permissions still apply.

| Priority | API or existing response | Useful addition | Boundary |
| --- | --- | --- | --- |
| High | Recommender `recommendations.list` and `insights.list` | Show Google's recommended partition column/time unit or cluster columns, estimated impact, update time and supporting workload insight. Use recommender ID `google.bigquery.table.PartitionClusterRecommender` and insight type `google.bigquery.table.StatsInsight`. | Requires the Recommender API and relevant viewer permissions; fetch by project/location and cache locally. Read the REST endpoints instead of querying INFORMATION_SCHEMA.RECOMMENDATIONS. |
| High | BigQuery `models.list` / `models.get` | A model inventory alongside tables and routines: model type, description, labels, lifecycle, feature/label schema and remote-model information. | `bigquery.models.list` and `bigquery.models.getMetadata`; metadata access does not train, predict or evaluate a model. Track denied/partial coverage separately from an empty inventory. |
| High | BigQuery `rowAccessPolicies.list` | Policy presence/count and names explain why a preview or cost estimate is restricted. | Separate `bigquery.rowAccessPolicies.list` permission on the table. Default to presence/names; filter expressions deserve explicit inspection because they can reveal business rules. Failure to list means unknown, not no policies. |
| Medium | BigQuery `projects.list`, then `datasets.list` | Discover projects and datasets instead of requiring every dataset name. | Project discovery requires project-level access and includes only projects with the BigQuery API enabled. It can miss a project where access was granted only on an individual dataset. Paginate; dataset discovery can report unreachable locations. |
| Medium | Existing `tables.get` schema and resource | Preserve numeric precision/scale, max length, defaults, collation, rounding, RANGE element type, generated-column and direct data-policy metadata; surface streaming-buffer state. | These are already in the response. Preserve raw metadata before deciding which fields to render. Rows/bytes exclude streaming-buffer data, whose counts are lower-bound estimates. |
| Medium | Existing `routines.get` resource | Explain remote endpoints/connections and Python runtime dependencies in routine details. | Inspect definitions without invoking routines. Routine execution is a different action and may use external services. |
| Medium | Existing `jobs.get` resource | Add acceleration evidence: search/vector index usage, materialized-view rewrites, BI Engine and metadata-cache diagnostics. | Retain missing values as unknown. These are diagnostics for that execution, not proof a table is generally optimized. |
| Lower | BigQuery `tables.testIamPermissions` | Permission-aware preview/metadata controls. | A UI hint only: Google warns it can fail open and must not be used as the authorization boundary. |
| Lower | Reservation API read methods | Current reservations, capacity commitments and assignments for capacity users. | Separate API, regional admin-project scope and permissions. Configuration reads do not purchase capacity; creation/update methods do. This is a later capacity-focused extension. |

Sources for these candidates:

- **Recommender:** [BigQuery recommendations and REST endpoints](https://docs.cloud.google.com/bigquery/docs/manage-partition-cluster-recommendations), [Recommender pricing and API quotas](https://cloud.google.com/recommender/pricing). Recommendations use up to 30 days of workload and update approximately daily; estimates may overstate savings. Most recommendations are generated free, and all customers have API access, but basic support has only 100 reads/day per organization. Applying a recommendation can incur processing/storage charges. The viewer role is `roles/recommender.bigqueryPartitionClusterViewer`; check insight permissions separately.
- **Models:** [listing permissions](https://docs.cloud.google.com/bigquery/docs/listing-models), [get permission](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/models/get), [model resource fields](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/models).
- **Row policies:** [list method](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/rowAccessPolicies/list), [policy resource](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/rowAccessPolicies). Reading policy membership has a separate IAM requirement: [row-level security management](https://docs.cloud.google.com/bigquery/docs/managing-row-level-security).
- **Discovery:** [projects.list](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/projects/list), [datasets.list](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/datasets/list).
- **Existing payloads:** [table/schema fields](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/tables), [routine fields](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/routines), [job statistics](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/Job).
- **Permission hints and capacity:** [testIamPermissions limitations](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/tables/testIamPermissions), [Reservation API methods](https://docs.cloud.google.com/bigquery/docs/reference/reservations/rest), [read permissions](https://docs.cloud.google.com/bigquery/docs/reference/auditlogs/index-reservation).

## Preserve the cost boundary

| Source/action | Cost classification and implication |
| --- | --- |
| REST metadata reads | No query execution. Prefer the narrow read API and label its source explicitly. A broad statement that every Google Cloud API is free would be incorrect. |
| `tabledata.list` previews | Explicitly documented as no-charge previewing. Keep the current native-table restrictions and row cap. Row access policies require TRUE-filter access for this API; do not silently replace a failed preview with SQL. [Preview guidance](https://docs.cloud.google.com/bigquery/docs/best-practices-costs#avoid_running_queries_to_explore_table_data), [row-policy interaction](https://docs.cloud.google.com/bigquery/docs/using-row-level-security-with-features#tabledata.list_in_bigquery_api). |
| Dry runs | Free and do not consume query slots. A zero estimate can hide external-source or row-level-security costs. Keep unknown estimates unknown and never execute as a fallback. [Dry-run documentation](https://docs.cloud.google.com/bigquery/docs/running-queries#dry_run). |
| INFORMATION_SCHEMA SQL | On-demand execution has a 10 MB minimum; capacity execution consumes slots; results are not cached. This includes seemingly simple partition, column, index and recommendation queries. Keep all such execution opt-in. [Pricing and scope](https://docs.cloud.google.com/bigquery/docs/information-schema-intro#pricing). |
| Storage Read API | Has a 300 TiB/month bytes-read allowance, then a charge; outbound transfer is separate and is not covered by that allowance. It is not an unconditional free-preview replacement. [Storage Read pricing](https://cloud.google.com/bigquery/pricing#storage-read-api-pricing-details). |
| Cloud Monitoring | Google-provided metric ingestion can be free, but API reads that return time series are metered beyond the allowance. “No SQL” does not mean “free API.” [Monitoring pricing](https://cloud.google.com/products/observability/pricing#cloud-monitoring). |
| Cloud Logging | Querying existing log data has no additional query charge, but ingestion, retention and routed destinations have their own costs. It also requires separate access. Do not create sinks or change retention as part of metadata refresh. [Logging pricing](https://cloud.google.com/products/observability/pricing#cloud-logging). |
| Data Lineage / Knowledge Catalog | Automatic lineage processing and retained lineage metadata are separately billed. Enabling it is a separate decision, not a free catalog enhancement. [Lineage pricing](https://docs.cloud.google.com/knowledge-catalog/docs/about-data-lineage#pricing). |
| BigFrames, ML profiling, AI functions, validation and migrations | These are computation or resource workflows, not additional metadata reads. A skill's examples are not evidence that an operation is free. `ML.DESCRIBE_DATA` could inspire a future explicitly capped profiling feature. [Google ML profiling reference](https://github.com/google/skills/blob/main/skills/cloud/bigquery-ai-ml/references/ml_describe_data.md), [BigQuery pricing](https://cloud.google.com/bigquery/pricing). |

## Lessons from the Google skills

All twelve BigQuery-named skill entry points in the cloud directory were scanned.
The highest-value references were inspected in more depth; this is not an audit of
every example or a recommendation to install the whole collection.

| Skill reviewed | Useful lesson for Sounding |
| --- | --- |
| [bigquery-observability](https://github.com/google/skills/blob/main/skills/cloud/bigquery-observability/SKILL.md) | REST point lookups for individual jobs; SQL for historical cohorts and aggregates; explicit project/location/identity scope. Its job-performance, compute-billing, storage and schema references are useful maps of available fields. |
| [bigquery-optimization](https://github.com/google/skills/blob/main/skills/cloud/bigquery-optimization/SKILL.md) | Surface partition and ordered clustering annotations before proposing rewrites. Compare estimates with dry runs; treat partition, lifecycle, materialization and capacity changes as separate actions. Reviewed layout, SQL, storage, materialized-view, search-index, BI Engine and capacity references. |
| [bigquery-troubleshooting](https://github.com/google/skills/blob/main/skills/cloud/bigquery-troubleshooting/SKILL.md) | Compare slow runs with a baseline; distinguish queue delay, elapsed execution, slot time, row expansion and shuffle spill. A stage's records read are not bytes scanned. Reviewed execution-plan, contention/configuration and compute/storage-cost references. |
| [bigquery-basics](https://github.com/google/skills/blob/main/skills/cloud/bigquery-basics/SKILL.md) | Separate resource discovery, metadata and execution. Its CLI, IAM and MCP references help explain Sounding's interfaces. |
| [bigquery-udfs](https://github.com/google/skills/blob/main/skills/cloud/bigquery-udfs/SKILL.md) | Distinguish ordinary SQL/JavaScript routines from Python and remote execution dependencies. This supports richer routine metadata and keeping execution explicit. |
| [bigquery-ai-ml](https://github.com/google/skills/blob/main/skills/cloud/bigquery-ai-ml/SKILL.md) | Model inventory is useful; inference/training/profiling are separate. Inspected the ML.DESCRIBE_DATA reference as a potential future profiling capability. |
| [bigquery-bigframes](https://github.com/google/skills/blob/main/skills/cloud/bigquery-bigframes/SKILL.md) | Useful for cloud computation, with lazy operations and controlled materialization. It is not a replacement for the metadata client's free-call contract. |
| [bigquery-migration-metadata-extractor](https://github.com/google/skills/blob/main/skills/cloud/bigquery-migration-metadata-extractor/SKILL.md) | Borrow extraction-completeness reporting: succeeded/skipped/failed counts and type-aware inventory. Its detailed extraction-statistics reference distinguishes absent metrics from zero. The extractor itself targets migrations and can provision cloud resources. |
| [bigquery-migration-data-loader](https://github.com/google/skills/blob/main/skills/cloud/bigquery-migration-data-loader/SKILL.md) | Resource provisioning/loading is outside Sounding's read-only harvest. Row-count audit ideas are useful only when backed by the correct metadata source. |
| [bigquery-migration-sql-translator](https://github.com/google/skills/blob/main/skills/cloud/bigquery-migration-sql-translator/SKILL.md) | Could support a separate migration product; not a missing BigQuery catalog API. |
| [bigquery-migration-validator](https://github.com/google/skills/blob/main/skills/cloud/bigquery-migration-validator/SKILL.md) | Schema-only comparisons and aggregate/row validation are different operations. Avoid promoting row validation to a free metadata feature. |
| [datalineage-bigquery-asset-impact-analysis](https://github.com/google/skills/blob/main/skills/cloud/datalineage-bigquery-asset-impact-analysis/SKILL.md) | Good reporting pattern: show direction, location, graph depth and search limits. Its lineage service is a separate dependency with separate pricing. |

Do not copy every assertion literally. In particular:

- The observability billing template applies a conservative 50× multiplier to all
  CREATE_MODEL jobs. That is a heuristic, not an invoice calculation. Sounding's
  decision to leave model training unpriced is preferable without model-specific
  evidence. [Template](https://github.com/google/skills/blob/main/skills/cloud/bigquery-observability/references/compute_ondemand_billable.md), [model pricing](https://cloud.google.com/bigquery/pricing#bqml).
- The lineage skill tells readers to interpret an empty graph as no dependencies.
  Report “no dependencies found within this search” instead: collection scope,
  permissions, 30-day retention and unsupported relationships limit coverage.
  [Official lineage limitations](https://docs.cloud.google.com/knowledge-catalog/docs/about-data-lineage#data_lineage_considerations_and_limitations).
- The basics MCP reference and live product docs differ on SQL tool behavior.
  Current docs distinguish `execute_sql` and `execute_sql_readonly`. Read-only
  query execution is still query execution; it does not replace Sounding's
  dry-run-only tool. [Current MCP documentation](https://docs.cloud.google.com/bigquery/docs/use-bigquery-mcp#limitations).

## Correctness notes for the accompanying changes

1. **Call the table list “recent jobs referencing this table.”** `jobs.list` is
   newest-first, retains six months, and normally returns top-level jobs. Child
   script jobs require `parentJobId`. A 30-day/1,000-job cap is a coverage limit,
   not a complete ranking of all activity. Preserve the cutoff, scanned count,
   truncation and unreachable-location flags. [jobs.list](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/jobs/list).
2. **Keep the execution project distinct from the data project.** Read job
   history from the selected job project; readers of a table may run elsewhere.
   Default identity scope is appropriate. `allUsers=true` with `jobs.list` can
   expose redacted other-user entries; complete metadata requires
   `bigquery.jobs.listAll`. [jobs.list permissions](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/jobs/list#iam-permissions).
3. **Job totals are not table-attributed costs.** A multi-table job's full bytes
   and slot time must not be assigned to each referenced table. The Google
   observability template explicitly calls out this trap. [Billing/template reference](https://github.com/google/skills/blob/main/skills/cloud/bigquery-observability/references/compute_ondemand_billable.md).
4. **Use the right reference collections.** Job REST has `referencedTables`,
   `referencedLogicalViews` and `referencedRoutines`. Missing references do not
   establish non-use. Its `queryInfo` documents optimization details, not the
   normalized-literal hash available in INFORMATION_SCHEMA; keep the two products
   distinct. [Job resource](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/Job).
5. **Keep SQL inspection explicit.** `jobs.get` inspects an existing job; it does
   not rerun SQL. Use the job's project/location and avoid saving or logging SQL
   by default. [jobs.get](https://docs.cloud.google.com/bigquery/docs/reference/rest/v2/jobs/get).
6. **Show layout annotations accurately.** Clustering order matters. Ingestion-time
   partitioning uses pseudocolumns, so it needs a table-level annotation even when
   no schema row matches. Declared primary/foreign keys are not enforced.
   [Partition/clustering reference](https://github.com/google/skills/blob/main/skills/cloud/bigquery-optimization/references/table_partitioning_clustering.md),
   [table constraints](https://docs.cloud.google.com/bigquery/docs/primary-foreign-keys).
7. **Preserve snapshot and refresh semantics.** Keep per-dataset atomic publication,
   show failures separately, and leave failed datasets' previous snapshots visible.
   “Refresh metadata” should only repeat read calls. `tables.get` is also the
   documented way to see immediate schema changes when INFORMATION_SCHEMA lags.
   [Schema updates](https://docs.cloud.google.com/bigquery/docs/managing-table-schemas).

The cached, read-only Recommender view, models and row-policy discovery are now implemented.
Reservation inventory and external observability integrations can follow when capacity users need them.
