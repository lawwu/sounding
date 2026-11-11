"""Fixed workload-monitoring SQL and evidence-based triage; no network calls."""

from __future__ import annotations

import re

from .errors import SoundingError, integer, text

DEFAULT_MAX_BYTES = 100 * 1024**2
MAX_BYTES = 1024**3
MIN_BYTES = 10 * 1024**2
DEFAULT_MIN_BILLED_BYTES = 1024**3
DEFAULT_MIN_SLOT_MS = 60_000
RANKS = {"billed_bytes": "known_billed_bytes", "slot_ms": "known_slot_ms"}


def identifier(value: str, name: str) -> str:
    text(value, name, 256)
    # These are interpolated identifiers, never arbitrary SQL fragments.
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]*", value):
        raise SoundingError(f"{name} must contain only letters, digits, underscores and hyphens.")
    if name == "location" and value.lower().startswith("region-"):
        raise SoundingError("Use a location such as US or europe-west1, without the region- prefix.")
    return value


def validate(location, project, hours, scope, rank_by, limit, max_bytes_billed,
             min_billed_bytes, min_slot_ms, allow_billed):
    identifier(location, "location")
    if project is not None:
        identifier(project, "project")
    integer(hours, "hours", 1, 180 * 24)
    integer(limit, "limit", 1, 100)
    integer(max_bytes_billed, "max_bytes_billed", MIN_BYTES, MAX_BYTES)
    integer(min_billed_bytes, "min_billed_bytes", 0, 2**63 - 1)
    integer(min_slot_ms, "min_slot_ms", 0, 2**63 - 1)
    if scope not in ("user", "project"):
        raise SoundingError("scope must be user or project.")
    if rank_by not in RANKS:
        raise SoundingError("rank_by must be billed_bytes or slot_ms.")
    if type(allow_billed) is not bool:
        raise SoundingError("allow_billed must be true or false.")


def query_sql(project: str, location: str, scope: str, rank_by: str) -> str:
    """Aggregate before LIMIT so frequently repeated smaller jobs still rank highly.

    No business SQL, query text or user emails are selected. Missing hashes are kept
    separate by job ID. Script children count once; their parent wrappers are excluded.
    """
    rank = RANKS[rank_by]
    sample_rank = "total_bytes_billed" if rank_by == "billed_bytes" else "total_slot_ms"
    return f"""WITH jobs AS (
  SELECT
    COALESCE(CONCAT('hash:', query_info.query_hashes.normalized_literals),
             CONCAT('job:', job_id)) AS query_key,
    query_info.query_hashes.normalized_literals AS query_hash,
    job_id, creation_time, statement_type, reservation_id,
    total_bytes_billed, total_bytes_processed, total_slot_ms,
    TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) AS duration_ms,
    query_info.resource_warning AS resource_warning,
    EXISTS (SELECT 1 FROM UNNEST(
      query_info.performance_insights.stage_performance_standalone_insights) i
      WHERE i.slot_contention) AS slot_contention,
    EXISTS (SELECT 1 FROM UNNEST(
      query_info.performance_insights.stage_performance_standalone_insights) i
      WHERE i.insufficient_shuffle_quota) AS insufficient_shuffle,
    EXISTS (SELECT 1 FROM UNNEST(
      query_info.performance_insights.stage_performance_standalone_insights) i
      WHERE ARRAY_LENGTH(i.high_cardinality_joins) > 0) AS high_cardinality_join,
    EXISTS (SELECT 1 FROM UNNEST(
      query_info.performance_insights.stage_performance_standalone_insights) i
      WHERE i.partition_skew IS NOT NULL) AS partition_skew
  FROM `{project}`.`region-{location.lower()}`.INFORMATION_SCHEMA.JOBS_BY_{scope.upper()}
  WHERE creation_time >= @since AND creation_time < @until
    AND job_type = 'QUERY' AND state = 'DONE' AND error_result IS NULL
    AND (statement_type != 'SCRIPT' OR statement_type IS NULL)
    AND (cache_hit IS NULL OR cache_hit = FALSE)
    AND NOT EXISTS (SELECT 1 FROM UNNEST(labels) l
                    WHERE l.key = 'sounding_action' AND l.value = 'query_opportunities')
), families AS (
  SELECT
    query_key, query_hash, statement_type, COUNT(*) AS executions,
    MIN(creation_time) AS first_seen, MAX(creation_time) AS last_seen,
    SUM(total_bytes_billed) AS known_billed_bytes,
    COUNTIF(total_bytes_billed IS NULL) AS missing_billed_jobs,
    SUM(total_bytes_processed) AS known_processed_bytes,
    COUNTIF(total_bytes_processed IS NULL) AS missing_processed_jobs,
    SUM(total_slot_ms) AS known_slot_ms,
    COUNTIF(total_slot_ms IS NULL) AS missing_slot_jobs,
    COUNTIF(reservation_id IS NOT NULL) AS reservation_jobs,
    AVG(duration_ms) AS avg_duration_ms, MAX(duration_ms) AS max_duration_ms,
    COUNTIF(resource_warning IS NOT NULL) AS resource_warning_jobs,
    COUNTIF(slot_contention) AS slot_contention_jobs,
    COUNTIF(insufficient_shuffle) AS insufficient_shuffle_jobs,
    COUNTIF(high_cardinality_join) AS high_cardinality_join_jobs,
    COUNTIF(partition_skew) AS partition_skew_jobs,
    ARRAY_AGG(STRUCT(job_id, creation_time, duration_ms, resource_warning)
              ORDER BY {sample_rank} DESC, job_id LIMIT 3) AS sample_jobs
  FROM jobs
  GROUP BY query_key, query_hash, statement_type
), candidates AS (
  SELECT query_key, query_hash, statement_type, executions, first_seen, last_seen,
    known_billed_bytes, missing_billed_jobs, known_processed_bytes, missing_processed_jobs,
    known_slot_ms, missing_slot_jobs, reservation_jobs, avg_duration_ms, max_duration_ms,
    resource_warning_jobs, slot_contention_jobs, insufficient_shuffle_jobs,
    high_cardinality_join_jobs, partition_skew_jobs, sample_jobs
  FROM families
  WHERE known_billed_bytes >= @min_billed_bytes OR known_slot_ms >= @min_slot_ms
    OR resource_warning_jobs > 0 OR slot_contention_jobs > 0
    OR insufficient_shuffle_jobs > 0 OR high_cardinality_join_jobs > 0 OR partition_skew_jobs > 0
)
SELECT query_key, query_hash, statement_type, executions, first_seen, last_seen,
  known_billed_bytes, missing_billed_jobs, known_processed_bytes, missing_processed_jobs,
  known_slot_ms, missing_slot_jobs, reservation_jobs, avg_duration_ms, max_duration_ms,
  resource_warning_jobs, slot_contention_jobs, insufficient_shuffle_jobs,
  high_cardinality_join_jobs, partition_skew_jobs, sample_jobs,
  COUNT(*) OVER () AS total_candidates
FROM candidates
ORDER BY {rank} DESC NULLS LAST, query_key, statement_type
LIMIT @limit"""


SIGNALS = {
    "resource_warning": "Inspect the job's resource warning and execution stages before choosing a rewrite.",
    "slot_contention": "Check concurrent workloads and reservation capacity; contention alone does not prove inefficient SQL.",
    "insufficient_shuffle": "Inspect join and aggregation stages; consider reducing or pre-aggregating their inputs.",
    "high_cardinality_join": "Check join keys, duplicate inputs and output cardinality; consider pre-aggregation where semantics allow.",
    "partition_skew": "Inspect skewed join or aggregation keys and their data distribution.",
}


def opportunity(row: dict) -> dict:
    from . import catalog

    out = dict(row)
    out.pop("total_candidates", None)
    for metric, missing in (("billed_bytes", "missing_billed_jobs"),
                            ("processed_bytes", "missing_processed_jobs"),
                            ("slot_ms", "missing_slot_jobs")):
        out[metric] = None if out[missing] else out[f"known_{metric}"]
    out["usd_on_demand_equivalent"] = (
        None if out["statement_type"] == "CREATE_MODEL" else catalog.usd(out["billed_bytes"]))
    out["findings"] = [{"signal": signal, "jobs": out[f"{signal}_jobs"], "next_step": action}
                       for signal, action in SIGNALS.items() if out[f"{signal}_jobs"]]
    if out["executions"] > 1:
        out["findings"].append({"signal": "repeated_work", "jobs": out["executions"],
            "next_step": "Inspect representative SQL for repeated scans or aggregations; evaluate reusable results or materialized views where query parameters, semantics and freshness requirements allow."})
    if not out["findings"]:
        out["findings"].append({"signal": "high_usage", "jobs": out["executions"],
            "next_step": "Inspect representative SQL and referenced table schemas for unnecessary columns, partition pruning and large join inputs. High usage alone does not prove waste."})
    out["warnings"] = []
    if any(out[k] for k in ("missing_billed_jobs", "missing_processed_jobs", "missing_slot_jobs")):
        out["warnings"].append("Some metrics are missing, possibly due to row-level security. Known totals are partial; missing values are not zero.")
    if out["reservation_jobs"]:
        out["warnings"].append("Includes reservation-backed jobs. Byte prices are on-demand equivalents, not capacity charges or promised savings.")
    if out["query_hash"] is None:
        out["warnings"].append("No normalized hash; this job has not been grouped with other queries.")
    if out["statement_type"] == "CREATE_MODEL":
        out["warnings"].append("Model-training pricing is not estimated here.")
    return out
