"""Read-only partition/cluster recommendations and table insights, cached for a day.

Uses the existing BigQuery client's authorized HTTP session. No jobs, API enablement,
recommendation state changes, or application of the suggested table changes.
"""

import datetime as dt
import hashlib
import json
import re
from urllib.parse import quote

from filelock import FileLock
from google.auth.exceptions import GoogleAuthError
from requests.exceptions import RequestException

from . import catalog

KINDS = {
    "recommendations": "recommenders/google.bigquery.table.PartitionClusterRecommender/recommendations",
    "insights": "insightTypes/google.bigquery.table.StatsInsight/insights",
}
TTL = dt.timedelta(hours=24)


def _record(item):
    # Preserve useful metadata without query text, principals, or operation payloads.
    targets = list(item.get("targetResources") or [])
    for group in (item.get("content") or {}).get("operationGroups", []):
        targets.extend(op.get("resource", "") for op in group.get("operations", []))
    tables = set()
    for target in targets:
        match = re.fullmatch(r"(?://bigquery.googleapis.com)?/?projects/([^/]+)/datasets/([^/]+)/tables/([^/]+)", target)
        if match:
            tables.add(".".join(match.groups()))
    content = item.get("content") or {}
    overview = content.get("overview") or {}
    if all(overview.get(k) for k in ("projectId", "datasetId", "tableId")):
        tables.add(".".join(overview[k] for k in ("projectId", "datasetId", "tableId")))
    keep = {"partitionColumn", "partitionTimeUnit", "clusterColumns", "bytesSavedMonthly", "slotMsSavedMonthly",
            "existingPartitionColumn", "tableSizeTb", "bytesReadMonthly", "slotMsConsumedMonthly", "queryJobsCountMonthly"}
    return {"name": item.get("name"), "description": item.get("description"),
            "subtype": item.get("recommenderSubtype") or item.get("insightSubtype"),
            "state": (item.get("stateInfo") or {}).get("state"), "last_updated": item.get("lastRefreshTime"),
            "observation_period": item.get("observationPeriod"), "primary_impact": item.get("primaryImpact"),
            "details": {k: v for k, v in (content | overview).items() if k in keep}, "tables": sorted(tables)}


def _failure(response):
    try:
        details = response.json().get("error", {}).get("details", [])
        reasons = {d.get("reason") for d in details}
    except (ValueError, AttributeError, TypeError):
        reasons = set()
    if "SERVICE_DISABLED" in reasons:
        return "disabled", "Recommender API is not enabled for the request's consumer project."
    if response.status_code == 429:
        return "quota", "Recommender read quota was reached."
    if response.status_code in (401, 403):
        return "denied", "Current credentials cannot read these recommendations or insights."
    return "unavailable", "Recommender metadata is unavailable for this project and location."


def refresh(client, project, location):
    """Fetch each regional collection at most once per 24h, including failed attempts.

    A failed refresh retains last successful data and marks it stale. The per-scope
    lock deduplicates concurrent dataset harvests without holding DuckDB over HTTP.
    """
    location = location.lower()
    key = hashlib.sha256(f"{project}/{location}".encode()).hexdigest()[:24]
    with FileLock(str(catalog.CATALOG) + f".recommender-{key}.lock", timeout=120):
        now = dt.datetime.now(dt.UTC)
        for kind, suffix in KINDS.items():
            with catalog.connect() as con:
                previous = con.execute("SELECT checked_at, fetched_at, items FROM recommendation_cache WHERE project=? AND location=? AND kind=?",
                                       [project, location, kind]).fetchone()
            if previous and now - previous[0] < TTL:
                continue
            items, token, seen = [], None, set()
            status, message = "harvested", None
            url = f"https://recommender.googleapis.com/v1/projects/{quote(project, safe='')}/locations/{quote(location, safe='')}/{suffix}"
            try:
                while True:
                    params = {"pageSize": 1000}
                    if token:
                        params["pageToken"] = token
                    response = client._http.get(url, params=params, timeout=30)
                    if response.status_code != 200:
                        status, message = _failure(response)
                        break
                    body = response.json()
                    items.extend(_record(item) for item in body.get(kind, []))
                    token = body.get("nextPageToken")
                    if not token:
                        break
                    if token in seen:
                        status, message = "unavailable", "Recommender pagination did not complete."
                        break
                    seen.add(token)
            except (RequestException, GoogleAuthError, ValueError):
                status, message = "unavailable", "Could not read Recommender metadata with the current connection."
            success = status == "harvested"
            saved_items = json.dumps(items) if success else previous[2] if previous else None
            fetched = now if success else previous[1] if previous else None
            with catalog.connect(write=True) as con:
                con.execute("INSERT OR REPLACE INTO recommendation_cache VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            [project, location, kind, now, fetched, status, message, saved_items])
    with catalog.connect() as con:
        return [s for s in catalog.recommendations(con, project)["scopes"] if s["location"] == location]
