const $ = (s, el = document) => el.querySelector(s);
const esc = s => String(s ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const hl = (s, q) => { s = String(s ?? ""); if (!q) return esc(s);
  // Match raw text, then escape each segment so highlighting cannot split HTML entities.
  const match = new RegExp(q.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"), "iu").exec(s);
  return !match ? esc(s) : esc(s.slice(0, match.index)) + "<mark>" + esc(match[0]) + "</mark>" + esc(s.slice(match.index + match[0].length)); };
const bytes = b => { if (b == null) return "—"; const u = ["B","KB","MB","GB","TB","PB"]; let i = 0;
  while (b >= 1000 && i < u.length - 1) { b /= 1000; i++; } return (i ? b.toFixed(b < 10 ? 2 : 1) : b) + " " + u[i]; };
const num = n => n == null ? "—" : Number(n).toLocaleString();
const usd = d => d == null ? "unknown" : d < 0.01 ? (d === 0 ? "$0" : "<$0.01") : "$" + d.toFixed(2);
const token = $('meta[name="sounding-token"]').content;
const ago = iso => { if (!iso) return "—"; const d = (Date.now() - new Date(iso)) / 864e5;
  return d < 1 ? "today" : d < 2 ? "1 day ago" : d < 60 ? Math.round(d) + " days ago" : d < 730 ? Math.round(d / 30) + " months ago" : Math.round(d / 365) + " years ago"; };
async function api(path, body) {
  const headers = {"X-Sounding-Token": token};
  if (body) headers["Content-Type"] = "application/json";
  const r = await fetch(path, {headers, ...(body ? {method: "POST", body: JSON.stringify(body)} : {})});
  const j = await r.json(); if (!r.ok || j.error) throw new Error(j.error || r.statusText); return j;
}

// A receipt is evidence copied from BigQuery's response (dry runs) or the call type (free API calls).
function receiptHtml(r) {
  if (!r) return "";
  if (r.call.startsWith("jobs.insert")) {
    const n = r.calls ? `${r.calls} dry runs · ` : "";
    const jobs = r.calls ? `${r.jobs_created} jobs created` : (r.job_id ? `job ${esc(r.job_id)} created` : "no job created");
    return `<div class="receipt${r.free ? "" : " unconfirmed"}" title="${esc(r.source)}">${r.free ? "✓" : "⚠"} Receipt from BigQuery: ${n}dry run ${r.dry_run_confirmed ? "confirmed" : "NOT confirmed"} · billed ${r.billed_bytes == null ? "unknown" : bytes(r.billed_bytes)} · ${jobs}</div>`;
  }
  return `<div class="receipt" title="${esc(r.source)}">✓ Receipt: <code>${esc(r.call)}</code> · free API calls, no job created</div>`;
}

let current = null, seq = 0, viewSeq = 0, renderSeq = 0, estimateSeq = 0;
let nextOffset = null, displayed = 0;
let hierarchy = [], scopeProject = "", scopeDataset = "", browseSeq = 0;
let detailKind = "overview";
let appPage = "catalog", refreshState = {status: "idle"}, refreshTimer, refreshPolling = false, completedRefresh = null, refreshGeneration = 0;
let selectedProjects = new Set(), selectedDatasets = new Set();
let importedProgress = "", projectOpening = false;

function setPage(page) {
  appPage = page;
  $("main").classList.toggle("workspace", page !== "catalog");
  $("#results").hidden = page !== "catalog";
  $("#catalog-browser").hidden = page !== "catalog";
  $("#q").hidden = page !== "catalog";
  document.querySelectorAll("[data-page]").forEach(b => {
    b.classList.toggle("active", b.dataset.page === page);
    b.setAttribute("aria-current", b.dataset.page === page ? "page" : "false");
  });
}

function reportPage() {
  setPage("report"); detailKind = "report"; ++viewSeq; ++renderSeq;
  history.replaceState(null, "", "#cost-report");
  $("#detail").innerHTML = `<h2 class="overview-title">Cost report</h2><p class="desc">See jobs your Google identity ran in the configured job project, using the free jobs API.</p>
    <div class="toolbar"><label>Time window<select id="report-window"><option value="24">24 hours</option><option value="168">7 days</option><option value="720">30 days</option><option value="4320">180 days</option></select></label><button id="load-report" class="primary">Load cost report</button></div>
    <p class="hint">Uses existing Google credentials. Scan-price equivalents are not your Cloud Billing invoice.</p>`;
  $("#load-report").onclick = () => costReport(Number($("#report-window").value));
}

async function docsPage() {
  setPage("docs"); detailKind = "docs"; const my = ++viewSeq; ++renderSeq;
  history.replaceState(null, "", "#docs");
  $("#detail").textContent = "Loading documentation…";
  try { const d = await api("/api/docs"); if (my === viewSeq) $("#detail").innerHTML = d.html; }
  catch (err) { if (my === viewSeq) $("#detail").textContent = err.message; }
}

function isRefreshing() { return ["pending", "running"].includes(refreshState.status); }

function progressHtml(s) {
  if (s.status === "idle") return "";
  const p = s.progress, active = ["pending", "running"].includes(s.status);
  const label = active ? (s.phase === "discovering" ? "Discovering datasets…" : "Refreshing metadata…") : `Refresh ${s.status}`;
  const finished = p?.datasets_done || 0, total = p?.datasets_total || 0;
  return `<strong class="status-${esc(s.status)}">${label}</strong>
    <progress aria-label="Metadata refresh progress" ${s.phase === "discovering" ? "" : `max="${Math.max(total, 1)}" value="${active ? finished : Math.max(total, 1)}"`}></progress>
    <div class="hint">${finished} / ${total} datasets finished${p?.total ? ` · ${num(p.done)} / ${num(p.total)} discovered metadata items processed` : ""}</div>
    ${s.message ? `<p class="err">${esc(s.message)}</p>` : ""}`;
}

function updateRefreshProgress() {
  $("#open-project").disabled = isRefreshing() || projectOpening;
  $("#refresh-banner").hidden = refreshState.status === "idle";
  $("#refresh-banner").innerHTML = progressHtml(refreshState) + '<button data-page="refresh">View refresh</button>';
  if (appPage !== "refresh" || !$("#refresh-progress")) return;
  $("#start-refresh").disabled = isRefreshing();
  if ($("#resume-import")) $("#resume-import").disabled = isRefreshing();
  const rows = [...(refreshState.projects || []).map(r => ({...r, target:r.project, progress:`${r.datasets} datasets discovered`})),
    ...(refreshState.datasets || []).map(r => ({...r, target:r.dataset, progress:`${r.done} / ${r.total} items`}))];
  $("#refresh-progress").innerHTML = refreshState.status === "idle" ? '<p class="hint">Choose a scope and start a refresh. Progress will remain visible while you browse other tabs.</p>' :
    `<div class="refresh-box">${progressHtml(refreshState)}<p class="hint">Mode: ${refreshState.inventory_only ? "inventory" : "full metadata"} · Include: ${esc((refreshState.filters?.include_datasets || []).join(", ") || "all datasets")} · Exclude: ${esc((refreshState.filters?.exclude_datasets || []).join(", ") || "none")} · Hidden datasets: ${refreshState.filters?.include_hidden ? "included" : "excluded"} · ${num((refreshState.excluded_datasets || []).length)} datasets excluded</p>${(refreshState.warnings || []).map(w => `<p class="hint">${esc(w)}</p>`).join("")}</div>
     <div class="scroll"><table><thead><tr><th>Project / dataset</th><th>Status</th><th>Progress</th></tr></thead><tbody>${rows.map(r => `<tr><td>${esc(r.target)}${r.message ? `<div class="err">${esc(r.message)}</div>` : ""}</td><td class="status-${esc(r.status)}">${esc(r.status)}</td><td>${esc(r.progress)}</td></tr>`).join("")}</tbody></table></div>`;
}

function refreshPage(project, dataset) {
  if (project || dataset) { selectedProjects = new Set(project ? [project] : []); selectedDatasets = new Set(dataset ? [dataset] : []); }
  setPage("refresh"); detailKind = "refresh"; ++viewSeq; ++renderSeq;
  history.replaceState(null, "", "#refresh");
  $("#detail").innerHTML = `<h2 class="overview-title">Refresh metadata</h2>
    <p class="desc">Import schemas, storage, partitions, clusters, routines, models and row-access policies. Recommendations and table insights are cached per project and location for 24 hours. Choose individual datasets or whole projects to discover accessible datasets, including new ones.</p>
    <p class="hint">Row samples are optional: load them on a table or use the CLI's --include-preview-rows flag. Including rows can add a material delay.</p>
    <p class="hint">Free metadata calls · uses existing Google credentials · no SQL runs. Failed datasets keep their previous snapshot. Column dry-run costs are recomputed separately.</p>
    <div class="selection-list">${hierarchy.map(p => `<div><label><input type="checkbox" data-select-project="${esc(p.project)}" ${selectedProjects.has(p.project) ? "checked" : ""}><span><b>${esc(p.project)}</b><br><small>Entire project · discover all accessible datasets</small></span></label>
      <details><summary class="hint">Choose from ${num(p.datasets.length)} saved datasets</summary>${p.datasets.map(d => `<label><input type="checkbox" data-select-dataset="${esc(d.dataset)}" ${selectedDatasets.has(d.dataset) ? "checked" : ""}><span>${esc(d.dataset_id)} <small>· ${num(d.tables)} tables</small></span></label>`).join("")}</details></div>`).join("")}</div>
    <div class="toolbar"><label>Other project IDs<input id="refresh-projects" placeholder="project-one, project-two"></label><label>Other datasets<input id="refresh-datasets" placeholder="project.dataset, project.other"></label><button id="start-refresh" class="primary">Start refresh</button></div>
    <div class="toolbar"><label>Import mode<select id="refresh-mode"><option value="full">Full metadata</option><option value="inventory">Inventory only · smaller catalog</option></select></label>
      <label>Include datasets<input id="refresh-include" placeholder="*prod*, project.dataset"></label>
      <label>Exclude datasets<input id="refresh-exclude" placeholder="stage*, *_scratch"></label>
      <label>Concurrent projects<input id="refresh-concurrency" type="number" min="1" max="4" value="1"></label></div>
    <p><label><input id="refresh-hidden" type="checkbox"> Include hidden datasets</label> ${refreshState.id ? '<button id="resume-import">Resume last import</button>' : ""}</p>
    <p class="hint">Patterns match dataset names or project.dataset. Exclusions win. Inventory omits schemas and view SQL until you load them on a table. Resume uses the same targets, filters and mode; completed datasets are skipped.</p>
    <div id="refresh-error" class="err" role="status"></div><div id="refresh-progress"></div>`;
  $("#start-refresh").onclick = startRefresh;
  if ($("#resume-import")) $("#resume-import").onclick = async () => {
    $("#resume-import").disabled = true;
    ++refreshGeneration;
    const my = viewSeq;
    try {
      refreshState = await api("/api/refresh", {projects:refreshState.requested_projects, datasets:refreshState.requested_datasets,
        inventory_only:!!refreshState.inventory_only, include_preview_rows:!!refreshState.include_preview_rows, ...(refreshState.filters || {}),
        project_concurrency:refreshState.project_concurrency || 1, resume:true});
      updateRefreshProgress(); pollRefresh();
    } catch (err) { if (my === viewSeq) { $("#refresh-error").textContent = err.message; $("#resume-import").disabled = false; } }
  };
  updateRefreshProgress();
}

async function reloadCatalog() {
  await Promise.all([loadCatalog(), loadStats()]);
  if (appPage !== "catalog") return;
  if (detailKind === "dataset") await openDataset(scopeDataset);
  else if (detailKind === "model" && current) await openModel(current);
  else { search(); if (detailKind === "table" && current) await open(current); }
}

async function startRefresh() {
  const split = s => s.split(/[\s,]+/).filter(Boolean);
  const projects = [...new Set([...selectedProjects, ...split($("#refresh-projects").value)])];
  const datasets = [...new Set([...selectedDatasets, ...split($("#refresh-datasets").value)])];
  if (!projects.length && !datasets.length) { $("#refresh-error").textContent = "Select a project or dataset to refresh."; return; }
  $("#start-refresh").disabled = true; $("#refresh-error").textContent = "";
  ++refreshGeneration;
  const my = viewSeq;
  try {
    refreshState = await api("/api/refresh", {projects, datasets,
      inventory_only: $("#refresh-mode").value === "inventory", include_datasets: split($("#refresh-include").value),
      exclude_datasets: split($("#refresh-exclude").value), include_hidden: $("#refresh-hidden").checked,
      project_concurrency: Number($("#refresh-concurrency").value), resume: false});
    updateRefreshProgress(); pollRefresh();
  } catch (err) { if (my === viewSeq) { $("#refresh-error").textContent = err.message; $("#start-refresh").disabled = false; } }
}

async function pollRefresh() {
  if (refreshPolling) return;
  clearTimeout(refreshTimer); refreshPolling = true;
  const generation = refreshGeneration;
  try {
    const state = await api("/api/refresh");
    if (generation !== refreshGeneration) return;
    refreshState = state;
    updateRefreshProgress();
    const imported = `${state.id}:${state.progress?.done || 0}`;
    if (isRefreshing() && importedProgress !== imported) {
      importedProgress = imported;
      await Promise.all([loadCatalog(), loadStats()]);
      if (appPage === "catalog") search();
    }
    if (!isRefreshing() && refreshState.id && completedRefresh !== refreshState.id) {
      completedRefresh = refreshState.id;
      await reloadCatalog();
    }
    if (isRefreshing()) refreshTimer = setTimeout(pollRefresh, 1000);
  } catch (err) {
    if (generation !== refreshGeneration) return;
    $("#refresh-banner").hidden = false;
    $("#refresh-banner").textContent = "Cannot read refresh progress: " + err.message + " Retrying…";
    refreshTimer = setTimeout(pollRefresh, 3000);
  } finally {
    refreshPolling = false;
    if (generation !== refreshGeneration) refreshTimer = setTimeout(pollRefresh, 0);
  }
}

function scopeQuery() {
  return (scopeProject ? "&project=" + encodeURIComponent(scopeProject) : "") +
    (scopeDataset ? "&dataset=" + encodeURIComponent(scopeDataset) : "");
}

function renderTree() {
  $("#tree").innerHTML = hierarchy.map(p => `<details class="tree-project" open>
    <summary>${esc(p.project)}</summary>
    <button class="tree-button${scopeProject === p.project && !scopeDataset ? " active" : ""}" data-project="${esc(p.project)}"><span>All datasets</span><small>${num(p.datasets.length)}</small></button>
    ${p.datasets.map(d => `<button class="tree-button dataset${scopeDataset === d.dataset ? " active" : ""}" data-dataset="${esc(d.dataset)}" title="${esc(d.dataset)}"><span>${esc(d.dataset_id)}</span><small>${num(d.tables)}</small></button>`).join("")}
    </details>`).join("") || '<p class="hint">No saved projects yet.</p>';
  $("#all-tables").classList.toggle("active", !scopeProject && !scopeDataset);
}

function selectScope(project = "", dataset = "") {
  scopeProject = project; scopeDataset = dataset;
  current = null; nextOffset = null; ++seq;
  $("#more").hidden = true;
  $("#list").textContent = "Loading tables…";
  $("#count").textContent = "";
  $("#scope").textContent = dataset || project || "All projects";
  $("#q").placeholder = dataset ? "Search this dataset…" : project ? "Search this project…" : "Search tables, columns, descriptions…  (/ to focus)";
  renderTree();
  search();
}

function crumbs(project, dataset) {
  return `<div class="breadcrumbs"><button data-project="">All projects</button>${project ? ` / <button data-project="${esc(project)}">${esc(project)}</button>` : ""}${dataset ? ` / <button data-dataset="${esc(dataset)}">${esc(dataset.slice(dataset.lastIndexOf(".") + 1))}</button>` : ""}</div>`;
}

function overview(project = "") {
  setPage("catalog");
  detailKind = "overview"; ++viewSeq; ++renderSeq;
  selectScope(project);
  history.replaceState(null, "", project ? "#project/" + encodeURIComponent(project) : "#");
  renderOverview();
}

function renderOverview() {
  const my = viewSeq, project = scopeProject;
  const p = hierarchy.find(p => p.project === scopeProject);
  const datasets = p ? p.datasets : [];
  $("#detail").innerHTML = `${crumbs(scopeProject)}<div class="eyebrow">Local catalog · free to browse</div>
    <h2 class="overview-title">${project ? esc(project) : "Explore your catalog"}</h2>
    <p class="desc">${p ? datasets.length ? `${num(datasets.length)} datasets · ${num(p.tables)} tables · ${num(p.routines)} routines · ${num(p.models || 0)} models. Choose a dataset to explore its tables and settings.` : "No accessible, non-hidden datasets were found during the last project discovery." : project ? "Datasets appear here as their imports finish. Follow progress in the sidebar or Refresh metadata tab." : "Enter a Google Cloud project ID to start importing its datasets, or browse a saved project below."}</p>
    <div class="toolbar">${p ? `<button class="primary" data-refresh-project="${esc(p.project)}">Refresh this project…</button>` : '<button class="primary" data-page="refresh">Refresh projects or datasets…</button>'}</div>
    <div class="cards">${project ? datasets.map(d => `<button class="catalog-card" data-dataset="${esc(d.dataset)}">
      <strong>${esc(d.dataset_id)}</strong><span class="hint">${num(d.tables)} tables · ${num(d.routines)} routines · ${num(d.models || 0)} models${d.location ? " · " + esc(d.location) : ""}</span>
      ${d.description ? `<p>${esc(d.description.slice(0, 180))}</p>` : ""}<span class="hint">${bytes(d.bytes)} · harvested ${ago(d.harvested_at)}</span></button>`).join("")
      : hierarchy.map(p => `<button class="catalog-card" data-project="${esc(p.project)}"><strong>${esc(p.project)}</strong><span class="hint">${num(p.datasets.length)} datasets · ${num(p.tables)} tables</span><p>Explore datasets →</p></button>`).join("")}</div>${project ? '<div id="project-recommendations"></div>' : ""}`;
  if (project) api("/api/recommendations?project=" + encodeURIComponent(project)).then(r => {
    if (my === viewSeq && detailKind === "overview" && scopeProject === project) $("#project-recommendations").innerHTML = recommendationsSection(r);
  }).catch(err => { if (my === viewSeq && detailKind === "overview") $("#project-recommendations").textContent = err.message; });
}

async function openDataset(dataset) {
  setPage("catalog");
  const project = dataset.slice(0, dataset.lastIndexOf(".")), my = ++viewSeq;
  detailKind = "dataset"; ++renderSeq;
  selectScope(project, dataset);
  history.replaceState(null, "", "#dataset/" + encodeURIComponent(dataset));
  $("#detail").textContent = "Loading dataset…";
  try {
    const d = await api("/api/dataset?dataset=" + encodeURIComponent(dataset));
    if (my !== viewSeq) return;
    const saved = hierarchy.find(p => p.project === project)?.datasets.find(d => d.dataset === dataset);
    const settings = [["Storage billing", d.storage_billing_model], ["Time travel", d.max_time_travel_hours == null ? null : d.max_time_travel_hours + " hours"],
      ["Default table expiry", d.default_table_expiration_ms == null ? null : num(d.default_table_expiration_ms / 864e5) + " days"],
      ["Default partition expiry", d.default_partition_expiration_ms == null ? null : num(d.default_partition_expiration_ms / 864e5) + " days"]].filter(([, v]) => v != null);
    $("#detail").innerHTML = `${crumbs(project)}<div class="eyebrow">Dataset · local snapshot</div><h2 class="overview-title">${esc(dataset.slice(dataset.lastIndexOf(".") + 1))}</h2>
      <div class="chips"><span class="chip">${num(d.tables)} tables</span><span class="chip">${num(d.routines.length)} routines</span>${d.location ? `<span class="chip">${esc(d.location)}</span>` : ""}${saved ? `<span class="chip" title="${esc(saved.harvested_at)}">harvested ${ago(saved.harvested_at)}</span>` : ""}</div>
      ${d.friendly_name ? `<p>${esc(d.friendly_name)}</p>` : ""}${d.description ? `<p class="desc">${esc(d.description)}</p>` : ""}${labelChips(d.labels)}
      <p class="desc">${d.tables ? "Choose a table from the list to inspect its schema and metadata. Search is scoped to this dataset." : "This dataset has no tables in its latest saved snapshot."}</p>
      <div class="toolbar"><button class="primary" data-refresh-dataset="${esc(dataset)}">Refresh this dataset…</button></div>
      ${settings.length ? `<h3>Dataset settings</h3><dl class="details">${settings.map(([k, v]) => `<dt>${k}</dt><dd>${esc(v)}</dd>`).join("")}</dl>` : ""}
      ${d.routines_status === "denied" ? '<p class="notes">Routine metadata was unavailable during the last harvest because access was denied.</p>' : ""}
      ${d.routines.length ? `<h3>Routines</h3><div class="scroll"><table><thead><tr><th>Name</th><th>Type</th><th>Language</th><th>Returns</th></tr></thead><tbody>${d.routines.map(r => `<tr><td class="path" title="${esc(r.signature)}">${esc(r.routine.slice(r.routine.lastIndexOf(".") + 1))}</td><td>${esc(r.type)}</td><td>${esc(r.language)}</td><td>${esc(r.returns)}</td></tr>`).join("")}</tbody></table></div>` : ""}
      <h3>Models <span class="hint">${num((d.models || []).length)} saved</span></h3>${metadataStatus(d.models_status, d.models_message)}
      ${(d.models || []).length ? `<table><thead><tr><th>Name</th><th>Type</th><th>Details</th></tr></thead><tbody>${d.models.map(m => `<tr><td><button data-model="${esc(m.fqn)}">${esc(m.model_id)}</button></td><td>${esc(m.type || "Unknown")}</td><td>${esc(m.metadata_status)}</td></tr>`).join("")}</tbody></table>` : d.models_status === "harvested" ? '<p class="hint">No models found in this dataset.</p>' : ""}
      ${recommendationsSection(d.recommendations)}`;
  } catch (err) { if (my === viewSeq) $("#detail").innerHTML = `<div class="err">${esc(err.message)}</div>`; }
}

async function loadCatalog() {
  const my = ++browseSeq;
  try {
    const result = await api("/api/browse");
    if (my !== browseSeq) return;
    hierarchy = result.projects;
    renderTree();
    if (detailKind === "overview") renderOverview();
  } catch (err) { if (my === browseSeq) $("#tree").textContent = err.message; }
}

async function search(offset = 0) {
  const q = $("#q").value.trim(), my = ++seq;
  $("#more").disabled = true;
  try {
  const page = await api("/api/search?q=" + encodeURIComponent(q) + "&offset=" + offset + scopeQuery());
  if (my !== seq) return;
  const rows = page.items;
  displayed = (offset ? displayed : 0) + rows.length;
  nextOffset = page.next_offset;
  $("#count").textContent = `${displayed} table${displayed === 1 ? "" : "s"}${nextOffset != null ? " shown · more available" : ""}, largest first`;
  const html = rows.map(r => {
    const parts = r.table.split("."), t = parts.pop(), d = parts.pop(), p = parts.join(".");
    const cols = r.matched_columns.slice(0, 4).map(c => hl(c, q)).join(", ") + (r.matched_column_count > 4 ? ` +${r.matched_column_count - 4}` : "");
    return `<div class="r${r.table === current ? " sel" : ""}" data-fqn="${esc(r.table)}">
      <div class="name"><span class="ds">${!scopeProject ? hl(p, q) + "." : ""}${!scopeDataset ? hl(d, q) + "." : ""}</span><b>${hl(t, q)}</b></div>
      <div class="meta"><span>${bytes(r.bytes)}</span><span>${num(r.rows)} rows</span><span>${r.type === "VIEW" ? "view" : "full scan " + usd(r.full_scan_usd)}</span><span>${ago(r.modified)}</span></div>
      ${cols ? `<div class="cols">${cols}</div>` : ""}</div>`;
  }).join("");
  if (offset) $("#list").insertAdjacentHTML("beforeend", html);
  else $("#list").innerHTML = html || `<div class="empty">${q ? `No tables match “${esc(q)}” in this scope.` : "No tables in this catalog scope."}</div>`;
  $("#more").hidden = nextOffset == null;
  } catch (err) {
    if (my === seq) $("#count").textContent = err.message;
  } finally {
    if (my === seq) $("#more").disabled = false;
  }
}

function depth(path) { return (path.match(/\./g) || []).length; }

function labelChips(labels) {
  const entries = Object.entries(labels || {});
  if (!entries.length) return "";
  const shown = entries.slice(0, 8).map(([k, v]) => `<span class="chip label">${esc(k)}${v ? "=" + esc(v) : ""}</span>`).join("");
  return `<div class="chips labels">${shown}${entries.length > 8 ? `<span class="hint">+${entries.length - 8} labels</span>` : ""}</div>`;
}

function metadataStatus(status, message) {
  if (status === "harvested") return "";
  const fallback = {denied:"Access denied; the contents are unknown.", partial:"Some details could not be read.",
    unavailable:"Metadata was unavailable during refresh.", quota:"The read quota was reached.",
    disabled:"The API is not enabled.", not_applicable:"Not supported for this table type.", pending:"Sample is loading in the background. Reload the catalog to see the latest saved rows.",
    not_requested:"This metadata was not requested. Run a full metadata refresh to collect it."};
  return `<p class="notes">${esc(message || fallback[status] || "Not collected yet. Refresh this dataset to collect it.")}</p>`;
}

function recommendationsSection(result) {
  const scopes = result?.scopes || [];
  return `<h3>Recommendations &amp; insights</h3><p class="hint">Partitioning and clustering suggestions from Google · saved locally for 24 hours · changes are never applied automatically.</p>
    ${!scopes.length ? '<p class="hint">Not collected yet. Refresh a dataset in this project to collect recommendations for its location.</p>' : scopes.map(s => `<details ${s.kind === "recommendations" ? "open" : ""}><summary>${esc(s.location)} · ${esc(s.kind)} · ${num(s.items.length)} saved${s.stale ? " · stale / incomplete" : ""}</summary>
      ${metadataStatus(s.status, s.message)}<p class="hint">Last attempt: ${esc(s.checked_at)}${s.fetched_at ? " · last successful fetch: " + esc(s.fetched_at) : " · no successful fetch"}</p>
      ${!s.items.length && s.status === "harvested" ? '<p class="hint">None found for this scope in the saved response. This is not a guarantee that the tables are fully optimized.</p>' : ""}
      ${s.items.map(item => `<article class="refresh-box"><strong>${esc(item.description || item.subtype || "Table insight")}</strong><p class="hint">${esc(item.state)} · source updated ${esc(item.last_updated || "unknown")}</p>
        ${(item.tables || []).map(t => `<button data-open-table="${esc(t)}">${esc(t)}</button>`).join(" ")}
        ${columnMetadata(item.details)}${item.primary_impact ? `<details><summary>Estimated impact</summary><pre>${esc(JSON.stringify(item.primary_impact, null, 2))}</pre><p class="hint">Google's estimate; actual savings depend on workload and pricing.</p></details>` : ""}</article>`).join("")}</details>`).join("")}`;
}

function policiesSection(t) {
  const policies = t.row_access_policies || [];
  return `<h3>Row-access policies</h3>${metadataStatus(t.row_access_policies_status, t.row_access_policies_message || (t.metadata_level === "inventory" ? "Row-access policies were not requested in inventory mode. Load full table metadata to collect them." : null))}
    ${t.row_access_policies_status === "harvested" && !policies.length ? '<p class="hint">No row-access policies found at refresh time.</p>' : ""}
    ${policies.map(p => `<details><summary>${esc(p.rowAccessPolicyReference?.policyId || "Row policy")}</summary><p class="hint">Modified ${esc(p.lastModifiedTime || "unknown")}</p><pre>${esc(p.filterPredicate || "Filter unavailable")}</pre></details>`).join("")}`;
}

function sampleSection(t) {
  const rows = t.preview_rows || [], keys = Object.keys(rows[0] || {});
  const cell = v => esc(typeof v === "object" && v !== null ? JSON.stringify(v) : v);
  return `${metadataStatus(t.preview_status || "not_requested", t.preview_message || ((!t.preview_status || t.preview_status === "not_requested") ? "Row samples are not included by default. Load a sample here, or import with --include-preview-rows (can add a material delay)." : null))}${t.preview_sampled_at ? `<p class="hint">Saved ${esc(t.preview_sampled_at)} · ${rows.length} rows · arbitrary order, not a random sample.</p>` : ""}
    ${t.preview_status === "harvested" && t.preview_message ? `<p class="hint">${esc(t.preview_message)}</p>` : ""}
    ${rows.length ? `<div class="scroll"><table><thead><tr>${keys.map(k => `<th>${esc(k)}</th>`).join("")}</tr></thead><tbody>${rows.map(r => `<tr>${keys.map(k => `<td title="${cell(r[k])}">${cell(r[k])}</td>`).join("")}</tr>`).join("")}</tbody></table></div>` : t.preview_status === "harvested" ? `<p class="hint">${t.preview_message ? "No rows fit within the saved sample limit." : "No rows returned."}</p>` : ""}`;
}

async function openModel(fqn) {
  setPage("catalog"); detailKind = "model"; const my = ++viewSeq; ++renderSeq;
  const dataset = fqn.slice(0, fqn.lastIndexOf(".")), project = dataset.slice(0, dataset.lastIndexOf("."));
  selectScope(project, dataset); current = fqn;
  history.replaceState(null, "", "#model/" + encodeURIComponent(fqn));
  $("#detail").textContent = "Loading model…";
  try {
    const m = await api("/api/model?fqn=" + encodeURIComponent(fqn));
    if (my !== viewSeq) return;
    const columns = (title, values) => values == null ? "" : `<h3>${title}</h3><table><thead><tr><th>Name</th><th>Type</th></tr></thead><tbody>${values.map(c => `<tr><td>${esc(c.name)}</td><td>${esc(c.type)}</td></tr>`).join("")}</tbody></table>`;
    $("#detail").innerHTML = `${crumbs(project, dataset)}<div class="eyebrow">Model · local snapshot</div><h2>${esc(fqn)}</h2><div class="chips"><span class="chip">${esc(m.type || "Unknown model type")}</span><span class="chip">${esc(m.location)}</span></div>
      ${metadataStatus(m.metadata_status, m.metadata_message)}${m.description ? `<p class="desc">${esc(m.description)}</p>` : ""}${labelChips(m.labels)}
      <dl class="details"><dt>Created</dt><dd>${esc(m.created || "Unknown")}</dd><dt>Modified</dt><dd>${esc(m.modified || "Unknown")}</dd><dt>Expires</dt><dd>${esc(m.expires || "Not specified")}</dd></dl>
      ${columns("Input features", m.feature_columns)}${columns("Labels / targets", m.label_columns)}${m.remote_info ? `<details><summary>Remote model metadata</summary><pre>${esc(JSON.stringify(m.remote_info, null, 2))}</pre></details>` : ""}
      <p class="hint">Metadata only. No training, evaluation or inference runs.</p><button data-refresh-dataset="${esc(dataset)}">Refresh this dataset…</button>`;
  } catch (err) { if (my === viewSeq) $("#detail").textContent = err.message; }
}

async function openProject(e) {
  e.preventDefault();
  const project = $("#project-id").value.trim();
  const my = viewSeq;
  if (!project) { $("#project-error").textContent = "Enter a project ID."; return; }
  projectOpening = true; $("#open-project").disabled = true; $("#project-error").textContent = "";
  ++refreshGeneration;
  try {
    refreshState = await api("/api/refresh", {projects:[project], datasets:[]});
    if (my === viewSeq) overview(project);
    updateRefreshProgress(); pollRefresh();
  } catch (err) { $("#project-error").textContent = err.message; }
  finally { projectOpening = false; $("#open-project").disabled = isRefreshing(); }
}

// Declared keys, storage and lineage. Missing values are unknown, so their rows are omitted.
function detailsSection(t) {
  const rows = [];
  if (t.primary_key) rows.push(["Primary key", esc(t.primary_key.join(", ")) + ' <span class="hint">declared, not enforced</span>']);
  for (const fk of t.foreign_keys || [])
    rows.push(["Foreign key", `${esc(fk.columns.map(c => c[0]).join(", "))} → ${esc(fk.references)} (${esc(fk.columns.map(c => c[1]).join(", "))})`]);
  const s = t.storage;
  if (s) {
    const part = (label, active, longTerm) => active != null || longTerm != null
      ? `${label} ${bytes((active || 0) + (longTerm || 0))} <span class="hint">(${bytes(active)} active, ${bytes(longTerm)} long-term)</span>` : "";
    const logical = part("logical", s.active_logical, s.long_term_logical);
    const physical = part("physical", s.active_physical, s.long_term_physical);
    const travel = s.time_travel_physical ? ` · time travel ${bytes(s.time_travel_physical)}` : "";
    if (logical || physical) rows.push(["Storage", [logical, physical].filter(Boolean).join(" · ") + travel]);
  }
  if (t.base_table) rows.push([t.base_kind === "SNAPSHOT" ? "Snapshot of" : "Clone of", esc(t.base_table)]);
  if (t.external) rows.push(["External source", esc([t.external.sourceFormat, ...(t.external.sourceUris || []).slice(0, 3)].filter(Boolean).join(" · "))]);
  if (t.partition_expiration_ms != null) rows.push(["Partition expiry", `${num(t.partition_expiration_ms / 864e5)} days`]);
  if (t.kms_key) rows.push(["Encryption", esc(t.kms_key)]);
  if (!rows.length) return "";
  return `<h3>Details</h3><dl class="details">${rows.map(([k, v]) => `<dt>${k}</dt><dd>${v}</dd>`).join("")}</dl>`;
}

function columnMetadata(metadata) {
  if (!metadata) return "";
  const labels = {maxLength:"max length", precision:"precision", scale:"scale", defaultValueExpression:"default", collation:"collation", roundingMode:"rounding", rangeElementType:"range element", generatedColumn:"generated column", dataPolicies:"data policies"};
  return Object.entries(metadata).map(([key, value]) => `<div><span class="hint">${esc(labels[key] || key)}:</span> ${esc(typeof value === "object" ? JSON.stringify(value) : value)}</div>`).join("");
}

function renderTable(t) {
  const view = viewSeq, render = ++renderSeq;
  const active = () => view === viewSeq && render === renderSeq && current === t.fqn;
  const maxScan = Math.max(1, ...t.columns.map(c => c.scan_bytes || 0));
  const hasCosts = t.columns.some(c => c.scan_bytes != null);
  const hasDesc = t.columns.some(c => c.description);  // public datasets rarely have any
  const hasMetadata = t.columns.some(c => c.metadata && Object.keys(c.metadata).length);
  const fullUsd = t.full_scan_usd;
  const tableDataset = t.dataset || t.fqn.slice(0, t.fqn.lastIndexOf("."));
  const tableProject = tableDataset.slice(0, tableDataset.lastIndexOf("."));
  $("#detail").innerHTML = `${crumbs(tableProject, tableDataset)}
    <h2>${esc(t.fqn)}</h2>
    <div class="chips">
      <span class="chip">${esc(t.type)}</span>
      <span class="chip num">${num(t.num_rows)} rows</span>
      <span class="chip num">${bytes(t.num_bytes)}</span>
      <span class="chip cost" title="US on-demand scan estimate including the billing minimum; excludes free tier and discounts">full scan ${usd(fullUsd)}</span>
      <span class="chip" title="${esc(t.modified)}">modified ${ago(t.modified)}</span>
      ${t.partitioning ? `<span class="chip">partitioned ${esc(t.partitioning)}${t.num_partitions != null ? ` · ${num(t.num_partitions)} partitions` : ""}</span>` : ""}
      ${t.require_partition_filter ? `<span class="chip cost">partition filter required</span>` : ""}
      ${t.clustering ? `<span class="chip">clustered ${esc(t.clustering)}</span>` : ""}
      ${t.location ? `<span class="chip">${esc(t.location)}</span>` : ""}
      ${t.expires ? `<span class="chip cost" title="${esc(t.expires)}">expires ${esc(t.expires.slice(0, 10))}</span>` : ""}
      <span class="chip free">from catalog · free</span>
    </div>
    ${labelChips(t.labels)}
    ${t.description ? `<p class="desc">${esc(t.description)}</p>` : ""}
    ${(t.notes || []).length ? `<ul class="notes">${t.notes.map(n => `<li>${esc(n)}</li>`).join("")}</ul>` : ""}
    ${t.metadata_level === "inventory" ? '<p class="notes">Inventory only: schemas and view SQL have not been loaded. Column searches do not cover this table.</p><button id="load-metadata">Load full table metadata · free</button><span id="metadata-error" class="err"></span>' : ""}
    ${detailsSection(t)}
    ${t.partition?.ingestion_time ? '<p class="notes">Ingestion-time partitioning uses the virtual _PARTITIONTIME column (and _PARTITIONDATE for daily partitions). It is not a stored schema column.</p>' : ""}
    ${t.partition?.range ? `<p class="hint">Partition range: ${esc(t.partition.range.start)} to ${esc(t.partition.range.end)} (exclusive), interval ${esc(t.partition.range.interval)}.</p>` : ""}

    <h3>Schema <span class="hint">${t.metadata_level === "inventory" ? "not loaded" : `${t.columns.length} column paths`}</span>
      ${t.type === "TABLE" && t.metadata_level !== "inventory" ? `<button id="costs">${hasCosts ? "Refresh" : "Compute"} column costs</button><span class="hint">one free dry run per top-level column</span>` : ""}</h3>
    ${(t.cost_warnings || []).map(w => `<p class="hint">${esc(w)}</p>`).join("")}
    ${receiptHtml(t.receipt)}
    <table><thead><tr><th>Column</th><th>Type</th><th>Mode</th><th>Scan cost</th>${hasDesc ? "<th>Description</th>" : ""}${hasMetadata ? "<th>Field metadata</th>" : ""}</tr></thead><tbody>
    ${t.columns.map(c => {
      const parts = c.path.split("."), leaf = parts.pop();
      const pre = parts.length ? `<span class="p">${esc(parts.join("."))}.</span>` : "";
      const bar = c.scan_bytes != null ? `<span class="bar" style="width:${Math.max(2, 120 * c.scan_bytes / maxScan)}px"></span><span class="num">${bytes(c.scan_bytes)}</span>` : "";
      const tag = (c.policy_tags || []).length ? ` <span class="tag" title="${esc(c.policy_tags.join("\n"))}">policy tag</span>` : "";
      const roles = [c.partition_column ? `partition · ${esc(t.partition?.kind || "")}` : "", c.clustering_order ? `cluster ${c.clustering_order}` : "", c.primary_key ? "primary key" : "", c.foreign_key ? "foreign key" : ""].filter(Boolean).map(r => `<span class="role-tag">${r}</span>`).join("");
      return `<tr><td class="path" style="padding-left:${8 + 14 * depth(c.path)}px">${pre}${esc(leaf)}${tag}${roles}</td><td class="type">${esc(c.type)}</td>
        <td class="type">${esc(c.mode)}</td><td class="cost">${bar}</td>${hasDesc ? `<td>${esc(c.description)}</td>` : ""}${hasMetadata ? `<td>${columnMetadata(c.metadata)}</td>` : ""}</tr>`;
    }).join("")}</tbody></table>

    <h3>Preview ${["TABLE", "SNAPSHOT"].includes(t.type) ? `<button id="prev">${t.preview_sampled_at ? "Refresh sample" : "Load 10 rows"}</button>` : ""}<span class="hint">saves up to 10 rows locally · tabledata.list · free</span></h3><div id="prevout">${sampleSection(t)}</div>
    ${policiesSection(t)}${recommendationsSection(t.recommendations)}

    <h3>Top queries <span class="hint">individual jobs referencing this table · free jobs API</span></h3>
    <div class="toolbar"><label>Last N days<input id="query-days" type="number" min="1" max="180" value="30"></label>
      <label>Job project<input id="query-project" placeholder="Configured job project"></label>
      <label>Visibility<select id="query-scope"><option value="user">My jobs</option><option value="project">All project jobs</option></select></label>
      <label>Rank by<select id="query-rank"><option value="billed_bytes">Billed bytes</option><option value="slot_ms">Slot time</option></select></label>
      <button id="load-queries">Load top queries</button></div>
    <p class="hint">Uses existing credentials. Searches the job project where queries ran, which may differ from the table's project. Project-wide visibility requires permission to see other users' job metadata.</p><div id="queries-out"></div>
    <h3>Cost guard <span class="hint">paste SQL, see what it would scan · dry run · free, nothing executes</span></h3>
    <textarea id="sql" spellcheck="false" maxlength="100000">SELECT *\nFROM \`${esc(t.fqn.replace(/\\/g, "\\\\").replace(/`/g, "\\`"))}\`\nLIMIT 10</textarea>
    <div><button id="est">Estimate</button></div>
    <div id="estout" class="est"></div>`;

  if ($("#load-metadata")) $("#load-metadata").onclick = async e => {
    e.target.disabled = true;
    try { const updated = await api("/api/load-metadata", {fqn:t.fqn}); if (active()) renderTable(updated); }
    catch (err) { if (active()) { $("#metadata-error").textContent = err.message; e.target.disabled = false; } }
  };
  $("#est").onclick = estimate;
  let querySeq = 0;
  const invalidateQueries = () => { ++querySeq; $("#queries-out").textContent = ""; $("#load-queries").disabled = false; };
  for (const id of ["query-days", "query-project", "query-scope", "query-rank"]) $("#" + id).oninput = invalidateQueries;
  $("#load-queries").onclick = async () => {
    const mine = ++querySeq, stillActive = () => active() && mine === querySeq;
    const days = Number($("#query-days").value);
    if (!Number.isInteger(days) || days < 1 || days > 180) { $("#queries-out").textContent = "Choose between 1 and 180 days."; return; }
    $("#load-queries").disabled = true; $("#queries-out").textContent = "Reading job history…";
    try {
      const r = await api("/api/table-queries", {table:t.fqn, days, project:$("#query-project").value.trim(), scope:$("#query-scope").value, rank_by:$("#query-rank").value});
      if (!stillActive()) return;
      $("#queries-out").innerHTML = `<p><b>${esc(r.project)}</b> · ${r.scope === "project" ? "all visible project jobs" : "your jobs"} · last ${r.days} days · ${num(r.jobs_scanned)} jobs scanned · ${num(r.matching_jobs)} matches${r.results_limited ? " (top " + r.jobs.length + " shown)" : ""}</p>
        ${r.truncated ? '<p class="notes">Partial history: ranking covers only the newest scanned jobs.</p>' : ""}
        ${r.jobs.length ? `<div class="scroll"><table><thead><tr><th>Created / query job</th><th>Billed bytes</th><th>Slot seconds</th><th>Details</th></tr></thead><tbody>${r.jobs.map((j, i) => `<tr><td>${esc(j.created)}<br><code>${esc(j.job_id)}</code><br><small>${esc(j.statement_type)}${j.cache_hit ? " · cached" : ""}</small></td><td>${bytes(j.billed_bytes)}</td><td>${j.slot_ms == null ? "—" : num(j.slot_ms / 1000)}</td><td><button data-query-job="${i}">Show SQL &amp; details</button></td></tr><tr><td colspan="4"><div id="query-job-${i}"></div></td></tr>`).join("")}</tbody></table></div>`
          : '<p class="hint">No matching jobs were found in the scanned history. This does not establish that the table was unused.</p>'}
        <details><summary class="hint">Coverage and interpretation</summary>${r.warnings.map(w => `<p class="hint">${esc(w)}</p>`).join("")}</details>${receiptHtml(r.receipt)}`;
      $("#queries-out").querySelectorAll("[data-query-job]").forEach(button => button.onclick = async () => {
        const j = r.jobs[Number(button.dataset.queryJob)]; button.disabled = true;
        try {
          const details = await api("/api/inspect-job", {job_id:j.job_id, project:j.project, location:j.location, include_sql:true});
          if (!stillActive()) return;
          $("#query-job-" + button.dataset.queryJob).innerHTML = `<pre>${esc(details.sql ?? "SQL is unavailable.")}</pre>${details.sql_truncated ? '<p class="hint">SQL truncated.</p>' : ""}${details.findings.map(f => `<p class="hint">${esc(f.signal)}: ${esc(f.next_step)}</p>`).join("")}${receiptHtml(details.receipt)}`;
        } catch (err) { if (stillActive()) { $("#query-job-" + button.dataset.queryJob).textContent = err.message; button.disabled = false; } }
      });
    } catch (err) { if (stillActive()) $("#queries-out").textContent = err.message; }
    finally { if (stillActive()) $("#load-queries").disabled = false; }
  };
  $("#sql").oninput = () => { ++estimateSeq; $("#estout").textContent = ""; };
  $("#sql").onkeydown = e => { if ((e.metaKey || e.ctrlKey) && e.key === "Enter") estimate(); };
  if ($("#costs")) $("#costs").onclick = async e => {
    e.target.disabled = true; e.target.textContent = "Dry-running…";
    try { const updated = await api("/api/column-costs", {fqn: t.fqn}); if (active()) renderTable(updated); }
    catch (err) { if (active()) { e.target.insertAdjacentHTML("afterend", `<span class="err">${esc(err.message)}</span>`); e.target.disabled = false; } }
  };
  if ($("#prev")) $("#prev").onclick = async e => {
    const previousLabel = e.target.textContent;
    e.target.disabled = true;
    e.target.textContent = "Loading sample…";
    try {
      const res = await api("/api/refresh-sample", {fqn: t.fqn});
      if (!active()) return;
      $("#prevout").innerHTML = sampleSection(res) + receiptHtml(res.receipt);
      e.target.textContent = res.preview_sampled_at ? "Refresh sample" : previousLabel;
    } catch (err) { if (active()) { $("#prevout").innerHTML = `<div class="err">${esc(err.message)}</div>`; e.target.textContent = previousLabel; } }
    if (active()) e.target.disabled = false;
  };
}

async function estimate() {
  const my = ++estimateSeq, render = renderSeq;
  const active = () => my === estimateSeq && render === renderSeq;
  const out = $("#estout"); out.innerHTML = "Dry-running…";
  try {
    const r = await api("/api/estimate", {sql: $("#sql").value});
    if (!active()) return;
    const price = r.usd_on_demand_range;
    const cost = !price ? "unknown" : price.min == null ? `up to ${usd(price.max)}` : price.min === price.max ? usd(price.max) : `${usd(price.min)}–${usd(price.max)}`;
    out.innerHTML = `<span class="big num">${r.bytes == null ? "Unknown scan size" : bytes(r.bytes)}</span> · ${esc(r.estimate_status)} · <b>${cost}</b>
      <span class="chip free">dry run · free</span><br><span class="hint">${esc(r.price_basis)}<br>
      Reported tables${r.referenced_tables_complete ? "" : " (may be incomplete)"}: ${r.referenced_tables.map(esc).join(", ") || "none reported"}<br>
      ${r.warnings.map(esc).join("<br>")}</span>${receiptHtml(r.receipt)}`;
  } catch (err) { if (active()) out.innerHTML = `<div class="err">${esc(err.message)}</div>`; }
}

async function open(fqn) {
  setPage("catalog");
  detailKind = "table";
  const my = ++viewSeq;
  ++renderSeq;
  current = fqn;
  document.querySelectorAll(".r").forEach(el => el.classList.toggle("sel", el.dataset.fqn === fqn));
  history.replaceState(null, "", "#" + encodeURIComponent(fqn));
  $("#detail").textContent = "Loading…";
  try { const t = await api("/api/table?fqn=" + encodeURIComponent(fqn)); if (my === viewSeq) renderTable(t); }
  catch (err) { if (my === viewSeq) $("#detail").innerHTML = `<div class="err">${esc(err.message)}</div>`; }
}

async function costReport(hours = 24) {
  setPage("report");
  history.replaceState(null, "", "#cost-report");
  detailKind = "report";
  const my = ++viewSeq;
  ++renderSeq;
  current = null;
  document.querySelectorAll(".r.sel").forEach(el => el.classList.remove("sel"));
  $("#detail").textContent = "Listing jobs…";
  try {
    const r = await api("/api/cost-report", {hours});
    if (my !== viewSeq) return;
    const windows = [[24, "24 hours"], [168, "7 days"], [720, "30 days"], [4320, "180 days"]];
    $("#detail").innerHTML = `<h2>Cost report · ${esc(r.project)}</h2>
      <div class="chips">${windows.map(([h, label]) => `<button data-hours="${h}"${h === r.hours ? " disabled" : ""}>${label}</button>`).join("")}</div>
      <div class="report-total num">${r.job_count} jobs · ${r.billed_bytes == null ? "billed bytes unknown" : `${bytes(r.billed_bytes)} billed`} · ${usd(r.usd_on_demand)}</div>
      <div class="hint">${esc(new Date(r.since).toLocaleString())} – ${esc(new Date(r.until).toLocaleString())}<br>${esc(r.source)}<br>${esc(r.price_basis)}${r.warnings.length ? "<br>" + r.warnings.map(esc).join("<br>") : ""}</div>
      <h3>Top offending queries</h3><div class="toolbar"><label>Compare<select id="offender-mode"><option value="groups">Repeated queries combined</option><option value="runs">Individual runs</option></select></label><label>Rank by<select id="offender-rank"><option value="bytes">Billed bytes / scan cost</option><option value="slots">Slot time</option><option value="runs">Execution count</option></select></label></div><p class="hint">Top 20 within the scanned history. Repeated queries match exact SQL, including formatting and literals. Slot time helps identify heavy capacity workloads; scan list prices are not your invoice.</p><div id="offenders"></div>
      ${r.jobs.length ? `<h3>Jobs</h3><p class="hint">Show query fetches SQL on demand using the free jobs API. It does not execute or save the query.</p><div class="scroll"><table><thead><tr><th>Created</th><th>Job</th><th>Type</th><th>State</th><th>Billed</th><th>List price</th><th>Cache</th><th>Query</th></tr></thead><tbody>
        ${r.jobs.map((j, i) => `<tr><td>${esc(new Date(j.created).toLocaleString())}</td><td class="path">${esc(j.job_id)}</td><td>${esc(j.statement_type || j.type)}</td><td>${esc(j.state)}</td>
          <td class="num">${bytes(j.billed_bytes)}</td><td class="num">${usd(j.usd_on_demand)}</td><td>${j.cache_hit ? "hit" : ""}</td><td>${j.type === "query" ? `<button data-report-query="${i}">Show query</button>` : "—"}</td></tr>${j.type === "query" ? `<tr id="report-query-row-${i}" hidden><td colspan="8"><div id="report-query-${i}"></div></td></tr>` : ""}`).join("")}</tbody></table></div>`
        : `<div class="receipt">✓ No jobs: nothing you ran in this project during this window could have been billed.</div>`}`;
    $("#detail").querySelectorAll("[data-hours]").forEach(b => b.onclick = () => costReport(Number(b.dataset.hours)));
    const rank = () => renderCostRanking(r, my);
    $("#offender-mode").onchange = rank; $("#offender-rank").onchange = rank; rank();
    $("#detail").querySelectorAll("[data-report-query]").forEach(b => {
      const i = Number(b.dataset.reportQuery), job = r.jobs[i];
      b.onclick = () => showReportQuery(job, i, b, my);
    });
  } catch (err) { if (my === viewSeq) $("#detail").innerHTML = `<div class="err">${esc(err.message)}</div>`; }
}

function renderCostRanking(report, view) {
  const groups = $("#offender-mode").value !== "runs";
  const rows = groups ? [...(report.query_groups || [])] : report.jobs.filter(j => j.type === "query" && !j.parent_job_id).map(j => ({
    representative:j, executions:1, known_billed_bytes:j.billed_bytes || 0, known_slot_ms:j.slot_ms || 0,
    unknown_billed:j.billed_bytes == null ? 1 : 0, unknown_slots:j.slot_ms == null ? 1 : 0, usd_on_demand:j.usd_on_demand}));
  const field = {slots:"known_slot_ms",runs:"executions"}[$("#offender-rank").value] || "known_billed_bytes";
  rows.sort((a,b) => b[field] - a[field]);
  const top = rows.slice(0,20), generation = ++renderSeq;
  $("#offenders").innerHTML = top.length ? `<div class="scroll"><table><thead><tr><th>Rank / representative job</th><th>Runs</th><th>Total billed</th><th>Scan list price</th><th>Slot seconds</th><th>SQL</th></tr></thead><tbody>${top.map((g,i) => `<tr><td>${i+1}. <code>${esc(g.representative.job_id)}</code>${groups && g.grouped === false ? '<br><small>SQL unavailable; ungrouped</small>' : ""}</td><td>${num(g.executions)}</td><td>${bytes(g.known_billed_bytes)}${g.unknown_billed ? ` + ${g.unknown_billed} unknown` : ""}</td><td>${usd(g.usd_on_demand)}</td><td>${num(g.known_slot_ms / 1000)}${g.unknown_slots ? ` + ${g.unknown_slots} unknown` : ""}</td><td><button data-offender="${i}">Show query</button></td></tr><tr id="report-query-row-top-${generation}-${i}" hidden><td colspan="6"><div id="report-query-top-${generation}-${i}"></div></td></tr>`).join("")}</tbody></table></div>` : '<p class="hint">No query jobs in the scanned history.</p>';
  $("#offenders").querySelectorAll("[data-offender]").forEach(button => {
    const i = Number(button.dataset.offender);
    button.onclick = () => showReportQuery(top[i].representative, `top-${generation}-${i}`, button, view);
  });
}

async function showReportQuery(job, index, button, view) {
  const active = () => view === viewSeq && appPage === "report";
  if (!active()) return;
  const row = $("#report-query-row-" + index), out = $("#report-query-" + index);
  if (button.dataset.loaded) {
    row.hidden = !row.hidden;
    button.textContent = row.hidden ? "Show query" : "Hide query";
    return;
  }
  row.hidden = false; button.disabled = true; out.textContent = "Loading query…";
  try {
    const result = await api("/api/inspect-job", {job_id:job.job_id, project:job.project, location:job.location, include_sql:true});
    if (!active()) return;
    out.innerHTML = `<pre>${esc(result.sql ?? "Query text is unavailable for this job.")}</pre>${result.sql_truncated ? '<p class="hint">Query text was truncated.</p>' : ""}${receiptHtml(result.receipt)}`;
    button.dataset.loaded = "true"; button.textContent = "Hide query";
  } catch (err) { if (active()) out.textContent = err.message; }
  finally { if (active()) button.disabled = false; }
}

let timer;
function navigatePage(page) {
  if (page === "catalog") overview(scopeProject);
  else if (page === "report") reportPage();
  else if (page === "refresh") refreshPage();
  else if (page === "docs") docsPage();
}
$("#app-menu").addEventListener("click", e => { const b = e.target.closest("[data-page]"); if (b) navigatePage(b.dataset.page); });
$("#refresh-banner").addEventListener("click", () => refreshPage());
$("#all-tables").onclick = () => overview();
function navigateCatalog(e) {
  const model = e.target.closest("[data-model]"), table = e.target.closest("[data-open-table]");
  if (model) return openModel(model.dataset.model);
  if (table) return open(table.dataset.openTable);
  const doc = e.target.closest("[data-doc-section]");
  if (doc) {
    document.querySelectorAll("[data-doc-content]").forEach(section => { section.hidden = section.dataset.docContent !== doc.dataset.docSection; });
    document.querySelectorAll("[data-doc-section]").forEach(button => { button.classList.toggle("primary", button === doc); });
    return;
  }
  const refreshProject = e.target.closest("[data-refresh-project]"), refreshDataset = e.target.closest("[data-refresh-dataset]"), page = e.target.closest("[data-page]");
  if (refreshProject) return refreshPage(refreshProject.dataset.refreshProject);
  if (refreshDataset) return refreshPage(null, refreshDataset.dataset.refreshDataset);
  if (page) return navigatePage(page.dataset.page);
  const dataset = e.target.closest("[data-dataset]"), project = e.target.closest("[data-project]");
  if (dataset) openDataset(dataset.dataset.dataset);
  else if (project) overview(project.dataset.project);
}
$("#tree").addEventListener("click", navigateCatalog);
$("#open-project-form").addEventListener("submit", openProject);
$("#detail").addEventListener("click", navigateCatalog);
$("#detail").addEventListener("change", e => {
  const project = e.target.dataset.selectProject, dataset = e.target.dataset.selectDataset;
  if (project) e.target.checked ? selectedProjects.add(project) : selectedProjects.delete(project);
  if (dataset) e.target.checked ? selectedDatasets.add(dataset) : selectedDatasets.delete(dataset);
});
$("#refresh-catalog").onclick = async e => {
  e.target.disabled = true;
  try { await reloadCatalog(); }
  finally { e.target.disabled = false; }
};
$("#q").addEventListener("input", () => { ++seq; nextOffset = null; $("#more").hidden = true; clearTimeout(timer); timer = setTimeout(search, 150); });
$("#more").onclick = () => { if (nextOffset != null) search(nextOffset); };
$("#list").addEventListener("click", e => { const r = e.target.closest(".r"); if (r) open(r.dataset.fqn); });
document.addEventListener("keydown", e => { if (appPage === "catalog" && e.key === "/" && document.activeElement.tagName !== "INPUT" && document.activeElement.tagName !== "TEXTAREA") { e.preventDefault(); $("#q").focus(); } });

function loadStats() { return api("/api/stats").then(s => { $("#stats").textContent = `${s.datasets} datasets · ${num(s.tables)} tables${s.routines ? ` · ${num(s.routines)} routines` : ""} · ${bytes(s.bytes)} · harvested ${ago(s.harvested_at)}`; })
  .catch(err => { $("#stats").textContent = err.message; });
}
loadStats();
const catalogReady = loadCatalog();
pollRefresh();
if (location.hash.startsWith("#project/")) overview(decodeURIComponent(location.hash.slice(9)));
else if (location.hash.startsWith("#dataset/")) openDataset(decodeURIComponent(location.hash.slice(9)));
else if (location.hash.startsWith("#model/")) openModel(decodeURIComponent(location.hash.slice(7)));
else if (location.hash === "#cost-report") reportPage();
else if (location.hash === "#docs") docsPage();
else if (location.hash === "#refresh") { setPage("refresh"); detailKind = "refresh"; const my = ++viewSeq; catalogReady.then(() => { if (my === viewSeq) refreshPage(); }); }
else { search(); if (location.hash.length > 1) open(decodeURIComponent(location.hash.slice(1))); }
