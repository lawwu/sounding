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

let current = null, seq = 0, viewSeq = 0, renderSeq = 0, estimateSeq = 0;
let nextOffset = null, displayed = 0;

async function search(offset = 0) {
  const q = $("#q").value.trim(), my = ++seq;
  $("#more").disabled = true;
  try {
  const page = await api("/api/search?q=" + encodeURIComponent(q) + "&offset=" + offset);
  if (my !== seq) return;
  const rows = page.items;
  displayed = (offset ? displayed : 0) + rows.length;
  nextOffset = page.next_offset;
  $("#count").textContent = `${displayed} table${displayed === 1 ? "" : "s"}${nextOffset != null ? " shown · more available" : ""}, largest first`;
  const html = rows.map(r => {
    const [p, d, t] = r.table.split(".");
    const cols = r.matched_columns.slice(0, 4).map(c => hl(c, q)).join(", ") + (r.matched_column_count > 4 ? ` +${r.matched_column_count - 4}` : "");
    return `<div class="r${r.table === current ? " sel" : ""}" data-fqn="${esc(r.table)}">
      <div class="name"><span class="ds">${hl(d, q)}.</span><b>${hl(t, q)}</b></div>
      <div class="meta"><span>${bytes(r.bytes)}</span><span>${num(r.rows)} rows</span><span>${r.type === "VIEW" ? "view" : "full scan " + usd(r.full_scan_usd)}</span><span>${ago(r.modified)}</span></div>
      ${cols ? `<div class="cols">${cols}</div>` : ""}</div>`;
  }).join("");
  if (offset) $("#list").insertAdjacentHTML("beforeend", html);
  else $("#list").innerHTML = html || `<div class="empty">No tables match “${esc(q)}”.</div>`;
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

function renderTable(t) {
  const view = viewSeq, render = ++renderSeq;
  const active = () => view === viewSeq && render === renderSeq && current === t.fqn;
  const maxScan = Math.max(1, ...t.columns.map(c => c.scan_bytes || 0));
  const hasCosts = t.columns.some(c => c.scan_bytes != null);
  const hasDesc = t.columns.some(c => c.description);  // public datasets rarely have any
  const fullUsd = t.full_scan_usd;
  $("#detail").innerHTML = `
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
    ${detailsSection(t)}

    <h3>Schema <span class="hint">${t.columns.length} column paths</span>
      ${t.type === "TABLE" ? `<button id="costs">${hasCosts ? "Refresh" : "Compute"} column costs</button><span class="hint">one free dry run per top-level column</span>` : ""}</h3>
    <table><thead><tr><th>Column</th><th>Type</th><th>Mode</th><th>Scan cost</th>${hasDesc ? "<th>Description</th>" : ""}</tr></thead><tbody>
    ${t.columns.map(c => {
      const parts = c.path.split("."), leaf = parts.pop();
      const pre = parts.length ? `<span class="p">${esc(parts.join("."))}.</span>` : "";
      const bar = c.scan_bytes != null ? `<span class="bar" style="width:${Math.max(2, 120 * c.scan_bytes / maxScan)}px"></span><span class="num">${bytes(c.scan_bytes)}</span>` : "";
      const tag = (c.policy_tags || []).length ? ` <span class="tag" title="${esc(c.policy_tags.join("\n"))}">policy tag</span>` : "";
      return `<tr><td class="path" style="padding-left:${8 + 14 * depth(c.path)}px">${pre}${esc(leaf)}${tag}</td><td class="type">${esc(c.type)}</td>
        <td class="type">${esc(c.mode)}</td><td class="cost">${bar}</td>${hasDesc ? `<td>${esc(c.description)}</td>` : ""}</tr>`;
    }).join("")}</tbody></table>

    ${t.type === "TABLE" ? `<h3>Preview <button id="prev">Load 10 rows</button><span class="hint">tabledata.list · free</span></h3><div id="prevout"></div>` : ""}

    <h3>Cost guard <span class="hint">paste SQL, see what it would scan · dry run · free, nothing executes</span></h3>
    <textarea id="sql" spellcheck="false" maxlength="100000">SELECT *\nFROM \`${esc(t.fqn.replace(/\\/g, "\\\\").replace(/`/g, "\\`"))}\`\nLIMIT 10</textarea>
    <div><button id="est">Estimate</button></div>
    <div id="estout" class="est"></div>`;

  $("#est").onclick = estimate;
  $("#sql").oninput = () => { ++estimateSeq; $("#estout").textContent = ""; };
  $("#sql").onkeydown = e => { if ((e.metaKey || e.ctrlKey) && e.key === "Enter") estimate(); };
  if ($("#costs")) $("#costs").onclick = async e => {
    e.target.disabled = true; e.target.textContent = "Dry-running…";
    try { const updated = await api("/api/column-costs", {fqn: t.fqn}); if (active()) renderTable(updated); }
    catch (err) { if (active()) { e.target.insertAdjacentHTML("afterend", `<span class="err">${esc(err.message)}</span>`); e.target.disabled = false; } }
  };
  if ($("#prev")) $("#prev").onclick = async e => {
    e.target.disabled = true;
    try {
      const rows = await api("/api/preview", {fqn: t.fqn});
      if (!active()) return;
      const keys = Object.keys(rows[0] || {});
      const cell = v => esc(typeof v === "object" && v !== null ? JSON.stringify(v) : v);
      $("#prevout").innerHTML = rows.length ? `<div class="scroll"><table><thead><tr>${keys.map(k => `<th>${esc(k)}</th>`).join("")}</tr></thead>
        <tbody>${rows.map(r => `<tr>${keys.map(k => `<td title="${cell(r[k])}">${cell(r[k])}</td>`).join("")}</tr>`).join("")}</tbody></table></div>` : `<div class="empty">No rows.</div>`;
    } catch (err) { if (active()) $("#prevout").innerHTML = `<div class="err">${esc(err.message)}</div>`; }
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
      ${r.warnings.map(esc).join("<br>")}</span>`;
  } catch (err) { if (active()) out.innerHTML = `<div class="err">${esc(err.message)}</div>`; }
}

async function open(fqn) {
  const my = ++viewSeq;
  ++renderSeq;
  current = fqn;
  document.querySelectorAll(".r").forEach(el => el.classList.toggle("sel", el.dataset.fqn === fqn));
  history.replaceState(null, "", "#" + encodeURIComponent(fqn));
  $("#detail").textContent = "Loading…";
  try { const t = await api("/api/table?fqn=" + encodeURIComponent(fqn)); if (my === viewSeq) renderTable(t); }
  catch (err) { if (my === viewSeq) $("#detail").innerHTML = `<div class="err">${esc(err.message)}</div>`; }
}

let timer;
$("#q").addEventListener("input", () => { ++seq; nextOffset = null; $("#more").hidden = true; clearTimeout(timer); timer = setTimeout(search, 150); });
$("#more").onclick = () => { if (nextOffset != null) search(nextOffset); };
$("#list").addEventListener("click", e => { const r = e.target.closest(".r"); if (r) open(r.dataset.fqn); });
document.addEventListener("keydown", e => { if (e.key === "/" && document.activeElement.tagName !== "INPUT" && document.activeElement.tagName !== "TEXTAREA") { e.preventDefault(); $("#q").focus(); } });

api("/api/stats").then(s => { $("#stats").textContent = `${s.datasets} datasets · ${num(s.tables)} tables${s.routines ? ` · ${num(s.routines)} routines` : ""} · ${bytes(s.bytes)} · harvested ${ago(s.harvested_at)}`; })
  .catch(err => { $("#stats").textContent = err.message; });
search();
if (location.hash) open(decodeURIComponent(location.hash.slice(1)));
