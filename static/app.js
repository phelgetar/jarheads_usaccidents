// Web UI logic: load facets, build filters, run search, render table, and show live active count.

async function fetchJSON(url) {
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return await r.json();
}

function setActiveCount(n) {
  const el = document.getElementById("activeCount");
  el.textContent = (typeof n === "number") ? n : "0";
}

async function updateActiveCount() {
  try {
    const data = await fetchJSON("/incidents/active_count");
    setActiveCount(data.active_count ?? 0);
  } catch (e) {
    console.error("active_count fetch failed:", e);
  }
}

function ensureOption(selectEl, text) {
  const opt = document.createElement("option");
  opt.value = text;
  opt.textContent = text;
  selectEl.appendChild(opt);
}

function fillSelect(selectId, values) {
  const el = document.getElementById(selectId);
  el.innerHTML = "";
  if (Array.isArray(values) && values.length > 0) {
    values.forEach(v => ensureOption(el, String(v)));
  }
}

function isoLocal(dtLocalValue) {
  // Convert <input type="datetime-local"> value to ISO string with Z
  if (!dtLocalValue) return null;
  const d = new Date(dtLocalValue);
  if (Number.isNaN(+d)) return null;
  return d.toISOString();
}

function collectFilters() {
  const qs = new URLSearchParams();

  const multiIds = [
    ["state", "f_state"], ["county", "f_county"], ["route", "f_route"],
    ["route_class", "f_route_class"], ["direction", "f_direction"],
    ["event_type", "f_event_type"], ["closure_status", "f_closure_status"],
    ["severity_flag", "f_severity_flag"],
  ];
  for (const [name, id] of multiIds) {
    const sel = document.getElementById(id);
    if (!sel) continue;
    const values = Array.from(sel.selectedOptions).map(o => o.value).filter(Boolean);
    values.forEach(v => qs.append(name, v));
  }

  const sevmin = document.getElementById("f_sevmin").value;
  const sevmax = document.getElementById("f_sevmax").value;
  if (sevmin) qs.set("severity_score_min", sevmin);
  if (sevmax) qs.set("severity_score_max", sevmax);

  const upd = isoLocal(document.getElementById("f_updated_since").value);
  const rep = isoLocal(document.getElementById("f_reported_since").value);
  if (upd) qs.set("updated_since", upd);
  if (rep) qs.set("reported_since", rep);

  const activeOnly = document.getElementById("f_active_only").checked;
  if (activeOnly) qs.set("active_only", "true");

  const order = document.getElementById("f_order").value || "updated_time_desc";
  const limit = document.getElementById("f_limit").value || "200";
  qs.set("order", order);
  qs.set("limit", limit);

  return qs;
}

function renderLatestTable(items) {
  const root = document.getElementById("incidentsTable");
  if (!Array.isArray(items) || items.length === 0) {
    root.innerHTML = "<p>No incidents match your filters.</p>";
    return;
  }
  let html = `
    <table class="table">
      <thead>
        <tr>
          <th>UUID</th>
          <th>State</th>
          <th>Route</th>
          <th>Class</th>
          <th>Dir</th>
          <th>Status</th>
          <th>Active</th>
          <th>Severity</th>
          <th>Updated</th>
        </tr>
      </thead>
      <tbody>
  `;
  for (const r of items) {
    html += `
      <tr>
        <td>${r.uuid || ""}</td>
        <td>${r.state || ""}</td>
        <td>${r.route || ""}</td>
        <td>${r.route_class || ""}</td>
        <td>${r.direction || ""}</td>
        <td>${r.closure_status || ""}</td>
        <td>${r.is_active === true ? "Yes" : (r.is_active === false ? "No" : "")}</td>
        <td>${r.severity_flag || ""}${r.severity_score != null ? ` (${r.severity_score})` : ""}</td>
        <td>${r.updated_time || ""}</td>
      </tr>
    `;
  }
  html += "</tbody></table>";
  root.innerHTML = html;
}

async function loadFacets() {
  const f = await fetchJSON("/incidents/facets");
  fillSelect("f_state", f.state);
  fillSelect("f_county", f.county);
  fillSelect("f_route", f.route);
  fillSelect("f_route_class", f.route_class);
  fillSelect("f_direction", f.direction);
  fillSelect("f_event_type", f.event_type);
  fillSelect("f_closure_status", f.closure_status);
  fillSelect("f_severity_flag", f.severity_flag);
}

async function runSearch() {
  const qs = collectFilters();
  const url = `/incidents/search?${qs.toString()}`;
  const data = await fetchJSON(url);
  document.getElementById("resTotal").textContent = data.total ?? 0;
  document.getElementById("resCount").textContent = data.count ?? 0;
  document.getElementById("queryEcho").textContent = url;
  renderLatestTable(data.items || []);
}

function resetFilters() {
  ["f_state","f_county","f_route","f_route_class","f_direction","f_event_type","f_closure_status","f_severity_flag"]
    .forEach(id => {
      const el = document.getElementById(id);
      if (el) Array.from(el.options).forEach(o => o.selected = false);
    });
  document.getElementById("f_sevmin").value = "";
  document.getElementById("f_sevmax").value = "";
  document.getElementById("f_updated_since").value = "";
  document.getElementById("f_reported_since").value = "";
  document.getElementById("f_active_only").checked = true;
  document.getElementById("f_order").value = "updated_time_desc";
  document.getElementById("f_limit").value = "200";
}

document.addEventListener("DOMContentLoaded", async () => {
  try {
    await loadFacets();
  } catch (e) {
    console.error("facets load failed:", e);
  }

  updateActiveCount();
  setInterval(updateActiveCount, 30000); // 30s

  await runSearch();

  const applyBtn = document.getElementById("applyBtn");
  const resetBtn = document.getElementById("resetBtn");
  const refreshBtn = document.getElementById("refreshBtn");

  applyBtn?.addEventListener("click", runSearch);
  refreshBtn?.addEventListener("click", async () => {
    await updateActiveCount();
    await runSearch();
  });
  resetBtn?.addEventListener("click", async () => {
    resetFilters();
    await runSearch();
  });
});
