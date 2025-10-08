// Minimal UI helpers to show active count and a small latest table.
// Auto-refresh active count every 30 seconds.

async function fetchJSON(url) {
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return await r.json();
}

async function updateActiveCount() {
  try {
    const data = await fetchJSON("/incidents/active_count");
    const el = document.getElementById("activeCount");
    el.textContent = (data && typeof data.active_count === "number") ? data.active_count : "0";
  } catch (e) {
    console.error("active_count fetch failed:", e);
  }
}

function renderLatestTable(rows) {
  const root = document.getElementById("incidentsTable");
  if (!Array.isArray(rows) || rows.length === 0) {
    root.innerHTML = "<p>No incidents.</p>";
    return;
  }
  let html = `
    <table class="table">
      <thead>
        <tr>
          <th>UUID</th>
          <th>Route</th>
          <th>Dir</th>
          <th>Status</th>
          <th>Severity</th>
          <th>Updated</th>
        </tr>
      </thead>
      <tbody>
  `;
  for (const r of rows) {
    html += `
      <tr>
        <td>${r.uuid || ""}</td>
        <td>${r.route || ""}</td>
        <td>${r.direction || ""}</td>
        <td>${r.closure_status || ""}</td>
        <td>${r.severity_flag || ""} ${r.severity_score != null ? `(${r.severity_score})` : ""}</td>
        <td>${r.updated_time || ""}</td>
      </tr>
    `;
  }
  html += "</tbody></table>";
  root.innerHTML = html;
}

async function loadLatest() {
  try {
    const data = await fetchJSON("/incidents/latest?limit=25");
    renderLatestTable(data);
  } catch (e) {
    console.error("latest fetch failed:", e);
    document.getElementById("incidentsTable").innerHTML = "<p>Error loading incidents.</p>";
  }
}

document.addEventListener("DOMContentLoaded", () => {
  updateActiveCount();
  loadLatest();

  setInterval(updateActiveCount, 30000); // 30s
  const btn = document.getElementById("refreshBtn");
  if (btn) btn.addEventListener("click", () => {
    updateActiveCount();
    loadLatest();
  });
});
