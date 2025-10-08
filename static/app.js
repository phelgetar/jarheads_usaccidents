async function fetchIncidents(params) {
  const qs = new URLSearchParams(params);
  const res = await fetch(`/api/incidents?${qs}`);
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}`);
  }
  return await res.json();
}

function boolToYesNo(v) { return v ? "Yes" : (v === false ? "No" : ""); }

function renderRows(items) {
  const tbody = document.getElementById("rows");
  tbody.innerHTML = "";
  for (const it of items) {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${it.uuid || ""}</td>
      <td>${it.event_type || ""}</td>
      <td>${it.state || ""}</td>
      <td>${it.route || ""}</td>
      <td>${it.direction || ""}</td>
      <td>${boolToYesNo(it.is_active)}</td>
      <td>${it.updated_time || ""}</td>
    `;
    tbody.appendChild(tr);
  }
}

function readForm(form) {
  const data = new FormData(form);
  const obj = {};
  for (const [k,v] of data.entries()) {
    if (v !== "") obj[k] = v;
  }
  return obj;
}

async function init() {
  const form = document.getElementById("filterForm");
  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    const params = readForm(form);
    try {
      const items = await fetchIncidents(params);
      renderRows(items);
    } catch (err) {
      alert("Failed to fetch incidents: " + err.message);
    }
  });

  // initial load
  form.dispatchEvent(new Event("submit"));
}

window.addEventListener("DOMContentLoaded", init);
