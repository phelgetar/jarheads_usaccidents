async function fetchText(url) {
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return await r.text();
}

async function loadTail() {
  const lines = document.getElementById("lines").value || "500";
  const text = await fetchText(`/logs/tail?lines=${encodeURIComponent(lines)}`);
  const box = document.getElementById("logBox");
  const atBottom = box.scrollTop + box.clientHeight >= box.scrollHeight - 5;
  box.textContent = text || "(empty)";
  if (atBottom) {
    box.scrollTop = box.scrollHeight;
  }
}

document.addEventListener("DOMContentLoaded", async () => {
  const btn = document.getElementById("refreshBtn");
  const auto = document.getElementById("autorefresh");

  btn?.addEventListener("click", loadTail);
  auto?.addEventListener("change", () => {
    // no-op; the interval watches this flag
  });

  await loadTail();

  setInterval(async () => {
    if (document.getElementById("autorefresh").checked) {
      await loadTail();
    }
  }, 5000);
});
