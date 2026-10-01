/** Local-first application tracker (Tier 2). Persists in localStorage. */
(function (global) {
  const KEY = "dataforge_app_tracker_v1";

  function load() {
    try {
      return JSON.parse(localStorage.getItem(KEY) || "[]");
    } catch (_) {
      return [];
    }
  }

  function save(rows) {
    localStorage.setItem(KEY, JSON.stringify(rows));
  }

  function upsert(job, status) {
    const rows = load();
    const id = String(job.job_id || job.id || "");
    if (!id) return rows;
    const idx = rows.findIndex((r) => r.job_id === id);
    const row = {
      job_id: id,
      title: job.title || "",
      company: job.company || "",
      url: job.job_url || job.url || "",
      status: status || "saved",
      updated_at: new Date().toISOString(),
    };
    if (idx >= 0) rows[idx] = { ...rows[idx], ...row };
    else rows.unshift(row);
    save(rows.slice(0, 200));
    return rows;
  }

  function remove(jobId) {
    const rows = load().filter((r) => r.job_id !== String(jobId));
    save(rows);
    return rows;
  }

  const STATUSES = ["saved", "applied", "interview", "offer", "rejected"];

  function render(el) {
    if (!el) return;
    const rows = load();
    if (!rows.length) {
      el.innerHTML = '<p class="df-text-muted">No saved applications yet.</p>';
      return;
    }
    el.innerHTML = rows
      .map((r) => {
        const opts = STATUSES.map(
          (s) =>
            `<option value="${s}"${s === r.status ? " selected" : ""}>${s}</option>`
        ).join("");
        return (
          `<div class="df-tracker-row" data-id="${escapeHtml(r.job_id)}" style="margin-bottom:0.5rem">` +
          `<strong>${escapeHtml(r.title)}</strong> — ${escapeHtml(r.company)}` +
          ` <select class="df-select" data-status="${escapeHtml(r.job_id)}" style="display:inline;width:auto">${opts}</select>` +
          (r.url ? ` <a href="${escapeHtml(r.url)}" target="_blank" rel="noopener">open</a>` : "") +
          ` <button type="button" class="df-chip" data-remove="${escapeHtml(r.job_id)}">remove</button>` +
          `</div>`
        );
      })
      .join("");
    el.querySelectorAll("[data-remove]").forEach((btn) => {
      btn.addEventListener("click", () => {
        remove(btn.getAttribute("data-remove"));
        render(el);
      });
    });
    el.querySelectorAll("[data-status]").forEach((sel) => {
      sel.addEventListener("change", () => {
        const id = sel.getAttribute("data-status");
        const row = load().find((r) => r.job_id === id);
        if (row) upsert(row, sel.value);
        render(el);
      });
    });
  }

  function escapeHtml(s) {
    return String(s || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function bindSaveButtons(root) {
    (root || document).querySelectorAll("[data-track-job]").forEach((btn) => {
      btn.addEventListener("click", () => {
        let job = {};
        try {
          job = JSON.parse(btn.getAttribute("data-track-job") || "{}");
        } catch (_) {}
        upsert(job, btn.getAttribute("data-track-status") || "saved");
        const panel = document.getElementById("app-tracker-panel");
        if (panel) render(panel);
        btn.textContent = "Saved ✓";
      });
    });
  }

  global.DataForgeTracker = { load, save, upsert, remove, render, bindSaveButtons };
})(window);
