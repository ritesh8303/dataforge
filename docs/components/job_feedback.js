/**
 * Local seeker feedback: closed / spam / wrong location.
 * Stored in localStorage only — feeds future filter denylists when exported.
 */
(function (global) {
  const KEY = "dataforge.job_feedback.v1";

  function load() {
    try {
      return JSON.parse(localStorage.getItem(KEY) || "{}");
    } catch (_) {
      return {};
    }
  }

  function save(map) {
    localStorage.setItem(KEY, JSON.stringify(map));
  }

  function record(jobId, reason, meta) {
    if (!jobId) return;
    const map = load();
    map[jobId] = {
      reason: String(reason || "other"),
      at: new Date().toISOString(),
      title: meta?.title || "",
      company: meta?.company || "",
      url: meta?.url || "",
    };
    save(map);
    return map[jobId];
  }

  function isHidden(jobId) {
    const row = load()[jobId];
    return Boolean(row && ["closed", "spam", "wrong_location", "not_early_career"].includes(row.reason));
  }

  function exportJson() {
    return JSON.stringify(load(), null, 2);
  }

  function bindButtons(root) {
    const el = root || document;
    el.querySelectorAll("[data-feedback-job]").forEach((btn) => {
      if (btn.dataset.boundFeedback) return;
      btn.dataset.boundFeedback = "1";
      btn.addEventListener("click", (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        const jobId = btn.getAttribute("data-feedback-job");
        const reason = btn.getAttribute("data-feedback-reason") || "closed";
        let meta = {};
        try {
          meta = JSON.parse(btn.getAttribute("data-feedback-meta") || "{}");
        } catch (_) {}
        record(jobId, reason, meta);
        const card = btn.closest("article.job-card");
        if (card) {
          card.style.opacity = "0.45";
          card.querySelector(".trust-feedback-note")?.remove();
          const note = document.createElement("div");
          note.className = "trust-feedback-note";
          note.style.cssText = "font-size:0.72rem;color:var(--df-text-muted);margin-top:0.35rem";
          note.textContent = "Marked " + reason.replaceAll("_", " ") + " (saved locally). Hidden on next filter.";
          card.appendChild(note);
        }
      });
    });
  }

  global.DataForgeFeedback = { record, isHidden, exportJson, bindButtons, load };
})(window);
