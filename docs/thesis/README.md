# UE thesis — DataForge

**Programme:** M.Sc. Data Science, University of Europe for Applied Sciences, Potsdam  
**Author:** Ritesh Rakesh Jadhav  
**Reference implementation:** this repository (live lakehouse on AWS).

Aligned to Prof.\ Dr.\ Rand Kouatly, SS 2026: Master Colloquium, Writing Good thesis, Thesis Structure.

---

## Document index

| Document | Use |
|---|---|
| **[latex/overleaf_main.tex](latex/overleaf_main.tex)** | **Binding draft** — Overleaf, pdfLaTeX, UE layout + Harvard |
| [UE_REQUIREMENTS.md](UE_REQUIREMENTS.md) | Rules extracted from the three lecture PDFs |
| [COLLOQUIUM_OUTLINE.md](COLLOQUIUM_OUTLINE.md) | 10-minute exam slide outline (planning) |
| **[COLLOQUIUM_SLIDES.md](COLLOQUIUM_SLIDES.md)** | **Speaker-ready deck** — 10 slides, notes, Q&A backups, timing table |
| **[latex/colloquium_slides.tex](latex/colloquium_slides.tex)** | **Beamer PDF slides** — 16:9, pdfLaTeX on Overleaf |
| **[ADMIN_CHECKLIST.md](ADMIN_CHECKLIST.md)** | **Manual admin steps** — supervisors, CampusNet, declaration, character count |
| [THESIS_PROPOSAL.md](THESIS_PROPOSAL.md) | Registration / supervisor proposal |
| [THESIS_EXPOSE.md](THESIS_EXPOSE.md) | Short email exposé |
| [RESULTS_DRAFT.md](RESULTS_DRAFT.md) | Living measured RQ tables |
| **[PACKAGING.md](PACKAGING.md)** | **CV bullets, Loom links, honesty checklist** |
| [THESIS_PAPER.md](THESIS_PAPER.md) | Older ARM-style content draft (superseded as structure; numbers still valid) |
| **[latex/ieee_short_paper.tex](latex/ieee_short_paper.tex)** | Optional 4–8 page IEEE conference stub |
| [../DEMO_SCRIPT.md](../DEMO_SCRIPT.md) | 2–3 min product demo (not the colloquium) |
| [../TECHNICAL_GUIDE_EASY_ENGLISH.md](../TECHNICAL_GUIDE_EASY_ENGLISH.md) | Plain-English architecture guide (portfolio / onboarding) |

---

## Automated in this repo vs you must still do

### Done here (automated / generated artefacts)

| Item | Status |
|------|--------|
| UE chapter LaTeX structure + Band-6 English body draft | `latex/overleaf_main.tex` + chapters |
| Colloquium 10-slide Markdown deck with speaker notes & Q&A | `COLLOQUIUM_SLIDES.md` |
| Beamer slide PDF source (16:9) | `latex/colloquium_slides.tex` |
| IEEE short-paper stub (abstract → conclusion + results table) | `latex/ieee_short_paper.tex` |
| Admin checklist + supervisor email template | `ADMIN_CHECKLIST.md` |
| RQ1 architecture + integration evidence | code + thesis Chapter Results; nightly OpenAI enrich live |
| RQ2 live OpenAI Pareto | `evals/results/rq2_pareto.json` — gpt-4o-mini 40/40, ~$0.0011 |
| Rules-only RQ2 baseline scripts | `evals/run_rq2_pareto.py` |
| RQ3 matching numbers (packaging dense 0.217 vs wizard 0.146 on **103×96**) | `RESULTS_DRAFT.md`, `evals/results/matching_eval.json` |
| CI local-tfidf nDCG pin (dense 0.1258) | `evals/baselines/ndcg_baseline.json` |
| RQ4 modelled unit cost (~€0.10 / 1k jobs OpenAI-first) | `evals/results/roi_report.json` |
| Error / ablation analysis | `evals/results/error_analysis.md` |
| Architecture + RQ3 figures | `latex/figures/` (PNG + SVG) |
| Expanded literature + DSR methodology + discussion | `latex/chapters/` (~74k+ chars w/o spaces target band) |
| List of abbreviations / tables / figures in LaTeX | `latex/overleaf_main.tex` |

### You must still do manually

| Item | Where to track |
|------|----------------|
| **Email supervisors** (Erst + Zweit) with proposal PDF | [ADMIN_CHECKLIST.md](ADMIN_CHECKLIST.md) — sample email included |
| **Paper form signatures** + **CampusNet registration** (title frozen) | ADMIN_CHECKLIST + private `D:\JOB\ue-coordination\` |
| **Statutory declaration** — hand-sign bound copy | Wording in `latex/overleaf_main.tex` |
| **Character count** without spaces (track by ECTS) | ADMIN_CHECKLIST targets |
| **Rewrite in your own voice** — UE fails >20 % AI-written text | All LaTeX/Markdown drafts are starting points only |
| **Loom / practice recording** (optional self-study) | Not stored in repo |
| **Complete RQ2 live Bedrock Pareto** after AWS quota approval | Re-run `evals/run_rq2_pareto.py --live-providers --provider bedrock`; OpenAI cells are already in RESULTS_DRAFT |
| **Examination Office contacts** | Keep private — placeholder in ADMIN_CHECKLIST |
| **Attend all three colloquium periods** | Dates in [UE_REQUIREMENTS.md](UE_REQUIREMENTS.md) |
| Fill **matriculation number** and final supervisor names in LaTeX cover | `latex/overleaf_main.tex` |
| Expand literature with **page numbers** after reading sources | Chapters 2–2b |

**Privacy:** Admin emails, Prüfungsamt, Betreuungsvertrag, and personal coordination live in `D:\JOB\ue-coordination\` — **not** in this public repo.

---

## Status (2026-10-02)

LaTeX project follows the official UE chapter list. Body text is written in **clear IELTS Band 6 academic English**. Fill matriculation number and supervisors. Expand literature with page numbers after you read the cited works. **RQ2:** OpenAI `gpt-4o-mini` live Pareto is **complete** (40/40, ~$0.0011). Bedrock Nova Micro is still quota-blocked. Production is **OpenAI-first**; nightly enrichment sample rate 0.25. **RQ4:** modelled ≈ **€0.10 / 1k jobs**. **RQ3:** packaging table dense 0.2174; CI local-tfidf pin 0.1258 — both reported. **Rewrite in your own voice before submission** — the writing lecture lists LLM-authored text above 20 % as a fail reason.

**Packaging:** [PACKAGING.md](PACKAGING.md) for CV bullets + Loom + admin order.  
**Colloquium prep:** Start with [COLLOQUIUM_SLIDES.md](COLLOQUIUM_SLIDES.md) or compile [latex/colloquium_slides.tex](latex/colloquium_slides.tex) on Overleaf.

**Overleaf upload (thesis):** ready-made ZIP — [`dist/DataForge_UE_Thesis_Overleaf_2026-10-02.zip`](dist/DataForge_UE_Thesis_Overleaf_2026-10-02.zip) (main file `overleaf_main.tex`, pdfLaTeX). See `OVERLEAF_README.txt` inside the archive.
