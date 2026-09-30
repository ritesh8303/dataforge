# Thesis admin checklist — manual steps (student)

**Programme:** M.Sc. Data Science, University of Europe for Applied Sciences, Potsdam  
**Author:** Ritesh Rakesh Jadhav  
**Topic:** Augmented analytics / GenAI integration on DataForge

This repo holds **research artefacts** (LaTeX, slides, results). **Personal admin** (supervisor emails, Prüfungsamt addresses, signed PDFs) stays **outside** the public repo — e.g. `D:\JOB\ue-coordination\`. Do **not** commit private email threads or examination-office inboxes here.

---

## What the repo already automates

| Done in repo | Location |
|--------------|----------|
| UE chapter LaTeX draft | `latex/overleaf_main.tex` |
| Colloquium slide outline | `COLLOQUIUM_OUTLINE.md` |
| Speaker-ready slide deck (Markdown) | `COLLOQUIUM_SLIDES.md` |
| Beamer PDF slides (10 slides, 16:9) | `latex/colloquium_slides.tex` |
| Optional IEEE short-paper stub | `latex/ieee_short_paper.tex` |
| Proposal / exposé for supervisors | `THESIS_PROPOSAL.md`, `THESIS_EXPOSE.md` |
| Living RQ results tables | `RESULTS_DRAFT.md`, `evals/results/` |
| UE rules extracted from lectures | `UE_REQUIREMENTS.md` |

---

## What you must still do manually

### 1. Contact supervisors (email)

- [ ] Choose **Erstbetreuung** (Prof. Dr. required): e.g. Prof. Dr. Iftikhar Ahmed (ML/LLM) or Prof. Dr. Rand Kouatly (SE/LLM apps).
- [ ] Choose **Zweitbetreuung** from the colloquium list (paid UE lecturer — not an unpaid external).
- [ ] Attach **PDF** of `THESIS_PROPOSAL.md` (export from Markdown or print-to-PDF) and optionally `THESIS_EXPOSE.md`.
- [ ] Offer a **15-minute call**; mention live DataForge link and honest RQ2 quota status.
- [ ] Save sent/received mail in your **private** coordination folder (not this repo).

**Sample outreach email**

```
Subject: MSc Data Science thesis request — GenAI on production lakehouse (DataForge)

Dear Prof. Dr. [Surname],

My name is Ritesh Rakesh Jadhav (M.Sc. Data Science, UE Potsdam). I would like to ask
whether you could supervise my master's thesis on integrating generative AI into an
existing serverless medallion lakehouse for European job intelligence (project: DataForge).

The study is design-science oriented: four research questions cover (1) integration without
breaking SCD Type 2 history, (2) multi-provider cost/quality trade-offs, (3) embedding-based
matching vs a rule-based baseline, and (4) unit economics. A labelled evaluation already
shows dense retrieval at nDCG@10 0.22 vs 0.16 for the heuristic wizard; live Bedrock RQ2
runs are pending AWS quota approval — I report limits openly.

I attach a short proposal and exposé. The live system and evaluation scripts are in my
GitHub repository [link]. I am available for a 15-minute meeting in [weeks].

Would you be open to Erstbetreuung (or Zweitbetreuung) for this topic?

Kind regards,
Ritesh Rakesh Jadhav
[matriculation number when assigned]
[your student email]
```

---

### 2. Paper form + CampusNet registration

- [ ] Both supervisors sign the **paper registration form** (Betreuungsvertrag / thesis registration — exact form from Student Service or Examination Office).
- [ ] Submit signed form to **Student Service** (Campus Potsdam).
- [ ] Register online in **CampusNet**: programme, **first supervisor**, **exact thesis title**, start date → submit.
- [ ] **Title is frozen** after registration — choose the final wording before submit.
- [ ] Read the **confirmation email** from the Examination Office (start date, deadline, ECTS track).
- [ ] Read *Handreichung zum wissenschaftlichen Arbeiten* (linked from UE / examination pages).

---

### 3. Statutory declaration (Eidesstattliche Erklärung)

- [ ] Use the wording from `latex/overleaf_main.tex` (declaration section) — do not paraphrase legal text without checking current UE template.
- [ ] Sign **by hand** on the bound print copy before submission.
- [ ] Include declaration in the PDF **after** the cover sheet, before the table of contents.

---

### 4. Character count check

- [ ] Confirm your **ECTS track** (60 / 90 / 120) with the Examination Office.
- [ ] Count **characters without spaces** in the main body (introduction through implications — exclude cover, declaration, TOC, bibliography, appendices per UE guidance; confirm with supervisor).

| Track | Characters (no spaces) | Writing time (lecture) |
|-------|------------------------:|------------------------|
| Master 60 ECTS | 55,000–62,000 | 12 weeks + 2 |
| Master 90 ECTS | 60,000–70,000 | 14 weeks + 2 |
| Master 120 ECTS | 72,000–82,000 | 16 weeks + 3 |

- [ ] Word/LibreOffice: review → word count → **exclude spaces**; or export to plain text and use a counter tool.
- [ ] Also target ~**50 pages ±10 %** of body text at 35 lines (lecture rule of thumb).

---

### 5. AI-writing rule — rewrite in your own voice

- [ ] UE writing lecture: thesis text **>20 % AI-generated** is a **fail reason** (software also checks plagiarism).
- [ ] Use repo drafts as **structure and facts only** — rewrite every chapter in your own sentences before submission.
- [ ] Keep a log of tools used (Cursor, ChatGPT, etc.) for transparency with your supervisor.
- [ ] Ask supervisor how to phrase AI assistance in the declaration or methodology if required.
- [ ] Do **not** paste colloquium speaker notes verbatim into the thesis body.

---

### 6. Colloquium exam (10 + 10 minutes)

- [ ] Prepare from `COLLOQUIUM_SLIDES.md` / `latex/colloquium_slides.tex`.
- [ ] Practise with timer (~100 words/minute; timing table in slide deck).
- [ ] Attend **all three** exam periods for your group (see `UE_REQUIREMENTS.md`).
- [ ] Optional: record a **Loom** practice run for yourself — not required by UE; keep link private if you do.

| Group | Period 1 | Period 2 | Period 3 |
|-------|----------|----------|----------|
| DS 120C | 24 Jun 2026, 11:00–13:15 | 1 Jul 2026, 11:00–13:15 | 8 Jul 2026, 11:00–13:15 |
| DS 90 | 24 Jun 2026, 14:00–16:15 | 1 Jul 2026, 14:00–16:15 | 8 Jul 2026, 14:00–16:15 |

---

### 7. Examination Office & Student Service contacts

**Do not store real private inboxes in this public repository.**

Keep a local file, e.g. `D:\JOB\ue-coordination\ue_contacts_private.md`:

```
# UE thesis contacts (PRIVATE — not for git)

Examination Office (Prüfungsamt) Potsdam:
  Email: [paste from CampusNet / student portal]
  Phone: [paste]
  Office hours: [paste]

Student Service Potsdam:
  Email: [paste]
  Room: [paste]

My assigned case officer (if any):
  Name: [paste after first reply]

Supervisor Erst:
  Name: [after agreement]
  Email: [university address only in repo if supervisor publishes it]

Supervisor Zweit:
  Name: [after agreement]
  Email: [...]
```

- [ ] Copy official addresses from the **student portal**, CampusNet confirmation, or campus intranet — not from memory.
- [ ] Use **student email** for all official correspondence.

---

### 8. Submission pack (later — Feb/Mar 2027)

- [ ] Binding layout per `UE_REQUIREMENTS.md` (margins, Harvard citations, one-sided A4).
- [ ] Digital upload per Examination Office instructions (usually PDF + sometimes source).
- [ ] Optional **IEEE short paper** (4–8 pages) — stub in `latex/ieee_short_paper.tex`; confirm if it earns extra points in your cohort.
- [ ] Complete **RQ2** Bedrock runs after quota approval; update `RESULTS_DRAFT.md` and thesis Chapter Results before final print.

---

## Quick priority order (next 2–4 weeks)

1. Email supervisors (template above)  
2. Agree Erst + Zweit  
3. Signed paper form → Student Service  
4. CampusNet registration (freeze title)  
5. Colloquium practice (slides ready in repo)  
6. Continue writing + character count tracking  
7. Rewrite AI-assisted drafts in own voice ongoing  
8. Optional: run OpenAI RQ2 Pareto (`PACKAGING.md` / `RESULTS_DRAFT.md`) when API key available  

**CV / Loom packaging:** [PACKAGING.md](PACKAGING.md)

---

## Related docs

- [README.md](README.md) — index of all thesis files  
- [UE_REQUIREMENTS.md](UE_REQUIREMENTS.md) — lecture rules  
- [COLLOQUIUM_SLIDES.md](COLLOQUIUM_SLIDES.md) — exam deck + Q&A backups  
