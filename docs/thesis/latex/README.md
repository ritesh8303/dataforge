# DataForge Master thesis — University of Europe (UE)

**Use `overleaf_main.tex` on Overleaf as the Main document.** Compiler: **pdfLaTeX**.

This project follows the Summer Semester 2026 materials of Prof.\ Dr.\ Rand Kouatly:

- *Master Colloquium* (topic selection, proposal, registration, 10-minute exam)
- *Writing Good thesis* (layout, Harvard, declaration, chapter weights)
- *Thesis Structure* (cover → indices → abstract → introduction → literature → research questions → methodology → results → discussion → implications → bibliography)

Official Overleaf tech-master template mentioned in the lecture (confirm with instructors):  
https://www.overleaf.com/read/xkywsyxtqvfd#039c70

## Layout applied here (from the writing lecture)

| Item | Setting |
|------|---------|
| Paper | DIN A4, **single-sided**, 80 g/m² white |
| Margins | 2 cm left, **4 cm right**, 2.5 cm top, 3 cm bottom |
| Font | Times (Times New Roman equivalent), **12 pt** body, 10 pt captions/footnotes |
| Paragraphs | Justified, **1 cm** first-line indent, **6 pt** paragraph skip, 1.5 line spacing |
| Outline | DIN 1421 decimal numbering; no deeper than four levels |
| Citations | **Harvard** author–year (`natbib` + `apalike`); paraphrases use `cf.` |
| Declaration | Statutory wording from the writing lecture (replace if Prüfungsamt differs) |

## English style (IELTS Band 6)

Body chapters use **clear academic English** at about IELTS Band 6:

- short and medium sentences;
- common academic words (*show, use, test, add, keep, cost, quality*);
- simple linkers (*First, However, Therefore, For example*);
- technical terms kept, but explained in plain words the first time.

Do not add slang. Do not add grammar mistakes on purpose. The statutory declaration keeps the official lecture wording.

## Character volume (clock starts at registration)

| Programme | Characters *without* spaces | Time box |
|-----------|----------------------------|----------|
| Master 60 ECTS | 55{,}000 – 62{,}000 | 12 weeks + 2 |
| Master 90 ECTS | 60{,}000 – 70{,}000 | 14 weeks + 2 |
| Master 120 ECTS (2-year Data Science typical) | 72{,}000 – 82{,}000 | 16 weeks + 3 |

The lecture also gives a working length of about **50 pages of text** at ~35 lines (±10 %). Tables and figures lengthen the bound copy proportionally.

After compiling, count characters without spaces (Word / Overleaf word count, exclude bibliography if your supervisor so instructs) and expand Chapter 2–3 first if you are short: **literature is the major scholarly portion**.

## Chapter map (UE)

| Ch. | Title | Role / weight (lecture) |
|-----|--------|-------------------------|
| 1 | Introduction | Context, problem, structure (~2–5 %) |
| 2 | Theoretical background I | Digitalisation, lakehouse, IR (~literature block) |
| 3 | Theoretical background II | GenAI, LLMOps, law, gap (~literature block; 15 % in writing lecture / 50 % on the one-page structure sheet — treat literature as **the** major chapter) |
| 4 | Research questions | RQs, hypotheses (~1 %) |
| 5 | Methodology | Why design science + case, not a survey (~5 %) |
| 6 | Results | Artefact + tables (no unsolicited opinion) |
| 7 | Discussion | Own judgement, practical application (~5–15 % with results) |
| 8 | Implications and future research | Theory, practice, next work (~5–10 %) |
| — | Bibliography | One alphabetical Harvard list |

## Binding checklist (non-negotiable in the lectures)

- Two supervisors; **one must be a tenured Prof.\ Dr.** of UE.
- Title **cannot be changed** after CampusNet registration.
- Deadline is hard; extension form ≥10 days before due date (max. 21 days).
- Plagiarism, wrong citation, missing character minimum, “essay not a thesis”, or **LLM-written text above 20 %** are listed as fail reasons.
- Ask the first supervisor for feedback **before** Prüfungsamt submission.

## How to compile locally

```text
pdflatex overleaf_main.tex
bibtex overleaf_main
pdflatex overleaf_main.tex
pdflatex overleaf_main.tex
```

Unused older chapter files (`03_existing_system.tex`, `04_requirements.tex`, `05_implementation.tex`, `07_roi.tex`, `08_discussion.tex`, `09_conclusion.tex`) are leftovers from the pre-UE outline and are **not** `\input` by `overleaf_main.tex`.
