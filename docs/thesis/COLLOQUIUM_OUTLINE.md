# Master Colloquium presentation (10 minutes)

**Same topic as the thesis.** Exam is 10 minutes talk + 10 minutes questions. You must attend all three periods.

Suggested slide count: **10 slides**. Do not demo for 8 minutes and leave 2 minutes of theory. The lecture grades a *research* presentation.

Use clear English (IELTS Band 6): short sentences, common words, define one technical term if you use it.

## Slide outline

1. **Title** — Augmented analytics on a production lakehouse (DataForge). Name, programme, proposed supervisors.
2. **Problem** — EU job data is spread out. If GenAI is added without tests, it can break data history, quality, and cost.
3. **Gap** — Few studies combine a live medallion system, controlled GenAI, an honest non-AI baseline, and unit cost.
4. **Research questions** — RQ1 integration; RQ2 providers; RQ3 matching vs wizard; RQ4 unit cost. Say hypothesis H1 out loud.
5. **Theory (90 seconds)** — Lakehouse / SCD2; BM25 vs dense retrieval; GDPR / AI Act boundary (advice tool for seekers is not employer screening). Name two authors (Hevner; Järvelin and Kekäläinen).
6. **Method** — Design science + one case. Why not a literature-only thesis or a survey.
7. **Artefact** — One architecture figure: Bronze → Silver SCD2 → Gold; extra rules / LLM; Match API.
8. **Results** — Table: dense 0.217 nDCG@10 vs wizard 0.146 vs BM25 0.135 (packaging run). Cost about €0.10 / 1k jobs (OpenAI-first). RQ2 OpenAI 40/40; Bedrock still pending quota (say this; do not hide it).
9. **Implications** — Rules before tokens; freeze identity; measure against a simple baseline; in-region inference.
10. **Next 12–16 weeks** — Supervisor signatures, CampusNet registration (title cannot change), complete **Bedrock** RQ2, write to the character count.

## Backup slides (only if asked)

- Threats to validity (103 queries, author labels).
- Statutory / AI-writing rule (phrase this honestly with your supervisor).
- Reproduction commands.

## Timing

| Minutes | Content |
|---------|---------|
| 0–1 | Title + why the problem matters for juniors in DE/EU |
| 1–3 | Gap + RQs |
| 3–5 | Method + artefact |
| 5–8 | Results table and honesty about quota |
| 8–10 | Implications + close |

Speak about 100 words per minute. Practise with a timer. Prepare answers for: Why hybrid if dense wins? Is this high-risk AI? Why not OpenAI only? Why is literature 15–50 % of a coding thesis?
