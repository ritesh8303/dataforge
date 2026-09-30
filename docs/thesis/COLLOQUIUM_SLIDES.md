# Master Colloquium — Speaker-Ready Slides (10 minutes)

**Topic:** Augmented analytics on a production lakehouse (DataForge)  
**Author:** Ritesh Rakesh Jadhav · M.Sc. Data Science · University of Europe for Applied Sciences, Potsdam  
**Exam format:** 10 min presentation + 10 min Q&A · Attend **all three** exam periods  
**Language:** Clear English (IELTS Band 6) — short sentences, define one term per slide if needed  
**Source outline:** [COLLOQUIUM_OUTLINE.md](COLLOQUIUM_OUTLINE.md)

---

## Slide 1 — Title

**On slide**

- **Augmented Analytics on a Production Lakehouse**
- Integrating Generative AI into European Job Intelligence — *DataForge*
- Ritesh Rakesh Jadhav · M.Sc. Data Science · UE Potsdam
- Proposed supervisors: Prof. Dr. Iftikhar Ahmed (Erst) · Prof. Dr. Rand Kouatly (alt.) · Zweit TBD from colloquium list

**Speaker notes (~60 s)**

- Good morning. My name is Ritesh Jadhav. I study Data Science at UE Potsdam.
- My thesis topic is **augmented analytics**: how to add generative AI to a **live** job-data lakehouse without breaking data history or cost control.
- The system is **DataForge** — a serverless medallion pipeline on AWS with public APIs and a job-matching wizard.
- This is a **research** presentation, not a long product demo. I will show the problem, the gap, four research questions, method, results, and next steps.

---

## Slide 2 — Problem

**On slide**

- EU job data is **spread out**: BA Jobsuche, EURES, aggregators, company ATS feeds
- Teams want **AI features** (semantic match, enrichment) on top of existing pipelines
- If GenAI is added **without tests**, you risk:
  - broken **SCD Type 2** history (wrong job versions)
  - **quality** claims with no baseline
  - **runaway cost** on serverless schedules
- **Who cares:** junior developers and career seekers in DE/EU need trustworthy, affordable job intelligence

**Speaker notes (~90 s)**

- European vacancy data does not sit in one database. It comes from government portals, aggregators, and many company career pages.
- Employers and universities now ask for more than CSV exports. They want semantic matching and structured enrichment.
- But production teams already have **medallion pipelines** with Bronze, Silver, and Gold layers and historical keys. Adding an LLM is not like opening a chat window.
- Without controlled evaluation, you can corrupt slowly changing dimensions, ship unmeasured “AI”, or blow the AWS budget on nightly enrichment.
- For junior talent in Germany and the EU, bad matching or opaque automation wastes time and trust. That is why this is a **Data Science** thesis, not only an engineering project.

---

## Slide 3 — Research gap

**On slide**

- Many papers study **new RAG demos** or chatbots
- Few combine all of the following in **one live system**:
  1. multi-source **medallion lakehouse** (Bronze → Silver SCD2 → Gold)
  2. **controlled GenAI** layer (gateway, kill switch, cost log)
  3. **honest non-AI baseline** (rule-based Career Matching Wizard)
  4. **unit cost** per 1k jobs (€/operation, not vendor slides)
- **DataForge** is the missing production substrate for this study

**Speaker notes (~60 s)**

- The literature has strong work on dense retrieval and LLM ops. Many MSc projects build a small RAG notebook.
- What is rare is the **combination**: a system that is already in production shape, plus falsifiable experiments, plus a simple baseline you can beat or lose to honestly.
- DataForge already ingests five sources, keeps SCD2 history, serves APIs, and exposes a keyword wizard. The thesis **extends** it — it does not rebuild the lakehouse.
- The contribution is a **repeatable reference architecture** and evaluation protocol under EU residency and budget limits.

---

## Slide 4 — Research questions & hypothesis

**On slide**

| ID | Question (short) |
|----|------------------|
| **RQ1** | Integrate GenAI without breaking SCD2 / reproducibility / cost envelope |
| **RQ2** | Which provider/model wins on quality, cost, latency, EU residency? |
| **RQ3** | Do embeddings beat the rule-based Career Matching Wizard on ranked relevance? |
| **RQ4** | When does AI-on-pipeline improve unit economics vs raw open data? |

- **H1 (say aloud):** Embedding retrieval **outperforms** keyword/heuristic matching on **nDCG@10**

**Speaker notes (~60 s)**

- I have four research questions. RQ1 is architectural: integration without breaking history. RQ2 is multi-objective provider selection. RQ3 is the matching experiment. RQ4 is business: euros per thousand jobs.
- My main testable hypothesis is **H1**: dense retrieval beats the wizard on nDCG@10. I will show that table on slide 8.
- Secondary hypotheses on enrichment cost and routing are tied to RQ2 and RQ4; RQ2 live Bedrock runs are still pending quota — I will be explicit about that.

---

## Slide 5 — Theory (90 seconds)

**On slide**

- **Lakehouse / SCD2:** Bronze snapshots → Silver **slowly changing** job history → Gold analytics (Kimball; medallion practice)
- **Retrieval:** **BM25** = lexical match; **dense** = embedding similarity (Karpukhin et al.); **hybrid** = fuse both
- **Metric:** **nDCG@10** = graded ranking quality (Järvelin & Kekäläinen, 2002)
- **Method frame:** **Design science** — build and evaluate an artefact (Hevner et al., 2004)
- **Compliance boundary:** public vacancy text; Match API = **decision support for job seekers**, not automated employer screening → **not** high-risk AI Act hiring decision in this framing (GDPR + AI Act documentation either way)

**Speaker notes (~90 s)**

- Three theory blocks in ninety seconds.
- First, **data engineering**: medallion layers and SCD Type 2 mean job postings keep history when titles or locations change. AI outputs must be **additive**, not rewrite keys.
- Second, **information retrieval**: BM25 finds keyword overlap; dense retrieval finds semantic similarity. We measure ranking with nDCG, following Järvelin and Kekäläinen.
- Third, **design science** following Hevner: we produce a working integration layer and evaluate it with methods that match the claims — architecture checks, labelled IR experiment, cost model.
- On regulation: we process mainly **public job ads**. The wizard and Match API advise seekers; they are not automated hiring decisions. We still document GDPR and AI Act boundaries with the supervisor.

---

## Slide 6 — Methodology

**On slide**

- **Design science + single embedded case** (DataForge on AWS `eu-central-1`)
- **Why not literature-only?** Artefact + measured properties; coding thesis still needs **15–50 % literature** (UE rule)
- **Why not a broad survey?** One deep case with reproducible scripts beats many shallow stacks
- **Evaluation:**
  - RQ3: 42 labelled queries × 96 jobs · nDCG@10, P@5, Recall@20, MRR
  - RQ2: rules baseline now; live Bedrock Pareto when quota approved
  - RQ4: modelled €/1k jobs from public price lists + rules-first enrichment share

**Speaker notes (~60 s)**

- Method is design science on one real case. Yin’s case-study logic supports “how” and “why” questions when the system is information-rich.
- I defend the literature share: UE expects a major scholarly portion even in a coding thesis. Theory explains SCD2, IR metrics, LLMOps, and compliance — it is not padding.
- Experiments are scripted in `evals/` so numbers can be reproduced. Labels are author-graded with known validity limits — I name that in Q&A if asked.

---

## Slide 7 — Artefact architecture

**On slide**

```
Public jobs → Bronze → Silver (SCD2) → Gold + APIs
                ↓              ↓
         Rules-first enrich   Hybrid Match API
         (+ optional LLM)     (BM25 + dense + RRF)
```

- **ModelRouter:** task profiles, JSON schema check, `AI_ENABLED` kill switch, daily budget
- **Guardrails:** LLM never mutates `job_id` / SCD keys; prompt versions in git
- **Live today:** lakehouse, Jobs/Metrics APIs, Match Function URL (API key)
- **Paused:** nightly Bedrock enrichment (Nova Micro account quota = 0 — increase requested)

**Speaker notes (~60 s)**

- One architecture picture: the existing pipeline stays; generative components read Silver and Gold.
- Enrichment is **rules-first**; LLM calls only on unclear cases or sampled rows when enabled.
- Match API exposes hybrid retrieval and optional agent path; production uses API key and CORS.
- Honesty slide inside the artefact: enrichment is **paused** until AWS raises Bedrock quota. Architecture and eval harness are complete.

---

## Slide 8 — Results

**On slide**

**RQ3 — Matching (103 queries, 96 jobs)**

| Method | nDCG@10 | P@5 | Recall@20 |
|--------|--------:|----:|----------:|
| BM25 | 0.135 | 0.162 | 0.214 |
| **Dense** | **0.217** | **0.219** | **0.274** |
| Hybrid (product default) | 0.171 | 0.171 | 0.234 |
| Heuristic wizard | 0.146 | 0.191 | 0.240 |

- **H1 supported** on this label set: dense > wizard (0.217 vs 0.146)
- **RQ4:** rules-first path ≈ **€0.06 / 1k jobs** (modelled)
- **RQ2:** rules baseline done; **live Bedrock Pareto pending quota** — not hidden

**Speaker notes (~90 s)**

- Main empirical result: dense retrieval leads on nDCG@10 at 0.217 versus 0.146 for the rule-based wizard and 0.135 for BM25 alone. Hypothesis H1 is supported on this pilot set. Absolute scores are modest — far from perfect — which is scientifically useful.
- Hybrid is the **product default** because it balances lexical and semantic signals and supports citations; on this gold set it sits between dense and wizard.
- Unit cost under rules-first enrichment is about six euro cents per thousand jobs in the modelled scenario.
- RQ2: I have the rules-only point and the runner ready. Live Nova Micro completions were blocked at zero TPM/TPD; quota increase submitted 16 Sep 2026. I state that openly.

---

## Slide 9 — Implications

**On slide**

- **Rules before tokens** — cheap deterministic enrichers first; LLM on hard cases only
- **Freeze identity** — additive AI columns; never rewrite SCD keys
- **Measure against a simple baseline** — wizard + BM25, not only “we added AI”
- **In-region inference** — Bedrock `eu-central-1` for residency narrative (thesis track)
- **Operational controls** — kill switch, daily budget, API key, schedule pause

**Speaker notes (~60 s)**

- Four takeaways for practitioners.
- Do not send every row to an LLM; rules-first saves money and latency.
- Treat AI as a consumer of historical truth, not an editor of primary keys.
- Augmented analytics claims need IR metrics and baselines.
- For EU-facing student projects, regional inference and honest status reporting matter as much as model choice.

---

## Slide 10 — Next 12–16 weeks & close

**On slide**

- **Now → Oct/Nov 2026:** supervisor signatures · CampusNet registration (**title frozen**)
- **Build:** complete **RQ2** after Bedrock quota · enlarge gold label set (optional peer review)
- **Write:** expand literature with page numbers · hit **character count** (72k–82k w/o spaces for 120 ECTS)
- **Submit → Feb/Mar 2027:** binding PDF · colloquium on same topic
- **Reminder:** rewrite in **your own voice** — UE fails theses with **>20 % AI-written** text (check with supervisor)

**Thank you — questions?**

**Speaker notes (~60 s)**

- Close with the plan, not a demo cliffhanger.
- Immediate admin: two supervisors, paper form, CampusNet — details in ADMIN_CHECKLIST.md locally.
- Scientific debt: RQ2 live cells and optional larger label set.
- Writing debt: literature depth and personal voice per UE writing lecture.
- Thank the committee and invite questions. Keep 30 seconds buffer.

---

## Timing table (target 10:00)

| Minutes | Slide(s) | Content | ~Words |
|---------|----------|---------|-------:|
| 0:00–1:00 | 1–2 | Title + problem (juniors in DE/EU) | 100 |
| 1:00–3:00 | 3–4 | Gap + four RQs + H1 aloud | 200 |
| 3:00–5:00 | 5–7 | Theory (90 s) + method + architecture | 200 |
| 5:00–8:00 | 8 | Results table + quota honesty | 300 |
| 8:00–10:00 | 9–10 | Implications + timeline + close | 200 |

**Pace:** ~100 words/minute · **Total ~1,000 words** · Practise with a phone timer.

---

## Q&A backup answers

Use these if asked during the 10-minute Q&A. Keep answers under 45 seconds each.

### Why keep hybrid if dense wins on nDCG@10?

- Dense wins on **this** 42-query author label set. Hybrid still helps when queries are **short or misspelled** — BM25 catches exact tokens dense models skip.
- The **product** must be robust across German/English mixes and sparse résumés, not only optimise one metric on one gold set.
- Hybrid also supports **explainability**: lexical hits plus semantic rank are easier to cite in a career-advice UI.
- I report dense as the scientific winner for H1; hybrid remains the **default deploy** until a larger label set says otherwise.

### Is this high-risk under the EU AI Act?

- The thesis frames Match as **decision support for job seekers** browsing public vacancies, **not** automated hiring or worker profiling by employers.
- Inputs are mainly **public job text**; résumé fields are minimised and redacted where possible.
- Even with lower risk class, we still document **transparency**, human oversight, kill switch, and audit logs — final legal wording must be agreed with the supervisor.
- If the product later targets **employer screening**, the risk class and obligations change — that is out of scope for this student build.

### Why not OpenAI only?

- **EU data residency** and university ethics favour **in-region** Bedrock for the primary thesis track.
- **Vendor lock-in:** ModelRouter supports Bedrock, OpenAI, Anthropic, and local fallback for reproducibility and cost experiments (RQ2).
- OpenAI may win some quality benchmarks, but the research question is **multi-objective**: quality, €, latency, residency — not single-vendor accuracy slides.
- Rules-first enrichment already covers many fields at **€0** model cost.

### Why is literature 15–50 % of a coding thesis?

- UE structure and Prof. Kouatly’s lectures require a **major scholarly portion**: you must show you know medallion modelling, IR evaluation, LLMOps, and compliance **before** you claim novelty.
- Design science (Hevner) explicitly demands grounding in existing theory — the artefact is not exempt from literature.
- A coding thesis without theory reads as a **portfolio report**, which the writing lecture lists as a fail pattern.
- Implementation proves **feasibility**; literature proves **positioning** — what gap you fill and how you measure success.

---

## Optional backup slides (only if asked)

- **Threats to validity:** 103 queries, author labels, single region, modest absolute nDCG.
- **AI-writing rule:** disclose tool use; rewrite; target well below 20 % AI-generated prose per supervisor guidance.
- **Reproduction:** `evals/run_matching_eval.py`, `evals/results/eval_report.md`, `scripts/roi_report.py`.

---

## Beamer version

PDF slides: [latex/colloquium_slides.tex](latex/colloquium_slides.tex) — compile with pdfLaTeX on Overleaf (`aspectratio=169`).
