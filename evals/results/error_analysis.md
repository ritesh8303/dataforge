# Matching eval error analysis (expanded gold set)

**Generated:** 2026-09-18  
**Fixtures:** `evals/generate_phase_a_fixtures.py` → `evals/data/label_queries.json`, `sample_jobs.json`  
**Run:** `py -3 evals/run_matching_eval.py` → `evals/results/matching_eval.json`

## Corpus size

| Item | Count |
|------|------:|
| Queries (`n_queries`) | **103** |
| Jobs (`n_jobs`) | **96** |
| Fields covered | 16 (DE/EN titles, visa, seniority, remote) |
| Prior baseline | 42 queries × 96 jobs |

Queries span internship/Werkstudent/graduate/junior/mid/senior, visa sponsorship & work-permit signals, remote/home-office, and mixed German/English role phrasing.

## Full metric table (macro averages)

| Method | nDCG@10 | P@5 | Recall@20 | MRR |
|--------|--------:|----:|----------:|----:|
| **bm25** | 0.1351 | 0.1437 | 0.1926 | 0.1906 |
| **dense** | **0.2174** | **0.2175** | **0.2856** | **0.2706** |
| **hybrid** | 0.1668 | 0.1670 | 0.2225 | 0.2327 |
| **heuristic** | 0.1462 | 0.1728 | 0.2071 | 0.1952 |

Dense leads on all four metrics. Hybrid sits between dense and lexical baselines (product default combines BM25 + dense + citations). Heuristic word-overlap beats BM25 on P@5 and MRR but not nDCG@10 or recall@20.

Compared to the prior 42-query run: dense nDCG@10 **0.2203 → 0.2174** (stable); heuristic **0.1585 → 0.1462** (harder negatives from broader query mix).

## Qualitative patterns

### When dense beats heuristic (semantic / skills)

Dense wins clearly (+0.05 nDCG@10) on **25 / 103** queries. Typical cases:

- **Skill synonyms & stack phrasing:** e.g. *Data Engineer Python AWS Spark*, *Kafka streaming*, *NLP Engineer Transformer BERT* — embeddings align on toolchain even when exact tokens differ in job text.
- **Role-family matching:** *Computer vision*, *MLOps*, *Analytics Engineer dbt* — semantic neighborhood pulls same-field jobs before token overlap accumulates.
- **English/semantic seniority:** *Senior ML Engineer production models* — dense ranks seniority + domain jointly; heuristic splits on individual words.

Dense also helps **cross-language intent** (*Datenbankentwickler SQL ETL*, *Testingenieur Selenium*) where DE query terms partially overlap EN job boilerplate.

### When heuristic / lexical wins

Heuristic beats dense (+0.05) on **8 / 103** queries; BM25 beats dense on **8 / 103**:

- **Rare exact tokens:** *Senior BI Architect semantic layer KPI*, *Power BI developer* — rare proper nouns / product names reward literal overlap.
- **Short keyword-heavy DE queries:** *Datenanalyst Reporting KPI* — BM25/heuristic match repeated tokens in synthetic descriptions.
- **Visa / permit boilerplate:** *Security engineer valid work permit Germany* — lexical match on *work permit* / *EU citizens* snippets in job text; dense may spread mass across same-field jobs without visa stance.
- **Queries with weak embedding signal:** some intern/Werkstudent queries where local embedder under-ranks correct seniority bucket (dense nDCG ≈ 0).

**Hybrid** does not beat dense on macro nDCG here — likely because BM25 noise on short synthetic JDs dilutes dense signal; still preferred in production for citation traceability and lexical guardrails.

## Threats to validity

1. **Synthetic labels:** Jobs and relevance grades are author-constructed (`gold_*` metadata in fixture generator), not independent human annotation. Grades follow deterministic rules (field, seniority, visa, remote, language flags).
2. **Same-source bias:** Query `dream_role` and job `description` share vocabulary by construction → inflates all methods vs real user resumes; absolute nDCG is not externally calibrated.
3. **Local embedder:** Eval uses `ModelRouter` local provider, not production Bedrock Titan Embed; dense/hybrid numbers may shift on live embeddings.
4. **Graded relevance simplification:** Binary-ish grades (1/2) and cap at 12 relevant IDs per query may not reflect long-tail relevance in production search.
5. **No hard negatives from other fields:** Wrong-field jobs are grade 0 but still retrieved; expanding queries increased confusable same-field seniority/visa variants.

## Related artefacts

- Bar chart: `docs/thesis/latex/figures/rq3_comparison.svg`
- PNG (optional): `docs/thesis/latex/figures/rq3_bars.png` via `scripts/render_thesis_figures.py`
- Enrichment rules (same fixture pass): field/seniority macro-F1 = 1.0, visa macro-F1 = 0.77
