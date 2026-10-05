# Human label protocol — Match / ranking evals

Synthetic fixture nDCG is useful for CI regression. **Public product claims** about
Match quality should prefer this human protocol when labels exist.

## Goal

Grade relevance of DataForge Gold jobs to real seeker resumes for early-career
EU data/AI roles.

## Label grades

| Grade | Meaning |
|------:|---------|
| 0 | Irrelevant |
| 1 | Weak / stretch |
| 2 | Good fit |
| 3 | Excellent fit |

## Fields to judge

- Field match (data/AI vs unrelated tech)
- Seniority (fresher/WS/internship/thesis vs mid/senior)
- Location / work style vs seeker preference
- Language / visa only as soft signals (never hard-fail without evidence)

## Process

1. Pick 10–30 anonymized resumes (consent required; PII redacted).
2. For each resume, retrieve top 40 hybrid candidates from the product board.
3. Two raters grade independently; resolve disagreements by discussion.
4. Store rows in `evals/data/human_labels.csv` (see template).
5. Run `py -3 evals/run_matching_eval.py --labels evals/data/human_labels.csv` when wired.

Until human labels are populated, README / UI must say Match is a **suggested shortlist**,
not calibrated hiring intelligence.
