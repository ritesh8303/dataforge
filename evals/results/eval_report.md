# Matching eval report

- Queries: 42
- Jobs: 96

| Method | nDCG@10 | P@5 | Recall@20 | MRR |
|---|---:|---:|---:|---:|
| bm25 | 0.1452 | 0.1619 | 0.2143 | 0.1844 |
| dense | 0.2203 | 0.2190 | 0.2738 | 0.2677 |
| hybrid | 0.1714 | 0.1714 | 0.2341 | 0.2344 |
| heuristic | 0.1585 | 0.1905 | 0.2401 | 0.2117 |

## Multi-agent ablation (Phase D)

Local provider, 30 labelled queries (`evals/run_agent_ablation.py`):

| Method | Avg citation validity |
|---|---:|
| hybrid single-shot | 1.0 |
| multi-agent | 1.0 |

Structural citation validity is saturated on the local provider; thesis appendix should add a Bedrock sample for faithfulness + €/latency.
