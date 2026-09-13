"""Run matching evaluation: BM25 vs dense vs hybrid vs heuristic."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from ai_gateway.router import ModelRouter
from embedding_index import build_embedding_index, job_text
from evals.metrics import mean_reciprocal_rank, ndcg_at_k, precision_at_k, recall_at_k
from retrieval import rank_bm25, rank_hybrid


# CI guard: toy sets that saturate at 1.0 fail the build.
NDCG_SATURATION_EPS = 0.999
MIN_QUERIES = 40
MIN_JOBS = 80


def heuristic_rank(query: str, jobs: list[dict], top_k: int = 10) -> list[str]:
    q = query.lower()
    scored = []
    for job in jobs:
        text = job_text(job).lower()
        hits = sum(1 for w in q.split() if len(w) > 2 and w in text)
        scored.append((job.get("job_id", ""), hits))
    scored.sort(key=lambda x: x[1], reverse=True)
    return [jid for jid, _ in scored[:top_k] if jid]


def load_sample_jobs() -> list[dict]:
    sample_path = ROOT / "evals" / "data" / "sample_jobs.json"
    if sample_path.exists():
        return json.loads(sample_path.read_text(encoding="utf-8"))
    raise FileNotFoundError(f"Missing {sample_path}; run evals/generate_phase_a_fixtures.py")


def _grades(item: dict) -> dict[str, float] | set[str]:
    if item.get("relevance_grades"):
        return {k: float(v) for k, v in item["relevance_grades"].items()}
    return set(item["relevant_job_ids"])


def _metrics(relevant, ranked: list[str]) -> dict[str, float]:
    binary = set(relevant.keys()) if isinstance(relevant, dict) else set(relevant)
    return {
        "ndcg@10": round(ndcg_at_k(relevant, ranked, 10), 4),
        "p@5": round(precision_at_k(binary, ranked, 5), 4),
        "recall@20": round(recall_at_k(binary, ranked, 20), 4),
        "mrr": round(mean_reciprocal_rank(binary, ranked), 4),
    }


def main() -> int:
    labels = json.loads((ROOT / "evals" / "data" / "label_queries.json").read_text(encoding="utf-8"))
    jobs = load_sample_jobs()
    if len(labels) < MIN_QUERIES or len(jobs) < MIN_JOBS:
        raise SystemExit(
            f"Eval fixtures too small: {len(labels)} queries / {len(jobs)} jobs "
            f"(need ≥{MIN_QUERIES}/{MIN_JOBS})"
        )

    router = ModelRouter()
    index = build_embedding_index(jobs, router)
    vectors = {e["job_id"]: e["vector"] for e in index}

    methods = {"bm25": [], "dense": [], "hybrid": [], "heuristic": []}
    for item in labels:
        query = f"{item['dream_role']} {item['resume_excerpt']} {item['location']}"
        relevant = _grades(item)

        bm25_ranked = [j["job_id"] for j in rank_bm25(query, jobs, top_k=20)]
        hybrid_ranked = [
            j["job_id"]
            for j in rank_hybrid(
                query,
                jobs,
                vectors_by_id=vectors,
                embed_fn=lambda t, _r=router: _r.embed("embed", t).vector,
                top_k=20,
            )
        ]
        # dense-only via hybrid path with empty bm25 disabled: use hybrid lists' dense scores
        from retrieval import rank_dense

        dense_ranked = [
            j["job_id"]
            for j in rank_dense(
                query,
                jobs,
                vectors,
                embed_fn=lambda t, _r=router: _r.embed("embed", t).vector,
                top_k=20,
            )
        ]
        heur_ranked = heuristic_rank(query, jobs, top_k=20)

        for name, ranked in (
            ("bm25", bm25_ranked),
            ("dense", dense_ranked),
            ("hybrid", hybrid_ranked),
            ("heuristic", heur_ranked),
        ):
            row = {"query_id": item["query_id"], **_metrics(relevant, ranked)}
            methods[name].append(row)

    def avg(key: str, rows: list[dict]) -> float:
        return sum(r[key] for r in rows) / len(rows)

    summary = {
        "n_queries": len(labels),
        "n_jobs": len(jobs),
        "averages": {
            name: {
                "ndcg@10": round(avg("ndcg@10", rows), 4),
                "p@5": round(avg("p@5", rows), 4),
                "recall@20": round(avg("recall@20", rows), 4),
                "mrr": round(avg("mrr", rows), 4),
            }
            for name, rows in methods.items()
        },
        "details": methods,
        "cost_summary": router.cost_logger.summary(),
    }

    # Saturation guard: if every method hits ~1.0 nDCG the label set is still a toy.
    ndcgs = [summary["averages"][m]["ndcg@10"] for m in methods]
    if all(v >= NDCG_SATURATION_EPS for v in ndcgs):
        raise SystemExit(
            f"Saturated eval set: all methods nDCG@10 ≥ {NDCG_SATURATION_EPS}. "
            "Add harder negatives / graded labels."
        )

    out_dir = ROOT / "evals" / "results"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "matching_eval.json"
    out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    report = ROOT / "evals" / "results" / "eval_report.md"
    lines = [
        "# Matching eval report",
        "",
        f"- Queries: {summary['n_queries']}",
        f"- Jobs: {summary['n_jobs']}",
        "",
        "| Method | nDCG@10 | P@5 | Recall@20 | MRR |",
        "|---|---:|---:|---:|---:|",
    ]
    for name, av in summary["averages"].items():
        lines.append(
            f"| {name} | {av['ndcg@10']:.4f} | {av['p@5']:.4f} | "
            f"{av['recall@20']:.4f} | {av['mrr']:.4f} |"
        )
    report.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(json.dumps(summary["averages"], indent=2))
    print(f"Wrote {out_path}")
    print(f"Wrote {report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
