"""Run matching evaluation: embedding vs heuristic baseline."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from ai_gateway.router import ModelRouter
from embedding_index import build_embedding_index, cosine_similarity, job_text, rank_by_embedding
from evals.metrics import ndcg_at_k, precision_at_k


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
    return [
        {"job_id": "sem_example_python_berlin", "title": "Senior Data Engineer Python AWS", "company": "Example", "location": "Berlin", "description": "Python Spark Airflow AWS pipelines", "tags": "Python,AWS"},
        {"job_id": "sem_data_engineer_aws", "title": "Data Engineer", "company": "TechCo", "location": "Berlin", "description": "Build ETL with Python and AWS", "tags": "Python,SQL"},
        {"job_id": "sem_ml_engineer_remote", "title": "ML Engineer LLM", "company": "AI Labs", "location": "Remote", "description": "PyTorch NLP RAG LLM", "tags": "ML,LLM"},
        {"job_id": "sem_nlp_llm", "title": "NLP Engineer", "company": "NLP Inc", "location": "Remote", "description": "Transformers RAG LangChain", "tags": "NLP"},
        {"job_id": "sem_analyst_munich", "title": "Data Analyst", "company": "Analytics GmbH", "location": "Munich", "description": "SQL Excel Power BI reporting", "tags": "SQL,BI"},
        {"job_id": "sem_bi_analyst", "title": "BI Analyst", "company": "Corp", "location": "Munich", "description": "Tableau dashboards SQL", "tags": "Tableau"},
        {"job_id": "sem_unrelated", "title": "Frontend Developer React", "company": "WebCo", "location": "Hamburg", "description": "React TypeScript CSS", "tags": "React"},
    ]


def main():
    labels = json.loads((ROOT / "evals" / "data" / "label_queries.json").read_text(encoding="utf-8"))
    jobs = load_sample_jobs()
    router = ModelRouter()
    index = build_embedding_index(jobs, router)

    results = {"embedding": [], "heuristic": []}
    for item in labels:
        query = f"{item['dream_role']} {item['resume_excerpt']} {item['location']}"
        relevant = set(item["relevant_job_ids"])
        emb_ranked = [j.get("job_id") for j in rank_by_embedding(query, jobs, index, top_k=10, router=router)]
        heur_ranked = heuristic_rank(query, jobs, top_k=10)
        results["embedding"].append({
            "query_id": item["query_id"],
            "ndcg@10": round(ndcg_at_k(relevant, emb_ranked, 10), 4),
            "p@5": round(precision_at_k(relevant, emb_ranked, 5), 4),
        })
        results["heuristic"].append({
            "query_id": item["query_id"],
            "ndcg@10": round(ndcg_at_k(relevant, heur_ranked, 10), 4),
            "p@5": round(precision_at_k(relevant, heur_ranked, 5), 4),
        })

    out_dir = ROOT / "evals" / "results"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "matching_eval.json"
    summary = {
        "embedding_avg_ndcg@10": sum(r["ndcg@10"] for r in results["embedding"]) / len(results["embedding"]),
        "heuristic_avg_ndcg@10": sum(r["ndcg@10"] for r in results["heuristic"]) / len(results["heuristic"]),
        "details": results,
        "cost_summary": router.cost_logger.summary(),
    }
    out_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
