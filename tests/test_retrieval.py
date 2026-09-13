"""Tests for BM25 / hybrid retrieval and vector store fallback."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from ai_gateway.router import ModelRouter
from retrieval import BM25Index, rank_bm25, rank_hybrid, reciprocal_rank_fusion
from vector_store import VectorStore, build_store_from_embedding_index
from embedding_index import build_embedding_index


JOBS = [
    {
        "job_id": "j1",
        "title": "Data Engineer Python AWS",
        "company": "A",
        "description": "Spark Airflow lakehouse pipelines",
        "tags": "Python",
    },
    {
        "job_id": "j2",
        "title": "Frontend React Developer",
        "company": "B",
        "description": "React TypeScript CSS design systems",
        "tags": "React",
    },
    {
        "job_id": "j3",
        "title": "ML Engineer LLM",
        "company": "C",
        "description": "PyTorch NLP RAG embeddings",
        "tags": "ML",
    },
]


def test_bm25_ranks_relevant_first():
    ranked = rank_bm25("python data engineer spark airflow", JOBS, top_k=3)
    assert ranked[0]["job_id"] == "j1"


def test_rrf_merges_lists():
    fused = reciprocal_rank_fusion([["a", "b", "c"], ["b", "a", "d"]], k=60)
    assert fused[0][0] in {"a", "b"}


def test_hybrid_with_local_embeddings():
    router = ModelRouter()
    index = build_embedding_index(JOBS, router)
    vectors = {e["job_id"]: e["vector"] for e in index}
    ranked = rank_hybrid(
        "machine learning LLM RAG",
        JOBS,
        vectors_by_id=vectors,
        embed_fn=lambda t: router.embed("embed", t).vector,
        top_k=3,
    )
    assert ranked[0]["job_id"] == "j3"
    assert ranked[0]["match_method"] == "hybrid_rrf"


def test_vector_store_upsert_and_search(tmp_path):
    router = ModelRouter()
    index = build_embedding_index(JOBS, router)
    store = build_store_from_embedding_index(index)
    assert len(store.vectors_by_id()) == 3
    path = tmp_path / "idx.json"
    store.save_json(path)
    store2 = VectorStore()
    assert store2.load_json(path) == 3
    q = router.embed("embed", "data engineer python").vector
    hits = store2.search(q, top_k=2)
    assert hits[0]["job_id"] == "j1"


def test_bm25_empty_corpus():
    idx = BM25Index([])
    assert idx.rank("python") == []
