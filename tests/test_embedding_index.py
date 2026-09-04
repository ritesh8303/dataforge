import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from embedding_index import build_embedding_index, cosine_similarity, rank_by_embedding, job_text
from ai_gateway.router import ModelRouter


SAMPLE_JOBS = [
    {"job_id": "j1", "title": "Data Engineer Python", "company": "A", "description": "Python AWS Spark", "tags": "Python"},
    {"job_id": "j2", "title": "Frontend React", "company": "B", "description": "React TypeScript", "tags": "React"},
]


def test_cosine_similarity_identical():
    v = [1.0, 0.0, 1.0]
    assert cosine_similarity(v, v) == pytest.approx(1.0)


def test_job_text_combines_fields():
    text = job_text({"title": "Engineer", "company": "Co", "description": "Python", "tags": "AWS"})
    assert "Engineer" in text
    assert "Python" in text


def test_build_embedding_index():
    router = ModelRouter()
    index = build_embedding_index(SAMPLE_JOBS, router)
    assert len(index) == 2
    assert all("vector" in e for e in index)


def test_rank_by_embedding_prefers_relevant():
    router = ModelRouter()
    index = build_embedding_index(SAMPLE_JOBS, router)
    ranked = rank_by_embedding("Python data engineer AWS", SAMPLE_JOBS, index, top_k=2, router=router)
    assert ranked[0]["job_id"] == "j1"
