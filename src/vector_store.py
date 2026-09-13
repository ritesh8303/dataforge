"""LanceDB-backed vector store on S3 (with JSON fallback for tests/local).

Uses LanceDB when installed; otherwise persists the same schema via the
legacy embedding_index JSON helpers so CI stays dependency-light.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any, Sequence

logger = logging.getLogger(__name__)

try:
    import lancedb  # type: ignore

    HAS_LANCEDB = True
except ImportError:  # pragma: no cover - optional dependency
    HAS_LANCEDB = False


TABLE_NAME = "job_embeddings"


def _job_id(row: dict[str, Any]) -> str:
    return str(row.get("job_id") or row.get("id") or "")


class VectorStore:
    """Upsert / search job embedding vectors."""

    def __init__(self, uri: str | None = None, table_name: str = TABLE_NAME):
        self.uri = uri or os.environ.get("VECTOR_STORE_URI", "")
        self.table_name = table_name
        self._memory: dict[str, dict[str, Any]] = {}
        self._db = None
        self._table = None
        if self.uri and HAS_LANCEDB:
            try:
                self._db = lancedb.connect(self.uri)
            except Exception as exc:  # pragma: no cover
                logger.warning("LanceDB connect failed (%s); using memory fallback", exc)

    @property
    def backend(self) -> str:
        if self._db is not None:
            return "lancedb"
        if self.uri.startswith("s3://") or self.uri.endswith(".json"):
            return "json"
        return "memory"

    def upsert(self, entries: Sequence[dict[str, Any]]) -> int:
        """Upsert rows shaped like {job_id, vector, model, provider, text?}."""
        count = 0
        for entry in entries:
            jid = _job_id(entry)
            if not jid or not entry.get("vector"):
                continue
            self._memory[jid] = {
                "job_id": jid,
                "vector": list(entry["vector"]),
                "model": entry.get("model", ""),
                "provider": entry.get("provider", ""),
                "text": entry.get("text", ""),
            }
            count += 1

        if self._db is not None and count:
            rows = list(self._memory.values())
            try:
                existing = set(self._db.table_names())
                if self.table_name in existing:
                    self._table = self._db.open_table(self.table_name)
                    # Simple full rewrite for demo volumes (<50k).
                    self._db.drop_table(self.table_name)
                self._table = self._db.create_table(self.table_name, data=rows, mode="overwrite")
            except Exception as exc:  # pragma: no cover
                logger.warning("LanceDB upsert failed: %s", exc)
        return count

    def vectors_by_id(self) -> dict[str, list[float]]:
        if self._table is not None:
            try:
                df = self._table.to_pandas()
                return {str(r["job_id"]): list(r["vector"]) for _, r in df.iterrows()}
            except Exception as exc:  # pragma: no cover
                logger.warning("LanceDB read failed: %s", exc)
        return {k: v["vector"] for k, v in self._memory.items()}

    def to_json_entries(self) -> list[dict[str, Any]]:
        return [
            {
                "job_id": e["job_id"],
                "vector": e["vector"],
                "model": e.get("model", ""),
                "provider": e.get("provider", ""),
            }
            for e in self._memory.values()
        ]

    def save_json(self, path: str | Path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"version": 1, "backend": self.backend, "entries": self.to_json_entries()}),
            encoding="utf-8",
        )

    def load_json(self, path: str | Path) -> int:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        entries = data.get("entries", data if isinstance(data, list) else [])
        return self.upsert(entries)

    def search(self, query_vector: Sequence[float], top_k: int = 20) -> list[dict[str, Any]]:
        from embedding_index import cosine_similarity

        scored = []
        for jid, vec in self.vectors_by_id().items():
            scored.append(
                {
                    "job_id": jid,
                    "score": cosine_similarity(list(query_vector), list(vec)),
                }
            )
        scored.sort(key=lambda x: x["score"], reverse=True)
        return scored[:top_k]


def build_store_from_embedding_index(index: list[dict], uri: str | None = None) -> VectorStore:
    store = VectorStore(uri=uri)
    store.upsert(index)
    return store


def local_temp_store() -> VectorStore:
    """Ephemeral on-disk LanceDB (or memory) for tests."""
    if HAS_LANCEDB:
        tmp = tempfile.mkdtemp(prefix="dataforge-lancedb-")
        return VectorStore(uri=tmp)
    return VectorStore(uri="")
