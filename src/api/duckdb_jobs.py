"""DuckDB-backed Gold job search helper (local analytics / Jobs API alternative)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any


def _default_csv() -> Path:
    env = os.environ.get("GOLD_JOBS_CSV", "").strip()
    if env:
        return Path(env)
    return Path("data/gold/all_jobs.csv")


def search_jobs_duckdb(
    *,
    query: str = "",
    location: str = "",
    field: str = "",
    employment: str = "",
    limit: int = 50,
    csv_path: str | Path | None = None,
) -> dict[str, Any]:
    """Full-text-ish search over Gold CSV via DuckDB (requires duckdb package)."""
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError("duckdb not installed — pip install duckdb") from exc

    path = Path(csv_path) if csv_path else _default_csv()
    if not path.exists():
        raise FileNotFoundError(f"Gold CSV not found: {path}")

    con = duckdb.connect(database=":memory:")
    # Register CSV as view
    con.execute(
        f"CREATE VIEW jobs AS SELECT * FROM read_csv_auto('{path.as_posix()}', header=true, ignore_errors=true)"
    )
    clauses: list[str] = ["1=1"]
    params: list[Any] = []
    if query:
        clauses.append(
            "(lower(coalesce(title,'')) LIKE ? OR lower(coalesce(company,'')) LIKE ? "
            "OR lower(coalesce(tags,'')) LIKE ? OR lower(coalesce(canonical_title_en,'')) LIKE ? "
            "OR lower(coalesce(description,'')) LIKE ?)"
        )
        q = f"%{query.lower()}%"
        params.extend([q, q, q, q, q])
    if location:
        clauses.append("lower(coalesce(location,'')) LIKE ?")
        params.append(f"%{location.lower()}%")
    if field:
        clauses.append(
            "(lower(coalesce(ai_field,'')) LIKE ? OR lower(coalesce(field,'')) LIKE ?)"
        )
        f = f"%{field.lower()}%"
        params.extend([f, f])
    if employment:
        clauses.append(
            "(lower(coalesce(employment_type,'')) LIKE ? OR lower(coalesce(ai_seniority,'')) LIKE ?)"
        )
        e = f"%{employment.lower()}%"
        params.extend([e, e])

    sql = f"SELECT * FROM jobs WHERE {' AND '.join(clauses)} LIMIT ?"
    params.append(int(limit))
    rows = con.execute(sql, params).fetchdf()
    jobs = rows.to_dict(orient="records") if len(rows) else []
    # Convert non-JSON-native values
    clean = []
    for job in jobs:
        clean.append({k: (None if (hasattr(v, '__float__') and str(v) == 'nan') else v) for k, v in job.items()})
    return {"jobs": clean, "count": len(clean), "backend": "duckdb", "source": str(path)}
