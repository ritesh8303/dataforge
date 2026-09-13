"""FastAPI application for DataForge Match / Jobs / health endpoints."""

from __future__ import annotations

import os
from typing import Any, Literal

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel, Field

from api.match_service import jobs_to_markdown, load_jobs_and_index, match_jobs
from agent.graph import run_match_agent

APP_TITLE = "DataForge Match API"
API_KEY_ENV = "MATCH_API_KEY"


class MatchRequest(BaseModel):
    resume: str = ""
    dream_role: str = ""
    location: str = ""
    method: Literal["hybrid", "bm25", "dense", "embedding", "keyword", "agent"] = "hybrid"
    limit: int = Field(default=15, ge=1, le=50)
    visa_status: str = Field(
        default="",
        description="eu_citizen | blue_card | chancenkarte_or_job_seeker | student_visa | needs_visa_from_abroad",
    )
    german_level: str = Field(default="", description="CEFR level, e.g. A1–C2")
    entry_level_only: bool = False
    english_ok_only: bool = False
    tech_only: bool = True


def _expected_api_key() -> str:
    return os.environ.get(API_KEY_ENV, "").strip()


async def require_api_key(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> None:
    expected = _expected_api_key()
    if not expected:
        # Local/dev: key optional when unset
        return
    if not x_api_key or x_api_key != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing X-API-Key")


app = FastAPI(
    title=APP_TITLE,
    version="0.2.0",
    description=(
        "Semantic job matching over the DataForge gold lakehouse. "
        "Hybrid BM25+dense retrieval with PII redaction, visa-aware filters, and cited reasons."
    ),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[os.environ.get("ALLOWED_ORIGIN", "*")],
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "dataforge-match"}


@app.get("/jobs")
def list_jobs(
    limit: int = Query(default=50, ge=1, le=500),
    search: str = "",
    tech_only: bool = True,
    _: None = Depends(require_api_key),
) -> dict[str, Any]:
    jobs, _ = load_jobs_and_index()
    if tech_only:
        jobs = [j for j in jobs if j.get("is_tech") is None or str(j.get("is_tech")).lower() in {"1", "true", "yes"}]
    if search:
        q = search.lower()
        jobs = [
            j
            for j in jobs
            if q in str(j.get("title", "")).lower()
            or q in str(j.get("company", "")).lower()
            or q in str(j.get("tags", "")).lower()
        ]
    return {"jobs": jobs[:limit], "count": min(len(jobs), limit)}


@app.post("/match")
async def match_endpoint(
    body: MatchRequest,
    request: Request,
    _: None = Depends(require_api_key),
) -> Response:
    try:
        if body.method == "agent":
            payload = run_match_agent(
                resume=body.resume,
                dream_role=body.dream_role,
                location=body.location,
                limit=body.limit,
                visa_status=body.visa_status,
                german_level=body.german_level,
                entry_level_only=body.entry_level_only,
                english_ok_only=body.english_ok_only,
                tech_only=body.tech_only,
            )
        else:
            payload = match_jobs(
                resume=body.resume,
                dream_role=body.dream_role,
                location=body.location,
                method=body.method,
                limit=body.limit,
                visa_status=body.visa_status,
                german_level=body.german_level,
                entry_level_only=body.entry_level_only,
                english_ok_only=body.english_ok_only,
                tech_only=body.tech_only,
            )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    accept = (request.headers.get("accept") or "").lower()
    if "text/markdown" in accept:
        return PlainTextResponse(jobs_to_markdown(payload), media_type="text/markdown")
    return JSONResponse(payload)


@app.post("/match/agent")
async def match_agent_endpoint(
    body: MatchRequest,
    request: Request,
    _: None = Depends(require_api_key),
) -> Response:
    body.method = "agent"
    return await match_endpoint(body, request, None)


@app.get("/match")
async def match_get(
    request: Request,
    resume: str = "",
    dream_role: str = "",
    search: str = "",
    location: str = "",
    method: str = "hybrid",
    limit: int = Query(default=15, ge=1, le=50),
    visa_status: str = "",
    german_level: str = "",
    entry_level_only: bool = False,
    english_ok_only: bool = False,
    tech_only: bool = True,
    _: None = Depends(require_api_key),
) -> Response:
    body = MatchRequest(
        resume=resume,
        dream_role=dream_role or search,
        location=location,
        method=method if method in {"hybrid", "bm25", "dense", "embedding", "keyword", "agent"} else "hybrid",
        limit=limit,
        visa_status=visa_status if visa_status else "",  # type: ignore[arg-type]
        german_level=german_level,
        entry_level_only=entry_level_only,
        english_ok_only=english_ok_only,
        tech_only=tech_only,
    )
    return await match_endpoint(body, request, None)
