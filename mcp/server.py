#!/usr/bin/env python3
"""Thin MCP server over DataForge HTTPS Match/Jobs APIs (stdio for Cursor).

Tools:
  - search_jobs
  - get_job
  - match_resume

Env:
  DATAFORGE_API_BASE  — e.g. http://127.0.0.1:8001 or Function URL
  DATAFORGE_API_KEY   — optional X-API-Key
"""

from __future__ import annotations

import json
import os
import sys
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE = os.environ.get("DATAFORGE_API_BASE", "http://127.0.0.1:8001").rstrip("/")
API_KEY = os.environ.get("DATAFORGE_API_KEY", "").strip()


def _request(method: str, path: str, body: dict | None = None, query: dict | None = None) -> Any:
    url = f"{BASE}{path}"
    if query:
        url += "?" + urlencode({k: v for k, v in query.items() if v is not None and v != ""})
    data = None
    headers = {"Accept": "application/json", "User-Agent": "dataforge-mcp/0.1"}
    if API_KEY:
        headers["X-API-Key"] = API_KEY
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc
    except URLError as exc:
        raise RuntimeError(f"Request failed: {exc}") from exc


def tool_search_jobs(query: str = "", limit: int = 20) -> dict:
    return _request("GET", "/jobs", query={"search": query, "limit": limit, "tech_only": "true"})


def tool_get_job(job_id: str) -> dict:
    payload = _request("GET", "/jobs", query={"limit": 500})
    for job in payload.get("jobs") or []:
        if str(job.get("job_id")) == job_id:
            return job
    return {"error": "not_found", "job_id": job_id}


def tool_match_resume(
    resume: str,
    dream_role: str = "",
    location: str = "",
    method: str = "agent",
    visa_status: str = "",
    limit: int = 10,
) -> dict:
    return _request(
        "POST",
        "/match",
        body={
            "resume": resume,
            "dream_role": dream_role,
            "location": location,
            "method": method,
            "visa_status": visa_status,
            "limit": limit,
            "tech_only": True,
        },
    )


TOOLS = {
    "search_jobs": {
        "description": "Search DataForge tech jobs by free-text query",
        "handler": lambda args: tool_search_jobs(args.get("query", ""), int(args.get("limit", 20))),
        "schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
                "limit": {"type": "integer", "default": 20},
            },
        },
    },
    "get_job": {
        "description": "Fetch one job by job_id from the gold board",
        "handler": lambda args: tool_get_job(str(args.get("job_id", ""))),
        "schema": {
            "type": "object",
            "required": ["job_id"],
            "properties": {"job_id": {"type": "string"}},
        },
    },
    "match_resume": {
        "description": "Match a resume to jobs (hybrid or multi-agent)",
        "handler": lambda args: tool_match_resume(
            resume=str(args.get("resume", "")),
            dream_role=str(args.get("dream_role", "")),
            location=str(args.get("location", "")),
            method=str(args.get("method", "agent")),
            visa_status=str(args.get("visa_status", "")),
            limit=int(args.get("limit", 10)),
        ),
        "schema": {
            "type": "object",
            "required": ["resume"],
            "properties": {
                "resume": {"type": "string"},
                "dream_role": {"type": "string"},
                "location": {"type": "string"},
                "method": {"type": "string", "enum": ["agent", "hybrid", "bm25", "keyword"]},
                "visa_status": {"type": "string"},
                "limit": {"type": "integer", "default": 10},
            },
        },
    },
}


def _send(msg: dict) -> None:
    sys.stdout.write(json.dumps(msg) + "\n")
    sys.stdout.flush()


def _handle(msg: dict) -> dict | None:
    """Minimal JSON-RPC style loop compatible with simple MCP clients / demos."""
    mid = msg.get("id")
    method = msg.get("method")
    params = msg.get("params") or {}

    if method == "initialize":
        return {
            "jsonrpc": "2.0",
            "id": mid,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "dataforge", "version": "0.1.0"},
            },
        }
    if method == "tools/list":
        tools = [
            {
                "name": name,
                "description": meta["description"],
                "inputSchema": meta["schema"],
            }
            for name, meta in TOOLS.items()
        ]
        return {"jsonrpc": "2.0", "id": mid, "result": {"tools": tools}}
    if method == "tools/call":
        name = params.get("name")
        args = params.get("arguments") or {}
        if name not in TOOLS:
            return {
                "jsonrpc": "2.0",
                "id": mid,
                "error": {"code": -32601, "message": f"Unknown tool {name}"},
            }
        try:
            result = TOOLS[name]["handler"](args)
            return {
                "jsonrpc": "2.0",
                "id": mid,
                "result": {
                    "content": [{"type": "text", "text": json.dumps(result, indent=2)}],
                    "isError": False,
                },
            }
        except Exception as exc:
            return {
                "jsonrpc": "2.0",
                "id": mid,
                "result": {
                    "content": [{"type": "text", "text": str(exc)}],
                    "isError": True,
                },
            }
    if method in {"notifications/initialized", "ping"}:
        return None
    return {
        "jsonrpc": "2.0",
        "id": mid,
        "error": {"code": -32601, "message": f"Method not found: {method}"},
    }


def main() -> None:
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        resp = _handle(msg)
        if resp is not None:
            _send(resp)


if __name__ == "__main__":
    main()
