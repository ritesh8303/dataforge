# DataForge MCP server

Thin stdio MCP bridge over the Match/Jobs HTTPS API. No always-on host.

## Tools

| Tool | Purpose |
|------|---------|
| `search_jobs` | `GET /jobs?search=…` |
| `get_job` | Lookup by `job_id` |
| `match_resume` | `POST /match` (`method=agent` or `hybrid`) |

## Production (recommended)

1. Ensure `aws-keys-do-not-commit.txt` exists in the repo root (gitignored) with:
   ```
   MATCH_API_KEY=…
   MATCH_FUNCTION_URL=https://22oqvlj4pyl4geb2lzgeisnquy0dscjk.lambda-url.eu-central-1.on.aws/
   ```
2. Copy `mcp/cursor.mcp.example.json` into your Cursor MCP settings (or merge the `dataforge` block).
3. Restart Cursor MCP. The server auto-loads `MATCH_API_KEY` from that file if `DATAFORGE_API_KEY` is unset.

## Local Match API (no key)

```bash
# terminal 1
py -3 scripts/run_match_api_local.py

# Cursor MCP env
DATAFORGE_API_BASE=http://127.0.0.1:8001
```

## Auth

Production Match requires `X-API-Key`. Prefer the gitignored key file over pasting secrets into committed JSON.

Optional override:

```bash
set DATAFORGE_API_KEY=<key>
set DATAFORGE_API_BASE=https://22oqvlj4pyl4geb2lzgeisnquy0dscjk.lambda-url.eu-central-1.on.aws/
py -3 mcp/server.py
```
