# DataForge MCP server

Thin stdio MCP bridge over the Match/Jobs HTTPS API. No always-on host.

## Tools

| Tool | Purpose |
|------|---------|
| `search_jobs` | `GET /jobs?search=…` |
| `get_job` | Lookup by `job_id` |
| `match_resume` | `POST /match` (`method=agent` or `hybrid`) |

## Run locally

```bash
# terminal 1
py -3 scripts/run_match_api_local.py

# terminal 2 / Cursor MCP config
set DATAFORGE_API_BASE=http://127.0.0.1:8001
py -3 mcp/server.py
```

## Cursor MCP snippet

```json
{
  "mcpServers": {
    "dataforge": {
      "command": "py",
      "args": ["-3", "mcp/server.py"],
      "env": {
        "DATAFORGE_API_BASE": "http://127.0.0.1:8001",
        "DATAFORGE_API_KEY": ""
      }
    }
  }
}
```

## Auth

Production Match requires `X-API-Key` when `MATCH_API_KEY` is set on the Lambda.

```bash
set DATAFORGE_API_BASE=https://22oqvlj4pyl4geb2lzgeisnquy0dscjk.lambda-url.eu-central-1.on.aws/
set DATAFORGE_API_KEY=<from aws-keys-do-not-commit.txt>
```

Or in Cursor MCP `env`:

```json
"DATAFORGE_API_KEY": "<your key>"
```

Do not commit the key. Local Match (`scripts/run_match_api_local.py`) leaves the key unset by default.
