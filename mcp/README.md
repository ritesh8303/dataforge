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

Replace `DATAFORGE_API_BASE` with the live Function URL from `terraform/eu-outputs.json` (`match_function_url`).
