# money — ⚠️ ARCHIVED / DEAD

**This standalone repo is no longer developed. It was folded into the homelab monorepo.**

The personal-finance dashboard, scrapers, and ingest pipeline that lived here were
reimplemented inside `~/projects/homelab`:

| What was here                          | Where it lives now                     |
| -------------------------------------- | -------------------------------------- |
| React dashboard (`frontend/`)          | `homelab/apps/money`                   |
| Python ingest / scrapers (`src/money/`)| `homelab/services/ingest/src/money`    |
| Chrome capture extension (`extension/`)| `homelab/extension`                    |

The homelab version is the live, actively-synced system (multiple institutions,
holdings-level data, and MCP tooling). **Do all money work there.**

## Status
- Last commit here: **2026-04-06**.
- Superseded by homelab (actively developed and syncing as of 2026-07).
- Kept only for git history / reference. Do not run, sync, or extend this code.

## If you want to fully remove it
The history is pushed to `origin/main`, so the working copy is safe to delete:

```sh
rm -rf ~/projects/money
```

The stale `money-db` MCP entry in this repo's `.mcp.json` points at
`money.mcp_server` here — the homelab exposes its own money MCP tools, so that
entry can be dropped.
