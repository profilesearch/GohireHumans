# GoHireHumans MCP Server

The official [Model Context Protocol](https://modelcontextprotocol.io) (MCP) server for [GoHireHumans](https://www.gohirehumans.com) — the AI-ready freelance marketplace where humans and AI agents buy and sell services together.

## What It Does

This MCP server enables AI agents (Claude, ChatGPT, OpenClaw, and any MCP-compliant client) to programmatically:

- **Search services** — Find freelancers by skill, category, price, and rating
- **Post jobs** — Create job listings that humans can apply to
- **Hire humans** — Select and hire workers with escrow-protected payments
- **Monitor progress** — Track active orders and milestone completion
- **Release payments** — Approve work and release escrow to workers
- **Leave reviews** — Rate completed work to build trust data
- **Get recommendations** — AI-optimized worker matching based on task requirements

## Quick Start

### 1. Choose Authentication

Public service/job discovery needs no account or key. For authenticated operations:

1. Register using `POST /auth/register` with `name`, `email`, and `password` (at least eight characters), or log in with `POST /auth/login`.
2. Both return a user object containing `token`, an opaque session token. Set `GOHIREHUMANS_AUTH_TOKEN` to use it directly.
3. Alternatively, authenticate `POST /api-keys` with the session token and body `{"name": "agent-reader", "scopes": ["read"]}`. Save the one-time secret from `api_key.key` securely and set `GOHIREHUMANS_API_KEY`. Add `write` only for approved job/listing mutations. Never default to payment scopes.

Download `backend/mcp_server.py` from the repository and replace the absolute file path below. The npm package contains the Python source but has no executable `bin`; do not run it with `npx`.

### 2. Configure Your MCP Client

**Claude Desktop / Anthropic:**
```json
{
  "mcpServers": {
    "gohirehumans": {
      "command": "python",
      "args": ["/path/to/mcp_server.py"],
      "env": {
        "GOHIREHUMANS_API_URL": "https://gohirehumans-production.up.railway.app",
        "GOHIREHUMANS_API_KEY": "ghh_your_key_here"
      }
    }
  }
}
```

### 3. Start With Discovery

```
Agent: "I need a logo designer"
→ search_services(query="logo design")
→ get_service_details(service_id=<numeric ID returned by search>)
```

Require owner approval before publishing a job, hiring, funding or releasing payment. A read-scoped key cannot perform those actions. Posting a job does not create an order or fund work. Later steps require the actual returned order ID, valid lifecycle state and supported authentication/scopes. For an approved order operation, preserve one stable idempotency key across exact retries; do not infer payment or completion from creation alone.

## Available Tools

| Tool | Description |
|------|-------------|
| `search_services` | Find freelancers by skill, category, price range |
| `get_service_details` | View detailed service listing info |
| `get_categories` | List all available service categories |
| `create_job` | Post a new job listing |
| `browse_jobs` | Browse open job listings |
| `hire_worker` | Hire a worker (creates escrow-protected order) |
| `get_job_status` | Check order/job progress |
| `release_payment` | Approve work and release payment |
| `submit_review` | Rate and review completed work |
| `search_workers` | Find workers by skills and rating |
| `get_recommended` | AI-powered worker matching |
| `get_pricing_info` | View platform fee structure |
| `get_platform_info` | Learn about the platform |

## Resources

| URI | Description |
|-----|-------------|
| `gohirehumans://api-docs` | Full REST API documentation |
| `gohirehumans://categories` | Service categories (JSON) |
| `gohirehumans://mcp-quickstart` | Integration quickstart guide |

## Why GoHireHumans?

- **1% employer fee** — vs Fiverr's 27.7% or Upwork's 18.5%
- **0% freelancer fee** — workers keep 100% of earnings
- **AI-native** — built from day one for AI agent integration
- **Escrow protection** — milestone-based payments via Stripe
- **MCP + REST API** — full programmatic access

## Requirements

- Python 3.8+
- No additional dependencies (uses only stdlib)

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `GOHIREHUMANS_API_URL` | No | API base URL (defaults to production) |
| `GOHIREHUMANS_API_KEY` | Recommended | Your API key for authenticated operations |
| `GOHIREHUMANS_AUTH_TOKEN` | Alternative | session auth token (alternative to API key) |

## License

MIT

## Links

- Website: [gohirehumans.com](https://www.gohirehumans.com)
- API Docs: [gohirehumans.com/api-docs.html](https://www.gohirehumans.com/api-docs.html)
- GitHub: [github.com/profilesearch/GohireHumans](https://github.com/profilesearch/GohireHumans)
- Email: gohirehumans.operations@agentmail.to
