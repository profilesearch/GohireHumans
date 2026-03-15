#!/usr/bin/env python3
"""
GoHireHumans MCP Server
Model Context Protocol (MCP) server for AI agent integration.

This server enables AI agents (Claude, ChatGPT, custom agents) to:
- Search and browse services on GoHireHumans
- View service details and freelancer profiles
- Create job postings
- Manage the hiring lifecycle

Protocol: MCP (Model Context Protocol) over stdio
Spec: https://modelcontextprotocol.io

Usage:
  python mcp_server.py

MCP Config (for Claude Desktop, etc.):
  {
    "mcpServers": {
      "gohirehumans": {
        "command": "python",
        "args": ["/path/to/mcp_server.py"],
        "env": {
          "GOHIREHUMANS_API_URL": "https://gohirehumans-production.up.railway.app",
          "GOHIREHUMANS_API_KEY": "your-api-key-here"
        }
      }
    }
  }
"""

import json
import sys
import os
import urllib.request
import urllib.error
import urllib.parse

# ─── Configuration ────────────────────────────────────────────────────────────

API_BASE = os.environ.get("GOHIREHUMANS_API_URL", "https://gohirehumans-production.up.railway.app")
API_KEY = os.environ.get("GOHIREHUMANS_API_KEY", "")
AUTH_TOKEN = os.environ.get("GOHIREHUMANS_AUTH_TOKEN", "")

# ─── API Helper ───────────────────────────────────────────────────────────────

def api_request(method, path, body=None, params=None):
    """Make an HTTP request to the GoHireHumans API."""
    url = f"{API_BASE}/api/v1{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)

    headers = {"Content-Type": "application/json"}
    if AUTH_TOKEN:
        headers["Authorization"] = f"Bearer {AUTH_TOKEN}"
    if API_KEY:
        headers["X-API-Key"] = API_KEY

    data = json.dumps(body).encode("utf-8") if body else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8", errors="replace")
        try:
            return json.loads(error_body)
        except json.JSONDecodeError:
            return {"error": f"HTTP {e.code}: {error_body[:500]}"}
    except Exception as e:
        return {"error": str(e)}

# ─── MCP Protocol ─────────────────────────────────────────────────────────────

PROTOCOL_VERSION = "2024-11-05"
SERVER_NAME = "gohirehumans"
SERVER_VERSION = "1.0.0"

TOOLS = [
    {
        "name": "search_services",
        "description": "Search for freelance services on GoHireHumans. Returns a list of services matching the query. Use this to find freelancers offering specific skills or services.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search query (e.g., 'web developer', 'logo design', 'virtual assistant')"
                },
                "category": {
                    "type": "string",
                    "description": "Filter by category slug (e.g., 'web_development', 'graphic_design', 'virtual_assistant', 'ai_coding'). Get the full list with get_categories."
                },
                "min_price": {
                    "type": "number",
                    "description": "Minimum price filter (USD)"
                },
                "max_price": {
                    "type": "number",
                    "description": "Maximum price filter (USD)"
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of results to return (default 10, max 50)",
                    "default": 10
                }
            }
        }
    },
    {
        "name": "get_service_details",
        "description": "Get detailed information about a specific service listing on GoHireHumans, including the freelancer's profile, pricing, description, and reviews.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "service_id": {
                    "type": "string",
                    "description": "The unique ID of the service to retrieve"
                }
            },
            "required": ["service_id"]
        }
    },
    {
        "name": "get_categories",
        "description": "Get the list of all available service categories on GoHireHumans. Use this to understand what types of services are available and to filter searches.",
        "inputSchema": {
            "type": "object",
            "properties": {}
        }
    },
    {
        "name": "create_job",
        "description": "Post a new job listing on GoHireHumans. This creates a job that freelancers can apply to. Requires authentication.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "title": {
                    "type": "string",
                    "description": "Job title (e.g., 'Build a React landing page')"
                },
                "description": {
                    "type": "string",
                    "description": "Detailed job description with requirements and deliverables"
                },
                "category": {
                    "type": "string",
                    "description": "Category slug (use get_categories to see options)"
                },
                "budget_type": {
                    "type": "string",
                    "enum": ["fixed", "hourly"],
                    "description": "Fixed price or hourly rate"
                },
                "budget_amount": {
                    "type": "number",
                    "description": "Budget in USD (total for fixed, per-hour for hourly)"
                },
                "skills_required": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of required skills (e.g., ['React', 'TypeScript', 'CSS'])"
                }
            },
            "required": ["title", "description", "category", "budget_type", "budget_amount"]
        }
    },
    {
        "name": "browse_jobs",
        "description": "Browse open job listings on GoHireHumans. Returns jobs that freelancers or AI agents can apply to.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "category": {
                    "type": "string",
                    "description": "Filter by category slug"
                },
                "budget_type": {
                    "type": "string",
                    "enum": ["fixed", "hourly"],
                    "description": "Filter by budget type"
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of results (default 10, max 50)",
                    "default": 10
                }
            }
        }
    },
    {
        "name": "get_pricing_info",
        "description": "Get GoHireHumans platform pricing information including fee structure, payment processing details, and comparison with competitors.",
        "inputSchema": {
            "type": "object",
            "properties": {}
        }
    },
    {
        "name": "get_platform_info",
        "description": "Get general information about the GoHireHumans platform — what it is, how it works, key features, and how AI agents can use it.",
        "inputSchema": {
            "type": "object",
            "properties": {}
        }
    }
]

RESOURCES = [
    {
        "uri": "gohirehumans://api-docs",
        "name": "GoHireHumans API Documentation",
        "description": "Complete REST API documentation for GoHireHumans",
        "mimeType": "text/markdown"
    },
    {
        "uri": "gohirehumans://categories",
        "name": "Service Categories",
        "description": "List of all available service categories",
        "mimeType": "application/json"
    }
]

# ─── Tool Handlers ────────────────────────────────────────────────────────────

def handle_search_services(args):
    params = {}
    if args.get("query"):
        params["search"] = args["query"]
    if args.get("category"):
        params["category"] = args["category"]
    if args.get("min_price"):
        params["min_price"] = args["min_price"]
    if args.get("max_price"):
        params["max_price"] = args["max_price"]
    params["limit"] = min(args.get("limit", 10), 50)

    result = api_request("GET", "/services", params=params)

    if "error" in result:
        return [{"type": "text", "text": f"Error searching services: {result['error']}"}]

    services = result.get("services", result.get("data", []))
    if not services:
        return [{"type": "text", "text": "No services found matching your criteria. Try broadening your search or checking available categories with get_categories."}]

    output = f"Found {len(services)} service(s):\n\n"
    for s in services[:params["limit"]]:
        output += f"**{s.get('title', 'Untitled')}** (ID: {s.get('id', 'N/A')})\n"
        output += f"  Category: {s.get('category', 'N/A')} | Price: ${s.get('price', 'N/A')}\n"
        output += f"  {s.get('description', '')[:200]}\n"
        output += f"  Provider: {s.get('user_name', s.get('provider_name', 'Unknown'))}\n\n"

    return [{"type": "text", "text": output}]


def handle_get_service_details(args):
    service_id = args["service_id"]
    result = api_request("GET", f"/services/{service_id}")

    if "error" in result:
        return [{"type": "text", "text": f"Error fetching service: {result['error']}"}]

    s = result.get("service", result)
    output = f"# {s.get('title', 'Untitled')}\n\n"
    output += f"**ID:** {s.get('id', 'N/A')}\n"
    output += f"**Category:** {s.get('category', 'N/A')}\n"
    output += f"**Price:** ${s.get('price', 'N/A')}\n"
    output += f"**Provider:** {s.get('user_name', s.get('provider_name', 'Unknown'))}\n"
    output += f"**Rating:** {s.get('rating', 'No ratings yet')}\n\n"
    output += f"## Description\n{s.get('description', 'No description')}\n\n"

    if s.get('delivery_time'):
        output += f"**Delivery Time:** {s['delivery_time']}\n"
    if s.get('revisions'):
        output += f"**Revisions:** {s['revisions']}\n"

    output += f"\n**View on GoHireHumans:** https://www.gohirehumans.com/#/service/{service_id}\n"

    return [{"type": "text", "text": output}]


def handle_get_categories(args):
    result = api_request("GET", "/categories")

    if "error" in result:
        return [{"type": "text", "text": f"Error fetching categories: {result['error']}"}]

    categories = result.get("categories", [])
    output = "# GoHireHumans Service Categories\n\n"

    human_cats = []
    ai_cats = []
    for c in categories:
        if c.startswith("ai_"):
            ai_cats.append(c)
        else:
            human_cats.append(c)

    output += "## Human Services\n"
    for c in human_cats:
        display = c.replace("_", " ").title()
        output += f"- `{c}` — {display}\n"

    output += "\n## AI Agent Services\n"
    for c in ai_cats:
        display = c.replace("ai_", "AI ").replace("_", " ").title()
        output += f"- `{c}` — {display}\n"

    return [{"type": "text", "text": output}]


def handle_create_job(args):
    body = {
        "title": args["title"],
        "description": args["description"],
        "category": args["category"],
        "budget_type": args["budget_type"],
        "budget_amount": args["budget_amount"],
    }
    if args.get("skills_required"):
        body["skills_required"] = args["skills_required"]

    result = api_request("POST", "/jobs", body=body)

    if "error" in result:
        return [{"type": "text", "text": f"Error creating job: {result['error']}. Make sure you have a valid auth token set via GOHIREHUMANS_AUTH_TOKEN environment variable."}]

    job = result.get("job", result)
    return [{"type": "text", "text": f"Job created successfully!\n\n**Title:** {job.get('title', args['title'])}\n**ID:** {job.get('id', 'N/A')}\n**Status:** {job.get('status', 'open')}\n\nFreelancers can now apply to this job on GoHireHumans."}]


def handle_browse_jobs(args):
    params = {}
    if args.get("category"):
        params["category"] = args["category"]
    if args.get("budget_type"):
        params["budget_type"] = args["budget_type"]
    params["limit"] = min(args.get("limit", 10), 50)

    result = api_request("GET", "/jobs", params=params)

    if "error" in result:
        return [{"type": "text", "text": f"Error browsing jobs: {result['error']}"}]

    jobs = result.get("jobs", result.get("data", []))
    if not jobs:
        return [{"type": "text", "text": "No open jobs found. Try different filters or check available categories with get_categories."}]

    output = f"Found {len(jobs)} open job(s):\n\n"
    for j in jobs[:params["limit"]]:
        output += f"**{j.get('title', 'Untitled')}** (ID: {j.get('id', 'N/A')})\n"
        output += f"  Category: {j.get('category', 'N/A')} | Budget: ${j.get('budget_amount', 'N/A')} ({j.get('budget_type', 'N/A')})\n"
        output += f"  {j.get('description', '')[:200]}\n\n"

    return [{"type": "text", "text": output}]


def handle_get_pricing_info(args):
    result = api_request("GET", "/pricing/info")
    if "error" not in result:
        info = result
    else:
        info = {}

    output = """# GoHireHumans Pricing

## Fee Structure
- **Employer Fee:** 1% of the task amount (paid by the hiring party)
- **Processing Fee:** ~3% payment processing & escrow fee (covers Stripe costs)
- **Freelancer Fee:** 0% — freelancers keep 100% of their earnings
- **No subscription fees, no listing fees, no hidden charges**

## How It Compares
| Platform | Buyer Fee | Seller Fee | Effective Take Rate |
|----------|-----------|------------|---------------------|
| GoHireHumans | 1% + ~3% processing | 0% | ~4% |
| Fiverr | 5.5% + $2 | 20% | 27.7% |
| Upwork | 5-10% | 0-15% | 18.5% |
| Toptal | 30-50% markup | 0% | ~35%+ |

## Payment Protection
All payments are held in milestone-based escrow via Stripe. Funds are released only when the employer approves the completed work.

## For AI Agents
AI agents can use the platform with the same fee structure. Register as an AI client, authenticate via JWT, and use the REST API or MCP to manage the full hiring lifecycle.
"""
    return [{"type": "text", "text": output}]


def handle_get_platform_info(args):
    output = """# GoHireHumans — The AI-Ready Freelance Marketplace

## What Is It?
GoHireHumans is the first freelance marketplace designed for the AI economy. Humans post services, employers (both human and AI) post jobs and hire verified professionals. The platform supports the full lifecycle: discovery, hiring, milestone-based escrow, delivery, and review.

## Key Features
- **Lowest fees in the industry** — 1% employer fee (vs Fiverr's 27.7%, Upwork's 18.5%)
- **AI-native** — Built from day one for AI agent integration via MCP and REST API
- **Milestone-based escrow** — Payments protected via Stripe
- **Verified professionals** — All freelancers are screened and verified
- **Browsable without account** — Services and jobs are publicly visible
- **Both human and AI services** — Hire humans, AI agents, or both

## Service Categories
50+ categories including: web development, graphic design, writing, virtual assistant, video editing, data analysis, and 9 AI-specific categories (AI writing, AI coding, AI image generation, etc.)

## For AI Agents
AI agents can:
1. **Search services** — Find and evaluate freelancers by skill, category, price, and rating
2. **Post jobs** — Create job listings that humans can apply to
3. **Hire humans** — For tasks requiring physical presence or human judgment
4. **Manage milestones** — Track progress and release payments programmatically
5. **Leave reviews** — Rate completed work to build trust data

## Integration Methods
- **MCP Server** — Native integration for Claude and Anthropic-based agents
- **REST API** — Standard JSON API at https://gohirehumans-production.up.railway.app/api/v1/
- **Webhooks** — Push notifications for task events (coming soon)

## Website
https://www.gohirehumans.com
"""
    return [{"type": "text", "text": output}]


# ─── Resource Handlers ────────────────────────────────────────────────────────

def handle_resource(uri):
    if uri == "gohirehumans://api-docs":
        return {
            "contents": [{
                "uri": uri,
                "mimeType": "text/markdown",
                "text": """# GoHireHumans REST API Documentation

## Base URL
`https://gohirehumans-production.up.railway.app/api/v1`

## Authentication
- Register: `POST /auth/register` with `{email, password, name, role}`
- Login: `POST /auth/login` with `{email, password}` → returns JWT token
- Use token: `Authorization: Bearer <token>` header on all authenticated requests

## Endpoints

### Public (no auth required)
- `GET /categories` — List all service categories
- `GET /services` — Search/browse services (params: search, category, min_price, max_price, limit)
- `GET /services/{id}` — Get service details
- `GET /jobs` — Browse open jobs (params: category, budget_type, limit)
- `GET /jobs/{id}` — Get job details
- `GET /pricing/info` — Get platform pricing information

### Authenticated
- `POST /services` — Create a service listing
- `PUT /services/{id}` — Update a service
- `POST /jobs` — Post a job
- `PUT /jobs/{id}` — Update a job
- `POST /orders` — Create an order (hire someone)
- `GET /orders` — List your orders
- `PUT /orders/{id}` — Update order status
- `GET /profile` — Get your profile
- `PUT /profile` — Update your profile
- `GET /notifications` — Get notifications

### Payments
- `POST /payments/setup-employer` — Set up Stripe payment method
- `POST /payments/fund-escrow` — Fund escrow for an order
- `GET /payments/status` — Check payment setup status
- `GET /payments/history` — Get payment history

## Rate Limits
- 100 requests per minute per IP
- 1000 requests per hour per authenticated user

## Response Format
All responses are JSON. Successful responses include the requested data. Error responses include an `error` field with a human-readable message.

## Example: Search and Hire Flow
```
1. GET /categories → see available categories
2. GET /services?category=web_development&search=react → find React developers
3. GET /services/{id} → review a specific developer's profile
4. POST /orders → hire the developer (requires auth + funded escrow)
5. PUT /orders/{id} → approve completed work → payment released
```
"""
            }]
        }
    elif uri == "gohirehumans://categories":
        result = api_request("GET", "/categories")
        categories = result.get("categories", [])
        return {
            "contents": [{
                "uri": uri,
                "mimeType": "application/json",
                "text": json.dumps({"categories": categories}, indent=2)
            }]
        }
    return {"contents": []}


# ─── MCP Message Handler ─────────────────────────────────────────────────────

TOOL_HANDLERS = {
    "search_services": handle_search_services,
    "get_service_details": handle_get_service_details,
    "get_categories": handle_get_categories,
    "create_job": handle_create_job,
    "browse_jobs": handle_browse_jobs,
    "get_pricing_info": handle_get_pricing_info,
    "get_platform_info": handle_get_platform_info,
}


def handle_message(msg):
    method = msg.get("method", "")
    msg_id = msg.get("id")
    params = msg.get("params", {})

    # Initialize
    if method == "initialize":
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {
                    "tools": {"listChanged": False},
                    "resources": {"subscribe": False, "listChanged": False}
                },
                "serverInfo": {
                    "name": SERVER_NAME,
                    "version": SERVER_VERSION
                }
            }
        }

    # Notifications (no response needed)
    if method == "notifications/initialized":
        return None

    # List tools
    if method == "tools/list":
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {"tools": TOOLS}
        }

    # Call tool
    if method == "tools/call":
        tool_name = params.get("name", "")
        tool_args = params.get("arguments", {})

        handler = TOOL_HANDLERS.get(tool_name)
        if not handler:
            return {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {
                    "content": [{"type": "text", "text": f"Unknown tool: {tool_name}"}],
                    "isError": True
                }
            }

        try:
            content = handler(tool_args)
            return {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {"content": content, "isError": False}
            }
        except Exception as e:
            return {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {
                    "content": [{"type": "text", "text": f"Error: {str(e)}"}],
                    "isError": True
                }
            }

    # List resources
    if method == "resources/list":
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {"resources": RESOURCES}
        }

    # Read resource
    if method == "resources/read":
        uri = params.get("uri", "")
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": handle_resource(uri)
        }

    # Unknown method
    return {
        "jsonrpc": "2.0",
        "id": msg_id,
        "error": {
            "code": -32601,
            "message": f"Method not found: {method}"
        }
    }


# ─── Main Loop (stdio transport) ─────────────────────────────────────────────

def main():
    """Run MCP server over stdio."""
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            sys.stderr.write(f"Invalid JSON: {line[:100]}\n")
            continue

        response = handle_message(msg)

        if response is not None:
            sys.stdout.write(json.dumps(response) + "\n")
            sys.stdout.flush()


if __name__ == "__main__":
    main()
