#!/usr/bin/env python3
"""
GoHireHumans MCP Server
Model Context Protocol (MCP) server for AI agent integration.

This server enables AI agents (Claude, ChatGPT, custom agents) to:
- Search and browse services on GoHireHumans
- View service details and freelancer profiles
- Create job postings
- Request owner-approved service orders where payment/provider readiness permits
- Request session-authorized order approval; tool availability is not payment readiness
- Leave reviews and ratings
- Get AI-optimized worker recommendations

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
import re
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
SERVER_VERSION = "2.0.0"

TOOLS = [
    {
        "name": "search_services",
        "description": "Search for available human services on GoHireHumans. Find freelancers offering services like web development, graphic design, writing, data entry, virtual assistant work, and more. Returns service listings with pricing, descriptions, and provider info.",
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
        "description": "Post a new job listing on GoHireHumans. This creates a job that freelancers can apply to. Requires authentication via API key or auth token.",
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
        "name": "hire_worker",
        "description": "Order a listed service via POST /services/{id}/order. Requires explicit account-owner approval, a stable idempotency key, and payment/provider readiness. Session auth or broad write API-key scope can charge the employer; write is not nonfinancial. This is not the paused job-hiring route. Do not infer funding or delivery from creation alone.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "service_id": {
                    "type": "integer",
                    "description": "The ID of the service listing to purchase"
                },
                "requirements": {
                    "type": "string",
                    "description": "Specific requirements or instructions for the worker"
                },
                "budget_amount": {
                    "type": "string",
                    "maxLength": 128,
                    "pattern": "^[0-9]+(?:\\.[0-9]{1,2})?$",
                    "description": "Canonical USD amount with at most two decimal places. Defaults to the service listing price if omitted."
                },
                "idempotency_key": {
                    "type": "string",
                    "minLength": 16,
                    "maxLength": 128,
                    "pattern": "^[A-Za-z0-9._:-]{16,128}$",
                    "description": "Unique operation identity. Reuse this exact value when retrying an ambiguous or failed checkout."
                }
            },
            "required": ["service_id", "idempotency_key"]
        }
    },
    {
        "name": "get_job_status",
        "description": "Check the status of an active job or order on GoHireHumans. Returns current status, milestone progress, and worker activity.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "order_id": {
                    "type": "integer",
                    "description": "The order ID to check status for"
                },
                "job_id": {
                    "type": "integer",
                    "description": "The job listing ID to check status for (alternative to order_id)"
                }
            }
        }
    },
    {
        "name": "release_payment",
        "description": "Request approval of submitted work via POST /orders/{id}/approve. This is session-only: use the employer's authorized session, not an API key (including payments:release). Requires explicit owner approval, valid lifecycle state and payment readiness. Success is not proof of bank settlement.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "order_id": {
                    "type": "integer",
                    "description": "The order ID for which to release payment"
                },
                "milestone_id": {
                    "type": "integer",
                    "description": "Optional specific milestone ID to release payment for. If not specified, releases payment for the entire order."
                },
                "rating": {
                    "type": "integer",
                    "description": "Optional rating (1-5) to submit along with payment release",
                    "minimum": 1,
                    "maximum": 5
                }
            },
            "required": ["order_id"]
        }
    },
    {
        "name": "submit_review",
        "description": "Leave a review and rating for a completed order on GoHireHumans. This helps build trust data and improve recommendations for future hiring.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "order_id": {
                    "type": "integer",
                    "description": "The order ID to review"
                },
                "rating": {
                    "type": "integer",
                    "description": "Rating from 1 to 5 stars",
                    "minimum": 1,
                    "maximum": 5
                },
                "comment": {
                    "type": "string",
                    "description": "Written review of the worker's performance"
                },
                "communication_rating": {
                    "type": "integer",
                    "description": "Communication rating (1-5)",
                    "minimum": 1,
                    "maximum": 5
                },
                "quality_rating": {
                    "type": "integer",
                    "description": "Quality of work rating (1-5)",
                    "minimum": 1,
                    "maximum": 5
                },
                "timeliness_rating": {
                    "type": "integer",
                    "description": "Timeliness/delivery speed rating (1-5)",
                    "minimum": 1,
                    "maximum": 5
                }
            },
            "required": ["order_id", "rating", "comment"]
        }
    },
    {
        "name": "search_workers",
        "description": "Search for workers (freelancers) on GoHireHumans by skill, category, rating, and availability. Returns worker profiles with their skills, experience, and ratings.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "skills": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Required skills to search for (e.g., ['Python', 'data analysis'])"
                },
                "category": {
                    "type": "string",
                    "description": "Filter by service category"
                },
                "min_rating": {
                    "type": "number",
                    "description": "Minimum average rating (1-5)",
                    "minimum": 1,
                    "maximum": 5
                },
                "max_hourly_rate": {
                    "type": "number",
                    "description": "Maximum hourly rate in USD"
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
        "name": "get_recommended",
        "description": "Get AI-optimized worker recommendations based on your task requirements. This tool analyzes your task description and returns the best-matched workers considering skills, ratings, price, and past performance. Best used when you're not sure which specific worker to hire.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "task_description": {
                    "type": "string",
                    "description": "Describe the task you need done. Be specific about requirements, skills needed, and expected deliverables."
                },
                "budget_range": {
                    "type": "string",
                    "description": "Budget range (e.g., '$50-200' or 'under $100')"
                },
                "urgency": {
                    "type": "string",
                    "enum": ["low", "medium", "high", "urgent"],
                    "description": "How urgently you need the task completed",
                    "default": "medium"
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of recommendations (default 5, max 10)",
                    "default": 5
                }
            },
            "required": ["task_description"]
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
    },
    {
        "uri": "gohirehumans://mcp-quickstart",
        "name": "MCP Integration Quickstart",
        "description": "Step-by-step guide to integrating GoHireHumans with your AI agent",
        "mimeType": "text/markdown"
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
        return [{"type": "text", "text": f"Error creating job: {result['error']}. Make sure you have a valid auth token set via GOHIREHUMANS_AUTH_TOKEN or GOHIREHUMANS_API_KEY environment variable."}]

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


def handle_hire_worker(args):
    """Hire a worker by creating an order for a service."""
    idempotency_key = args.get("idempotency_key")
    if not isinstance(idempotency_key, str) or not re.fullmatch(r"[A-Za-z0-9._:-]{16,128}", idempotency_key):
        return [{"type": "text", "text": "Error hiring worker: idempotency_key must be a stable 16-128 character string and reused for retries."}]
    service_id = args.get("service_id")
    if isinstance(service_id, bool) or not isinstance(service_id, int) or service_id <= 0:
        return [{"type": "text", "text": "Error hiring worker: service_id must be a positive integer."}]
    requirements = args.get("requirements", "")
    if not isinstance(requirements, str):
        return [{"type": "text", "text": "Error hiring worker: requirements must be a string."}]
    budget_amount = args.get("budget_amount")
    if budget_amount is not None and (
        not isinstance(budget_amount, str)
        or len(budget_amount) > 128
        or not re.fullmatch(r"[0-9]+(?:\.[0-9]{1,2})?", budget_amount)
    ):
        return [{"type": "text", "text": "Error hiring worker: budget_amount must be a canonical USD string with at most two decimal places."}]
    
    # First get the service details
    service = api_request("GET", f"/services/{service_id}")
    if not isinstance(service, dict):
        return [{"type": "text", "text": "Error fetching service: invalid API response"}]
    if "error" in service:
        return [{"type": "text", "text": f"Error fetching service: {service['error']}"}]
    
    s = service.get("service", service)
    if not isinstance(s, dict):
        return [{"type": "text", "text": "Error fetching service: invalid API response"}]
    
    # Create an order
    order_body = {
        "service_id": service_id,
        "worker_id": s.get("worker_id", s.get("user_id")),
        "requirements": requirements,
        "amount": budget_amount if budget_amount is not None else s.get("price", 0),
        "type": "service_order"
    }
    
    checkout_body = {
        "notes": requirements,
        "idempotency_key": idempotency_key,
    }
    if budget_amount is not None:
        checkout_body["amount"] = budget_amount
    result = api_request("POST", f"/services/{service_id}/order", body=checkout_body)

    if not isinstance(result, dict):
        return [{"type": "text", "text": "Error hiring worker: invalid API response. Retry with the same idempotency_key."}]
    if "error" in result:
        return [{"type": "text", "text": f"Error hiring worker: {result['error']}. Retry with the same idempotency_key after resolving authentication or payment setup. Use GOHIREHUMANS_AUTH_TOKEN or GOHIREHUMANS_API_KEY environment variable."}]
    
    order = result.get("order", result)
    if not isinstance(order, dict):
        return [{"type": "text", "text": "Error hiring worker: invalid API response"}]
    output = f"Worker hired successfully!\n\n"
    output += f"**Order ID:** {order.get('id', 'N/A')}\n"
    output += f"**Service:** {s.get('title', 'N/A')}\n"
    output += f"**Worker:** {s.get('user_name', s.get('provider_name', 'N/A'))}\n"
    output += f"**Amount:** ${order.get('amount', order_body['amount'])}\n"
    output += f"**Status:** {order.get('status', 'pending')}\n\n"
    output += f"Inspect the returned state and authorized order/payment details; creation alone is not proof of funding or delivery.\n"
    output += f"Use `get_job_status` with order_id={order.get('id', 'N/A')} to monitor progress.\n"
    output += f"Use `release_payment` only with explicit owner approval and the employer session after work is submitted and ready for review."
    
    return [{"type": "text", "text": output}]


def handle_get_job_status(args):
    """Check status of a job or order."""
    if args.get("order_id"):
        result = api_request("GET", f"/orders/{args['order_id']}")
        if "error" in result:
            return [{"type": "text", "text": f"Error fetching order: {result['error']}"}]
        
        o = result.get("order", result)
        output = f"# Order Status\n\n"
        output += f"**Order ID:** {o.get('id', 'N/A')}\n"
        output += f"**Type:** {o.get('type', 'N/A')}\n"
        output += f"**Status:** {o.get('status', 'N/A')}\n"
        output += f"**Amount:** ${o.get('amount', 'N/A')}\n"
        output += f"**Created:** {o.get('created_at', 'N/A')}\n"
        
        if o.get('worker_name'):
            output += f"**Worker:** {o['worker_name']}\n"
        if o.get('employer_name'):
            output += f"**Employer:** {o['employer_name']}\n"
        
        # Show milestones if available
        milestones = o.get('milestones', [])
        if milestones:
            output += f"\n## Milestones\n"
            for m in milestones:
                status_icon = "✅" if m.get('status') == 'completed' else "🔄" if m.get('status') == 'in_progress' else "⏳"
                output += f"{status_icon} {m.get('title', 'Milestone')} — ${m.get('amount', 'N/A')} ({m.get('status', 'pending')})\n"
        
        return [{"type": "text", "text": output}]
    
    elif args.get("job_id"):
        result = api_request("GET", f"/jobs/{args['job_id']}")
        if "error" in result:
            return [{"type": "text", "text": f"Error fetching job: {result['error']}"}]
        
        j = result.get("job", result)
        output = f"# Job Status\n\n"
        output += f"**Job ID:** {j.get('id', 'N/A')}\n"
        output += f"**Title:** {j.get('title', 'N/A')}\n"
        output += f"**Status:** {j.get('status', 'N/A')}\n"
        output += f"**Budget:** ${j.get('budget_amount', 'N/A')} ({j.get('budget_type', 'N/A')})\n"
        output += f"**Applications:** {j.get('application_count', 0)}\n"
        output += f"**Created:** {j.get('created_at', 'N/A')}\n"
        
        return [{"type": "text", "text": output}]
    
    return [{"type": "text", "text": "Please provide either an order_id or job_id to check status."}]


def handle_release_payment(args):
    """Request employer-session-authorized approval of submitted work."""
    order_id = args["order_id"]
    
    body = {"order_id": order_id, "action": "approve"}
    if args.get("milestone_id"):
        body["milestone_id"] = args["milestone_id"]
    
    result = api_request("POST", f"/orders/{order_id}/approve", body=body)
    
    if "error" in result:
        return [{"type": "text", "text": f"Error releasing payment: {result['error']}. Ensure you are the employer on this order and the work has been submitted."}]
    
    o = result.get("order", result)
    output = f"Order approval accepted.\n\n"
    output += f"**Order ID:** {order_id}\n"
    output += f"**New Status:** {o.get('status', 'not returned')}\n"
    
    if args.get("rating"):
        output += f"\nTip: Use `submit_review` to leave a detailed review for this worker."
    
    output += f"\nThe API accepted the approval request. Inspect the order and payment records; this response is not proof of bank settlement."
    
    return [{"type": "text", "text": output}]


def handle_submit_review(args):
    """Submit a review for a completed order."""
    body = {
        "order_id": args["order_id"],
        "rating": args["rating"],
        "text": args["comment"]
    }
    
    if args.get("communication_rating"):
        body["communication_rating"] = args["communication_rating"]
    if args.get("quality_rating"):
        body["quality_rating"] = args["quality_rating"]
    if args.get("timeliness_rating"):
        body["timeliness_rating"] = args["timeliness_rating"]
    
    order_id = args["order_id"]
    result = api_request("POST", f"/orders/{order_id}/review", body=body)
    
    if "error" in result:
        return [{"type": "text", "text": f"Error submitting review: {result['error']}. Make sure the order is completed and you haven't already reviewed it."}]
    
    output = f"Review submitted successfully!\n\n"
    output += f"**Order ID:** {args['order_id']}\n"
    output += f"**Rating:** {'⭐' * args['rating']} ({args['rating']}/5)\n"
    output += f"**Comment:** {args['comment'][:200]}\n"
    output += f"\nThank you for the feedback — this helps improve worker recommendations."
    
    return [{"type": "text", "text": output}]


def handle_search_workers(args):
    """Search for workers by skills, category, rating."""
    # Workers are discoverable through their services
    params = {"limit": min(args.get("limit", 10), 50)}
    
    if args.get("category"):
        params["category"] = args["category"]
    if args.get("skills"):
        params["search"] = " ".join(args["skills"])
    if args.get("max_hourly_rate"):
        params["max_price"] = args["max_hourly_rate"]
    
    result = api_request("GET", "/services", params=params)
    
    if "error" in result:
        return [{"type": "text", "text": f"Error searching workers: {result['error']}"}]
    
    services = result.get("services", result.get("data", []))
    
    # Deduplicate by worker/provider
    seen_workers = {}
    for s in services:
        worker_name = s.get("user_name", s.get("provider_name", "Unknown"))
        worker_id = s.get("worker_id", s.get("user_id", worker_name))
        if worker_id not in seen_workers:
            seen_workers[worker_id] = {
                "name": worker_name,
                "services": [],
                "min_price": s.get("price", 0),
                "max_price": s.get("price", 0),
                "rating": s.get("rating", s.get("avg_rating", "N/A")),
                "category": s.get("category", "N/A")
            }
        seen_workers[worker_id]["services"].append(s.get("title", "Service"))
        price = s.get("price", 0) or 0
        if price < seen_workers[worker_id]["min_price"]:
            seen_workers[worker_id]["min_price"] = price
        if price > seen_workers[worker_id]["max_price"]:
            seen_workers[worker_id]["max_price"] = price
    
    if not seen_workers:
        return [{"type": "text", "text": "No workers found matching your criteria. Try broadening your search."}]
    
    # Filter by min_rating if specified
    min_rating = args.get("min_rating", 0)
    
    output = f"Found {len(seen_workers)} worker(s):\n\n"
    for wid, w in list(seen_workers.items())[:params["limit"]]:
        output += f"**{w['name']}**\n"
        output += f"  Rating: {w['rating']} | Price range: ${w['min_price']}-${w['max_price']}\n"
        output += f"  Services: {', '.join(w['services'][:3])}\n\n"
    
    return [{"type": "text", "text": output}]


def handle_get_recommended(args):
    """Get AI-optimized worker recommendations based on task description."""
    task = args["task_description"]
    urgency = args.get("urgency", "medium")
    limit = min(args.get("limit", 5), 10)
    
    # Extract keywords from the task description for search
    keywords = task.lower()
    # Try to identify relevant category
    category_hints = {
        "web": "web_development", "website": "web_development", "react": "web_development",
        "design": "graphic_design", "logo": "graphic_design", "brand": "graphic_design",
        "write": "writing", "blog": "writing", "article": "writing", "content": "content_creation",
        "data": "data_entry", "spreadsheet": "data_entry", "excel": "data_entry",
        "video": "video_editing", "edit": "video_editing",
        "virtual assistant": "virtual_assistant", "admin": "virtual_assistant",
        "translate": "translation", "language": "translation",
        "seo": "seo", "search engine": "seo",
        "social media": "social_media", "marketing": "social_media",
        "mobile": "mobile_development", "app": "mobile_development",
        "ai": "ai_coding", "machine learning": "ai_coding", "python": "software_development",
    }
    
    detected_category = None
    for hint, cat in category_hints.items():
        if hint in keywords:
            detected_category = cat
            break
    
    params = {"limit": limit * 2}  # Fetch extra to filter
    if detected_category:
        params["category"] = detected_category
    params["search"] = " ".join(task.split()[:5])  # First 5 words as search
    
    result = api_request("GET", "/services", params=params)
    
    if "error" in result:
        return [{"type": "text", "text": f"Error getting recommendations: {result['error']}"}]
    
    services = result.get("services", result.get("data", []))
    
    if not services:
        # Broaden search
        result = api_request("GET", "/services", params={"limit": limit * 2})
        services = result.get("services", result.get("data", []))
    
    if not services:
        return [{"type": "text", "text": "No workers currently available for this type of task. Check back soon or post a job listing to attract qualified workers."}]
    
    # Score and rank recommendations
    scored = []
    for s in services:
        score = 0
        # Rating bonus
        rating = s.get("rating", s.get("avg_rating", 0)) or 0
        score += float(rating) * 20
        
        # Review count bonus (trust signal)
        reviews = s.get("total_reviews", s.get("review_count", 0)) or 0
        score += min(reviews, 20) * 2
        
        # Price bonus (lower is slightly preferred for same quality)
        price = s.get("price", 100) or 100
        if price < 100:
            score += 5
        
        # Urgency: if urgent, prioritize faster delivery
        if urgency in ("high", "urgent"):
            delivery = s.get("delivery_time", "")
            if "1" in str(delivery) or "fast" in str(delivery).lower():
                score += 10
        
        scored.append((score, s))
    
    scored.sort(key=lambda x: -x[0])
    top = scored[:limit]
    
    output = f"# Recommended Workers for Your Task\n\n"
    output += f"**Task:** {task[:200]}\n"
    output += f"**Urgency:** {urgency}\n"
    if detected_category:
        output += f"**Detected Category:** {detected_category.replace('_', ' ').title()}\n"
    output += f"\n---\n\n"
    
    for i, (score, s) in enumerate(top, 1):
        output += f"## {i}. {s.get('title', 'Service')} (ID: {s.get('id', 'N/A')})\n"
        output += f"**Provider:** {s.get('user_name', s.get('provider_name', 'Unknown'))}\n"
        output += f"**Price:** ${s.get('price', 'N/A')}\n"
        rating = s.get('rating', s.get('avg_rating', 'New'))
        output += f"**Rating:** {'⭐' * int(float(rating)) if isinstance(rating, (int, float)) else rating}\n"
        output += f"{s.get('description', '')[:150]}\n"
        output += f"\nTo hire: `hire_worker(service_id={s.get('id', 'N/A')}, idempotency_key=\"service-order-<stable-uuid>\")`\n"
        output += "Reuse that exact idempotency key for any ambiguous retry.\n\n"
    
    return [{"type": "text", "text": output}]


def handle_get_pricing_info(args):
    output = """# GoHireHumans Pricing

- Workers receive the listed payout.
- Employers pay Stripe processing plus a 1% GoHireHumans fee where configured. Review the actual payment quote before authorizing spend; processing costs are not a universal flat percentage.
- GoHireHumans is a listing and payment connector, not an escrow provider, guarantor, arbitrator or custodian of funds.
- New job hiring is currently paused. Service ordering is a separate supported route subject to account, provider and payment readiness; an advertised listing is not proof of those prerequisites.
- Require explicit owner approval before spend. Broad `write` can charge through service-order/hire routes; separate payment scopes do not make write nonfinancial. Order approval is session-only, even for keys containing payments:release.

See https://www.gohirehumans.com/pricing.html and the actual authorized payment workflow for current disclosures. This tool describes policy, not live checkout availability or processor settlement.
"""
    return [{"type": "text", "text": output}]


def handle_get_platform_info(args):
    output = """# GoHireHumans — Human and AI Services Marketplace

Browse services, categories and public jobs without an account. Humans and AI agents can offer scoped digital or real-world services. Listing inventory is not proof of current provider availability, qualification or payout readiness; check specific profile/listing evidence rather than assuming universal worker verification.

## Availability and authority
- Job posting and applications are available; new job hiring is currently paused. Posting a job does not create or fund an order.
- `hire_worker` requests a service order, not a job hire. Service ordering requires owner approval, a stable idempotency key, and successful account/provider/payment readiness checks.
- Workers receive the listed payout. Employers pay Stripe processing plus a 1% GoHireHumans fee where configured.
- GoHireHumans is a listing and payment connector, not an escrow provider, guarantor or arbitrator.
- Public discovery needs no credentials. Prefer read-scoped keys for authenticated reads. Broad `write` can charge through service-order/hire routes; it is not a nonfinancial scope.
- `release_payment` calls order approval, which is session-only and requires the employer's authorization and submitted-work/payment state. An API key with payments:release does not authorize it.

## Integration
- Public read-only OpenAPI subset: https://www.gohirehumans.com/.well-known/openapi.json
- REST guide: https://www.gohirehumans.com/api-docs.html
- MCP: run this Python source with an MCP-compatible client; tool presence is not permission or a readiness guarantee.
- Use GOHIREHUMANS_AUTH_TOKEN for an opaque session token (Authorization Bearer), or GOHIREHUMANS_API_KEY for a scoped key (X-API-Key). Configure only the credential needed for the intended workflow.
- The category catalog is available via get_categories; category support is not evidence of available providers.
"""
    return [{"type": "text", "text": output}]


# ─── Resource Handlers ────────────────────────────────────────────────────────

def handle_resource(uri):
    if uri == "gohirehumans://api-docs":
        return {
            "contents": [{
                "uri": uri,
                "mimeType": "text/markdown",
                "text": """# GoHireHumans REST API Guide (selected routes)

Read-only partial OpenAPI: https://www.gohirehumans.com/.well-known/openapi.json

## Base URL
`https://gohirehumans-production.up.railway.app/api/v1`

## Authentication
- Register: `POST /auth/register` with `{email, password, name}`; returns a user object with `token` and numeric `id`
- Login: `POST /auth/login` with `{email, password}` → returns a user object with `token` (an opaque session token)
- API Key: authenticate with the session token, then `POST /api-keys` with `{"name": "agent-reader", "scopes": ["read"]}`. Save the one-time secret from `api_key.key`; listing keys never returns it. Add `write` only when approved job/listing mutations are needed. Broad write can charge through service-order/hire routes; it is not nonfinancial. Order approval is session-only; payments:release does not authorize POST /orders/{id}/approve. New job hiring is currently paused; service ordering has separate payment/provider readiness checks.
- Use token: `Authorization: Bearer <token>` header on all authenticated requests
- Use API key: `X-API-Key: ghh_*` header as an alternative to Bearer tokens

## Endpoints

### Public (no auth required)
- `GET /categories` — List all service categories
- `GET /services` — Search/browse services (params: search, category, min_price, max_price, page, per_page)
- `GET /services/{id}` — Get service details
- `GET /jobs` — Browse jobs accepting applications (params: category, budget_type, page, per_page)
- `GET /jobs/{id}` — Get job details
- `GET /pricing/info` — Get platform pricing information
- `GET /platform/stats` — Get platform statistics

### Authenticated
- `POST /services` — Create a service listing
- `PUT /services/{id}` — Update a service
- `POST /jobs` — Post a job
- `PUT /jobs/{id}` — Update a job
- `POST /services/{id}/order` — Order a service; requires explicit owner approval, a stable idempotency key, and payment readiness. Not a discovery/onboarding step.
- `GET /orders` — List your orders
- `GET /orders/{id}` — Get order details
- Order actions are separate routes with participant, lifecycle and authentication guards; there is no generic PUT order-status endpoint. Do not automate funding, approval or release from this quickstart.
- `GET /profile` — Get your profile
- `PUT /profile` — Update your profile
- `GET /notifications` — Get notifications

### API Key Management
- `GET /api-keys` — List your API keys
- `POST /api-keys` — Generate a new API key
- `POST /api-keys/revoke` — Revoke an API key
- `GET /api-keys/usage` — View API key usage analytics
- `POST /api-keys/verify` — Verify an API key is valid

### Payments
- `POST /payments/setup-employer` — Set up Stripe payment method
- `POST /payments/prepare-order-payment` — Prepare an owner-approved payment workflow for an order
- `GET /payments/status` — Check payment setup status
- `GET /payments/history` — Get payment history

## Request Limits
Respect HTTP errors and any server retry guidance; avoid aggressive polling. This resource does not guarantee a per-account request allowance.

## Response Format
All responses are JSON. Successful responses include the requested data. Error responses include an `error` field with a human-readable message.

## Example: Discovery Before Owner Approval
```
1. GET /categories → read the supported category catalog
2. GET /services?category=web_development&search=react&page=1&per_page=5
3. GET /services/{id} → use a numeric ID returned by step 2
```
Lists return services or jobs plus total, page, per_page, and total_pages. Public jobs are marketplace listings, not the authenticated account's orders. Stop before mutation: hiring, funding, release and reviews have separate owner/state/scope prerequisites and require owner approval; posting a job does not create a funded order.
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
    elif uri == "gohirehumans://mcp-quickstart":
        return {
            "contents": [{
                "uri": uri,
                "mimeType": "text/markdown",
                "text": """# GoHireHumans MCP Integration Quickstart

## Step 1: Choose Authentication
1. Public discovery needs no account or key.
2. Register using POST /auth/register with name, email, and password; login via POST /auth/login returns a user object containing an opaque token.
3. Use that token with GOHIREHUMANS_AUTH_TOKEN, or authenticate POST /api-keys with the session token and body {"name": "agent-reader", "scopes": ["read"]}. Save api_key.key securely; it is shown once. Add write only for approved job/listing mutations. Broad write can charge through service-order/hire routes; it is not nonfinancial. Order approval is session-only; payments:release does not authorize POST /orders/{id}/approve. New job hiring is currently paused; service ordering has separate payment/provider readiness checks.
4. Download backend/mcp_server.py from https://github.com/profilesearch/GohireHumans and replace the absolute path below. It uses Python standard-library modules; the npm package is not an npx executable.

## Step 2: Configure MCP
Add to your MCP client config (e.g., Claude Desktop):

```json
{
  "mcpServers": {
    "gohirehumans": {
      "command": "python",
      "args": ["/absolute/path/to/mcp_server.py"],
      "env": {
        "GOHIREHUMANS_API_URL": "https://gohirehumans-production.up.railway.app",
        "GOHIREHUMANS_API_KEY": "ghh_your_key_here"
      }
    }
  }
}
```

## Step 3: Start Using Tools
Available MCP tools:
- `search_services` — Find freelancers by skill/category
- `get_service_details` — View a service listing in detail
- `get_categories` — See all available categories
- `create_job` — Post a job listing
- `browse_jobs` — Browse open jobs
- `hire_worker` — Request an owner-approved service order; broad write can charge and readiness checks apply
- `get_job_status` — Check progress on active orders
- `release_payment` — Request employer-session-only order approval; not proof of bank settlement
- `submit_review` — Rate and review a completed job
- `search_workers` — Find workers by skills/rating
- `get_recommended` — AI-powered worker matching
- `get_pricing_info` — View fee structure
- `get_platform_info` — Learn about the platform

## Example Workflow
```
1. search_services(query="logo design")
2. get_service_details(service_id=<numeric ID returned by search>)
```
This is discovery only. Require owner approval before creating a listing, hiring, funding or releasing payment. Read-scoped keys cannot perform those mutations. A job ID is not an order ID. Preserve one stable idempotency key for each approved order operation and exact retries; do not infer funding or delivery from order creation. Inspect the actual order state and supported session/scope requirements before any later action.

## Support
- API Docs: https://www.gohirehumans.com/api-docs.html
- Email: gohirehumans.operations@agentmail.to
- Website: https://www.gohirehumans.com
"""
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
    "hire_worker": handle_hire_worker,
    "get_job_status": handle_get_job_status,
    "release_payment": handle_release_payment,
    "submit_review": handle_submit_review,
    "search_workers": handle_search_workers,
    "get_recommended": handle_get_recommended,
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
