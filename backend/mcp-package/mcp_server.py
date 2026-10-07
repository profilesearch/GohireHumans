#!/usr/bin/env python3
"""
GoHireHumans MCP Server
Model Context Protocol (MCP) server for AI agent integration.

This server enables AI agents (Claude, ChatGPT, custom agents) to:
- Search and browse services on GoHireHumans
- View service details and freelancer profiles
- Create job postings
- Hire workers and manage the full lifecycle
- Approve completed work through the configured Stripe payment flow
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
from decimal import Decimal, InvalidOperation

# ─── Configuration ────────────────────────────────────────────────────────────

API_BASE = os.environ.get("GOHIREHUMANS_API_URL", "https://gohirehumans-production.up.railway.app")
API_KEY = os.environ.get("GOHIREHUMANS_API_KEY", "")
AUTH_TOKEN = os.environ.get("GOHIREHUMANS_AUTH_TOKEN", "")

# Product label for the MCP client named in `initialize` (for example "claude" or
# "cursor"). Only this fixed label is sent with API calls so GoHireHumans can count
# MCP usage per client; the raw client name and nothing else about the user,
# machine or conversation leaves this process.
CLIENT_LABEL = ""

# Each rule is a product label and the word sequences a client name may START with.
# Matching whole leading words (not substrings) keeps names like "Discontinued ..."
# or "Jean-Claude ..." from being counted as a product; anything else is "other".
CLIENT_LABEL_RULES = (
    ("claude-code", (("claude", "code"), ("claudecode",))),
    ("claude", (("claude",),)),
    ("cursor", (("cursor",),)),
    ("vscode", (("vscode",), ("visual", "studio", "code"), ("github", "copilot"), ("copilot",))),
    ("windsurf", (("windsurf",), ("codeium",))),
    ("roo-code", (("roo",), ("roocode",), ("roocline",))),
    ("cline", (("cline",),)),
    ("continue", (("continue",),)),
    ("zed", (("zed",),)),
    ("goose", (("goose",),)),
    ("mcp-inspector", (("mcp", "inspector"), ("inspector",))),
    ("glama", (("glama",),)),
    ("smithery", (("smithery",),)),
    ("gemini", (("gemini",),)),
    ("openai", (("openai",), ("chatgpt",), ("codex",))),
    ("librechat", (("librechat",),)),
)


def client_label(raw):
    """Map an MCP client's self-reported name to a fixed ASCII product label."""
    if not isinstance(raw, str) or not raw.strip():
        return ""
    name = raw.strip().lower()
    if name.startswith("ghh-"):
        return "ghh-internal"
    words = tuple(re.findall(r"[a-z0-9]+", name))
    for label, prefixes in CLIENT_LABEL_RULES:
        if any(words[:len(prefix)] == prefix for prefix in prefixes):
            return label
    return "other"


# ─── API Helper ───────────────────────────────────────────────────────────────

class APIRequestError(Exception):
    """An upstream HTTP failure, retaining status and operation identity."""

    def __init__(self, status, message, method, idempotent=False):
        super().__init__(message)
        self.status = status
        self.method = method
        self.idempotent = idempotent

    def payload(self):
        message = str(self)
        lower = message.lower()
        if self.status in (401, 403):
            category = "auth"
        elif self.status == 402 or (self.status == 409 and
                ("payment" in lower or "billing" in lower or "stripe" in lower) and
                ("setup" in lower or "set up" in lower or "method" in lower)):
            category = "payment_setup_required"
        elif self.status == 409:
            category = "lifecycle_conflict"
        elif self.status in (400, 422):
            category = "validation"
        elif self.status == 429:
            category = "rate_limit"
        elif self.status is not None and 500 <= self.status < 600:
            category = "server_error"
        else:
            category = "api_error" if self.status is not None else "transport_error"
        return {"category": category, "http_status": self.status, "message": message,
                "retry_safe": category == "server_error" and self.idempotent}


def api_request(method, path, body=None, params=None):
    """Make an HTTP request to the GoHireHumans API."""
    url = f"{API_BASE}/api/v1{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)

    headers = {"Content-Type": "application/json", "User-Agent": f"gohirehumans-mcp/{SERVER_VERSION}"}
    if CLIENT_LABEL:
        headers["X-GHH-MCP-Client"] = CLIENT_LABEL
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
            parsed = json.loads(error_body)
            message = parsed.get("error", parsed.get("message", error_body[:500])) if isinstance(parsed, dict) else error_body[:500]
        except json.JSONDecodeError:
            message = error_body[:500]
        raise APIRequestError(e.code, str(message), method,
                              method in ("GET", "HEAD") or bool(isinstance(body, dict) and body.get("idempotency_key"))) from e
    except urllib.error.URLError as e:
        raise APIRequestError(None, str(e), method) from e

# ─── MCP Protocol ─────────────────────────────────────────────────────────────

PROTOCOL_VERSION = "2024-11-05"
SERVER_NAME = "gohirehumans"
SERVER_VERSION = "2.2.0"

TOOLS = [
    {
        "name": "search_services",
        "description": "Search individual service listings on GoHireHumans (one result per listing) by keyword, category and price. Read-only; no API key needed. Returns up to `limit` listings, highest listing rating first, each with its numeric ID, title, category, price (fixed price, hourly rate or custom), a short description and the provider's name. Use this when you know what kind of work you need and want listing IDs to act on. Use search_workers to compare providers instead of listings, get_recommended to rank listings for a plain-language task, and get_service_details for one listing in full.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Keywords matched against listing titles, descriptions and tags (e.g., 'logo design', 'phone call', 'virtual assistant')"
                },
                "category": {
                    "type": "string",
                    "description": "Filter by category slug (e.g., 'web_development', 'graphic_design', 'virtual_assistant', 'ai_coding'). Get the full list with get_categories."
                },
                "min_price": {
                    "type": "number",
                    "description": "Minimum listed price or hourly rate in USD"
                },
                "max_price": {
                    "type": "number",
                    "description": "Maximum listed price or hourly rate in USD"
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of listings to return (default 10, max 50). Only the first page of matches is returned.",
                    "default": 10
                }
            }
        },
        "annotations": {
            "title": "Search service listings",
            "readOnlyHint": True,
            "openWorldHint": True
        }
    },
    {
        "name": "get_service_details",
        "description": "Get the full public details of one service listing by its numeric ID: title, category, price (fixed price, hourly rate or custom), provider name, rating, full description, delivery time and a link to the listing on gohirehumans.com. Read-only; no API key needed. Get IDs from search_services or get_recommended. Call this just before hire_worker so the account owner can check the scope and current price; for a custom-priced listing, agree the amount with the provider first. Returns an error message if the listing doesn't exist or was removed; a paused listing can still be shown here but can't be ordered.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "service_id": {
                    "type": "string",
                    "description": "Numeric service listing ID (e.g., '42') from search_services or get_recommended"
                }
            },
            "required": ["service_id"]
        },
        "annotations": {
            "title": "Get service listing details",
            "readOnlyHint": True,
            "openWorldHint": True
        }
    },
    {
        "name": "get_categories",
        "description": "List every service category slug on GoHireHumans, grouped into human services and AI-agent services, each with a readable name. Read-only; no API key needed. Call this when you need a valid `category` value for search_services, search_workers, browse_jobs or create_job. For an overview of the platform itself, use get_platform_info.",
        "inputSchema": {
            "type": "object",
            "properties": {}
        },
        "annotations": {
            "title": "List service categories",
            "readOnlyHint": True,
            "openWorldHint": True
        }
    },
    {
        "name": "create_job",
        "description": "Publish a new job post on GoHireHumans that workers can apply to. Get the account owner's approval of the title, description and budget first. Needs GOHIREHUMANS_AUTH_TOKEN or an API key with the write scope. The job is published immediately with status 'open', and workers with services in the same category may be notified. It does not hire anyone or charge anything: applicants are reviewed and hired on gohirehumans.com. This server cannot edit or close a job. Returns the new job ID and status; track it with get_job_status(job_id). To order an existing service listing instead, use hire_worker.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "title": {
                    "type": "string",
                    "description": "Job title (e.g., 'Build a React landing page')"
                },
                "description": {
                    "type": "string",
                    "description": "Public job description: the task, deliverables, deadline and anything a worker needs to know"
                },
                "category": {
                    "type": "string",
                    "description": "Category slug from get_categories (e.g., 'research', 'phone_call', 'data_entry')"
                },
                "budget_type": {
                    "type": "string",
                    "enum": ["fixed", "hourly"],
                    "description": "Use 'fixed'. Hourly jobs are not accepted right now and return an error."
                },
                "budget_amount": {
                    "type": "number",
                    "description": "Total budget in USD for a fixed-price job: greater than 0, in whole cents, at most 999,999.99"
                },
                "skills_required": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of required skills (e.g., ['React', 'TypeScript', 'CSS'])"
                }
            },
            "required": ["title", "description", "category", "budget_type", "budget_amount"]
        },
        "annotations": {
            "title": "Post a job",
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": False,
            "openWorldHint": True
        }
    },
    {
        "name": "browse_jobs",
        "description": "Browse public job posts on GoHireHumans that are accepting applications, newest first. Read-only; no API key needed. Returns up to `limit` jobs, each with its ID, title, category, budget and a short description. Use this to find work to apply for or to see what buyers are asking for; workers apply on gohirehumans.com, not through this server. To find people or services to hire, use search_services or search_workers. For one job's status and application count, use get_job_status(job_id).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "category": {
                    "type": "string",
                    "description": "Filter by category slug from get_categories"
                },
                "budget_type": {
                    "type": "string",
                    "enum": ["fixed", "hourly"],
                    "description": "Filter by budget type. Only fixed-price jobs can be posted right now, so 'hourly' usually returns none."
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of jobs to return (default 10, max 50). Only the first page is returned.",
                    "default": 10
                }
            }
        },
        "annotations": {
            "title": "Browse open jobs",
            "readOnlyHint": True,
            "openWorldHint": True
        }
    },
    {
        "name": "hire_worker",
        "description": "Order one service listing: hires its provider and pays for the work. Call only after the account owner has explicitly approved the listing, price and requirements. Needs GOHIREHUMANS_AUTH_TOKEN, or an API key with both the read scope and the write scope, plus a saved payment method on the employer account. Where checkout is configured, the saved card is charged immediately for the listing's current price plus a 1% platform fee and a fixed 3% processing charge. The charge isn't locked to an earlier quote, so re-check get_service_details just before ordering. The worker is paid only when the employer approves the delivered work with release_payment. A funded order has no self-serve cancel option. Safe to retry: the same idempotency_key resumes the original order instead of charging twice. Returns the order ID, amount and status for get_job_status(order_id).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "service_id": {
                    "type": "integer",
                    "description": "Numeric ID of the service listing to order, from search_services, get_recommended or get_service_details"
                },
                "requirements": {
                    "type": "string",
                    "description": "Instructions for the worker (what to deliver, details, deadline). Saved on the order."
                },
                "budget_amount": {
                    "type": "string",
                    "maxLength": 128,
                    "pattern": "^[0-9]+(?:\\.[0-9]{1,2})?$",
                    "description": "USD amount with at most two decimal places, used only for custom-priced listings (required there). It is not a spending cap: fixed-price listings always charge the listed price, and hourly listings charge one hour at the listed rate."
                },
                "idempotency_key": {
                    "type": "string",
                    "minLength": 16,
                    "maxLength": 128,
                    "pattern": "^[A-Za-z0-9._:-]{16,128}$",
                    "description": "Unique key for this order (16-128 letters, digits, '.', '_', ':' or '-'). Reuse the exact same key when retrying an ambiguous or failed checkout; a different order with the same key is rejected."
                }
            },
            "required": ["service_id", "idempotency_key"]
        },
        "annotations": {
            "title": "Hire a worker (charges the saved card)",
            "readOnlyHint": False,
            "destructiveHint": True,
            "idempotentHint": True,
            "openWorldHint": True
        }
    },
    {
        "name": "get_job_status",
        "description": "Check one order or one job post by ID. Read-only. With order_id: returns the order's status, amount, worker and employer names, and each milestone's amount and status; needs GOHIREHUMANS_AUTH_TOKEN or an API key with the read scope, and only the order's employer or worker (or a site admin) can see it. With job_id: returns a job post's status, budget and application count; jobs still accepting applications are public, after that only the job's owner, its applicants, the hired worker or a site admin can see it. Order IDs come from hire_worker; job IDs come from create_job or browse_jobs. Orders and jobs are numbered separately, so the same number can be both an order and a job. If both are given, order_id is used.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "order_id": {
                    "type": "integer",
                    "description": "Order ID from hire_worker. Takes precedence over job_id."
                },
                "job_id": {
                    "type": "integer",
                    "description": "Job post ID from create_job or browse_jobs (not an order ID)"
                }
            }
        },
        "annotations": {
            "title": "Check order or job status",
            "readOnlyHint": True,
            "openWorldHint": True
        }
    },
    {
        "name": "release_payment",
        "description": "Approve the worker's submitted work on an order and release their payout. This pays the worker and cannot be undone, so only call it after the account owner has reviewed the delivered work and explicitly approved payment. Must be called as the order's employer with GOHIREHUMANS_AUTH_TOKEN; unset GOHIREHUMANS_API_KEY first, because API-key authentication isn't supported for this action and a valid API key is rejected even alongside the session token. The order must be in 'submitted' status (check with get_job_status). Approves the order's current submitted milestone; where checkout is configured, Stripe releases the worker's listed payout. If the order has another milestone, the employer's saved card is charged for it at the same time. After the last milestone the order is completed and can be reviewed with submit_review.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "order_id": {
                    "type": "integer",
                    "description": "ID of the order whose submitted work you are approving"
                },
                "milestone_id": {
                    "type": "integer",
                    "description": "Ignored: the API always approves the order's current submitted milestone. Kept for backward compatibility."
                },
                "rating": {
                    "type": "integer",
                    "description": "Not recorded by this tool; rate the worker with submit_review after the order completes. Kept for backward compatibility.",
                    "minimum": 1,
                    "maximum": 5
                }
            },
            "required": ["order_id"]
        },
        "annotations": {
            "title": "Approve work and release payout",
            "readOnlyHint": False,
            "destructiveHint": True,
            "idempotentHint": False,
            "openWorldHint": True
        }
    },
    {
        "name": "submit_review",
        "description": "Leave a 1-5 star rating and written review for the other party on a completed order: the employer reviews the worker and the worker reviews the employer. Needs GOHIREHUMANS_AUTH_TOKEN or an API key with the write scope, from an account that is part of the order, and the order must be completed. Each participant can review an order once, and reviews can't be edited or deleted through the API. A review stays hidden until both parties have reviewed or 14 days have passed since completion; then it appears publicly on the reviewed person's profile.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "order_id": {
                    "type": "integer",
                    "description": "ID of the completed order to review"
                },
                "rating": {
                    "type": "integer",
                    "description": "Overall rating from 1 to 5 stars; shown publicly once the review is visible",
                    "minimum": 1,
                    "maximum": 5
                },
                "comment": {
                    "type": "string",
                    "description": "Written review of the other party's work or conduct; shown publicly once the review is visible"
                },
                "communication_rating": {
                    "type": "integer",
                    "description": "Optional communication sub-rating (1-5). Accepted but not currently stored or shown.",
                    "minimum": 1,
                    "maximum": 5
                },
                "quality_rating": {
                    "type": "integer",
                    "description": "Optional quality sub-rating (1-5). Accepted but not currently stored or shown.",
                    "minimum": 1,
                    "maximum": 5
                },
                "timeliness_rating": {
                    "type": "integer",
                    "description": "Optional timeliness sub-rating (1-5). Accepted but not currently stored or shown.",
                    "minimum": 1,
                    "maximum": 5
                }
            },
            "required": ["order_id", "rating", "comment"]
        },
        "annotations": {
            "title": "Review a completed order",
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": False,
            "openWorldHint": True
        }
    },
    {
        "name": "search_workers",
        "description": "Find workers (people or agents offering services) on GoHireHumans, with one result per provider instead of one per listing. Read-only; no API key needed. Searches public service listings by skills, category, price and minimum rating, then groups the matching listings by provider; each result shows the provider's name, rating, the price range of those matching listings and up to three of their service titles. Providers are listed in order of their highest-rated matching listing; the rating shown is the provider's profile rating when one exists, so it can differ from that order. Use this to compare providers. Use search_services when you need listing IDs to hire, and get_recommended to rank options for a task description.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "skills": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Skills or keywords to match against listing titles, descriptions and tags (e.g., ['Python', 'data analysis']); combined into one text search"
                },
                "category": {
                    "type": "string",
                    "description": "Filter by category slug from get_categories"
                },
                "min_rating": {
                    "type": "number",
                    "description": "Minimum profile/listing average rating (1-5); applied locally to returned services (unrated workers excluded), fetching more pages until enough providers match",
                    "minimum": 1,
                    "maximum": 5
                },
                "max_hourly_rate": {
                    "type": "number",
                    "description": "Maximum listed price or hourly rate in USD"
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of providers to return (default 10, max 50)",
                    "default": 10
                }
            }
        },
        "annotations": {
            "title": "Search workers",
            "readOnlyHint": True,
            "openWorldHint": True
        }
    },
    {
        "name": "get_recommended",
        "description": "Rank service listings for a task described in plain language. Read-only; no API key needed. It guesses a category from keywords in the description, searches listings using the description's first few words, and scores matches by rating, number of reviews, price (listings under $100 score slightly higher) and, for 'high' or 'urgent' tasks, delivery within 2 days. If nothing matches, it falls back to the best-rated listings overall, so check that results are relevant. Returns up to `limit` listings with service IDs for get_service_details and hire_worker. Use this when you have a task but no precise search terms; use search_services for exact keyword and price filters.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "task_description": {
                    "type": "string",
                    "description": "The task in plain language (e.g., 'call 20 restaurants to confirm opening hours'). The first few words drive the search, so lead with the core task."
                },
                "budget_range": {
                    "type": "string",
                    "description": "USD range compared with each listing's fixed price or hourly rate: '$50-200' or 'under $100'. Filtered locally before ranking; custom-priced listings are left out when a range is given, and unrated providers can still match."
                },
                "urgency": {
                    "type": "string",
                    "enum": ["low", "medium", "high", "urgent"],
                    "description": "How soon the task is needed. 'high' or 'urgent' boosts listings that deliver within 2 days; 'low' and 'medium' don't change the ranking.",
                    "default": "medium"
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of recommendations (default 5, max 10)",
                    "default": 5
                }
            },
            "required": ["task_description"]
        },
        "annotations": {
            "title": "Recommend listings for a task",
            "readOnlyHint": True,
            "openWorldHint": True
        }
    },
    {
        "name": "get_pricing_info",
        "description": "Get GoHireHumans' fees and payment terms: workers receive the listed payout, and employers pay a 1% platform fee plus a fixed 3% processing charge where checkout is configured; joining and listing are free. Also compares fees with Fiverr, Upwork, Freelancer.com and Toptal, and explains how payment and approval work (GoHireHumans is not an escrow provider). Read-only; no API key needed. Use this for cost questions or before quoting a total to the account owner. For what the platform is and how it works, use get_platform_info.",
        "inputSchema": {
            "type": "object",
            "properties": {}
        },
        "annotations": {
            "title": "Get fees and payment terms",
            "readOnlyHint": True,
            "openWorldHint": True
        }
    },
    {
        "name": "get_platform_info",
        "description": "Get an overview of GoHireHumans: what the marketplace is, key facts, the kinds of work available and the typical workflow from search to approval and review. Read-only; no API key needed. Call this first if you are new to GoHireHumans. For fees and payment terms use get_pricing_info; for exact category slugs use get_categories.",
        "inputSchema": {
            "type": "object",
            "properties": {}
        },
        "annotations": {
            "title": "Get platform overview",
            "readOnlyHint": True,
            "openWorldHint": False
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

def _service_worker_name(service):
    """Service rows expose the provider as `worker_name` (see GET /services)."""
    return service.get("worker_name") or service.get("user_name") or service.get("provider_name") or "Unknown"


def _service_rating(service, default="No ratings yet"):
    """Service rows carry `worker_rating` (profile rating) and `avg_rating` (listing)."""
    rating = service.get("worker_rating")
    if rating in (None, 0, ""):
        rating = service.get("avg_rating")
    return rating if rating not in (None, 0, "") else default


def _service_delivery_days(service):
    """Service rows expose `delivery_time_days` (integer days), not `delivery_time`."""
    days = service.get("delivery_time_days")
    if isinstance(days, bool) or not isinstance(days, (int, float)):
        return None
    return int(days)


def _pricing_kind(service):
    """Service rows carry `pricing_type`; older rows may omit it."""
    kind = service.get("pricing_type")
    if kind in ("fixed", "hourly", "custom"):
        return kind
    if service.get("price") is None and service.get("hourly_rate") is not None:
        return "hourly"
    return "fixed"


def _service_amount(service):
    """Comparable USD amount: the fixed price or the hourly rate; None when custom or missing."""
    kind = _pricing_kind(service)
    if kind == "custom":
        return None
    value = service.get("hourly_rate") if kind == "hourly" else service.get("price")
    if value is None or isinstance(value, bool):
        return None
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return amount if amount.is_finite() else None


def _money_text(amount):
    """40.0 -> '40', 40.5 -> '40.50' (SQLite REAL columns come back as floats)."""
    if amount == amount.to_integral_value():
        return str(int(amount))
    return str(amount.quantize(Decimal("0.01")))


def _service_price_label(service):
    """Hourly listings store `hourly_rate` (price is null); custom listings are priced at order time."""
    kind = _pricing_kind(service)
    if kind == "custom":
        return "Custom (amount agreed at order time)"
    amount = _service_amount(service)
    if amount is None:
        return "N/A"
    text = _money_text(amount)
    return f"${text}/hour" if kind == "hourly" else f"${text}"


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
    limit = min(args.get("limit", 10), 50)
    params["per_page"] = limit  # GET /services paginates with page/per_page

    result = api_request("GET", "/services", params=params)

    if "error" in result:
        return [{"type": "text", "text": f"Error searching services: {result['error']}"}]

    services = result.get("services", result.get("data", []))
    if not services:
        return [{"type": "text", "text": "No services found matching your criteria. Try broadening your search or checking available categories with get_categories."}]

    output = f"Found {len(services)} service(s):\n\n"
    for s in services[:limit]:
        output += f"**{s.get('title', 'Untitled')}** (ID: {s.get('id', 'N/A')})\n"
        output += f"  Category: {s.get('category', 'N/A')} | Price: {_service_price_label(s)}\n"
        output += f"  {s.get('description', '')[:200]}\n"
        output += f"  Provider: {_service_worker_name(s)}\n\n"

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
    output += f"**Price:** {_service_price_label(s)}\n"
    output += f"**Provider:** {_service_worker_name(s)}\n"
    output += f"**Rating:** {_service_rating(s)}\n\n"
    output += f"## Description\n{s.get('description', 'No description')}\n\n"

    delivery_days = _service_delivery_days(s)
    if delivery_days is not None:
        output += f"**Delivery Time:** {delivery_days} day(s)\n"
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
    # The API stores this as `required_skills`; the tool keeps its documented
    # `skills_required` input name for backward compatibility.
    skills = args.get("skills_required") or args.get("required_skills")
    if skills:
        body["required_skills"] = skills

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
    limit = min(args.get("limit", 10), 50)
    params["per_page"] = limit  # GET /jobs paginates with page/per_page

    result = api_request("GET", "/jobs", params=params)

    if "error" in result:
        return [{"type": "text", "text": f"Error browsing jobs: {result['error']}"}]

    jobs = result.get("jobs", result.get("data", []))
    if not jobs:
        return [{"type": "text", "text": "No open jobs found. Try different filters or check available categories with get_categories."}]

    output = f"Found {len(jobs)} open job(s):\n\n"
    for j in jobs[:limit]:
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
    
    # Create an order through the authoritative service checkout route
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
    output += f"**Worker:** {_service_worker_name(s)}\n"
    amount = order.get('total_amount')
    if amount is None:
        amount = checkout_body.get('amount')
    output += f"**Amount:** {f'${amount}' if amount is not None else _service_price_label(s)}\n"
    output += f"**Status:** {order.get('status', 'pending')}\n\n"
    output += f"Where checkout is configured, the payment is processed through Stripe and released to the worker when you approve the completed work.\n"
    output += f"Use `get_job_status` with order_id={order.get('id', 'N/A')} to monitor progress.\n"
    output += f"Use `release_payment` when work is complete to pay the worker."
    
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
        output += f"**Amount:** ${o.get('total_amount', 'N/A')}\n"
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
    """Approve completed work and release the worker payout."""
    order_id = args["order_id"]
    
    body = {"order_id": order_id, "action": "approve"}
    if args.get("milestone_id"):
        body["milestone_id"] = args["milestone_id"]
    
    result = api_request("POST", f"/orders/{order_id}/approve", body=body)
    
    if "error" in result:
        return [{"type": "text", "text": f"Error releasing payment: {result['error']}. Ensure you are the employer on this order and the work has been submitted."}]
    
    o = result.get("order", result)
    output = f"Payment released successfully!\n\n"
    output += f"**Order ID:** {order_id}\n"
    output += f"**New Status:** {o.get('status', 'completed')}\n"
    
    if args.get("rating"):
        output += f"\nTip: Use `submit_review` to leave a detailed review for this worker."
    
    output += f"\nThe worker's listed payout has been released through the configured payment flow."
    
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
    limit = min(args.get("limit", 10), 50)
    # Rating is not a GET /services query parameter; filter returned profile
    # ratings locally. Fetch a full page before applying the output limit.
    params = {"per_page": 100 if args.get("min_rating") else limit}
    
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
    min_rating = args.get("min_rating")
    if min_rating is not None:
        def meets_rating(service):
            try:
                return float(_service_rating(service, default=0)) >= min_rating
            except (ValueError, TypeError):
                return False

        services = [s for s in services if meets_rating(s)]
        def worker_key(service):
            return service.get("worker_id", service.get("user_id", _service_worker_name(service)))

        page = 1
        while (len({worker_key(s) for s in services}) < limit and
               page < result.get("total_pages", 1)):
            page += 1
            result = api_request("GET", "/services", params={**params, "page": page})
            services.extend(s for s in result.get("services", []) if meets_rating(s))
    
    # Deduplicate by worker/provider
    seen_workers = {}
    for s in services:
        worker_name = _service_worker_name(s)
        worker_id = s.get("worker_id", s.get("user_id", worker_name))
        if worker_id not in seen_workers:
            seen_workers[worker_id] = {
                "name": worker_name,
                "services": [],
                "amounts": [],
                "has_custom": False,
                "rating": _service_rating(s, default="N/A"),
                "category": s.get("category", "N/A")
            }
        seen_workers[worker_id]["services"].append(s.get("title", "Service"))
        amount = _service_amount(s)
        if amount is not None:
            seen_workers[worker_id]["amounts"].append(amount)
        elif _pricing_kind(s) == "custom":
            seen_workers[worker_id]["has_custom"] = True
    
    if not seen_workers:
        return [{"type": "text", "text": "No workers found matching your criteria. Try broadening your search."}]
    
    # Rating is enforced on service rows above, before worker deduplication.
    output = f"Found {len(seen_workers)} worker(s):\n\n"
    for wid, w in list(seen_workers.items())[:limit]:
        output += f"**{w['name']}**\n"
        if w["amounts"]:
            price_range = f"${_money_text(min(w['amounts']))}-${_money_text(max(w['amounts']))}"
            if w["has_custom"]:
                price_range += " (plus custom-priced listings)"
        else:
            price_range = "Custom pricing" if w["has_custom"] else "N/A"
        output += f"  Rating: {w['rating']} | Price range: {price_range}\n"
        output += f"  Services: {', '.join(w['services'][:3])}\n\n"
    
    return [{"type": "text", "text": output}]


def _budget_bounds(value):
    """Parse the two advertised USD formats; never silently drop a constraint."""
    if not value:
        return None
    amount = r"\$?([0-9]+(?:\.[0-9]{1,2})?)"
    match = re.fullmatch(rf"\s*(?:under|up to)\s+{amount}\s*", value, re.I)
    if match:
        return (Decimal(0), Decimal(match.group(1)), not value.strip().lower().startswith("under"))
    match = re.fullmatch(rf"\s*{amount}\s*-\s*{amount}\s*", value)
    if match:
        low, high = (Decimal(n) for n in match.groups())
        if low <= high:
            return (low, high, True)
    raise ValueError("budget_range must be 'under $100' or '$50-200' (USD)")


def _within_budget(service, bounds):
    price = _service_amount(service)
    if price is None:
        return False
    return bounds[0] <= price and (price <= bounds[1] if bounds[2] else price < bounds[1])


def handle_get_recommended(args):
    """Get AI-optimized worker recommendations based on task description."""
    try:
        bounds = _budget_bounds(args.get("budget_range"))
    except (ValueError, TypeError) as e:
        return [{"type": "text", "text": f"Error getting recommendations: {e}"}]
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
    
    params = {"per_page": 100 if bounds else min(limit * 2, 100)}
    if bounds:
        # GET /services max_price is an OR over fixed price and hourly rate;
        # client-side price filtering below remains authoritative.
        params["max_price"] = str(bounds[1])
    if detected_category:
        params["category"] = detected_category
    params["search"] = " ".join(task.split()[:5])  # First 5 words as search
    
    def fetch_matching(query):
        response = api_request("GET", "/services", params=query)
        found = response.get("services", response.get("data", []))
        if bounds:
            found = [s for s in found if _within_budget(s, bounds)]
            page = 1
            while len(found) < limit and page < response.get("total_pages", 1):
                page += 1
                next_page = api_request("GET", "/services", params={**query, "page": page})
                found.extend(s for s in next_page.get("services", []) if _within_budget(s, bounds))
        return found

    services = fetch_matching(params)
    if not services:
        # Broaden keywords/category, never the buyer's budget constraint. This also runs
        # when every keyword match was over budget, not only when the raw page was empty.
        fallback_params = {"per_page": params["per_page"]}
        if bounds:
            fallback_params["max_price"] = str(bounds[1])
        services = fetch_matching(fallback_params)

    if not services:
        return [{"type": "text", "text": "No workers currently available for this type of task. Check back soon or post a job listing to attract qualified workers."}]
    
    # Score and rank recommendations
    scored = []
    for s in services:
        score = 0
        # Rating bonus
        rating = _service_rating(s, default=0) or 0
        score += float(rating) * 20
        
        # Review count bonus (trust signal)
        reviews = s.get("total_reviews") or s.get("worker_review_count") or 0
        score += min(reviews, 20) * 2
        
        # Price bonus (lower is slightly preferred for same quality)
        amount = _service_amount(s)
        price = amount if amount is not None else 100
        if price < 100:
            score += 5
        
        # Urgency: if urgent, prioritize faster delivery
        if urgency in ("high", "urgent"):
            delivery_days = _service_delivery_days(s)
            if delivery_days is not None and delivery_days <= 2:
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
        output += f"**Provider:** {_service_worker_name(s)}\n"
        output += f"**Price:** {_service_price_label(s)}\n"
        rating = _service_rating(s, default='New')
        output += f"**Rating:** {'⭐' * int(float(rating)) if isinstance(rating, (int, float)) else rating}\n"
        output += f"{s.get('description', '')[:150]}\n"
        output += f"\nTo hire: `hire_worker(service_id={s.get('id', 'N/A')}, idempotency_key=\"service-order-<stable-uuid>\")`\n"
        output += "Reuse that exact idempotency key for any ambiguous retry.\n\n"
    
    return [{"type": "text", "text": output}]


def handle_get_pricing_info(args):
    result = api_request("GET", "/pricing/info")
    if "error" not in result:
        info = result
    else:
        info = {}

    output = """# GoHireHumans Pricing

## Fee structure
- Workers receive the listed payout.
- Employers pay a 1% platform fee plus a fixed 3% processing charge where checkout is configured.
- Free to join. No subscription fees and no listing fees.

## How it compares
| Platform | Buyer-side fee | Seller-side fee |
|----------|----------------|-----------------|
| GoHireHumans | 1% platform fee + fixed 3% processing charge | 0% |
| Fiverr | 5.5% buyer fee | 20% commission |
| Upwork | Up to 7.99% client fee (Basic) | 0–15% freelancer fee per contract |
| Freelancer.com | 3% client fee | 10% project fee |
| Toptal | Markup on rates | 0% |

Upwork freelancer fees are shown before a proposal or offer is sent and locked per contract. Qualifying U.S. Basic clients paying by bank account pay 3%; Business Plus client fees are higher.

Competitor figures are published rates as of 2026; confirm current terms on each platform.

## Payments
GoHireHumans is a listing and payment connector, not an escrow provider, guarantor, or arbitrator. Where checkout is configured, Stripe processes the employer's payment and the worker receives the listed payout after the employer approves the delivered work. Review the scope and the evidence before approving.

## For AI agents
Agents use the same fee structure. Register for an API key, authenticate via session or API key, and use the REST API or MCP. Spend and hiring actions require account-owner authorization.
"""
    return [{"type": "text", "text": output}]


def handle_get_platform_info(args):
    output = """# GoHireHumans — a services marketplace for humans and AI agents

## What is it?
GoHireHumans is a marketplace for small, scoped human work. People and authorized agents list services, employers (human or agent) post bounded jobs, and delivered work is reviewed against the agreed scope before payment is approved. GoHireHumans is a listing and payment connector, not an escrow provider, guarantor, or arbitrator.

## Key facts
- Workers receive the listed payout; employers pay a 1% platform fee plus a fixed 3% processing charge where checkout is configured.
- Built for agent integration via MCP and the REST API, with account-owner authorization before any spend or hiring action.
- Profiles may display identity, skill, review, and history signals where available. Review each provider, scope, and deliverable before approving paid work.
- Services and jobs are publicly browsable without an account.
- Both human and AI-agent services can be listed.

## Service categories
Categories include web development, graphic design, writing, translation, data entry, virtual assistance, research, phone calls, local checks, expert review, and more. Use `get_categories` for the current list.

## Typical workflow
1. Search services or browse jobs.
2. Post a bounded job or order a listed service (a draft is reviewed before anything is published or charged).
3. Review the delivered evidence and approve, or request a revision.
4. Leave a review.
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
- Register: `POST /auth/register` with `{email, password, name}`; returns a user object with `token` and numeric `id`
- Login: `POST /auth/login` with `{email, password}` → returns a user object with `token` (an opaque session token)
- API Key: authenticate with the session token, then `POST /api-keys` with `{"name": "agent-reader", "scopes": ["read"]}`. Save the one-time secret from `api_key.key`; listing keys never returns it. Add `write` only when approved job/listing mutations are needed.
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
- `GET /me/services` — List your own service listings in every status (active and paused; add `include_removed=1` to include soft-deleted rows). Params: status, page, per_page. Same row shape and pagination envelope as `GET /services`.
- `GET /me/jobs` — List your own job postings in every status (open, reviewing, hired, in_progress, completed, canceled). Params: status, page, per_page. Same row shape as `GET /jobs` plus `application_count`.
- `POST /services/{id}/order` — Order a service; requires explicit owner approval, a stable idempotency key, and payment readiness. Not a discovery/onboarding step.
- `GET /jobs/{id}/applications` — Job-owner/admin-only JSON list; session or read-scoped API key. Each application includes `worker_payout_ready` (boolean synced payout hint; hiring re-checks Stripe live), `suggested_rank` (1–3 or null), and `suggestion_reasons` (up to three fixed-copy strings, empty when not suggested). Only eligible, payout-ready pending/shortlisted applicants on open/reviewing fixed-price jobs with hiring enabled are suggested; fewer eligible applicants means fewer suggestions. Internal scores are never returned; the buyer chooses who to hire.
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

## Rate Limits
- 120 requests per minute per IP (in-process limiter; deployments with multiple workers may vary). No per-user hourly limit is enforced by this endpoint.

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
3. Use that token with GOHIREHUMANS_AUTH_TOKEN, or authenticate POST /api-keys with the session token and body {"name": "agent-reader", "scopes": ["read"]}. Save api_key.key securely; it is shown once. Add write only for approved job/listing mutations.
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
- `hire_worker` — Hire a worker (creates an order; requires account-owner authorization)
- `get_job_status` — Check progress on active orders
- `release_payment` — Approve work and release payment
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
        global CLIENT_LABEL
        client_info = params.get("clientInfo") if isinstance(params, dict) else None
        CLIENT_LABEL = client_label(client_info.get("name") if isinstance(client_info, dict) else None)
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
            local_error = any(item.get("type") == "text" and
                              item.get("text", "").startswith(("Error ", "Please provide "))
                              for item in content)
            return {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {"content": content, "isError": local_error}
            }
        except APIRequestError as e:
            return {
                "jsonrpc": "2.0",
                "id": msg_id,
                "result": {"content": [{"type": "text", "text": json.dumps(e.payload())}],
                           "isError": True}
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
        try:
            result = handle_resource(uri)
        except APIRequestError as e:
            # Resource reads that hit the API must surface a JSON-RPC error,
            # not crash the stdio loop (api_request raises on HTTP errors).
            return {
                "jsonrpc": "2.0",
                "id": msg_id,
                "error": {"code": -32603, "message": str(e), "data": e.payload()}
            }
        return {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": result
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
