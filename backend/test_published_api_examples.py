"""Replay published job-creation requests locally, with their actual route and auth.

Discovery reads public HTML code containers, never executes their shell/Python code,
and fails closed on an unparseable request. A census of every visible mention of the
job-collection route then requires each mention to belong to a replayed example or to a
reviewed, non-request reference, so a request written in any other syntax cannot ship
unreviewed. The real handler uses disposable SQLite;
outbound HTTP is blocked and notification/email transports are mocked.
"""
import ast
import collections
import contextlib
import hashlib
import html
import io
import json
import re
import shlex
import tokenize
import unittest
import urllib.request
from dataclasses import dataclass, replace
from decimal import Decimal
from html.parser import HTMLParser
from pathlib import Path
from unittest import mock
from urllib.parse import urlsplit

import test_deep_audit_regressions as deep_audit

ROOT = Path(__file__).resolve().parents[1]
FRONTEND = ROOT / "frontend"
EXCLUDED = {"node_modules", "tests", "test-results", "playwright-report"}
STORED_FIELDS = {
    "title", "description", "category", "location_type", "location_detail",
    "budget_type", "budget_amount", "estimated_hours", "required_skills", "due_by",
}
ORIGIN = "https://gohirehumans-production.up.railway.app"
# Keep literal URL forms, including aliases, and multiplicity: additions need review too.
EXPECTED_EXAMPLES = [
    ("agent-onboarding.html", ORIGIN + "/jobs"),
    ("ai-integration.html", "/api/v1/jobs"),
    ("api-docs.html", ORIGIN + "/api/v1/jobs"),
    ("api-docs.html", "/api/v1/jobs"),
    ("blog/how-to-find-human-workers-ai-tasks.html", "/api/v1/jobs"),
    ("blog/human-as-a-service.html", ORIGIN + "/api/v1/jobs"),
    ("blog/mcp-for-marketplaces.html", ORIGIN + "/api/v1/jobs"),
    ("how-it-works.html", ORIGIN + "/api/v1/jobs"),
]
HTTP_REQUEST = re.compile(
    r"(?m)^[ \t]*(?P<method>GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)[ \t]+(?P<url>\S+)"
)
CURL_REQUEST = re.compile(r"(?m)^[ \t]*(?:\$[ \t]+)?curl\b")
PYTHON_REQUEST = re.compile(r"\brequests\.post\s*\(\s*(['\"])(?P<url>[^'\"]+)\1")


class DocumentText(HTMLParser):
    """Rendered text of a public page (no <script> or <style> content), independent of
    code-container markup. Block elements become line breaks."""

    BLOCK_TAGS = {"address", "article", "aside", "blockquote", "dd", "details", "div", "dl", "dt", "figcaption",
                  "figure", "footer", "h1", "h2", "h3", "h4", "h5", "h6", "header", "li", "main", "nav", "ol",
                  "p", "pre", "section", "summary", "table", "td", "th", "tr", "ul"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts, self.skip = [], 0

    def handle_starttag(self, tag, attrs):
        if tag in {"style", "script"}:
            self.skip += 1
        elif tag == "br":
            self.parts.append("\n")
        elif tag in self.BLOCK_TAGS:
            self.parts.append("\n\n")

    def handle_endtag(self, tag):
        if tag in {"style", "script"} and self.skip:
            self.skip -= 1
        elif tag in self.BLOCK_TAGS:
            self.parts.append("\n\n")

    def handle_data(self, data):
        if not self.skip:
            self.parts.append(data)


# Census of the job-collection route. Recognizing request syntaxes one by one cannot be
# complete (fetch with a quoted "method" key, requests.request, HTTPie, wget, Go, PHP ...), so
# every visible mention of the route must instead be (a) inside a replayed example or (b) a
# reviewed reference in REVIEWED_ROUTE_MENTIONS. A new request in any syntax adds an unreviewed
# mention and fails until it is replayable or reviewed. Keys are the mention plus up to 50
# characters of context on each side, whitespace-normalized; multiplicity counts.
ROUTE_MENTION = re.compile(r"/(?:api/v1/)?jobs(?![\w/.-])")
CENSUS_SUFFIXES = {".html", ".htm", ".txt", ".md", ".json", ".xml", ".yaml", ".yml"}
CONTEXT = 50
REVIEWED_ROUTE_MENTIONS = {
    'agent-onboarding.html': [
        'an exact category from GET /categories. This POST /jobs request publishes a real listing but does not cre',
    ],
    'ai-integration.html': [
        'POST to /api/v1/jobs with complete task instructions. Write the descri',
    ],
    'api-docs.html': [
        'GET /api/v1/jobs',
        'GET /api/v1/jobs',
        'GET /api/v1/jobs?category=phone_call&page=1&per_page=2',
        'GET /api/v1/me/jobs',
        'GET /api/v1/me/jobs',
        'GET /api/v1/me/jobs?status=completed&page=1&per_page=20',
        'GET/api/v1/me/jobs',
        'POST /api/v1/jobs',
        'POST /api/v1/jobs',
        'POST/api/v1/jobs',
        'posted, in every status, newest first. The public /api/v1/jobs browse only returns open and reviewing jobs; use ',
    ],
    'blog/ai-agent-marketplace-guide.html': [
        'Draft or post jobs: POST /api/v1/jobs creates a task listing with requirements, budget,',
    ],
    'changelog.html': [
        'ed listing endpoints: GET /me/services and GET /me/jobs return the caller’s own listings in every status,',
    ],
    'faq.html': [
        ' password, authenticate via session, and use POST /api/v1/jobs to create a job listing. Spending, hiring, and pa',
        'GET /api/v1/jobs: list jobs',
        'POST /api/v1/jobs: create a job listing',
    ],
    'how-it-works.html': [
        'POST to /api/v1/jobs with the task details. This publishes a listing s',
    ],
    'llms-full.txt': [
        '- `GET /api/v1/me/jobs`, `GET /api/v1/me/services`: your own listings.',
        '- `POST /api/v1/jobs`: publish a fixed-budget job (write scope or sess',
        "rl 'https://gohirehumans-production.up.railway.app/api/v1/jobs?category=phone_call&page=1&per_page=2'",
    ],
    'llms.txt': [
        '- Browse jobs: https://www.gohirehumans.com/#/jobs',
    ],
    'roadmap.html': [
        'GET /me/services and GET /me/jobs return the caller’s own listings in every status.',
        'obs read from the new GET /me/services and GET /me/jobs endpoints, so paused, hired, completed, and cance',
    ],
}


def mention_keys(text):
    keys = []
    for raw_line in text.splitlines():
        line = " ".join(raw_line.split())
        for match in ROUTE_MENTION.finditer(line):
            keys.append(line[max(0, match.start() - CONTEXT):match.end() + CONTEXT])
    return keys


def document_text(relative, source):
    if Path(relative).suffix in {".html", ".htm"}:
        parser = DocumentText()
        parser.feed(source)
        return "".join(parser.parts)
    return source


def published_sources():
    """Every public text surface a reader or agent can fetch, keyed by path under frontend/."""
    sources = {}
    for path in sorted(FRONTEND.rglob("*")):
        relative = path.relative_to(FRONTEND)
        if path.is_file() and not EXCLUDED.intersection(relative.parts) and path.suffix in CENSUS_SUFFIXES:
            sources[relative.as_posix()] = path.read_text(encoding="utf-8", errors="replace")
    return sources


def route_mention_census(sources):
    census = {}
    for relative, source in sources.items():
        keys = mention_keys(document_text(relative, source))
        if keys:
            census[relative] = collections.Counter(keys)
    return census


def census_differences(sources=None):
    """(unreviewed, stale): mentions no example or review accounts for, and reviewed mentions that vanished."""
    sources = published_sources() if sources is None else sources
    expected = {page: collections.Counter(keys) for page, keys in REVIEWED_ROUTE_MENTIONS.items()}
    for example in published_job_examples(sources):
        expected.setdefault(example.page, collections.Counter()).update(example.mentions)
    actual = route_mention_census(sources)
    unreviewed, stale = {}, {}
    for page in sorted(set(actual) | set(expected)):
        extra = actual.get(page, collections.Counter()) - expected.get(page, collections.Counter())
        missing = expected.get(page, collections.Counter()) - actual.get(page, collections.Counter())
        if extra:
            unreviewed[page] = sorted(extra.elements())
        if missing:
            stale[page] = sorted(missing.elements())
    return unreviewed, stale


@dataclass
class JobExample:
    page: str
    line: int
    route: str
    auth: str
    payload: dict
    mentions: tuple = ()


class ExampleDiscoveryError(ValueError):
    """A recognized job-creation request cannot be safely replayed."""


class CodeContainers(HTMLParser):
    """Collect outermost code containers once, decoding entities and span markup."""

    VOID_TAGS = {"area", "base", "br", "col", "embed", "hr", "img", "input",
                 "link", "meta", "param", "source", "track", "wbr"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.blocks = []
        self.tags = []
        self.parts = []
        self.line = 0

    def handle_starttag(self, tag, attrs):
        is_container = tag in {"pre", "code"} or (
            tag == "div" and "code-block" in (dict(attrs).get("class") or "").split()
        )
        if not self.tags and is_container:
            self.line = self.getpos()[0]
            self.parts = []
            self.tags = [tag]
        elif self.tags:
            if tag == "br":
                self.parts.append("\n")
            if tag not in self.VOID_TAGS:
                self.tags.append(tag)

    def handle_endtag(self, tag):
        if tag not in self.tags:
            return
        index = len(self.tags) - 1 - self.tags[::-1].index(tag)
        root_tag = self.tags[0]
        del self.tags[index:]
        if not self.tags:
            text = "".join(self.parts)
            # Inline endpoint references (e.g. FAQ links) are not request examples.
            if root_tag != "code" or "\n" in text or re.search(r"[{}(]|\bcurl\b", text):
                self.blocks.append((self.line, text))

    def handle_data(self, data):
        if self.tags:
            self.parts.append(data)


def is_job_creation_url(url):
    """Accept only the two creation routes, not job IDs, actions, or other sites."""
    parsed = urlsplit(url)
    return parsed.path in {"/jobs", "/api/v1/jobs"} and (
        not parsed.netloc or "gohirehumans" in (parsed.hostname or "")
    )


# The documented credential must match its header: API keys (ghh_...) go in X-API-Key, and
# Authorization: Bearer carries a session token from login. The API rejects a key sent as Bearer.
API_KEY_VALUE = re.compile(r"\bghh_\w+|API_KEY|\bapi[ _-]?key\b", re.I)
SESSION_VALUE = re.compile(r"session|token[\s_-]+from[\s_-]+login|login[\s_-]+token|auth[\s_-]*token", re.I)


def auth_class(headers):
    """Require exactly one documented, supported credential class whose value matches it."""
    classes = set()
    for header in headers:
        value = header.partition(":")[2]
        if re.match(r"\s*X-API-Key\s*:", header, re.I):
            if not API_KEY_VALUE.search(value) or SESSION_VALUE.search(value):
                raise ValueError(f"X-API-Key must carry an API key placeholder (ghh_...), not {value.strip()!r}")
            classes.add("X-API-Key")
        if re.match(r"\s*Authorization\s*:\s*Bearer\b", header, re.I):
            token = re.sub(r"(?i)^\s*Bearer\b", "", value)
            if API_KEY_VALUE.search(token) or not SESSION_VALUE.search(token):
                raise ValueError(f"Authorization: Bearer must carry a session token placeholder, not {token.strip()!r}")
            classes.add("Bearer")
    if len(classes) != 1:
        raise ValueError("request needs exactly one X-API-Key or Authorization: Bearer header")
    return classes.pop()


def parse_http_request(text, match):
    """Read a request body, stopping before another request or a response example."""
    request = text[match.end():]
    request = re.split(r"(?m)^\s*(?:#|//)\s*Response\b", request, maxsplit=1)[0]
    start = re.search(r"(?m)^[ \t]*[\[{]", request)
    if start is None:
        raise ValueError("request has no JSON object body")
    payload, end = json.JSONDecoder().raw_decode(request[start.start():].lstrip())
    body = request[start.start():].lstrip()
    trailing = body[end:].strip()
    if trailing and not trailing.startswith(("#", "//")):
        raise ValueError("unexpected text after JSON request body")
    return payload, auth_class(request[:start.start()].splitlines())


def parse_curl_request(text):
    """Parse curl arguments without executing a shell or substituting credentials."""
    words = shlex.split(text.replace("\\\n", " "))
    method, route, data, headers = None, None, None, []
    index = 1
    while index < len(words):
        word = words[index]
        if word in {"-X", "--request", "-H", "--header", "-d", "--data", "--data-raw", "--data-binary", "--json"}:
            if index + 1 == len(words):
                raise ValueError(f"curl option {word} has no value")
            index += 1
            value = words[index]
            if word in {"-X", "--request"}:
                method = value
            elif word in {"-H", "--header"}:
                headers.append(value)
            else:
                data = value
        elif word.startswith("-X"):
            method = word[2:]
        elif word.startswith("--request="):
            method = word.partition("=")[2]
        elif word.startswith("--header="):
            headers.append(word.partition("=")[2])
        elif any(word.startswith(option + "=") for option in ("--data", "--data-raw", "--data-binary", "--json")):
            data = word.partition("=")[2]
        elif is_job_creation_url(word):
            route = word
        index += 1
    if (method or ("POST" if data is not None else "GET")) != "POST" or route is None:
        return None
    if data is None:
        raise ValueError("curl POST has no JSON data")
    return route, auth_class(headers), json.loads(data)


def python_call_source(text):
    """Locate balanced call parentheses using Python tokens, not braces in strings."""
    depth = 0
    lines = text.splitlines(keepends=True)
    for token in tokenize.generate_tokens(io.StringIO(text).readline):
        if token.type == tokenize.OP:
            if token.string in "([{":
                depth += 1
            elif token.string in ")]}":
                depth -= 1
                if depth == 0:
                    line, column = token.end
                    return "".join(lines[:line - 1]) + lines[line - 1][:column]
    raise ValueError("unterminated requests.post call")


def parse_python_request(text):
    """Read literal requests.post json= or data=json.dumps(), never eval/execute."""
    call = ast.parse(python_call_source(text), mode="eval").body
    kwargs = {item.arg: item.value for item in call.keywords}
    if "json" in kwargs:
        payload = ast.literal_eval(kwargs["json"])
    elif "data" in kwargs:
        data = kwargs["data"]
        if not (isinstance(data, ast.Call) and isinstance(data.func, ast.Attribute)
                and isinstance(data.func.value, ast.Name) and data.func.value.id == "json"
                and data.func.attr == "dumps" and len(data.args) == 1):
            raise ValueError("Python data= must use json.dumps with a literal object")
        payload = ast.literal_eval(data.args[0])
    else:
        raise ValueError("requests.post has no literal JSON body")
    headers = kwargs.get("headers")
    if not isinstance(headers, ast.Dict):
        raise ValueError("requests.post needs a literal headers dictionary")
    header_lines = []
    for key, value in zip(headers.keys, headers.values):
        if key is None:
            raise ValueError("requests.post headers cannot use dictionary expansion")
        name = ast.literal_eval(key)
        # Keep the documented credential source (literal or e.g. os.environ["GOHIREHUMANS_API_KEY"]).
        shown = value.value if isinstance(value, ast.Constant) and isinstance(value.value, str) else ast.unparse(value)
        if name.lower() == "x-api-key":
            header_lines.append("X-API-Key: " + shown)
        elif name.lower() == "authorization":
            header_lines.append("Authorization: " + shown)
    return payload, auth_class(header_lines)


def discover_job_examples(page, source):
    """Discover every explicit POST-to-jobs code request, or fail with its source."""
    parser = CodeContainers()
    parser.feed(source)
    examples = []
    for line, block in parser.blocks:
        markers = sorted(
            [(m.start(), "http", m) for m in HTTP_REQUEST.finditer(block)]
            + [(m.start(), "curl", m) for m in CURL_REQUEST.finditer(block)]
            + [(m.start(), "python", m) for m in PYTHON_REQUEST.finditer(block)]
        )
        for index, (start, kind, match) in enumerate(markers):
            end = markers[index + 1][0] if index + 1 < len(markers) else len(block)
            text = block[start:end]
            request_line = line + block[:start].count("\n")
            if kind == "http" and (match["method"] != "POST" or not is_job_creation_url(match["url"])):
                continue
            if kind == "http" and not urlsplit(match["url"]).netloc:
                head = re.split(r"\n[ \t]*\n|\n[ \t]*[\[{]", text, maxsplit=1)[0]
                host = re.search(r"(?im)^[ \t]*Host[ \t]*:[ \t]*(\S+)", head)
                if host and "gohirehumans" not in host[1].lower():
                    continue
            if kind == "python" and not is_job_creation_url(match["url"]):
                continue
            if kind == "curl":
                # Recognize the target before shlex, so broken quoting cannot hide it.
                urls = re.findall(r"https?://[^\s'\"<>]+|/(?:api/v1/)?jobs[^\s'\"<>]*", text)
                if not any(is_job_creation_url(url) for url in urls):
                    continue
            try:
                if kind == "http":
                    local_match = HTTP_REQUEST.match(text)
                    payload, auth = parse_http_request(text, local_match)
                    route = match["url"]
                elif kind == "curl":
                    result = parse_curl_request(text)
                    if result is None:
                        continue
                    route, auth, payload = result
                else:
                    payload, auth = parse_python_request(text)
                    route = match["url"]
                if not isinstance(payload, dict):
                    raise ValueError("request JSON body must be an object")
            except (ValueError, SyntaxError, tokenize.TokenError) as error:
                raise ExampleDiscoveryError(
                    f"{page}:{request_line}: {error}; snippet: {text.strip()[:500]!r}"
                ) from error
            # Whole lines, so a call that starts mid-line ("response = requests.post(...") keeps its context.
            line_start = block.rfind("\n", 0, start) + 1
            examples.append(JobExample(page, request_line, route, auth, payload, tuple(mention_keys(block[line_start:end]))))
    return examples


def published_job_examples(sources=None):
    """Scan every public HTML page deterministically; never silently skip bad bodies."""
    sources = published_sources() if sources is None else sources
    examples = []
    for relative in sorted(sources):
        if Path(relative).suffix == ".html":
            examples.extend(discover_job_examples(relative, sources[relative]))
    return examples


class DiscoveryTests(unittest.TestCase):
    def test_malformed_curl_json_reports_page_and_snippet(self):
        for route in ("/jobs", "/api/v1/jobs", ORIGIN + "/jobs", ORIGIN + "/api/v1/jobs"):
            with self.subTest(route=route):
                source = f'''<div class="code-block"><pre><code>curl -X POST "{route}" \\
-H "X-API-Key: ghh_placeholder" --data '{{"title": "Broken",}}'</code></pre></div>'''
                with self.assertRaises(ExampleDiscoveryError) as caught:
                    discover_job_examples("broken-curl.html", source)
                self.assertIn("broken-curl.html:1:", str(caught.exception))
                self.assertIn("Broken", str(caught.exception))
                self.assertIn("snippet:", str(caught.exception))

    def test_malformed_http_json_is_not_skipped_or_replaced_by_response(self):
        for body in ('{"title": }', '', '["not an object"]'):
            with self.subTest(body=body), self.assertRaisesRegex(ExampleDiscoveryError, "broken-http.html"):
                discover_job_examples("broken-http.html", f'''<pre>POST /jobs
Authorization: Bearer session-placeholder
{body}
// Response 201
{{"title": "Not the request"}}</pre>''')

    def test_python_requests_literals_are_discovered(self):
        for argument in ('json={body}', 'data=json.dumps({body})'):
            with self.subTest(argument=argument):
                body = repr({"title": "Braces } and { in strings", "required_skills": [True, False, None]})
                source = f'''<pre>import json
response = requests.post("{ORIGIN}/jobs",
headers={{"X-API-Key": os.environ["GOHIREHUMANS_API_KEY"]}},
{argument.format(body=body)}, timeout=30)
response.raise_for_status()</pre>'''
                example, = discover_job_examples("synthetic-python.html", source)
                self.assertEqual(example.route, ORIGIN + "/jobs")
                self.assertEqual(example.auth, "X-API-Key")
                self.assertEqual(example.payload, ast.literal_eval(body))

    def test_malformed_python_body_fails(self):
        with self.assertRaisesRegex(ExampleDiscoveryError, "broken-python.html"):
            discover_job_examples("broken-python.html", '''<pre>requests.post("/jobs",
headers={"X-API-Key": "ghh_key"}, json={"title": })</pre>''')

    def test_references_gets_and_job_subroutes_are_not_creation_examples(self):
        source = '''<code>POST /api/v1/jobs</code>
<pre>GET /jobs</pre><pre>POST /jobs/1/apply</pre>
<pre>curl -X GET /jobs</pre><pre>curl -X POST /api/v1/jobs/1/hire -d '{}'</pre>
<pre>requests.post("/jobs/1/apply", json={})</pre>
<pre>POST https://unrelated.example/jobs</pre>'''
        self.assertEqual(discover_job_examples("references.html", source), [])

    def test_nested_highlighted_containers_and_multiple_requests(self):
        source = '''<div class="code-block"><pre><code>POST /jobs
<span>Authorization</span>: <span>Bearer</span> &lt;session&gt;
{&quot;title&quot;: &quot;First &amp; second&quot;}
POST /api/v1/jobs
X-API-Key: ghh_placeholder
{"title": "Second"}</code></pre></div>'''
        examples = discover_job_examples("highlighted.html", source)
        self.assertEqual([(e.route, e.auth) for e in examples],
                         [("/jobs", "Bearer"), ("/api/v1/jobs", "X-API-Key")])
        self.assertEqual(examples[0].payload["title"], "First & second")

    def test_absolute_and_native_http_routes_are_preserved(self):
        for route in ("/jobs", "/api/v1/jobs", ORIGIN + "/jobs", ORIGIN + "/api/v1/jobs"):
            with self.subTest(route=route):
                example, = discover_job_examples("routes.html", f'''<code>POST {route}
Authorization: Bearer session-placeholder
{{"title": "Route test"}}</code>''')
                self.assertEqual(example.route, route)
                self.assertEqual(example.auth, "Bearer")

    def test_missing_auth_is_an_explicit_discovery_failure(self):
        with self.assertRaisesRegex(ExampleDiscoveryError, "credential|header"):
            discover_job_examples("no-auth.html", '<pre>POST /jobs\n{"title": "No auth"}</pre>')


class PublishedJobExampleTests(unittest.TestCase):
    def assert_example_inventory(self, examples):
        """Check the reviewed multiset, so a duplicate cannot mask a missing example."""
        self.assertCountEqual([(e.page, e.route) for e in examples], EXPECTED_EXAMPLES)

    def test_examples_are_discovered(self):
        examples = published_job_examples()
        self.assert_example_inventory(examples)
        for example in examples:
            print(f"Published example: {example.page}:{example.line} POST {example.route} [{example.auth}]", flush=True)

    def test_inventory_rejects_a_removed_example(self):
        examples = [e for e in published_job_examples() if e.page != "agent-onboarding.html"]
        with self.assertRaisesRegex(AssertionError, "agent-onboarding.html"):
            self.assert_example_inventory(examples)

    def test_every_published_job_example_creates_the_job_it_describes(self):
        examples = published_job_examples()
        self.assert_example_inventory(examples)
        for example in examples:
            with self.subTest(page=example.page, line=example.line, route=example.route, auth=example.auth):
                self.assert_example_replays(example)

    def test_all_stored_fields_and_numeric_normalization_are_checked(self):
        example = published_job_examples()[0]
        payload = {
            "title": "Review a bounded artifact", "description": "Return a cited review of the artifact.",
            "category": "expert_review", "location_type": "on_site", "location_detail": "Denver, CO",
            "budget_type": "fixed", "budget_amount": "75.00", "estimated_hours": "2.5",
            "required_skills": ["research", "writing"], "due_by": "2027-03-10T17:00:00Z",
        }
        self.assertEqual(set(payload), STORED_FIELDS)
        self.assert_example_replays(replace(example, payload=payload))

    def test_invalid_category_and_ignored_location_are_rejected(self):
        example = next(e for e in published_job_examples()
                       if e.page == "blog/how-to-find-human-workers-ai-tasks.html")
        status, body, _ = self._replay(replace(example, payload=dict(example.payload, category="inspections")))
        self.assertEqual(status, 400, body)
        moved = dict(example.payload)
        moved["location"] = moved.pop("location_detail")
        with self.assertRaisesRegex(AssertionError, "location"):
            self.assert_persisted(replace(example, payload=moved), {})

    def assert_example_replays(self, example):
        """Require a real creation response and independently read back every sent field."""
        self.assertFalse(set(example.payload) - STORED_FIELDS,
                         f"{example.page} sends fields the jobs API ignores: {set(example.payload) - STORED_FIELDS}")
        status, body, row = self._replay(example)
        self.assertEqual(status, 201, f"{example.page}: {body}")
        self.assertIsNotNone(row, f"{example.page}: job was not persisted: {body}")
        self.assert_persisted(example, row)

    def assert_persisted(self, example, row):
        """Compare all supplied fields, decoding skills and SQLite numeric affinities."""
        unknown = set(example.payload) - STORED_FIELDS
        self.assertEqual(unknown, set(), f"{example.page} sends fields the jobs API ignores: {sorted(unknown)}")
        for field, expected in example.payload.items():
            self.assertIn(field, row, f"{example.page}: missing persisted {field}")
            actual = row[field]
            if field in {"budget_amount", "estimated_hours"} and expected is not None:
                self.assertIsNotNone(actual, f"{example.page}: {field} was dropped")
                actual, expected = Decimal(str(actual)), Decimal(str(expected))
            elif field == "required_skills":
                self.assertIsInstance(expected, list, f"{example.page}: required_skills must be a list")
                actual = json.loads(actual)
            self.assertEqual(actual, expected, f"{example.page}: persisted {field}")

    def _replay(self, example):
        """Use the real harness/handler with only the documented credential supplied."""
        case = deep_audit.BackendRegressionTests()
        credential = "ghh_example_replay_key" if example.auth == "X-API-Key" else "example-replay-session"
        with mock.patch.object(urllib.request, "urlopen", side_effect=RuntimeError("outbound HTTP blocked in example replay")):
            with contextlib.redirect_stdout(io.StringIO()):
                case.setUp()
            try:
                api = case.module
                db = api.get_db()
                try:
                    db.execute("INSERT INTO users (id, name, email, password_hash) "
                               "VALUES (1, 'Example replay', 'example-replay@example.invalid', 'unused')")
                    if example.auth == "X-API-Key":
                        db.execute("INSERT INTO api_keys (user_id, key_hash, key_prefix, name, scopes) "
                                   "VALUES (1, ?, ?, 'Published example', ?)",
                                   [hashlib.sha256(credential.encode()).hexdigest(), credential[:12], '["read","write"]'])
                    else:
                        self.assertEqual(example.auth, "Bearer")
                        db.execute("INSERT INTO sessions (user_id, token, expires_at) "
                                   "VALUES (1, ?, datetime('now', '+1 day'))", [credential])
                    db.commit()
                finally:
                    db.close()
                real_handler = api.handle_request
                route = urlsplit(example.route)

                def handle_with_documented_auth():
                    # _request_api initializes CGI state and clears X-API-Key; supply it last.
                    api._request_ctx.http_x_api_key = credential if example.auth == "X-API-Key" else ""
                    api._request_ctx.query_string = route.query
                    return real_handler()

                with (mock.patch.object(api, "push_notification"), mock.patch.object(api, "send_email"),
                      mock.patch.object(api, "handle_request", side_effect=handle_with_documented_auth)):
                    status, body = case._request_api("POST", route.path, example.payload,
                                                     credential if example.auth == "Bearer" else "")
                row = None
                if status == 201:
                    job_id = body.get("id") or (body.get("job") or {}).get("id")
                    db = api.get_db()
                    try:
                        found = db.execute("SELECT * FROM jobs WHERE id = ?", [job_id]).fetchone()
                        row = dict(found) if found is not None else None
                    finally:
                        db.close()
                return status, body, row
            finally:
                case.tearDown()


class CredentialAndInventoryTests(unittest.TestCase):
    def test_credential_value_must_match_its_header(self):
        for headers in (
            "Authorization: Bearer ghh_YOUR_API_KEY",
            "Authorization: Bearer YOUR_API_TOKEN",
            "X-API-Key: &lt;session-token&gt;",
        ):
            with self.subTest(headers=headers), self.assertRaisesRegex(ExampleDiscoveryError, "X-API-Key|Bearer"):
                discover_job_examples("mismatch.html", f'<pre>POST /api/v1/jobs\n{headers}\n\n{{"title": "x"}}</pre>')
        with self.assertRaisesRegex(ExampleDiscoveryError, "X-API-Key"):
            discover_job_examples("mismatch.html", """<pre>requests.post("/api/v1/jobs",
headers={"X-API-Key": os.environ["SESSION_TOKEN"]}, json={"title": "x"})</pre>""")

    def test_moving_a_published_api_key_into_bearer_fails_discovery(self):
        source = (FRONTEND / "agent-onboarding.html").read_text(encoding="utf-8")
        self.assertIn("X-API-Key: ghh_YOUR_API_KEY", source)
        with self.assertRaisesRegex(ExampleDiscoveryError, "Bearer"):
            discover_job_examples("agent-onboarding.html", source.replace(
                "X-API-Key: ghh_YOUR_API_KEY", "Authorization: Bearer ghh_YOUR_API_KEY", 1))

    def test_session_placeholders_are_accepted_for_bearer(self):
        for token in ("TOKEN_FROM_LOGIN", "&lt;token from login&gt;", "SESSION_TOKEN", "GOHIREHUMANS_AUTH_TOKEN"):
            with self.subTest(token=token):
                example, = discover_job_examples("bearer.html", f'<pre>POST /api/v1/jobs\nAuthorization: Bearer {token}\n\n{{"title": "x"}}</pre>')
                self.assertEqual(example.auth, "Bearer")

    def test_raw_http_host_header_decides_the_origin(self):
        body = '{"title": "x"}'
        foreign = f"<pre>POST /jobs HTTP/1.1\nHost: api.example.com\nAuthorization: Bearer SESSION_TOKEN\n\n{body}</pre>"
        self.assertEqual(discover_job_examples("foreign-host.html", foreign), [])
        ghh = foreign.replace("api.example.com", "gohirehumans-production.up.railway.app")
        example, = discover_job_examples("ghh-host.html", ghh)
        self.assertEqual(example.route, "/jobs")
        hostless, = discover_job_examples("hostless.html", f"<pre>POST /jobs\nAuthorization: Bearer SESSION_TOKEN\n\n{body}</pre>")
        self.assertEqual(hostless.auth, "Bearer")


class RouteMentionCensusTests(unittest.TestCase):
    PAGE = "blog/how-to-find-human-workers-ai-tasks.html"
    WRONG_BODY = {"description": "x" * 1300, "title": "Inspect a property", "category": "inspections",
                  "budget_type": "fixed", "budget_amount": 150, "location": "Denver, CO"}
    # Realistic documentation formats; each sends an API key as Bearer, an invalid category and
    # an ignored field. None may leave the module green, inside or outside a code container.
    VARIANTS = {
        "fetch-quoted-method": 'fetch("{origin}/api/v1/jobs", {{"method": "POST", "headers": {{"Authorization": '
                               '"Bearer ghh_YOUR_API_KEY", "Content-Type": "application/json"}}, "body": JSON.stringify({body})}})',
        "requests-request": 'response = requests.request("POST", "{origin}/api/v1/jobs", headers={{"Authorization": '
                            '"Bearer ghh_YOUR_API_KEY"}}, json={body})',
        "httpie": "http POST {origin}/api/v1/jobs Authorization:'Bearer ghh_YOUR_API_KEY' <<< '{body}'",
        "wget": "wget --method=POST --header='Authorization: Bearer ghh_YOUR_API_KEY' --body-data='{body}' {origin}/api/v1/jobs",
        "axios-config": 'axios({{method: "post", url: `${{BASE}}/api/v1/jobs`, headers: {{Authorization: "Bearer ghh_YOUR_API_KEY"}}, data: {body}}})',
        "go": 'req, _ := http.NewRequest("POST", "{origin}/api/v1/jobs", bytes.NewBuffer([]byte(`{body}`)))',
        "raw-http-long": "POST /api/v1/jobs\nAuthorization: Bearer ghh_YOUR_API_KEY\nContent-Type: application/json\n\n{body}",
        "split-url": 'const url = BASE_URL + "/api/v1/jobs";\nawait fetch(url, {{"method": "POST", body: JSON.stringify({body})}});',
    }

    def mutate(self, page, snippet, container):
        sources = published_sources()
        wrapped = {"pre": "<pre><code>{}</code></pre>", "div": '<div class="example-request">{}</div>',
                   "p": "<p>{}</p>"}[container].format(html.escape(snippet))
        sources[page] = sources[page].replace("</main>", wrapped + "</main>", 1)
        return sources

    def assert_module_fails(self, sources, page):
        """The mutated page must fail discovery or leave an unreviewed route mention (the census is per page)."""
        try:
            unreviewed, _ = census_differences({page: sources[page]})
        except ExampleDiscoveryError:
            return
        self.assertTrue(unreviewed, "a request was added without failing discovery or the census")

    def test_every_route_mention_is_replayed_or_reviewed(self):
        unreviewed, stale = census_differences()
        self.assertEqual(unreviewed, {}, "A public page mentions the jobs route outside a replayed example. "
                         "Make it a replayable example or, if it is not a request, add it to REVIEWED_ROUTE_MENTIONS.")
        self.assertEqual(stale, {}, "REVIEWED_ROUTE_MENTIONS lists mentions that no longer exist; remove them.")

    def test_every_replayed_example_accounts_for_its_own_mentions(self):
        for example in published_job_examples():
            with self.subTest(page=example.page, line=example.line):
                self.assertTrue(example.mentions)

    def test_reviewed_references_are_not_requests(self):
        for page, keys in REVIEWED_ROUTE_MENTIONS.items():
            for key in keys:
                with self.subTest(page=page, key=key):
                    # Read-only references (GET, prose, headings) only: nothing that sends a body or a credential.
                    self.assertNotRegex(key, r"(?i)X-API-Key|Authorization|Bearer|--data|\s-d\s|-X\s*POST|json=|body:|"
                                             r"\"title\"|fetch\(|requests\.(?:post|request)|--method|\bhttp\s+POST")

    def test_realistic_request_formats_cannot_ship_unreviewed(self):
        body = json.dumps(self.WRONG_BODY)
        for name, template in self.VARIANTS.items():
            for origin in (ORIGIN, "${GHH_API}"):
                snippet = template.format(origin=origin, body=body)
                for container in ("pre", "div", "p"):
                    with self.subTest(variant=name, origin=origin[:8], container=container):
                        self.assert_module_fails(self.mutate(self.PAGE, snippet, container), self.PAGE)
                for suffix in (".txt", ".md"):
                    with self.subTest(variant=name, origin=origin[:8], surface=suffix):
                        sources = published_sources()
                        sources["examples/hidden" + suffix] = snippet
                        self.assert_module_fails(sources, "examples/hidden" + suffix)

    def test_a_correct_new_example_is_accounted_for_but_needs_inventory_review(self):
        good = (f'curl -X POST {ORIGIN}/api/v1/jobs \\\n  -H "X-API-Key: ghh_YOUR_API_KEY" \\\n'
                f"  -d '{json.dumps({'title': 'Proofread a page', 'category': 'expert_review'})}'")
        sources = self.mutate("stats.html", good, "pre")
        self.assertEqual(census_differences(sources), ({}, {}))
        with self.assertRaisesRegex(AssertionError, "stats.html"):
            PublishedJobExampleTests().assert_example_inventory(published_job_examples(sources))

    def test_removing_a_reviewed_reference_is_reported_as_stale(self):
        sources = published_sources()
        sources["faq.html"] = sources["faq.html"].replace("GET /api/v1/jobs", "GET /api/v1/services")
        _, stale = census_differences(sources)
        self.assertIn("faq.html", stale)

    def test_census_ignores_scripts_styles_and_other_routes(self):
        source = ('<script>fetch("/api/v1/jobs", {method: "POST"})</script><style>/* /jobs */</style>'
                  '<script type="application/ld+json">{"text": "POST /api/v1/jobs"}</script>'
                  "<p>See /jobs/12/apply and /api/v1/jobs-archive.</p>")
        self.assertEqual(route_mention_census({"x.html": source}), {})
        self.assertEqual(route_mention_census({"x.txt": "POST /api/v1/jobs"}), {"x.txt": collections.Counter(
            ["POST /api/v1/jobs"])})

    def test_census_is_linear_on_adversarial_input(self):
        import time
        for unit in ("/jobs ", "POST /api/v1/jobs\n", "x" * 99 + "/jobs\n", "<p>/jobs</p>"):
            text = unit * (1_000_000 // len(unit))
            started = time.perf_counter()
            route_mention_census({"x.html": text})
            self.assertLess(time.perf_counter() - started, 10, unit)


if __name__ == "__main__":
    unittest.main()
