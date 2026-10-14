"""Replay published job-creation requests locally, with their actual route and auth.

Discovery reads public HTML code containers, never executes their shell/Python code,
and fails closed on an unparseable request. The real handler uses disposable SQLite;
outbound HTTP is blocked and notification/email transports are mocked.
"""
import ast
import contextlib
import hashlib
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


@dataclass
class JobExample:
    page: str
    line: int
    route: str
    auth: str
    payload: dict


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


def auth_class(headers):
    """Require exactly one documented, supported credential class."""
    classes = set()
    for header in headers:
        if re.match(r"\s*X-API-Key\s*:", header, re.I):
            classes.add("X-API-Key")
        if re.match(r"\s*Authorization\s*:\s*Bearer\b", header, re.I):
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
        if name.lower() == "x-api-key":
            header_lines.append("X-API-Key: placeholder")
        elif name.lower() == "authorization":
            header_lines.append("Authorization: " + ast.literal_eval(value))
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
            examples.append(JobExample(page, request_line, route, auth, payload))
    return examples


def published_job_examples():
    """Scan every public HTML page deterministically; never silently skip bad bodies."""
    examples = []
    for path in sorted(FRONTEND.rglob("*.html")):
        relative = path.relative_to(FRONTEND)
        if not EXCLUDED.intersection(relative.parts):
            examples.extend(discover_job_examples(relative.as_posix(), path.read_text(encoding="utf-8")))
    return examples


class DiscoveryTests(unittest.TestCase):
    def test_malformed_curl_json_reports_page_and_snippet(self):
        for route in ("/jobs", "/api/v1/jobs", ORIGIN + "/jobs", ORIGIN + "/api/v1/jobs"):
            with self.subTest(route=route):
                source = f'''<div class="code-block"><pre><code>curl -X POST "{route}" \\
-H "X-API-Key: placeholder" --data '{{"title": "Broken",}}'</code></pre></div>'''
                with self.assertRaises(ExampleDiscoveryError) as caught:
                    discover_job_examples("broken-curl.html", source)
                self.assertIn("broken-curl.html:1:", str(caught.exception))
                self.assertIn("Broken", str(caught.exception))
                self.assertIn("snippet:", str(caught.exception))

    def test_malformed_http_json_is_not_skipped_or_replaced_by_response(self):
        for body in ('{"title": }', '', '["not an object"]'):
            with self.subTest(body=body), self.assertRaisesRegex(ExampleDiscoveryError, "broken-http.html"):
                discover_job_examples("broken-http.html", f'''<pre>POST /jobs
Authorization: Bearer placeholder
{body}
// Response 201
{{"title": "Not the request"}}</pre>''')

    def test_python_requests_literals_are_discovered(self):
        for argument in ('json={body}', 'data=json.dumps({body})'):
            with self.subTest(argument=argument):
                body = repr({"title": "Braces } and { in strings", "required_skills": [True, False, None]})
                source = f'''<pre>import json
response = requests.post("{ORIGIN}/jobs",
headers={{"X-API-Key": os.environ["KEY"]}},
{argument.format(body=body)}, timeout=30)
response.raise_for_status()</pre>'''
                example, = discover_job_examples("synthetic-python.html", source)
                self.assertEqual(example.route, ORIGIN + "/jobs")
                self.assertEqual(example.auth, "X-API-Key")
                self.assertEqual(example.payload, ast.literal_eval(body))

    def test_malformed_python_body_fails(self):
        with self.assertRaisesRegex(ExampleDiscoveryError, "broken-python.html"):
            discover_job_examples("broken-python.html", '''<pre>requests.post("/jobs",
headers={"X-API-Key": "key"}, json={"title": })</pre>''')

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
X-API-Key: placeholder
{"title": "Second"}</code></pre></div>'''
        examples = discover_job_examples("highlighted.html", source)
        self.assertEqual([(e.route, e.auth) for e in examples],
                         [("/jobs", "Bearer"), ("/api/v1/jobs", "X-API-Key")])
        self.assertEqual(examples[0].payload["title"], "First & second")

    def test_absolute_and_native_http_routes_are_preserved(self):
        for route in ("/jobs", "/api/v1/jobs", ORIGIN + "/jobs", ORIGIN + "/api/v1/jobs"):
            with self.subTest(route=route):
                example, = discover_job_examples("routes.html", f'''<code>POST {route}
Authorization: Bearer placeholder
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


if __name__ == "__main__":
    unittest.main()
