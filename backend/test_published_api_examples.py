"""Replay every published `POST /api/v1/jobs` JSON example against the real job handler.

A copied example must create a job (HTTP 201) and persist the fields it sends; an example
with an invalid category or a field the API ignores (for example `location` instead of
`location_detail`) would mislead the agents and developers who copy it.
The handler runs against a disposable SQLite database, outbound HTTP is blocked, and
notification/email side effects are mocked.
"""
import contextlib
import html
import io
import json
import re
import unittest
import urllib.request
from pathlib import Path
from unittest import mock

import test_deep_audit_regressions as deep_audit

ROOT = Path(__file__).resolve().parents[1]
FRONTEND = ROOT / "frontend"
EXCLUDED = {"node_modules", "test-results", "playwright-report"}
# Fields the jobs handler stores; every one a published example sends must exist here.
STORED_FIELDS = {"title", "description", "category", "location_type", "location_detail",
                 "budget_type", "budget_amount", "estimated_hours", "required_skills", "due_by"}


def published_job_examples():
    examples = []
    for path in sorted(FRONTEND.rglob("*.html")):
        if EXCLUDED & set(path.relative_to(FRONTEND).parts):
            continue
        source = path.read_text(encoding="utf-8")
        for match in re.finditer(r"POST /api/v1/jobs\b", source):
            block_end = min(i for i in (source.find("</code>", match.end()), source.find("</div>", match.end()),
                                        len(source)) if i != -1)
            text = html.unescape(re.sub(r"<[^>]+>", "", source[match.end():block_end]))
            start = text.find("{")
            if start == -1:
                continue
            try:
                payload, _ = json.JSONDecoder().raw_decode(text[start:])
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict) and "title" in payload:
                examples.append((str(path.relative_to(FRONTEND)), payload))
    return examples


class PublishedJobExampleTests(unittest.TestCase):
    def test_examples_are_discovered(self):
        pages = {page for page, _ in published_job_examples()}
        self.assertIn("blog/how-to-find-human-workers-ai-tasks.html", pages)
        self.assertGreaterEqual(len(pages), 2)

    def test_every_published_job_example_creates_the_job_it_describes(self):
        examples = published_job_examples()
        self.assertTrue(examples)
        for page, payload in examples:
            with self.subTest(page=page, title=payload.get("title")):
                unknown = set(payload) - STORED_FIELDS
                self.assertEqual(unknown, set(), f"{page} sends fields the jobs API ignores: {sorted(unknown)}")
                status, body, row = self._replay(payload)
                self.assertEqual(status, 201, body)
                if row is None:
                    self.fail(f"{page}: job was not persisted: {body}")
                for field in ("title", "description", "category", "location_type", "location_detail", "budget_type"):
                    if field in payload:
                        self.assertEqual(row[field], payload[field], field)

    def test_invalid_category_and_ignored_location_are_rejected(self):
        page, payload = next((p, e) for p, e in published_job_examples()
                             if p == "blog/how-to-find-human-workers-ai-tasks.html")
        status, body, _ = self._replay(dict(payload, category="inspections"))
        self.assertEqual(status, 400, body)
        moved = {k: v for k, v in payload.items() if k != "location_detail"}
        moved["location"] = payload["location_detail"]
        self.assertIn("location", set(moved) - STORED_FIELDS)

    def _replay(self, payload):
        case = deep_audit.BackendRegressionTests()
        with contextlib.redirect_stdout(io.StringIO()):
            case.setUp()
        try:
            api = case.module
            db = api.get_db()
            db.execute("INSERT INTO users (id, name, email, password_hash) "
                       "VALUES (1, 'Example replay', 'example-replay@example.invalid', 'unused')")
            db.execute("INSERT INTO sessions (user_id, token, expires_at) "
                       "VALUES (1, 'example-replay-session', datetime('now', '+1 day'))")
            db.commit()
            db.close()
            blocked = mock.patch.object(urllib.request, "urlopen",
                                        side_effect=RuntimeError("outbound HTTP blocked in example replay"))
            with blocked, mock.patch.object(api, "push_notification"), mock.patch.object(api, "send_email"):
                status, body = case._request_api("POST", "/jobs", payload, "example-replay-session")
            row = None
            if status == 201:
                job_id = body.get("id") or (body.get("job") or {}).get("id")
                db = api.get_db()
                found = db.execute("SELECT * FROM jobs WHERE id = ?", [job_id]).fetchone()
                row = dict(found) if found is not None else None
                db.close()
                self.assertIsNotNone(row, body)
            return status, body, row
        finally:
            case.tearDown()


if __name__ == "__main__":
    unittest.main()
