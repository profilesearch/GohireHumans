import importlib.util
import json
import os
import sqlite3
import stat
import tempfile
import unittest
from pathlib import Path


TOOL_PATH = Path(__file__).with_name("tools") / "reconcile_funding_attempts.py"


def load_tool():
    spec = importlib.util.spec_from_file_location("reconcile_funding_attempts_v5", TOOL_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load reconciliation tool")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ReconciliationCliSecurityTests(unittest.TestCase):
    def test_least_privilege_redaction_and_protected_output_contract(self):
        tool = load_tool()
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "snapshot.db"
            db = sqlite3.connect(db_path)
            db.execute("CREATE TABLE marker (id INTEGER)")
            db.commit()
            db.close()

            os.chmod(db_path, 0o644)
            with self.assertRaisesRegex(PermissionError, "owner-only"):
                tool.connect_read_only(db_path)
            os.chmod(db_path, 0o600)
            readonly, opened = tool.connect_read_only(db_path)
            try:
                self.assertEqual(opened, db_path.resolve())
                self.assertEqual(readonly.execute("PRAGMA query_only").fetchone()[0], 1)
            finally:
                readonly.close()
            self.assertFalse(Path(str(db_path) + "-wal").exists())
            self.assertFalse(Path(str(db_path) + "-journal").exists())

            secret = "seti_cli_client_secret_never_emit"
            report = {
                "attempt_id": 7,
                "request_fingerprint": "fingerprint-sensitive",
                "stripe_payment_intent_id": "pi_sensitive",
                "local_error_code": "processor_intent_conflict",
                "local_error_message": "raw processor details",
                "processor_evidence": {
                    "processor_object_id": "pi_nested_sensitive",
                    "processor_event_id": "evt_nested_sensitive",
                    "client_secret": secret,
                },
            }
            redacted = tool.redact_report(report, reveal_sensitive=False)
            redacted_text = json.dumps(redacted, sort_keys=True)
            self.assertEqual(redacted["attempt_id"], 7)
            self.assertNotIn("fingerprint-sensitive", redacted_text)
            self.assertNotIn("pi_sensitive", redacted_text)
            self.assertNotIn("raw processor details", redacted_text)
            self.assertNotIn(secret, redacted_text)

            revealed = tool.redact_report(report, reveal_sensitive=True)
            revealed_text = json.dumps(revealed, sort_keys=True)
            self.assertIn("fingerprint-sensitive", revealed_text)
            self.assertIn("pi_sensitive", revealed_text)
            self.assertIn("raw processor details", revealed_text)
            self.assertNotIn(secret, revealed_text)

            output_path = Path(tmp) / "protected" / "report.json"
            tool.write_protected_output(output_path, redacted_text + "\n")
            self.assertEqual(stat.S_IMODE(output_path.stat().st_mode), 0o600)
            self.assertEqual(output_path.read_text(), redacted_text + "\n")
            with self.assertRaises(FileExistsError):
                tool.write_protected_output(output_path, "must not overwrite\n")


if __name__ == "__main__":
    unittest.main()
