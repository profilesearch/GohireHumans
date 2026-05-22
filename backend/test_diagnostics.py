import importlib.util
import os
from pathlib import Path
from types import ModuleType
from typing import Any, cast
import unittest


MODULE_PATH = Path(__file__).with_name("api_core.py")


def load_api_core() -> Any:
    spec = importlib.util.spec_from_file_location("api_core_under_test", MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("Could not load api_core.py for diagnostics tests")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cast(ModuleType, module))
    return module


class DiagnosticEndpointGateTests(unittest.TestCase):
    def test_diagnostic_endpoint_disabled_by_default(self):
        module = load_api_core()
        module.DIAGNOSTIC_ENDPOINT_ENABLED = False
        module.DIAGNOSTIC_SECRET = ""
        module._request_ctx.http_x_diagnostic_secret = "anything"

        self.assertFalse(module.diagnostic_endpoint_allowed())

    def test_diagnostic_endpoint_requires_matching_secret(self):
        module = load_api_core()
        module.DIAGNOSTIC_ENDPOINT_ENABLED = True
        module.DIAGNOSTIC_SECRET = "expected-secret"

        module._request_ctx.http_x_diagnostic_secret = "wrong-secret"
        self.assertFalse(module.diagnostic_endpoint_allowed())

        module._request_ctx.http_x_diagnostic_secret = "expected-secret"
        self.assertTrue(module.diagnostic_endpoint_allowed())

    def test_diagnostic_endpoint_falls_back_to_cgi_header_env(self):
        module = load_api_core()
        module.DIAGNOSTIC_ENDPOINT_ENABLED = True
        module.DIAGNOSTIC_SECRET = "expected-secret"
        module._request_ctx.http_x_diagnostic_secret = ""

        old = os.environ.get("HTTP_X_DIAGNOSTIC_SECRET")
        os.environ["HTTP_X_DIAGNOSTIC_SECRET"] = "expected-secret"
        try:
            self.assertTrue(module.diagnostic_endpoint_allowed())
        finally:
            if old is None:
                os.environ.pop("HTTP_X_DIAGNOSTIC_SECRET", None)
            else:
                os.environ["HTTP_X_DIAGNOSTIC_SECRET"] = old


class BackupEndpointGateTests(unittest.TestCase):
    def test_backup_endpoint_requires_matching_secret(self):
        module = load_api_core()
        module.BACKUP_SECRET = "expected-backup-secret"

        module._request_ctx.http_x_backup_secret = "wrong-secret"
        self.assertFalse(module.backup_endpoint_allowed())

        module._request_ctx.http_x_backup_secret = "expected-backup-secret"
        self.assertTrue(module.backup_endpoint_allowed())


class SeededSampleAccountTests(unittest.TestCase):
    def test_seeded_sample_email_detection(self):
        module = load_api_core()
        self.assertTrue(module.is_seeded_sample_email("Sarah.Chen@Example.com"))
        self.assertTrue(module.is_seeded_sample_email("hire@techstartup.io"))
        self.assertFalse(module.is_seeded_sample_email("real.customer@example.org"))


if __name__ == "__main__":
    unittest.main()
