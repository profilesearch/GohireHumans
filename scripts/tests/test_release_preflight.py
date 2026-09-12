import importlib.util
from pathlib import Path
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'release_preflight.py'
SHA = 'a' * 40


class ReleasePreflightTests(unittest.TestCase):
    def load_gate(self):
        self.assertTrue(SCRIPT.exists(), 'read-only release preflight is missing')
        spec = importlib.util.spec_from_file_location('release_preflight', SCRIPT)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_requires_success_for_every_named_check_on_exact_sha(self):
        gate = self.load_gate()
        checks = [dict(id=1, name='backend-tests', head_sha=SHA,
                       status='completed', conclusion='success')]
        self.assertEqual(gate.check_failures(SHA, checks, ['backend-tests']), [])
        for changes in [dict(head_sha='b' * 40), dict(status='in_progress'),
                        dict(conclusion='skipped'), dict(conclusion='failure')]:
            with self.subTest(changes=changes):
                self.assertTrue(gate.check_failures(SHA, [{**checks[0], **changes}], ['backend-tests']))
        self.assertTrue(gate.check_failures(SHA, [], ['backend-tests']))


if __name__ == '__main__':
    unittest.main()
