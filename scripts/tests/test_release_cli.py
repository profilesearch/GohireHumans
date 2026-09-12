import contextlib
import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location('gate', Path(__file__).resolve().parents[1] / 'release_preflight.py')
assert SPEC is not None and SPEC.loader is not None
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)
SHA = 'a' * 40


class ReleaseCliTests(unittest.TestCase):
    def test_validates_identity_before_network_and_reads_exact_sha(self):
        self.assertTrue(hasattr(gate, 'main'), 'preflight CLI is missing')
        with mock.patch('urllib.request.urlopen') as fetch:
            for sha in ['main', 'a' * 7, 'a' * 39 + '/', 'A' * 40]:
                with self.subTest(sha=sha), contextlib.redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit):
                        gate.main(['--repo', 'profilesearch/GohireHumans', '--sha', sha])
            fetch.assert_not_called()
        checks = [dict(id=i, name=n, head_sha=SHA, status='completed', conclusion='success')
                  for i, n in enumerate(gate.REQUIRED_CHECKS)]
        def response(req, **kwargs):
            self.assertEqual(req.get_method(), 'GET')
            self.assertIn('/commits/' + SHA + '/', req.full_url)
            payload = {'total_count': len(checks), 'check_runs': checks}
            if '/statuses?' in req.full_url:
                payload = [dict(context='Vercel', state='success')]
            res = mock.MagicMock()
            res.__enter__.return_value.read.return_value = json.dumps(payload).encode()
            return res
        with mock.patch('urllib.request.urlopen', side_effect=response), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(gate.main(['--repo', 'profilesearch/GohireHumans', '--sha', SHA,
                                        '--require-status', 'Vercel']), 0)
            checks.pop()
            self.assertEqual(gate.main(['--repo', 'profilesearch/GohireHumans', '--sha', SHA]), 1)


    def test_pagination_latest_status_and_failure_matrix(self):
        checks = [dict(id=i, name=n, head_sha=SHA, status='completed', conclusion='success')
                  for i, n in enumerate(gate.REQUIRED_CHECKS)]
        def invoke(responses, statuses=False):
            with mock.patch.object(gate, 'github_get', side_effect=responses), contextlib.redirect_stdout(io.StringIO()) as output:
                code = gate.main(['--repo', 'profilesearch/GohireHumans', '--sha', SHA]
                                 + (['--require-status', 'Vercel'] if statuses else []))
            return code, json.loads(output.getvalue())
        self.assertEqual(invoke([{'total_count': 3, 'check_runs': checks[:2]},
                                 {'total_count': 3, 'check_runs': checks[2:]}])[0], 0)
        for conclusion in ('failure', 'cancelled', 'skipped', 'neutral', 'timed_out',
                           'action_required', 'stale', None):
            changed = [*checks[:-1], {**checks[-1], 'conclusion': conclusion}]
            with self.subTest(conclusion=conclusion):
                self.assertEqual(invoke([{'total_count': 3, 'check_runs': changed}])[0], 1)
        for response in ({'total_count': 4, 'check_runs': []},
                         {'total_count': 2, 'check_runs': checks},
                         {'total_count': 4, 'check_runs': [*checks, checks[0]]},
                         {'total_count': 3, 'check_runs': [None]}, {},
                         {'total_count': '3', 'check_runs': checks}):
            with self.subTest(response=response):
                self.assertEqual(invoke([response])[0], 1)
        for statuses in ([], [{'context': 'Vercel', 'state': 'pending'},
                              {'context': 'Vercel', 'state': 'success'}]):
            self.assertEqual(invoke([{'total_count': 3, 'check_runs': checks}, statuses], True)[0], 1)
        page = [{'context': 'other', 'state': 'pending'}] * 100
        self.assertEqual(invoke([{'total_count': 3, 'check_runs': checks}, page,
                                 [{'context': 'Vercel', 'state': 'success'}]], True)[0], 0)

    def test_network_failure_is_redacted(self):
        import urllib.error
        with mock.patch.object(gate, 'github_get', side_effect=urllib.error.URLError('PRIVATE_TOKEN')), contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(gate.main(['--repo', 'profilesearch/GohireHumans', '--sha', SHA]), 1)
        self.assertNotIn('PRIVATE_TOKEN', output.getvalue())


if __name__ == '__main__':
    unittest.main()
