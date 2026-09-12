import importlib.util
import json
from pathlib import Path
import subprocess
import unittest
from unittest import mock

SCRIPT = Path(__file__).resolve().parents[1] / 'production_smoke.py'
SHA = 'a' * 40
SPEC = importlib.util.spec_from_file_location('smoke', SCRIPT)
assert SPEC is not None and SPEC.loader is not None
smoke = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(smoke)


class ProductionSmokeTests(unittest.TestCase):
    def exercise(self, failure=None, state=None):
        commands = []
        def run(command, **kwargs):
            commands.append(command)
            if command[0] == 'docker' and command[1] == failure:
                raise subprocess.CalledProcessError(1, command)
            if command[0] == 'docker' and command[1] == 'inspect':
                return subprocess.CompletedProcess(command, 0, json.dumps(state or {
                    'Status': 'running', 'Health': {'Status': 'healthy'}}))
            return subprocess.CompletedProcess(command, 0, '')
        with mock.patch.object(smoke.subprocess, 'run', side_effect=run):
            try:
                smoke.smoke(SHA)
            except (RuntimeError, subprocess.CalledProcessError):
                if not failure and not state:
                    raise
            else:
                self.assertFalse(failure or state, 'failure falsely passed')
        return commands

    def test_exact_committed_production_context_and_runtime_contract(self):
        commands = self.exercise()
        archive = next(c for c in commands if c[0] == 'git')
        self.assertIn(SHA + ':backend', archive)
        build = next(c for c in commands if c[1] == 'build')
        self.assertEqual(build[-1], '-')  # tracked immutable archive, not dirty workspace
        self.assertEqual(build[build.index('--file') + 1], 'Dockerfile')
        launch = next(c for c in commands if c[1] == 'run')
        self.assertIn('--network=none', launch)
        envs = [launch[i + 1] for i, value in enumerate(launch) if value == '--env']
        self.assertEqual(set(envs), {
            'PORT=8080', 'DATABASE_PATH=/data/gohirehumans.db', 'ENVIRONMENT=production',
            'ENABLE_AUTO_SEED=0', 'EMAIL_OUTBOX_WORKER_ENABLED=0',
            'NOTIFICATION_MAINTENANCE_WORKER_ENABLED=0', 'RAILWAY_GIT_COMMIT_SHA=' + SHA})
        self.assertFalse(set(launch) & {'-v', '--volume', '--mount', '-p', '--publish',
                                       '--env-file', '--entrypoint', '--user', '--no-healthcheck'})
        self.assertEqual(launch[-1], build[build.index('--tag') + 1])  # no CMD override
        probe = next(c for c in commands if c[1] == 'exec')[-1]
        for expected in ('/data/gohirehumans.db', 'mode=ro', 'quick_check',
                         'os.getuid()!=0', "d['service']", "d['version']", "r.status==200"):
            self.assertIn(expected, probe)
        compile(probe, '<container probe>', 'exec')
        self.assertEqual(commands[-2][1:3], ['rm', '-f'])
        self.assertEqual(commands[-1][1:3], ['image', 'rm'])

    def test_cleanup_on_every_docker_failure(self):
        for failure in ('build', 'run', 'inspect', 'exec'):
            with self.subTest(failure=failure):
                commands = self.exercise(failure=failure)
                self.assertEqual(commands[-1][1:3], ['image', 'rm'])
                if failure != 'build':
                    self.assertTrue(any(c[1:3] == ['rm', '-f'] for c in commands))

    def test_exited_unhealthy_and_missing_health_fail_closed(self):
        for state in ({'Status': 'exited'},
                      {'Status': 'running', 'Health': {'Status': 'unhealthy'}},
                      {'Status': 'running'}):
            with self.subTest(state=state):
                self.exercise(state=state)

    def test_health_timeout_is_bounded_and_cleaned(self):
        with mock.patch.object(smoke.time, 'monotonic', side_effect=[0, 101]):
            commands = self.exercise(state={'Status': 'running', 'Health': {'Status': 'starting'}})
        self.assertEqual(commands[-1][1:3], ['image', 'rm'])

    def test_invalid_sha_never_calls_docker(self):
        with mock.patch.object(smoke.subprocess, 'run') as run:
            for sha in ('main', 'a' * 7, 'A' * 40, SHA + ';'):
                with self.subTest(sha=sha), self.assertRaises(ValueError):
                    smoke.smoke(sha)
            run.assert_not_called()


if __name__ == '__main__':
    unittest.main()
