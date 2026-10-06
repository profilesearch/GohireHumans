"""Offline regression checks for the standalone MCP distribution."""
from pathlib import Path
import os
import shutil
import subprocess
import sys
import tempfile
import tomllib
import unittest

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / 'backend/mcp-package'


class MCPPackagingTests(unittest.TestCase):
    def test_installable_console_script_metadata(self):
        metadata = PACKAGE / 'pyproject.toml'
        self.assertTrue(metadata.is_file(), 'MCP package must have Python build metadata')
        project = tomllib.loads(metadata.read_text())
        self.assertEqual(project['build-system']['build-backend'], 'setuptools.build_meta')
        self.assertEqual(project['project']['name'], 'gohirehumans-mcp')
        self.assertEqual(project['project']['scripts']['gohirehumans-mcp'], 'mcp_server:main')
        self.assertEqual(project['project']['dependencies'], [])
        self.assertEqual(project['tool']['setuptools']['py-modules'], ['mcp_server'])
        self.assertEqual((ROOT / 'backend/mcp_server.py').read_bytes(),
                         (PACKAGE / 'mcp_server.py').read_bytes())

    def test_sync_rejects_stale_generated_source(self):
        script = ROOT / 'scripts/sync_mcp_package.py'
        self.assertTrue(script.is_file(), 'Canonical source needs a checked generation command')
        with tempfile.TemporaryDirectory(dir=os.environ.get('TMPDIR')) as directory:
            fixture = Path(directory)
            (fixture / 'scripts').mkdir()
            (fixture / 'backend/mcp-package').mkdir(parents=True)
            copied_script = fixture / 'scripts/sync_mcp_package.py'
            shutil.copyfile(script, copied_script)
            source = fixture / 'backend/mcp_server.py'
            generated = fixture / 'backend/mcp-package/mcp_server.py'
            source.write_bytes(b'# canonical\n')
            generated.write_bytes(b'# stale\n')
            stale = subprocess.run([sys.executable, str(copied_script), '--check'],
                                   capture_output=True, text=True)
            self.assertEqual(stale.returncode, 1)
            self.assertEqual(generated.read_bytes(), b'# stale\n')
            synced = subprocess.run([sys.executable, str(copied_script)],
                                    capture_output=True, text=True)
            self.assertEqual(synced.returncode, 0, synced.stderr)
            self.assertEqual(generated.read_bytes(), source.read_bytes())
            checked = subprocess.run([sys.executable, str(copied_script), '--check'],
                                     capture_output=True, text=True)
            self.assertEqual(checked.returncode, 0, checked.stderr)


if __name__ == '__main__':
    unittest.main()
