"""Offline regression checks for the standalone MCP distribution."""
from pathlib import Path
import os
import re
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
        # setuptools>=77 (needed for the SPDX `license` string) itself requires Python 3.9+, so a Git/sdist
        # install on 3.8 fails at build time. Declare the floor that actually installs.
        self.assertIn('setuptools>=77', project['build-system']['requires'])
        self.assertEqual(project['project']['requires-python'], '>=3.9')
        self.assertIn('- Python 3.9+', (PACKAGE / 'README.md').read_text())
        self.assertNotIn('Python 3.8', (PACKAGE / 'README.md').read_text())
        self.assertEqual((ROOT / 'backend/mcp_server.py').read_bytes(),
                         (PACKAGE / 'mcp_server.py').read_bytes())

    def test_license_files_cover_mcp_server_only(self):
        # Glama rejects servers without a non-empty LICENSE in the project or a parent directory.
        package_license = (PACKAGE / 'LICENSE')
        root_license = (ROOT / 'LICENSE')
        self.assertTrue(package_license.is_file(), 'MCP package needs its own MIT LICENSE file')
        self.assertTrue(root_license.is_file(), 'Repository root needs a LICENSE file')
        package_text = package_license.read_text()
        root_text = root_license.read_text()
        self.assertTrue(package_text.startswith('MIT License\n'))
        self.assertIn('Copyright (c) 2026 GoHireHumans', package_text)
        mit_body = package_text[package_text.index('Permission is hereby granted'):]
        self.assertIn('SOFTWARE.', mit_body)
        self.assertTrue(root_text.endswith(mit_body), 'Root LICENSE must carry the identical MIT text')
        # The root file scopes MIT to the MCP server; everything else stays proprietary.
        self.assertIn('backend/mcp-package/', root_text)
        self.assertIn('backend/mcp_server.py', root_text)
        self.assertIn('proprietary', root_text)
        self.assertIn('No license is granted', root_text)
        project = tomllib.loads((PACKAGE / 'pyproject.toml').read_text())
        self.assertEqual(project['project']['license'], 'MIT')

    def test_root_readme_leads_with_mcp_section(self):
        # Glama renders the repo's root README as the listing overview, so it must open with the MCP server.
        readme = (ROOT / 'README.md').read_text()
        install = "uvx --from 'git+https://github.com/profilesearch/GohireHumans#subdirectory=backend/mcp-package' gohirehumans-mcp"
        first_h2 = readme.index('\n## ')
        self.assertTrue(readme.startswith('# GoHireHumans\n'))
        self.assertEqual(readme[first_h2 + 1:].split('\n', 1)[0], '## For AI agents (MCP server)')
        mcp = readme[first_h2:readme.index('\n## ', first_h2 + 1)]
        self.assertIn(install, mcp)
        self.assertIn('Python 3.9+', mcp)
        self.assertNotIn('Python 3.8', readme)
        source = (PACKAGE / 'mcp_server.py').read_text()
        tools = set(re.findall(r'"name":\s*"([a-z_]+)",\s*"description"', source))
        self.assertEqual(len(tools), 13)
        for tool in tools:
            self.assertIn(f'`{tool}`', mcp)
        for fact in ("account owner's authorization", 'Workers receive the listed payout',
                     'Stripe processing plus a 1% GoHireHumans fee where checkout is configured',
                     'not an escrow provider', 'backend/mcp-package/LICENSE'):
            self.assertIn(fact, mcp)
        # The deployment guide stays in the file, after the MCP section.
        self.assertGreater(readme.index('## Deployment guide'), first_h2)
        self.assertIn('## Architecture', readme)
        self.assertIn('Deploy to Railway', readme)
        # Package README offers the same one-command install.
        self.assertIn(install, (PACKAGE / 'README.md').read_text())

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
