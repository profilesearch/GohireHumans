"""Keep first-party contact surfaces on the accessible support inbox."""
import html
from pathlib import Path
import subprocess
import unittest
from urllib.parse import unquote


ROOT = Path(__file__).resolve().parents[1]
# Assemble the retired identity so the regression does not publish it itself.
RETIRED_ADDRESS = "contact" + "@" + "gohirehumans.com"
SUPPORT_ADDRESS = "gohirehumans.operations@agentmail.to"
EXCLUDED_PARTS = {
    "node_modules", "vendor", ".venv", "venv", "__pycache__",
    "reports", "playwright-report", "test-results",
}


class ContactAddressMigrationTests(unittest.TestCase):
    def test_tracked_first_party_text_has_no_retired_contact_address(self):
        # Git enumerates only tracked files: never recurse into dependencies,
        # local reports, or symlinked directories, including in CI discovery.
        tracked = subprocess.check_output(
            ["git", "ls-files", "-z"], cwd=ROOT
        ).decode("utf-8").split("\0")
        self.assertTrue(any(tracked), "The tracked-file scan must not be empty")
        for name in filter(None, tracked):
            path = ROOT / name
            if EXCLUDED_PARTS.intersection(Path(name).parts) or path.is_symlink():
                continue
            try:
                source = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            with self.subTest(path=name):
                self.assertNotIn(
                    RETIRED_ADDRESS,
                    unquote(html.unescape(source)).lower(),
                    f"Retired support address remains in {name}",
                )

    def test_security_and_press_contacts_use_the_accessible_address(self):
        for name in ("frontend/.well-known/security.txt", "frontend/press.html"):
            source = (ROOT / name).read_text(encoding="utf-8")
            with self.subTest(path=name):
                self.assertIn("mailto:" + SUPPORT_ADDRESS, source)
                self.assertNotIn("@" + "gohirehumans.com", source)

    def test_public_and_mcp_support_surfaces_keep_the_accessible_address(self):
        surfaces = {
            "frontend/index.html": "mailto:" + SUPPORT_ADDRESS,
            "frontend/partials/public-footer.html": "mailto:" + SUPPORT_ADDRESS,
            "frontend/about.html": "mailto:" + SUPPORT_ADDRESS,
            "frontend/agent-onboarding.html": "mailto:" + SUPPORT_ADDRESS,
            "frontend/.well-known/ai-plugin.json": SUPPORT_ADDRESS,
            "backend/mcp_server.py": "Email: " + SUPPORT_ADDRESS,
            "backend/mcp-package/mcp_server.py": "Email: " + SUPPORT_ADDRESS,
            "backend/mcp-package/README.md": "Email: " + SUPPORT_ADDRESS,
            "backend/mcp-package/package.json": '"email": "' + SUPPORT_ADDRESS + '"',
        }
        for name, expected in surfaces.items():
            with self.subTest(path=name):
                self.assertIn(expected, (ROOT / name).read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
