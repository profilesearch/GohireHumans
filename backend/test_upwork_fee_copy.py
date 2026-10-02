"""Guard current Upwork fee ranges across public copy and agent resources."""
import html
import json
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[1]


class UpworkFeeCopyTests(unittest.TestCase):
    def test_upwork_fee_rows_and_prose_do_not_restore_retired_rates(self):
        failures = []
        for folder in ("frontend", "backend", "scripts"):
            for path in (ROOT / folder).rglob("*"):
                if path.is_symlink() or "node_modules" in path.parts or "tests" in path.parts or path.name.startswith("test"):
                    continue
                if path.suffix not in {".html", ".xml", ".txt", ".json", ".md", ".py", ".js"}:
                    continue
                source = path.read_text(encoding="utf-8", errors="ignore")
                if "upwork" not in source.lower():
                    continue
                # Inspect complete table cells, including rates on a separate line.
                for row in re.findall(r"<tr\b[^>]*>.*?</tr>", source, re.S):
                    cells = [html.unescape(re.sub(r"<[^>]+>", "", c)).strip() for c in re.findall(r"<t[dh]\b[^>]*>(.*?)</t[dh]>", row, re.S)]
                    if cells and cells[0].replace("Hire via ", "") == "Upwork":
                        for cell in cells[1:]:
                            if cell in {"10%", "5%", "10% freelancer fee", "5% client fee", "5% marketplace fee", "10% + 5% client", "5% + 10%"} or "$1,050" in cell:
                                failures.append(f"{path.relative_to(ROOT)}: retired Upwork cell {cell}")
                text = html.unescape(re.sub(r"<[^>]+>", " ", source))
                text = re.sub(r"\s+", " ", text)
                for pattern in [
                    r"Upwork.{0,80}?\b10% (?:freelancer|service) fee",
                    r"Upwork.{0,80}?\b5% (?:client|marketplace) fee",
                    r"Upwork(?:’s|'s)?(?: uses a)? (?:20% fee|20% on the first|sliding (?:fee|scale))|Upwork \(20% fee\)",
                    r"Upwork.{0,80}?18\.5%",
                    r"Fees range from 10% to 20% depending on your billing history",
                    r"per-client fee drops over time",
                ]:
                    if re.search(pattern, text, re.I):
                        failures.append(f"{path.relative_to(ROOT)}: {pattern}")
        self.assertEqual(failures, [])

    def test_mcp_copies_keep_identical_current_upwork_fee_row(self):
        expected = "| Upwork | Up to 7.99% client fee (Basic) | 0–15% freelancer fee per contract |"
        for rel in ("backend/mcp_server.py", "backend/mcp-package/mcp_server.py"):
            with self.subTest(rel=rel):
                self.assertIn(expected, (ROOT / rel).read_text())

    def test_corrected_switching_post_preserves_checkout_math_and_contract_fee_qualifier(self):
        source = (ROOT / "frontend/blog/freelancers-switching-lower-fee-platforms.html").read_text()
        self.assertIn("(about $88.40 vs $105.50)", source)
        self.assertNotIn("1% + Stripe", source)
        self.assertNotIn("1% plus Stripe processing", source)
        self.assertIn("separate one-time contract initiation fee ($0.99 to $14.99) and any taxes", source)
        self.assertIn("fee now varies from 0% to 15% per contract", source)

    def test_upwork_dollar_models_disclose_separate_contract_initiation_fee(self):
        for rel in (
            "frontend/tools/freelance-fee-calculator.html",
            "frontend/tools/fee-calculator.html",
            "frontend/tools/are-you-overpaying.html",
            "frontend/vs/upwork.html",
            "frontend/blog/alternatives-to-toptal.html",
            "frontend/blog/fiverr-vs-upwork-vs-gohirehumans.html",
            "frontend/blog/freelance-vs-full-time-2026.html",
            "frontend/blog/gig-economy-statistics-2026.html",
            "frontend/blog/gohirehumans-vs-fiverr.html",
        ):
            with self.subTest(rel=rel):
                source = (ROOT / rel).read_text().lower()
                self.assertIn("contract initiation fee", source, rel)
                self.assertIn("$0.99", source, rel)
                self.assertIn("$14.99", source, rel)

    def test_upwork_worked_examples_separate_seller_net_from_buyer_fees(self):
        source = (ROOT / "frontend/blog/fiverr-vs-upwork-vs-gohirehumans.html").read_text()
        rows = re.findall(r"<tr>\s*<td>Upwork</td>(.*?)</tr>", source, re.S)
        self.assertEqual(len(rows), 2)
        self.assertIn("$85&ndash;$100", rows[0])
        self.assertIn("Up to $22.99", rows[0])
        self.assertIn("$0&ndash;$75", rows[1])
        self.assertIn("Up to $39.95", rows[1])
        self.assertIn("Up to $114.95", rows[1])
        self.assertIn("$425&ndash;$500", rows[1])


if __name__ == "__main__":
    unittest.main()
