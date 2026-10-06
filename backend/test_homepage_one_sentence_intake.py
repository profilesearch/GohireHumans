"""Static contracts for the homepage-only, one-sentence task starter."""
import re
import unittest
from html.parser import HTMLParser
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class Element:
    def __init__(self, tag, attrs=(), parent=None):
        self.tag, self.attrs, self.parent = tag, dict(attrs), parent
        self.children, self.text = [], ""

    def descendants(self):
        for child in self.children:
            yield child
            yield from child.descendants()


class IntakeParser(HTMLParser):
    def __init__(self, source):
        super().__init__()
        self.root = Element("root")
        self.current = self.root
        self.feed(source)

    def handle_starttag(self, tag, attrs):
        node = Element(tag, attrs, self.current)
        self.current.children.append(node)
        if tag not in {"input", "br", "hr", "img", "meta", "link"}:
            self.current = node

    def handle_endtag(self, tag):
        node = self.current
        while node.parent:
            if node.tag == tag:
                self.current = node.parent
                return
            node = node.parent

    def handle_data(self, data):
        self.current.text += data


class HomepageOneSentenceIntakeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = (ROOT / "frontend/index.html").read_text()
        cls.css = (ROOT / "frontend/style.css").read_text()
        landing = cls.html.split('id="guided-task-intake"', 1)[1].split('data-home-section="start"', 1)[0]
        cls.nodes = list(IntakeParser('<div id="guided-task-intake"' + landing).root.descendants())

    def by_id(self, identifier):
        return next(node for node in self.nodes if node.attrs.get("id") == identifier)

    def function(self, name, next_name):
        self.assertIn(f"function {name}", self.html, msg=f"Missing helper: {name}")
        return self.html.split(f"function {name}", 1)[1].split(f"function {next_name}", 1)[0]

    def test_one_sentence_label_and_retired_copy(self):
        label = next(node for node in self.nodes if node.attrs.get("for") == "guided-task-need")
        self.assertEqual(label.text, "What do you need done?")
        self.assertIn("Build a task draft", self.html)
        self.assertNotIn("Answer four short prompts", self.html)
        self.assertEqual(self.by_id("guided-task-need").attrs["onfocus"], "markGuidedTaskIntakeStart('what_needs_to_be_done')")

    def test_optional_fields_are_inside_closed_native_details(self):
        details = next((node for node in self.nodes if node.tag == "details"), None)
        self.assertIsNotNone(details)
        assert details is not None
        self.assertEqual(details.attrs["class"], "lp-guided-field-wide lp-guided-optional")
        self.assertNotIn("open", details.attrs)
        self.assertEqual(details.children[0].tag, "summary")
        self.assertEqual(details.children[0].text, "Add details (optional)")
        optional = {node.attrs.get("id") for node in details.descendants() if node.tag == "input"}
        self.assertEqual(optional, {"guided-task-helper", "guided-task-deliverable", "guided-task-budget"})
        for identifier, handler in [("helper", "human_or_agent_needed"), ("deliverable", "suggested_deliverable"), ("budget", "suggested_budget_range")]:
            field = self.by_id("guided-task-" + identifier)
            self.assertEqual(field.attrs["onfocus"], f"markGuidedTaskIntakeStart('{handler}')")
            self.assertNotIn("required", field.attrs)

    def test_button_directly_follows_primary_group_before_details(self):
        group = self.by_id("guided-task-need").parent
        siblings = group.parent.children
        button = siblings[siblings.index(group) + 1]
        self.assertEqual(button.tag, "button")
        self.assertEqual(button.text, "Build my draft")
        self.assertIn("first_task_wizard_review_draft_click", button.attrs["onclick"])
        self.assertIn("createGuidedTaskDraft()", button.attrs["onclick"])
        self.assertEqual(siblings[siblings.index(button) + 1].tag, "details")

    def test_inline_error_is_accessibly_wired_and_clears_on_input(self):
        field = self.by_id("guided-task-need")
        self.assertEqual(field.attrs.get("aria-describedby"), "guided-task-error")
        self.assertEqual(field.attrs.get("aria-invalid"), "false")
        self.assertEqual(field.attrs.get("oninput"), "handleGuidedTaskNeedInput(this)")
        error = self.by_id("guided-task-error")
        self.assertEqual(error.attrs["role"], "alert")
        self.assertIn("hidden", error.attrs)
        self.assertIs(error.parent, field.parent)
        handler = self.function("handleGuidedTaskNeedInput", "parseFixedUsdDraftBudget")
        self.assertIn("setGuidedTaskIntakeError()", handler)
        setter = self.function("setGuidedTaskIntakeError", "handleGuidedTaskNeedInput")
        self.assertIn("'aria-invalid'", setter)
        self.assertIn("error.hidden", setter)

    def test_empty_guard_returns_before_storage_and_navigation(self):
        draft = self.function("createGuidedTaskDraft", "getStoredGuidedTaskDraft")
        guard = re.search(r"if \(!hasMeaningfulDraft\) \{(.*?)\n  \}", draft, re.S)
        self.assertIsNotNone(guard)
        assert guard is not None
        self.assertIn("const hasMeaningfulDraft = Boolean(task || deliverable)", draft)
        self.assertIn("setGuidedTaskIntakeError(", guard[1])
        self.assertIn(".focus()", guard[1])
        self.assertIn("return;", guard[1])
        self.assertIn("trackEvent('first_task_draft_empty_blocked', { source: 'homepage_guided_task_intake' })", guard[1])
        self.assertLess(guard.end(), draft.index("sessionStorage.setItem('ghh_guided_task_draft'"))
        self.assertLess(guard.end(), draft.index("navigate('#/post-job'"))
        self.assertNotIn("first_task_blank_form_opened", draft)

    def test_need_entered_event_is_deduped_without_user_text(self):
        handler = self.function("handleGuidedTaskNeedInput", "parseFixedUsdDraftBudget")
        self.assertIn("!input.value.trim()", handler)
        self.assertIn("firstTaskNeedEntered", handler)
        self.assertIn("firstTaskNeedEntered = true", handler)
        self.assertIn("sessionStorage.getItem(key)", handler)
        self.assertIn("sessionStorage.setItem(key, '1')", handler)
        self.assertIn("ghh_first_task_need_entered", handler)
        payload = re.search(r"trackEvent\('first_task_need_entered',\s*(\{[^}]*\})\)", handler)
        self.assertIsNotNone(payload)
        assert payload is not None
        self.assertEqual(re.sub(r"\s+", "", payload[1]), "{source:'homepage_guided_task_intake',has_task:true}")

    def test_other_blank_and_template_routes_are_kept(self):
        starter = self.function("startTaskDraft", "markGuidedTaskIntakeStart")
        self.assertIn("const suffix = templateKey ? '?template=' + encodeURIComponent(templateKey) : ''", starter)
        self.assertIn("navigate('#/post-job' + suffix)", starter)
        self.assertIn("if (!templateKey) trackEvent('first_task_blank_form_opened', { source })", starter)

    def test_optional_toggle_and_mobile_grid_styles(self):
        self.assertIn(".lp-guided-field-wide { grid-column: 1 / -1; }", self.css)
        self.assertRegex(self.css, r"\.lp-guided-optional summary\s*\{[^}]*cursor: pointer")
        self.assertIn(".lp-guided-optional summary:focus-visible", self.css)
        self.assertIn(".lp-guided-optional > .lp-guided-fields", self.css)
        mobile = self.css.split("@media (max-width: 640px)", 1)[1]
        self.assertIn(".lp-guided-fields", mobile)
        self.assertIn("grid-template-columns: minmax(0, 1fr)", mobile)
        self.assertIn(".lp-guided-fields .btn { justify-self: stretch; }", mobile)


if __name__ == "__main__":
    unittest.main()
