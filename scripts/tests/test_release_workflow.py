from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[2]


class ReleaseWorkflowTests(unittest.TestCase):
    def test_ci_matches_production_python_and_exercises_image_without_new_job(self):
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        dockerfile = (ROOT / 'backend/Dockerfile').read_text()
        runtime = re.search(r'FROM python:(\d+\.\d+)', dockerfile).group(1)
        versions = re.findall(r"python-version: '([^']+)'", workflow)
        self.assertEqual(versions, [runtime, runtime])
        self.assertIn('python -m unittest discover -s scripts/tests -v', workflow)
        self.assertIn('python scripts/production_smoke.py --sha "${{ github.sha }}"', workflow)
        backend = workflow.split('  backend-tests:', 1)[1].split('  static-frontend-checks:', 1)[0]
        self.assertIn('python -m unittest discover -s scripts/tests -v', backend)
        self.assertIn('python scripts/production_smoke.py --sha', backend)
        self.assertTrue((ROOT / 'scripts/release_gates.md').is_file())
        self.assertEqual(re.findall(r'^  ([\w-]+):$', workflow, re.M)[2:],
                         ['backend-tests', 'static-frontend-checks', 'browser-regression-checks'])
        self.assertNotIn('permissions:', workflow)
        self.assertNotIn('continue-on-error:', workflow)
        self.assertNotIn('cancel-in-progress:', workflow)


if __name__ == '__main__':
    unittest.main()
