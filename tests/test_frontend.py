import shutil
import subprocess
from pathlib import Path

import pytest


def test_browser_javascript_regressions():
    node = shutil.which("node")
    if node is None:
        pytest.skip("Install Node.js 22+ to run the frontend regression suite (required in CI).")
    result = subprocess.run([node, "--test", str(Path(__file__).with_name("ui.test.cjs"))],
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
