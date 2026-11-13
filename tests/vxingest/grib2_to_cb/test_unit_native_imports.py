import subprocess
import sys

import pytest


@pytest.mark.parametrize("modules", ["eccodes, pyproj", "pyproj, eccodes"])
def test_native_imports_exit_cleanly(modules):
    """Native library cleanup must not abort after otherwise successful tests."""
    result = subprocess.run(
        [sys.executable, "-X", "faulthandler", "-c", f"import {modules}"],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert result.returncode == 0, result.stderr
