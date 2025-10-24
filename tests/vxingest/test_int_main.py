

import os
import sys

import pytest

from vxingest.main import run_ingest


@pytest.mark.integration
def test_one_thread_specify_file_pattern(tmp_path):
    # Save original sys.argv
    original_argv = sys.argv.copy()
    job_id = "JS-TEST:METAR:OBS:NETCDF:schedule:job:V01"
    # need these args
    sys.argv = [
        "run_ingest",
        "-j", job_id,
        "-c", os.environ["CREDENTIALS"],
        "-m", str(tmp_path / "metrics"),
        "-o", str(tmp_path / "output"),
        "-x", str(tmp_path / "transfer"),
        "-l", str(tmp_path / "logs"),
        "-f", "[0123456789]???????_[0123456789]???",
        "-t", "1"
    ]
    try:
        result = run_ingest()
    finally:
        # Restore original sys.argv
        sys.argv = original_argv

    # Assert the expected outcome
    assert result == "Expected outcome"