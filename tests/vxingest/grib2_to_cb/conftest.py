from pathlib import Path
from types import SimpleNamespace

import pytest

from . import grib_utils


@pytest.fixture(scope="session")
def synthetic_grib2(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Generate a multi-message GRIB2 file with all level types
    that VxIngest reads in ``build_document()``. Use a session
    fixture scope as writting GRIB2 files is expensive.

    Returns the path to the generated file.
    """
    grib_path = tmp_path_factory.mktemp("grib") / "synthetic.grib2"
    with grib_path.open("wb") as fh:
        for type_of_level, level, short_name in grib_utils.MESSAGES:
            grib_utils.write_grib2_message(fh, type_of_level, level, short_name)
    return grib_path


@pytest.fixture(scope="session")
def grib_constants():
    """Allow tests access to constants defined in grib_utils via a fixture"""
    return SimpleNamespace(
        DATA_DATE=grib_utils.DATA_DATE,
        DATA_TIME=grib_utils.DATA_TIME,
        FILL_VALUE=grib_utils.FILL_VALUE,
        MESSAGES=grib_utils.MESSAGES,
        GRID_PARAMS=grib_utils.GRID_PARAMS,
        STEP_HOURS=grib_utils.STEP_HOURS,
    )
