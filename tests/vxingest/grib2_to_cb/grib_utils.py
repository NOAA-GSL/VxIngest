"""Fixtures for grib2_to_cb unit tests.

Provides a synthetic GRIB2 file containing all the message types
that VxIngest opens in grib_builder_parent.build_document(), generated
via the eccodes Python API.  No external data files or Couchbase
connection required.
"""


import eccodes
import numpy as np

# ---------------------------------------------------------------------------
# Lambert Conformal Conic grid parameters (mimics a small HRRR-like domain)
# ---------------------------------------------------------------------------
GRID_PARAMS: dict[str, int | float] = {
    "Nx": 4,
    "Ny": 4,
    "DxInMetres": 3000,
    "DyInMetres": 3000,
    "LaDInDegrees": 38.5,
    "LoVInDegrees": 262.5,
    "Latin1InDegrees": 38.5,
    "Latin2InDegrees": 38.5,
    "latitudeOfFirstGridPointInDegrees": 21.138,
    "longitudeOfFirstGridPointInDegrees": 237.281,
}

# Reference date/time embedded in every message
DATA_DATE = 20240101
DATA_TIME = 0  # 00Z
STEP_HOURS = 6

# Fill value written into every message (as float64)
FILL_VALUE = 280.0


def write_grib2_message(
    file_handle,
    type_of_level: str,
    level: int,
    short_name: str,
    *,
    step_type: str = "instant",
) -> None:
    """Write a single GRIB2 message to an open binary file handle."""
    msgid = eccodes.codes_grib_new_from_samples("GRIB2")
    try:
        # NCEP centre — required for cloudCeiling and mslet concepts
        eccodes.codes_set(msgid, "centre", "kwbc")

        # Lambert Conformal Conic grid
        eccodes.codes_set(msgid, "gridType", "lambert")
        for key, value in GRID_PARAMS.items():
            eccodes.codes_set(msgid, key, value)

        # Variable identity
        eccodes.codes_set(msgid, "typeOfLevel", type_of_level)
        eccodes.codes_set(msgid, "level", level)
        eccodes.codes_set(msgid, "stepType", step_type)
        eccodes.codes_set(msgid, "shortName", short_name)

        # Forecast reference time and step
        eccodes.codes_set(msgid, "dataDate", DATA_DATE)
        eccodes.codes_set(msgid, "dataTime", DATA_TIME)
        eccodes.codes_set(msgid, "stepUnits", 1)  # hours
        eccodes.codes_set(msgid, "stepRange", str(STEP_HOURS))

        # Grid values
        n_points = GRID_PARAMS["Nx"] * GRID_PARAMS["Ny"]
        eccodes.codes_set_values(
            msgid, np.full(n_points, FILL_VALUE, dtype=np.float64)
        )

        eccodes.codes_write(msgid, file_handle)
    finally:
        eccodes.codes_release(msgid)


# Each tuple: (typeOfLevel, level, shortName)
# Mirrors the five xr.open_dataset() calls in grib_builder_parent.build_document
MESSAGES: list[tuple[str, int, str]] = [
    # heightAboveGround level 2 — temperature, dewpoint, RH, specific humidity
    ("heightAboveGround", 2, "2t"),
    ("heightAboveGround", 2, "2d"),
    ("heightAboveGround", 2, "r"),
    ("heightAboveGround", 2, "q"),
    # heightAboveGround level 10 — U/V wind components
    ("heightAboveGround", 10, "10u"),
    ("heightAboveGround", 10, "10v"),
    # surface — pressure, orography, visibility, vegetation
    ("surface", 0, "sp"),
    ("surface", 0, "orog"),
    ("surface", 0, "vis"),
    ("surface", 0, "veg"),
    # cloudCeiling
    ("cloudCeiling", 0, "ceil"),
    # meanSea — MSLP
    ("meanSea", 0, "mslet"),
]
