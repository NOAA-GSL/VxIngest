"""
_test for VxIngest backfill_obs_with_rh.py
"""

import math

from vxingest.utilities.backfill_obs_with_rh import calc_components


def test_calc_components_backfills_rh():
    """test calculating missing RH"""
    doc = {
        "data": {
            "NZCM": {
                "Temperature": 25,
                "DewPoint": 20,
                "WS": 5,
                "WD": 180,
                "WindU": -6.123233995736766e-16,
                "WindV": 5,
            },
            "SUMU": {
                "Temperature": 30,
                "DewPoint": 15,
                "WS": 10,
                "WD": 270,
                "WindU": 10,
                "WindV": 1.8369701987210296e-15,
            },
        }
    }
    for entries in doc["data"]:
        assert "RH" not in entries
    calc_components(doc)
    assert math.isclose(
        doc["data"]["NZCM"]["RH"], 81.008, abs_tol=0.001
    ), "RH wrong value - not within 0.001"
    assert math.isclose(
        doc["data"]["NZCM"]["WindU"], -6.123233995736766e-16, abs_tol=0.001
    ), "WindU wrong value - not within 0.001"
    assert doc["data"]["NZCM"]["WindV"] == 5.0, "WindV wrong value"
    assert doc["data"]["NZCM"]["Temperature"] == 25, "temperature wrong value"
    assert doc["data"]["NZCM"]["DewPoint"] == 20, "DewPoint wrong value"
    assert doc["data"]["NZCM"]["WS"] == 5, "WS wrong value"
    assert doc["data"]["NZCM"]["WD"] == 180, "WD wrong value"

    assert math.isclose(
        doc["data"]["SUMU"]["RH"], 53.152, abs_tol=0.001
    ), "RH wrong value - not within 0.001"
    assert doc["data"]["SUMU"]["WindU"] == 10.0, "WindU wrong value"
    assert math.isclose(
        doc["data"]["SUMU"]["WindV"], 1.8369701987210296e-15, abs_tol=0.001
    ), "WindV wrong value - not within 0.001"
    assert doc["data"]["SUMU"]["Temperature"] == 30, "temperature wrong value"
    assert doc["data"]["SUMU"]["DewPoint"] == 15, "DewPoint wrong value"
    assert doc["data"]["SUMU"]["WS"] == 10, "WS wrong value"
    assert doc["data"]["SUMU"]["WD"] == 270, "WD wrong value"


def test_calc_components_backfills_windu_and_windv():
    """test calculating missing WindU and WindV"""
    doc = {
        "data": {
            "NZCM": {
                "Temperature": 25,
                "DewPoint": 20,
                "WS": 5,
                "WD": 180,
                "RH": 81.008,
            },
            "SUMU": {
                "Temperature": 30,
                "DewPoint": 15,
                "WS": 10,
                "WD": 270,
                "RH": 53.152,
            },
        }
    }
    calc_components(doc)
    assert math.isclose(
        doc["data"]["NZCM"]["RH"], 81.008, abs_tol=0.001
    ), "RH wrong value - not within 0.001"
    assert math.isclose(
        doc["data"]["NZCM"]["WindU"], -6.123233995736766e-16, abs_tol=0.001
    ), "WindU wrong value - not within 0.001"
    assert doc["data"]["NZCM"]["WindV"] == 5.0, "WindV wrong value"
    assert doc["data"]["NZCM"]["Temperature"] == 25, "temperature wrong value"
    assert doc["data"]["NZCM"]["DewPoint"] == 20, "DewPoint wrong value"
    assert doc["data"]["NZCM"]["WS"] == 5, "WS wrong value"
    assert doc["data"]["NZCM"]["WD"] == 180, "WD wrong value"

    assert math.isclose(
        doc["data"]["SUMU"]["RH"], 53.152, abs_tol=0.001
    ), "RH wrong value - not within 0.001"
    assert doc["data"]["SUMU"]["WindU"] == 10.0, "WindU wrong value"
    assert math.isclose(
        doc["data"]["SUMU"]["WindV"], 1.8369701987210296e-15, abs_tol=0.001
    ), "WindV wrong value - not within 0.001"
    assert doc["data"]["SUMU"]["Temperature"] == 30, "temperature wrong value"
    assert doc["data"]["SUMU"]["DewPoint"] == 15, "DewPoint wrong value"
    assert doc["data"]["SUMU"]["WS"] == 10, "WS wrong value"
    assert doc["data"]["SUMU"]["WD"] == 270, "WD wrong value"


def test_calc_components_backfills_nochange():
    """test no change if RH, WindU, and WindV are already present"""
    doc = {
        "data": {
            "NZCM": {
                "Temperature": 25,
                "DewPoint": 20,
                "WS": 5,
                "WD": 180,
                "WindU": 5,
                "WindV": -5,
                "RH": 75,
            },
            "SUMU": {
                "Temperature": 30,
                "DewPoint": 15,
                "WS": 10,
                "WD": 270,
                "WindU": 3,
                "WindV": -7,
                "RH": 70,
            },
        }
    }
    calc_components(doc)
    assert doc["data"]["NZCM"]["Temperature"] == 25, "temperature wrong value"
    assert doc["data"]["NZCM"]["DewPoint"] == 20, "DewPoint wrong value"
    assert doc["data"]["NZCM"]["WS"] == 5, "WS wrong value"
    assert doc["data"]["NZCM"]["WD"] == 180, "WD wrong value"
    assert doc["data"]["NZCM"]["WindU"] == 5, "WindU wrong value"
    assert doc["data"]["NZCM"]["WindV"] == -5, "WindV wrong value"
    assert math.isclose(
        doc["data"]["NZCM"]["RH"], 81.008, abs_tol=0.001
    ), "RH wrong value"

    assert doc["data"]["SUMU"]["Temperature"] == 30, "temperature wrong value"
    assert doc["data"]["SUMU"]["DewPoint"] == 15, "DewPoint wrong value"
    assert doc["data"]["SUMU"]["WS"] == 10, "WS wrong value"
    assert doc["data"]["SUMU"]["WD"] == 270, "WD wrong value"
    assert doc["data"]["SUMU"]["WindU"] == 3, "WindU wrong value"
    assert doc["data"]["SUMU"]["WindV"] == -7, "WindV wrong value"
    assert math.isclose(
        doc["data"]["SUMU"]["RH"], 53.152, abs_tol=0.001
    ), "RH wrong value"
