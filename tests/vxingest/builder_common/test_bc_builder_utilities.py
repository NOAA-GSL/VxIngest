# pylint: disable=missing-module-docstring
import pytest
from vxingest.builder_common.builder_utilities import convert_to_iso
from vxingest.builder_common.builder_utilities import get_geo_index
from vxingest.builder_common.builder_utilities import truncate_round

def test_get_geo_index():
    """test get_geo_index boundaries"""
    geo = [
        {"firstTime": 100, "lastTime": 200},
        {"firstTime": 300, "lastTime": 400},
        {"firstTime": 500, "lastTime": 600},
    ]
    assert get_geo_index(150, geo) == 0
    assert get_geo_index(350, geo) == 1
    assert get_geo_index(550, geo) == 2
    assert get_geo_index(50, geo) == 2
    assert get_geo_index(700, geo) == 2
    assert get_geo_index(200, geo) == 0

def test_convert_to_iso():
    """test convert_to_iso function"""
    assert convert_to_iso(1627893600) == "2021-08-02T08:40:00Z"
    assert convert_to_iso(1627897200) == "2021-08-02T09:40:00Z"
    assert convert_to_iso(1627900800) == "2021-08-02T10:40:00Z"
    assert convert_to_iso(1627904400) == "2021-08-02T11:40:00Z"
    assert convert_to_iso(1627908000) == "2021-08-02T12:40:00Z"
    assert convert_to_iso(1627911600) == "2021-08-02T13:40:00Z"
    assert convert_to_iso(1627915200) == "2021-08-02T14:40:00Z"
    assert convert_to_iso(1627918800) == "2021-08-02T15:40:00Z"
    assert convert_to_iso(1627922400) == "2021-08-02T16:40:00Z"
    assert convert_to_iso(1627926000) == "2021-08-02T17:40:00Z"
    assert convert_to_iso(1627929600) == "2021-08-02T18:40:00Z"
    assert convert_to_iso(1627933200) == "2021-08-02T19:40:00Z"
    assert convert_to_iso(1627936800) == "2021-08-02T20:40:00Z"
    assert convert_to_iso(1627940400) == "2021-08-02T21:40:00Z"
    assert convert_to_iso(1627944000) == "2021-08-02T22:40:00Z"
    assert convert_to_iso(1627947600) == "2021-08-02T23:40:00Z"
    assert convert_to_iso(1627951200) == "2021-08-03T00:40:00Z"
    assert convert_to_iso(1627954800) == "2021-08-03T01:40:00Z"
    assert convert_to_iso(1627958400) == "2021-08-03T02:40:00Z"
    assert convert_to_iso(1627962000) == "2021-08-03T03:40:00Z"
    assert convert_to_iso(1627965600) == "2021-08-03T04:40:00Z"
    assert convert_to_iso(1627969200) == "2021-08-03T05:40:00Z"
    assert convert_to_iso(1627972800) == "2021-08-03T06:40:00Z"
    assert convert_to_iso(1627976400) == "2021-08-03T07:40:00Z"
    assert convert_to_iso("1627976400") == "2021-08-03T07:40:00Z"
    assert convert_to_iso(1627976400.123) == "2021-08-03T07:40:00Z"
    with pytest.raises(ValueError):
        convert_to_iso("1627976400.123")
    with pytest.raises(ValueError):
        v = convert_to_iso("not_an_epoch")
        assert v == "1970-00-00T00:00:00Z"

def test_truncate_round():
    """test truncate_round function"""
    assert truncate_round(3.14159, 3) == 3.141
    assert truncate_round(3.14159, 2) == 3.14
    assert truncate_round(3.14159, 1) == 3.1
    assert truncate_round(3.14159, 0) == 3.0
    assert truncate_round(1234.5678, 3) == 1234.567
    assert truncate_round(1234.5678, 2) == 1234.56
    assert truncate_round(1234.5678, 1) == 1234.5
    assert truncate_round(1234.5678, 0) == 1234.0
    assert truncate_round(0.12345, 3) == 0.123
    assert truncate_round(0.12345, 2) == 0.12
    assert truncate_round(0.12345, 1) == 0.1
    assert truncate_round(0.12345, 0) == 0.0