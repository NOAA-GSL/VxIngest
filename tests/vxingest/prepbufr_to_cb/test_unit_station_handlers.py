import copy
import logging
from unittest.mock import Mock

import pytest

from vxingest.prepbufr_to_cb.prepbufr_builder import PrepbufrRaobsObsBuilderV01


@pytest.fixture
def builder():
    instance = object.__new__(PrepbufrRaobsObsBuilderV01)
    instance.raw_obs_data = {
        "40375": {
            120: {
                "fcst_valid_epoch": 200,
                "header": {"elevation": 100, "lat": 30, "lon": 40},
            },
            220: {},
        }
    }
    instance.interpolated_data = {"40375": {120: {"data": {"pressure": {1010: 1010}}}}}
    instance.stations = [
        {
            "id": "station:40375",
            "wmoid": "40375",
            "geo": [
                {"lat": 30, "lon": 40, "elev": 100, "firstTime": 100, "lastTime": 150}
            ],
        }
    ]
    instance.document_map = {}
    instance.print_debug_station_report = False
    return instance


@pytest.mark.parametrize("missing_elevation", ["header", "geo"])
def test_missing_elevation_preserves_station_metadata(builder, missing_elevation):
    if missing_elevation == "header":
        builder.raw_obs_data["40375"][120]["header"]["elevation"] = None
    else:
        builder.stations[0]["geo"][0]["elev"] = None
    original_stations = copy.deepcopy(builder.stations)

    assert builder.handle_station({"stationName": "40375", "level": 1010}) == "40375"
    assert builder.stations == original_stations
    assert builder.document_map == {}


def test_matching_station_updates_time_range(builder):
    assert builder.handle_station({"stationName": "40375", "level": 1010}) == "40375"
    assert builder.stations[0]["geo"][0]["lastTime"] == 200
    assert builder.document_map["station:40375"] == builder.stations[0]


def test_named_function_preserves_original_exception(builder, monkeypatch):
    def failing_handler(params):
        raise TypeError("original handler failure")

    monkeypatch.setattr(builder, "handle_station", failing_handler)
    with pytest.raises(TypeError, match="original handler failure"):
        builder.handle_named_function("40375", 1010, "&handle_station|*stationName")


def test_station_error_logging_includes_exception(builder, caplog):
    builder.raw_obs_data = {}
    with caplog.at_level(logging.ERROR), pytest.raises(KeyError, match="40375"):
        builder.handle_station({"stationName": "40375", "level": 1010})
    assert caplog.records[-1].exc_info is not None
    assert "40375" in caplog.text


@pytest.fixture
def cached_builder():
    return PrepbufrRaobsObsBuilderV01(
        load_spec={
            "cluster": Mock(),
            "cb_connection": {
                "bucket": "vxdata",
                "scope": "_default",
                "collection": "RAOB",
            },
        },
        ingest_document={"template": {"subset": "RAOB"}, "mnemonic_mapping": {}},
    )


@pytest.mark.parametrize("stations", [[], [{"wmoid": "40375", "geo": []}]])
def test_station_metadata_is_loaded_once(cached_builder, stations):
    cluster = cached_builder.load_spec["cluster"]
    cluster.query.return_value = iter(stations)

    first = cached_builder._get_station_metadata()
    second = cached_builder._get_station_metadata()

    assert first == stations
    assert second is first
    assert cached_builder.stations is first
    cluster.query.assert_called_once()
    statement = cluster.query.call_args.args[0]
    assert "SELECT RAOB.*" in statement
    assert "`vxdata`._default.RAOB" in statement
    assert "subset = 'RAOB'" in statement


def test_station_metadata_query_failure_can_be_retried(cached_builder):
    cluster = cached_builder.load_spec["cluster"]
    cluster.query.side_effect = [RuntimeError("query failed"), iter([])]

    with pytest.raises(RuntimeError, match="query failed"):
        cached_builder._get_station_metadata()
    assert cached_builder.stations is None
    assert cached_builder._get_station_metadata() == []
    assert cluster.query.call_count == 2


def test_station_metadata_cache_preserves_handler_updates(builder):
    builder.load_spec = {"cluster": Mock()}

    builder.handle_station({"stationName": "40375", "level": 1010})
    stations = builder._get_station_metadata()

    assert stations[0]["geo"][0]["lastTime"] == 200
    assert stations[0] is builder.document_map["station:40375"]
    builder.load_spec["cluster"].query.assert_not_called()
