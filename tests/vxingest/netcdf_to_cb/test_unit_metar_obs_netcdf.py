import os
import time
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

import netCDF4 as nc
import numpy as np
import pytest
from couchbase.mutation_state import MutationState
from couchbase.n1ql import QueryScanConsistency
from couchbase.options import QueryOptions

from vxingest.netcdf_to_cb.netcdf_metar_obs_builder import NetcdfMetarObsBuilderV01
from vxingest.netcdf_to_cb.run_ingest_threads import VXIngest

# various unit tests for the obs builder.
# to run one of these from the command line....
# python3 -m pytest -s -v  tests/netcdf_to_cb/test_unit_metar_obs_netcdf.py::TestNetcdfObsBuilderV01Unit::test....


def setup_connection():
    """test setup"""
    _vx_ingest = VXIngest()
    _vx_ingest.credentials_file = os.environ["CREDENTIALS"]
    _vx_ingest.cb_credentials = _vx_ingest.get_credentials(_vx_ingest.load_spec)
    _vx_ingest.connect_cb()
    _vx_ingest.load_spec["ingest_document_ids"] = _vx_ingest.collection.get(
        "JOB:V01:METAR:NETCDF:OBS"
    ).content_as[dict]["ingest_document_ids"]
    return _vx_ingest


@pytest.mark.integration
def test_credentials_and_load_spec():
    """test the get_credentials and load_spec"""
    try:
        vx_ingest = setup_connection()
        assert vx_ingest.load_spec["cb_connection"]["user"], "cb_user"
    except Exception as _e:
        pytest.fail(f"test_credentials_and_load_spec Exception failure: {_e}")
    finally:
        vx_ingest.close_cb()


@pytest.mark.integration
def test_cb_connect_disconnect():
    """test the cb connect and close"""
    try:
        vx_ingest = setup_connection()
        result = vx_ingest.cluster.query("SELECT raw CLOCK_LOCAL() as time")
        local_time = [list(result)[0]]
        assert local_time is not None
        vx_ingest.close_cb()
    except Exception as _e:
        pytest.fail(f"test_cb_connect_disconnect Exception failure: {_e}")
    finally:
        vx_ingest.close_cb()


@pytest.mark.integration
def test_write_load_job_to_files(tmp_path):
    """test write the load job"""
    try:
        vx_ingest = setup_connection()
        vx_ingest.load_job_id = "test_id"
        vx_ingest.output_dir = tmp_path
        vx_ingest.load_spec["load_job_doc"] = {"test": "a line of text"}
        vx_ingest.write_load_job_to_files()
        assert Path(tmp_path / "test_id.json").is_file()
    except Exception as _e:
        pytest.fail(f"test_write_load_job_to_files Exception failure: {_e}")
    finally:
        vx_ingest.close_cb()


@pytest.mark.integration
def test_build_load_job_doc(tmp_path):
    """test the build load job"""
    try:
        vx_ingest = setup_connection()
        vx_ingest.load_job_id = "test_id"
        vx_ingest.path = tmp_path
        vx_ingest.load_spec["load_job_doc"] = {"test": "a line of text"}
        ljd = vx_ingest.build_load_job_doc(vx_ingest.path)
        assert ljd["id"].startswith(
            "LJ:METAR:vxingest.netcdf_to_cb.run_ingest_threads:VXIngest"
        )
    except Exception as _e:
        pytest.fail(f"test_build_load_job_doc Exception failure: {_e}")
    finally:
        vx_ingest.close_cb()


@pytest.mark.integration
def test_retrieve_from_netcdf():
    """test the derive_valid_time_epoch
    requires file_name which should match the format for grib2 hrr_ops files
    i.e. "20210920_1700", and params_dict['file_name_mask'] = "%Y%m%d_%H%M"
    """
    try:
        # first we have to create a netcdf dataset and a temperature variable
        _nc = nc.Dataset(
            "inmemory.nc",
            format="NETCDF3_CLASSIC",
            mode="w",
            memory=1028,
            fill_value=3.402823e38,
        )
        _d = _nc.createDimension("recNum", None)
        _v = _nc.createVariable("temperature", float, ("recNum"))
        _v.units = "kelvin"
        _v.standard_name = "air_temperature"
        _v[0] = 250.15

        vx_ingest = setup_connection()
        _collection = vx_ingest.collection
        load_spec = vx_ingest.load_spec
        ingest_document_ids = vx_ingest.load_spec["ingest_document_ids"]
        ingest_document = _collection.get(ingest_document_ids[0]).content_as[dict]
        builder = NetcdfMetarObsBuilderV01(load_spec, ingest_document)
        builder.file_name = "20210920_1700"
        # assign our temporary in-memory dataset to the builder
        builder.ncdf_data_set = _nc
        # assign our handler parameters
        params_dict = {}
        params_dict["base_var_index"] = 0
        params_dict["temperature"] = "temperature"
        # call the handler
        temp = builder.retrieve_from_netcdf(params_dict)
        assert temp == 250.15
    except Exception as _e:
        pytest.fail(f"test_build_load_job_doc Exception failure: {_e}")
    finally:
        vx_ingest.close_cb()
        _nc.close()  # close returns memoryview


@pytest.mark.integration
def test_vxingest_get_file_list(tmp_path):
    """test the vxingest get_file_list sorting and filtering"""
    try:
        pattern = "%y%j%H%f"
        vx_ingest = setup_connection()
        vx_ingest.load_job_id = "test_id"
        # using a phony document "id": to test the function
        df_record = {
            "dataSourceId": "GSL",
            "fileType": "grib2",
            "id": "DF:metar:grib2:HRRR_OPS:f_fred_01",
            "interpolation": "nearest 4 weighted average",
            "loadJobId": "LJ:__main__:VXIngest:1636575702",
            "model": "HRRR_OPS",
            "mtime": 1636465996,
            "originType": "model",
            "projection": "lambert_conformal_conic",
            "subset": "metar",
            "type": "DF",
            "url": str(tmp_path / "1820013010000"),
        }
        vx_ingest.collection.upsert("DF:metar:grib2:HRRR_OPS:f_fred_01", df_record)
        # make a ".prev" and a ".tmp" directory to see if they get filtered out
        Path(tmp_path / ".prev").mkdir()
        Path(tmp_path / ".tmp").mkdir()
        # put a properly formed file (different name) into the .prev directory
        Path(tmp_path / ".prev/1820017010000").touch()
        # order is important to see if the files are getting returned sorted by mtime
        Path(tmp_path / "1820013010000").touch()
        Path(tmp_path / "1820013020000").touch()
        Path(tmp_path / "1820013040000").touch()
        Path(tmp_path / "1820013050000").touch()
        Path(tmp_path / "1820013030000").touch()
        Path(tmp_path / "1820014010000").touch()
        Path(tmp_path / "1820015010000").touch()
        Path(tmp_path / "1820016010000").touch()

        query = f""" SELECT url, mtime
            From `{vx_ingest.cb_credentials["bucket"]}`.{vx_ingest.cb_credentials["scope"]}.{vx_ingest.cb_credentials["collection"]}
            WHERE
            subset='metar'
            AND type='DF'
            AND fileType='grib2'
            AND originType='model'
            AND model='HRRR_OPS' order by url;"""
        # should get 1820013010000 because the mtime in the DF record is old. The file is newer.
        files = vx_ingest.get_file_list(query, tmp_path, "1820013*", pattern)
        assert set(files) == set(
            [
                str(tmp_path / "1820013010000"),
                str(tmp_path / "1820013020000"),
                str(tmp_path / "1820013040000"),
                str(tmp_path / "1820013050000"),
                str(tmp_path / "1820013030000"),
            ]
        ), "get_file_list 1 wrong list"
        # update the mtime in the df record so that the file will not be included
        df_record["mtime"] = round(time.time())
        vx_ingest.collection.upsert("DF:metar:grib2:HRRR_OPS:f_fred_01", df_record)
        time.sleep(1)
        # do a query with scan consistency set so that we know the record got persisted
        vx_ingest.cluster.query(
            query, QueryOptions(scan_consistency=QueryScanConsistency.REQUEST_PLUS)
        )
        files = vx_ingest.get_file_list(query, tmp_path, "1820013*", pattern)
        # should not get f_fred_01 because the DF record has a newer mtime
        assert set(files) == set(
            [
                str(tmp_path / "1820013020000"),
                str(tmp_path / "1820013040000"),
                str(tmp_path / "1820013050000"),
                str(tmp_path / "1820013030000"),
            ]
        ), "get_file_list 2 wrong list"

    except Exception as _e:
        pytest.fail(f"test_build_load_job_doc Exception failure: {_e}")
    finally:
        vx_ingest.collection.remove("DF:metar:grib2:HRRR_OPS:f_fred_01")
        vx_ingest.close_cb()


@pytest.mark.integration
def test_interpolate_time():
    """test the interpolate time routine in netcdf_builder"""
    vx_ingest = setup_connection()
    _cluster = vx_ingest.cluster
    _collection = vx_ingest.collection
    _load_spec = vx_ingest.load_spec
    _ingest_document_ids = vx_ingest.load_spec["ingest_document_ids"]
    _ingest_document = _collection.get(_ingest_document_ids[0]).content_as[dict]
    _builder = NetcdfMetarObsBuilderV01(_load_spec, _ingest_document)
    for delta in [
        0,
        -1,
        1,
        -1799,
        1799,
        -1800,
        1800,
        -1801,
        1801,
        -3599,
        3599,
        -3600,
        3600,
        -3601,
        3601,
    ]:
        _t = np.array([1636390800 + delta])
        _t.view(np.ma.MaskedArray)
        t_interpolated = _builder.interpolate_time({"timeObs": _t})
        print(
            "for an offset: "
            + str(delta)
            + " results in interpolation: "
            + str(t_interpolated)
        )
        if delta >= -1800 and delta <= 1799:
            assert t_interpolated == 1636390800, (
                f"1636390800 interpolated to {t_interpolated} is not equal"
            )
        if delta <= -1801:
            assert t_interpolated == 1636390800 - 3600, (
                f"{1636390800 - delta} interpolated to {t_interpolated} is not equal"
            )
        if delta >= 1800:
            assert t_interpolated == 1636390800 + 3600, (
                f"{1636390800 - delta} interpolated to {t_interpolated} is not equal"
            )


@pytest.mark.integration
def test_interpolate_time_iso():
    """test the interpolate time routine in netcdf_builder"""
    vx_ingest = setup_connection()
    _cluster = vx_ingest.cluster
    _collection = vx_ingest.collection
    load_spec = vx_ingest.load_spec
    ingest_document_ids = vx_ingest.load_spec["ingest_document_ids"]
    ingest_document = _collection.get(ingest_document_ids[0]).content_as[dict]
    _builder = NetcdfMetarObsBuilderV01(load_spec, ingest_document)
    for delta in [
        0,
        -1,
        1,
        -1799,
        1799,
        -1800,
        1800,
        -1801,
        1801,
        -3599,
        3599,
        -3600,
        3600,
        -3601,
        3601,
    ]:
        _t = np.array([1636390800 + delta])
        _t.view(np.ma.MaskedArray)
        t_interpolated = _builder.interpolate_time_iso({"timeObs": _t})
        if delta >= -1800 and delta <= 1799:
            assert (
                datetime.fromtimestamp(1636390800, timezone.utc).isoformat()
            ) == t_interpolated, (
                f"{1636390800 - delta} interpolated to {t_interpolated} is not equal"
            )
        if delta <= -1801:
            assert (
                datetime.fromtimestamp(1636390800 - 3600, timezone.utc).isoformat()
            ) == t_interpolated, (
                f"{1636390800 - delta} interpolated to {t_interpolated} is not equal"
            )
        if delta >= 1800:
            assert (
                datetime.fromtimestamp(1636390800 + 3600, timezone.utc).isoformat()
                == t_interpolated
            ), f"{1636390800 - delta} interpolated to {t_interpolated} is not equal"


@pytest.mark.integration
def test_handle_station():
    """Tests the ability to add or update a station with these possibilities...
    1) The station is new and there is no station document that yet exists so
    a new station document must be created.
    2) The corresponding station document exists but the location has changed so a
    new geo must be created.
    3) The corresponding station document exists and a matching geo exists for this
    station but the geo time range does not include the document validTime so the
    geo entry must be updated for the new time range.
    """
    try:
        _station_name = "ZBAA"
        vx_ingest = setup_connection()
        _cluster = vx_ingest.cluster
        _collection = vx_ingest.collection
        load_spec = vx_ingest.load_spec
        ingest_document_ids = vx_ingest.load_spec["ingest_document_ids"]
        ingest_document = _collection.get(ingest_document_ids[0]).content_as[dict]
        _builder = NetcdfMetarObsBuilderV01(load_spec, ingest_document)
        _builder.file_name = "20211108_0000"
        _pattern = "%Y%m%d_%H%M"
        # fmask is usually set in the run_ingest_threads
        _builder.load_spec["fmask"] = _pattern
        _builder.ncdf_data_set = nc.Dataset(
            "/opt/data/netcdf_to_cb/input_files/20211108_0000"
        )
        rec_num_length = _builder.ncdf_data_set["stationName"].shape[0]
        # find the rec_num of the stationName ZBAA
        for i in range(rec_num_length):
            if str(nc.chartostring(_builder.ncdf_data_set["stationName"][i])) == "ZBAA":
                break
        _rec_num = i
        # use a station that is in the netcdf file but is not used in any of our domains.
        # like Beijing China ZBAA.
        # first upsert the expected station (because it may have been corrupted by an interupted test)
        _zbaa_doc = {
            "description": "BEIJING/PEKING",
            "docType": "station",
            "geo": [
                {
                    "elev": 30,
                    "firstTime": 1636329600,
                    "lastTime": 1636329600,
                    "lat": 40.06999,
                    "lon": 116.58,
                }
            ],
            "id": "MD:V01:METAR:station:ZBAA",
            "name": "ZBAA",
            "subset": "METAR",
            "type": "MD",
            "updateTime": 1652990720,
            "version": "V01",
        }

        result = _collection.upsert("MD:V01:METAR:station:ZBAA", _zbaa_doc)

        result = _cluster.query(
            " ".join(
                (
                    f"""
            SELECT METAR.*
            From `{vx_ingest.cb_credentials["bucket"]}`.{vx_ingest.cb_credentials["scope"]}.{vx_ingest.cb_credentials["collection"]}
            WHERE type = 'MD'
            AND docType = 'station'
            AND version = 'V01'
            AND name = '"""
                    + _station_name
                    + "'"
                ).split()
            ),
            QueryOptions(scan_consistency=QueryScanConsistency.REQUEST_PLUS),
        )
        result_list = list(result)
        station_zbaa = result_list[0] if len(result_list) > 0 else None
        # keep a copy of station_zbaa around for future use
        station_zbaa_copy = deepcopy(station_zbaa)
        if station_zbaa_copy is not None:
            cleanup_builder_doc(_cluster, _collection, _builder, station_zbaa_copy)

        # ****************
        # 1) new station test
        # remove station station_zbaa from the database
        _ms = remove_station(_cluster, _collection, station_zbaa, _builder)
        result = _cluster.query(
            f"""
            SELECT METAR.*
            From `{vx_ingest.cb_credentials["bucket"]}`.{vx_ingest.cb_credentials["scope"]}.{vx_ingest.cb_credentials["collection"]}
            WHERE type = 'MD'
            AND docType = 'station'
            AND version = 'V01'
            AND name = 'ZBAA'""",
            QueryOptions(scan_consistency=QueryScanConsistency.REQUEST_PLUS),
        )
        # initialize builder with missing station_zbaa
        setup_builder_doc(_cluster, _builder)
        # handle_station should give us a new station_zbaa
        _builder.handle_station(
            {"base_var_index": _rec_num, "stationName": _station_name}
        )
        doc_map = _builder.get_document_map("rec_num")
        _id = next(iter(doc_map))
        result = _collection.upsert(_id, doc_map[_id])
        result = _cluster.query(
            f"""
            SELECT METAR.*
            From `{vx_ingest.cb_credentials["bucket"]}`.{vx_ingest.cb_credentials["scope"]}.{vx_ingest.cb_credentials["collection"]}
            WHERE type = 'MD'
            AND docType = 'station'
            AND version = 'V01'
            AND name = 'ZBAA'""",
            QueryOptions(scan_consistency=QueryScanConsistency.REQUEST_PLUS),
        )
        # assert for new station_zbaa
        assert_station(_cluster, station_zbaa_copy, _builder)
        cleanup_builder_doc(_cluster, _collection, _builder, station_zbaa_copy)

        # ****************
        # 2) changed location test
        new_station_zbaa = deepcopy(station_zbaa_copy)
        # add 1 to the existing lat for geo[0] and upsert the modified station_zbaa
        new_station_zbaa["geo"][0]["lat"] = 41.06999
        result = _collection.upsert(station_zbaa["id"], new_station_zbaa)
        # handle_station should see that the existing station_zbaa has a different
        # geo[0]['lat'] and make a new geo[1]['lat'] with the netcdf original lat
        # populate the builder list with the modified station by seting up
        setup_builder_doc(_cluster, _builder)
        _builder.handle_station(
            {"base_var_index": _rec_num, "stationName": _station_name}
        )
        result = _cluster.query(
            """
            SELECT METAR.*
            From `{vx_ingest.cb_credentials['bucket']}`.{vx_ingest.cb_credentials['scope']}.{vx_ingest.cb_credentials['collection']}
            WHERE type = 'MD'
            AND docType = 'station'
            AND version = 'V01'
            AND name = 'ZBAA'""",
            QueryOptions(scan_consistency=QueryScanConsistency.REQUEST_PLUS),
        )
        doc_map = _builder.get_document_map("rec_num")
        _id = next(iter(doc_map))
        result = _collection.upsert(_id, doc_map[_id])
        result = _cluster.query(
            f"""
            SELECT METAR.*
            From `{vx_ingest.cb_credentials["bucket"]}`.{vx_ingest.cb_credentials["scope"]}.{vx_ingest.cb_credentials["collection"]}
            WHERE type = 'MD'
            AND docType = 'station'
            AND version = 'V01'
            AND name = 'ZBAA'""",
            QueryOptions(scan_consistency=QueryScanConsistency.REQUEST_PLUS),
        )
        # station ZBAA should now have 2 geo entries
        assert len(doc_map["MD:V01:METAR:station:ZBAA"]["geo"]) == 2, (
            "new station ZBAA['geo'] does not have 2 elements"
        )
        # modify the station_zbaa to reflect what handle_station should have done
        station_zbaa["geo"][0]["lat"] = 41.06999
        station_zbaa["geo"].append(
            {
                "elev": 30,
                "firstTime": doc_map["MD:V01:METAR:station:ZBAA"]["geo"][0][
                    "firstTime"
                ],
                "lastTime": doc_map["MD:V01:METAR:station:ZBAA"]["geo"][0]["lastTime"],
                "lat": 40.06999,
                "lon": 116.58,
            }
        )
        assert_station(_cluster, station_zbaa, _builder)
        cleanup_builder_doc(_cluster, _collection, _builder, station_zbaa_copy)

        # ****************
        # 3) update time range test
        new_station_zbaa = deepcopy(station_zbaa_copy)
        # save the original firstTime
        orig_first_time = new_station_zbaa["geo"][0]["firstTime"]
        # add some time to the firstTime and lastTime of new_station_zbaa
        new_station_zbaa["geo"][0]["firstTime"] = (
            station_zbaa["geo"][0]["firstTime"] + 2 * _builder.cadence
        )
        new_station_zbaa["geo"][0]["lastTime"] = (
            station_zbaa["geo"][0]["lastTime"] + 2 * _builder.cadence
        )
        _collection.upsert(new_station_zbaa["id"], new_station_zbaa)
        # populate the builder list with the modified station by seting up
        setup_builder_doc(_cluster, _builder)
        # handle station should see that the real station_zbaa doesn't fit within
        # the existing timeframe of geo[0] and modify the geo element with the
        # original firstTime (matches the fcstValidEpoch of the file)
        _builder.handle_station(
            {"base_var_index": _rec_num, "stationName": _station_name}
        )
        result = _cluster.query(
            f"""
            SELECT METAR.*
            From `{vx_ingest.cb_credentials["bucket"]}`.{vx_ingest.cb_credentials["scope"]}.{vx_ingest.cb_credentials["collection"]}
            WHERE type = 'MD'
            AND docType = 'station'
            AND version = 'V01'
            AND name = 'ZBAA'""",
            QueryOptions(scan_consistency=QueryScanConsistency.REQUEST_PLUS),
        )
        doc_map = _builder.get_document_map("rec_num")
        _id = next(iter(doc_map))
        result = _collection.upsert(_id, doc_map[_id])
        # modify the new_station_zbaa['geo'] to reflect what handle_station should have done
        new_station_zbaa["geo"][0]["firstTime"] = orig_first_time
        assert_station(_cluster, new_station_zbaa, _builder)
        cleanup_builder_doc(_cluster, _collection, _builder, station_zbaa_copy)
    except Exception as _e:
        pytest.fail(f"test_handle_station Exception failure: {_e}")
    finally:
        # upsert the original ZBAA station document
        station_zbaa["geo"].pop(0)
        _collection.upsert(station_zbaa_copy["id"], station_zbaa_copy)


def remove_station(cluster, collection, station, builder):
    """
    Removes the station from the collection
    Args:
        _cluster (object): a couchbase cluster object
        _collection (object): a couchbase collection object
        station (object): a station object

    Returns:
        : station name
    """
    if station is None:
        return None
    res = collection.remove(station["id"])
    _ms = MutationState(res)
    result = cluster.query(
        " ".join(
            (
                f"""
                SELECT METAR.*
                FROM `{builder.load_spec["cb_connection"]["bucket"]}`.{builder.load_spec["cb_connection"]["scope"]}.{builder.load_spec["cb_connection"]["collection"]}
                WHERE type = 'MD'
                AND docType = 'station'
                AND version = 'V01'
                AND name = '"""
                + station["name"]
                + "'"
            ).split()
        ),
        QueryOptions(consistent_with=_ms),
    )
    check_station_zbaa = len(list(result)) == 0
    assert check_station_zbaa, "station " + station["name"] + " did not get deleted"
    return station["name"]


def setup_builder_doc(cluster, builder):
    """
    initialize the builder document map and populate the builder station list with data from the DB
    """
    result = cluster.query(
        " ".join(
            f"""SELECT METAR.*
            FROM `{builder.load_spec["cb_connection"]["bucket"]}`.{builder.load_spec["cb_connection"]["scope"]}.{builder.load_spec["cb_connection"]["collection"]}
            WHERE type = 'MD'
            AND docType = 'station'
            AND subset = 'METAR'
            AND version = 'V01';""".split()
        )
    )
    builder.stations = list(result)
    builder.initialize_document_map()


def cleanup_builder_doc(cluster, collection, builder, station_zbaa_copy):
    """
    upsert the zbaa station and then setup the builder doc (trying to be consistent
    with the upsert)
    """
    collection.upsert(station_zbaa_copy["id"], station_zbaa_copy)
    result = cluster.query(
        " ".join(
            f"""SELECT METAR.*
            From `{builder.load_spec["cb_connection"]["bucket"]}`.{builder.load_spec["cb_connection"]["scope"]}.{builder.load_spec["cb_connection"]["collection"]}
            WHERE type = 'MD'
            AND docType = 'station'
            AND subset = 'METAR'
            AND version = 'V01';""".split()
        ),
        QueryOptions(scan_consistency=QueryScanConsistency.REQUEST_PLUS),
    )
    builder.stations = list(result)
    builder.initialize_document_map()


def assert_station(cluster, station_zbaa, builder):
    """Asserts that a given station object matches the one that is in the database,"""
    new_result = cluster.query(
        " ".join(
            f"""
            SELECT METAR.*
            From `{builder.load_spec["cb_connection"]["bucket"]}`.{builder.load_spec["cb_connection"]["scope"]}.{builder.load_spec["cb_connection"]["collection"]}
            WHERE type = 'MD'
            AND docType = 'station'
            AND version = 'V01'
            AND name = 'ZBAA'
            """.split()
        ),
        QueryOptions(scan_consistency=QueryScanConsistency.REQUEST_PLUS),
    )
    new_station_zbaa = list(new_result)[0]
    if station_zbaa is None:
        assert new_station_zbaa["description"], "new station description is missing"
        assert new_station_zbaa["id"], "new station id is missing"
        assert new_station_zbaa["name"], "new station name is missing"
        assert new_station_zbaa["updateTime"], "new station updateTime is missing"
        assert new_station_zbaa["geo"], "new station geo is missing"
        assert len(new_station_zbaa["geo"]) == 1, "new station geo is not length 1"
        return
    assert new_station_zbaa["description"] == station_zbaa["description"], (
        "new 'description'"
        + new_station_zbaa["description"]
        + " does not equal old 'description'"
        + station_zbaa["description"]
    )
    assert new_station_zbaa["id"] == station_zbaa["id"], (
        "new 'id'"
        + new_station_zbaa["id"]
        + " does not equal old 'id'"
        + station_zbaa["id"]
    )
    assert new_station_zbaa["name"] == station_zbaa["name"], (
        "new 'name'"
        + new_station_zbaa["name"]
        + " does not equal old 'name'"
        + station_zbaa["name"]
    )
    for geo_index in range(len(new_station_zbaa["geo"])):
        assert (
            new_station_zbaa["geo"][geo_index]["lat"]
            == station_zbaa["geo"][geo_index]["lat"]
        ), (
            "new '['geo'][geo_index]['lat']'"
            + str(new_station_zbaa["geo"][geo_index]["lat"])
            + " does not equal old '['geo'][geo_index]['lat']'"
            + str(station_zbaa["geo"][geo_index]["lat"])
        )
        assert (
            new_station_zbaa["geo"][geo_index]["lon"]
            == station_zbaa["geo"][geo_index]["lon"]
        ), (
            "new '['geo'][geo_index]['lon']'"
            + str(new_station_zbaa["geo"][geo_index]["lon"])
            + " does not equal old '['geo'][geo_index]['lon']'"
            + str(station_zbaa["geo"][geo_index]["lon"])
        )
        assert (
            new_station_zbaa["geo"][geo_index]["elev"]
            == station_zbaa["geo"][geo_index]["elev"]
        ), (
            "new '['geo'][geo_index]['elev']'"
            + str(new_station_zbaa["geo"][geo_index]["elev"])
            + " does not equal old '['geo'][geo_index]['elev']'"
            + str(station_zbaa["geo"][geo_index]["elev"])
        )
        assert (
            new_station_zbaa["geo"][geo_index]["firstTime"]
            == station_zbaa["geo"][geo_index]["firstTime"]
        ), (
            "new '['geo'][geo_index]['firstTime']'"
            + str(new_station_zbaa["geo"][geo_index]["firstTime"])
            + " does not equal old '['geo'][geo_index]['firstTime']'"
            + str(station_zbaa["geo"][geo_index]["firstTime"])
        )
        assert (
            new_station_zbaa["geo"][geo_index]["lastTime"]
            == station_zbaa["geo"][geo_index]["lastTime"]
        ), (
            "new '['geo'][geo_index]['lastTime']'"
            + str(new_station_zbaa["geo"][geo_index]["lastTime"])
            + " does not equal old '['geo'][geo_index]['lastTime']'"
            + str(station_zbaa["geo"][geo_index]["lastTime"])
        )


@pytest.mark.integration
def test_derive_valid_time_epoch():
    """test the derive_valid_time_epoch routine in netcdf_builder"""
    vx_ingest = setup_connection()
    _collection = vx_ingest.collection
    load_spec = vx_ingest.load_spec
    ingest_document_ids = vx_ingest.load_spec["ingest_document_ids"]
    ingest_document = _collection.get(ingest_document_ids[0]).content_as[dict]
    _builder = NetcdfMetarObsBuilderV01(load_spec, ingest_document)
    _builder.file_name = "20211108_0000"
    _pattern = "%Y%m%d_%H%M"
    _file_utc_time = datetime.strptime(_builder.file_name, _pattern)
    expected_epoch = (_file_utc_time - datetime(1970, 1, 1)).total_seconds()
    derived_epoch = _builder.derive_valid_time_epoch({"file_name_pattern": _pattern})
    assert expected_epoch == derived_epoch, (
        f"derived epoch {derived_epoch} is not equal to 1636329600"
    )


@pytest.mark.integration
def test_derive_valid_time_iso():
    """test the derive_valid_time_iso routine in netcdf_builder"""
    vx_ingest = setup_connection()
    _cluster = vx_ingest.cluster
    _collection = vx_ingest.collection
    load_spec = vx_ingest.load_spec
    ingest_document_ids = vx_ingest.load_spec["ingest_document_ids"]
    ingest_document = _collection.get(ingest_document_ids[0]).content_as[dict]
    _builder = NetcdfMetarObsBuilderV01(load_spec, ingest_document)
    _builder.file_name = "20211108_0000"
    derived_epoch = _builder.derive_valid_time_iso({"file_name_pattern": "%Y%m%d_%H%M"})
    assert derived_epoch == "2021-11-08T00:00:00Z", (
        f"derived epoch {derived_epoch} is not equal to 1636390800"
    )
