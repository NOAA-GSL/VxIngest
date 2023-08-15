# pylint: disable=too-many-lines
"""
    integration tests for grib builder
    This test expects to find a valid grib file in the local directory /opt/public/data/grids/hrrr/conus/wrfprs/grib2.
This test expects to write to the local output directory /opt/data/grib_to_cb/output so that directory should exist.
21 196 14 000018 %y %j %H %f  treating the last 6 decimals as microseconds even though they are not.
these files are two digit year, day of year, hour, and forecast lead time (6 digit ??)
"""
import glob
import os
import json
from glob import glob
from pathlib import Path
from datetime import timedelta
import yaml

from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, ClusterTimeoutOptions
from couchbase.auth import PasswordAuthenticator
from grib2_to_cb.run_ingest_threads import VXIngest

cb_connection = {}


def connect_cb():
    """
    create a couchbase connection and maintain the collection and cluster objects.
    """
    # noinspection PyBroadException
    try:
        if cb_connection:  # pylint: disable=used-before-assignment
            return cb_connection
        else:
            credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
            assert (
                Path(credentials_file).is_file() is True
            ), f"*** credentials_file file {credentials_file} can not be found!"
            _f = open(credentials_file, encoding="utf-8")
            _yaml_data = yaml.load(_f, yaml.SafeLoader)
            cb_connection["host"] = _yaml_data["cb_host"]
            cb_connection["user"] = _yaml_data["cb_user"]
            cb_connection["password"] = _yaml_data["cb_password"]
            cb_connection["bucket"] = _yaml_data["cb_bucket"]
            cb_connection["collection"] = _yaml_data["cb_collection"]
            cb_connection["scope"] = _yaml_data["cb_scope"]
            _f.close()

            timeout_options = ClusterTimeoutOptions(
                kv_timeout=timedelta(seconds=25), query_timeout=timedelta(seconds=120)
            )
            options = ClusterOptions(
                PasswordAuthenticator(cb_connection["user"], cb_connection["password"]),
                timeout_options=timeout_options,
            )
            cb_connection["cluster"] = Cluster(
                "couchbase://" + cb_connection["host"], options
            )
            cb_connection["collection"] = (
                cb_connection["cluster"]
                .bucket(cb_connection["bucket"])
                .collection(cb_connection["collection"])
            )
            return cb_connection
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"test_unit_queries Exception failure connecting: {_e}"


def get_geo_index(fcst_valid_epoch, geo):
    """return the index of the geo list that corresponds to the given fcst_valid_epoch
    - the location of a station might change over time and this list contains the
    lat/lon of the station at different times.  The fcst_valid_epoch is used to
    determine which lat/lon to use.
    Args:
        fcst_valid_epoch (int): an epoch
        geo (list): a list of geo objects

    Returns:
        int : the index
    """
    latest_time = 0
    latest_index = 0
    try:
        for geo_index in range(len(geo)):  # pylint: disable=consider-using-enumerate
            if geo[geo_index]["lastTime"] > latest_time:
                latest_time = geo[geo_index]["lastTime"]
                latest_index = geo_index
            found = False
            if (
                geo[geo_index]["firstTime"] >= fcst_valid_epoch
                and fcst_valid_epoch <= geo[geo_index]["lastTime"]
            ):
                found = True
                break
        if found:
            return geo_index
        else:
            return latest_index
    except Exception as _e:  # pylint: disable=broad-except
        assert False, f"GribBuilder.get_geo_index: Exception  error: {_e}"


def test_grib_builder_one_thread_file_pattern_hrrr_ops_conus():
    """test gribBuilder with one thread"""
    try:
        # 1632412800 fcst_len 1 -> 1632412800 - 1 * 3600 -> 1632409200 September 23, 2021 15:00:00 -> 2126615000001
        # 1632412800 fcst_len 3 -> 1632412800 - 3 * 3600 -> 1632402000 September 23, 2021 13:00:00 -> 2126613000003
        # 1632412800 fcst_len 15 -> 1632412800 - 15 * 3600 -> 1632358800 September 22, 2021 19:00:00  ->  (missing)
        # 1632412800 fcst_len 18 -> 1632412800 - 18 * 3600 -> 1632348000 September 22, 2021 22:00:00 -> 2126522000018 (missing)
        # 1632420000 September 23, 2021 18:00:00  2126616000018
        # 1632423600  September 23, 2021 19:00:00 2126617000001
        # first_epoch = 1634252400 - 10
        # last_epoch = 1634252400 + 10
        credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
        # remove output files
        for _f in glob("/opt/data/grib2_to_cb/output/test1/*.json"):
            os.remove(_f)
        vx_ingest = VXIngest()
        # NOTE: the path is defined by the job document
        vx_ingest.runit(
            {
                "job_id": "JOB-TEST:V01:METAR:GRIB2:MODEL:HRRR",
                "credentials_file": credentials_file,
                "file_name_mask": "%y%j%H%f",
                "output_dir": "/opt/data/grib2_to_cb/output/test1",
                "threads": 1,
                "file_pattern": "21287230000[0123456789]?",
            }
        )
        # check the output files to see if they match the documents that were
        # preveously created by the real ingest process
        for _f in glob("/opt/data/grib2_to_cb/output/test1/*.json"):
            # read in the output file
            _json = None
            with open(_f, "r", encoding="utf-8") as _f:
                _json = json.load(_f)[0]
            _id = _json["id"]
            if _id.startswith("LJ"):
                for _k in _json.keys():
                    assert _k in [
                        "id",
                        "subset",
                        "type",
                        "lineageId",
                        "script",
                        "scriptVersion",
                        "loadSpec",
                        "note",
                    ], f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus LJ failure key {_k} not in {_json.keys()}"
                continue
            _statement = f"select METAR.* from `{connect_cb()['bucket']}`._default.METAR where meta().id = '{_id}'"
            qresult = connect_cb()["cluster"].query(_statement)
            result = list(qresult.rows())[0]
            # assert top level fields
            keys = _json.keys()
            for _k in result.keys():
                assert (
                    _k in keys
                ), f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus failure top level key {_k} not in {_json.keys()}"
            # assert the units
            assert (
                result["units"] == _json["units"]
            ), f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus failure units {result['units']} != {_json['units']}"
            # assert the data
            for _k in result["data"].keys():
                assert (
                    _k in _json["data"].keys()
                ), f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus failure data key {_k} not in {_json['data'].keys()}"
                for _dk in result["data"][_k].keys():
                    assert (
                        _dk in _json["data"][_k].keys()
                    ), f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus failure data key {_k}.{_dk} not in {_json['data'][_k].keys()}"
                    # assert data field matches to 2 decimal places
                    if _dk == "name":
                        assert (
                            result["data"][_k][_dk] == _json["data"][_k][_dk]
                        ), f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus failure name {result['data'][_k][_dk]} != {_json['data'][_k][_dk]}"
                    else:
                        decimals = 6
                        if _dk in ["Ceiling", "DewPoint"]:
                            decimals = 0
                        elif _dk == "RH":
                            decimals = 1
                        assert round(result["data"][_k][_dk], decimals) == round(
                            _json["data"][_k][_dk], decimals
                        ), f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus failure data equality {_k}.{_dk} {result['data'][_k][_dk]} != {_json['data'][_k][_dk]} within {decimals} decimal places"
    except Exception as _e:  # pylint: disable=broad-except
        assert (
            False
        ), f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus Exception failure: {_e}"
    finally:
        # remove the output files
        for _f in glob("/opt/data/grib2_to_cb/output/test1/*.json"):
            os.remove(_f)


def test_grib_builder_two_threads_file_pattern_hrrr_ops_conus():
    """test gribBuilder multi-threaded"""
    try:
        # 1632412800 fcst_len 1 -> 1632412800 - 1 * 3600 -> 1632409200 September 23, 2021 15:00:00 -> 2126615000001
        # 1632412800 fcst_len 3 -> 1632412800 - 3 * 3600 -> 1632402000 September 23, 2021 13:00:00 -> 2126613000003
        # 1632412800 fcst_len 15 -> 1632412800 - 15 * 3600 -> 1632358800 September 22, 2021 19:00:00  ->  (missing)
        # 1632412800 fcst_len 18 -> 1632412800 - 18 * 3600 -> 1632348000 September 22, 2021 22:00:00 -> 2126522000018 (missing)
        # 1632420000 September 23, 2021 18:00:00  2126616000018
        # 1632423600  September 23, 2021 19:00:00 2126617000001
        # first_epoch = 1634252400 - 10
        # last_epoch = 1634252400 + 10
        credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
        # remove output files
        for _f in glob("/opt/data/grib2_to_cb/output/test2/*.json"):
            os.remove(_f)
        vx_ingest = VXIngest()
        # NOTE: the path is defined by the job document
        vx_ingest.runit(
            {
                "job_id": "JOB:V01:METAR:GRIB2:MODEL:HRRR",
                "credentials_file": credentials_file,
                "file_name_mask": "%y%j%H%f",
                "output_dir": "/opt/data/grib2_to_cb/output/test2",
                "threads": 2,
                "file_pattern": "21287230000[0123456789]?",
            }
        )
    except Exception as _e:  # pylint: disable=broad-except
        assert (
            False
        ), f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus Exception failure: {_e} "
    finally:
        # remove output files
        for _f in glob("/opt/data/grib2_to_cb/output/test2/*.json"):
            os.remove(_f)
