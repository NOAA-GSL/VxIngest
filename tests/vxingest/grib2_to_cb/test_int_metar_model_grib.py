"""
integration tests for grib builder
This test expects to find a valid grib file in the local directory /opt/public/data/grids/hrrr/conus/wrfprs/grib2.
This test expects to write to the local output directory /opt/data/grib_to_cb/output so that directory should exist.
21 196 14 000018 %y %j %H %f  treating the last 6 decimals as microseconds even though they are not.
these files are two digit year, day of year, hour, and forecast lead time (6 digit ??)
"""

import json
import math
import os
from datetime import timedelta
from multiprocessing import Queue
from pathlib import Path

import pytest
import yaml
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, ClusterTimeoutOptions

from vxingest.grib2_to_cb.run_ingest_threads import VXIngest

cb_connection = {}


def stub_worker_log_configurer(queue: Queue):
    """A stub to replace log_config.worker_log_configurer"""
    pass


def connect_cb():
    """
    create a couchbase connection and maintain the collection and cluster objects.
    """
    if cb_connection:
        return cb_connection
    else:
        credentials_file = os.environ["CREDENTIALS"]
        assert Path(credentials_file).is_file() is True, (
            f"*** credentials_file file {credentials_file} can not be found!"
        )
        with Path(credentials_file).open(encoding="utf-8") as _f:
            _yaml_data = yaml.load(_f, yaml.SafeLoader)
        cb_connection["host"] = _yaml_data["cb_host"]
        cb_connection["user"] = _yaml_data["cb_user"]
        cb_connection["password"] = _yaml_data["cb_password"]
        cb_connection["bucket"] = _yaml_data["cb_bucket"]
        cb_connection["collection"] = _yaml_data["cb_collection"]
        cb_connection["scope"] = _yaml_data["cb_scope"]

        timeout_options = ClusterTimeoutOptions(
            kv_timeout=timedelta(seconds=25), query_timeout=timedelta(seconds=120)
        )
        options = ClusterOptions(
            PasswordAuthenticator(cb_connection["user"], cb_connection["password"]),
            timeout_options=timeout_options,
        )
        cb_connection["cluster"] = Cluster(cb_connection["host"], options)
        cb_connection["collection"] = (
            cb_connection["cluster"]
            .bucket(cb_connection["bucket"])
            .collection(cb_connection["collection"])
        )
        return cb_connection


@pytest.mark.integration
def test_grib_builder_one_thread_file_pattern_hrrr_ops_conus(tmp_path):
    """test gribBuilder with one thread.
    This test verifies the resulting data file against the one that is in couchbase already
    in order to make sure the calculations are proper."""
    # 1632412800 fcst_len 1 -> 1632412800 - 1 * 3600 -> 1632409200 September 23, 2021 15:00:00 -> 2126615000001
    # 1632412800 fcst_len 3 -> 1632412800 - 3 * 3600 -> 1632402000 September 23, 2021 13:00:00 -> 2126613000003
    # 1632412800 fcst_len 15 -> 1632412800 - 15 * 3600 -> 1632358800 September 22, 2021 19:00:00  ->  (missing)
    # 1632412800 fcst_len 18 -> 1632412800 - 18 * 3600 -> 1632348000 September 22, 2021 22:00:00 -> 2126522000018 (missing)
    # 1632420000 September 23, 2021 18:00:00  2126616000018
    # 1632423600  September 23, 2021 19:00:00 2126617000001
    # first_epoch = 1634252400 - 10
    # last_epoch = 1634252400 + 10
    credentials_file = os.environ["CREDENTIALS"]
    # remove possible existing DF test documents
    connect_cb()["cluster"].query("""DELETE
            FROM `vxdata`._default.METAR
            WHERE subset='METAR'
            AND type='DF'
            AND url LIKE "/opt/data/%""")

    log_queue = Queue()
    vx_ingest = VXIngest()
    vx_ingest.runit(
        {
            "job_id": "JOB-TEST:V01:METAR:GRIB2:MODEL:HRRR",
            "credentials_file": credentials_file,
            "file_name_mask": "%y%j%H%f",
            "output_dir": f"{tmp_path}",
            "threads": 1,
            "file_pattern": "21287230000[0123456789]?",
        },
        log_queue,
        stub_worker_log_configurer,
    )
    # check the output files to see if they match the documents that were
    # previously created by the real ingest process
    for _f in tmp_path.glob("*.json"):
        # read in the output file
        _json = None
        with _f.open(encoding="utf-8") as json_file:
            _json = json.load(json_file)[0]
        _id = _json["id"]
        if _id.startswith("LJ"):
            for _k in _json:
                assert _k in [
                    "id",
                    "subset",
                    "type",
                    "lineageId",
                    "script",
                    "scriptVersion",
                    "loadSpec",
                    "note",
                ], (
                    f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus LJ failure key {_k} not in {_json.keys()}"
                )
            continue
        _statement = f"select METAR.* from `{connect_cb()['bucket']}`._default.METAR where meta().id = '{_id}'"
        _qresult = connect_cb()["cluster"].query(_statement)
        result_rows = list(_qresult.rows())
        assert len(result_rows) > 0, (
            f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus failure test document {_id} not found in couchbase"
        )

        result = result_rows[0]
        # assert top level fields
        keys = _json.keys()
        for _k in result:
            assert _k in keys, (
                f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus failure top level key {_k} not in {_json.keys()}"
            )
        # assert the units
        assert result["units"] == _json["units"], (
            f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus failure units {result['units']} != {_json['units']}"
        )
        # assert the data
        for _k in result["data"]:
            assert _k in _json["data"], (
                f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus failure data key {_k} not in {_json['data'].keys()}"
            )
            for _dk in result["data"][_k]:
                assert _dk in _json["data"][_k], (
                    f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus failure data key {_k}.{_dk} not in {_json['data'][_k].keys()}"
                )
                # assert data field matches to 2 decimal places
                if _dk == "name":
                    # string compare
                    assert result["data"][_k][_dk] == _json["data"][_k][_dk], (
                        f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus failure name {result['data'][_k][_dk]} != {_json['data'][_k][_dk]}"
                    )
                else:
                    # math compare
                    # print(f"result {_k} {_dk} ", result["data"][_k][_dk])
                    abs_tol = 0.0
                    if _dk == "Ceiling":
                        abs_tol = 0.002  # ceiling values don't always have four decimals of resolution
                    elif _dk == "DewPoint":
                        abs_tol = 1.0001  # DewPoint only has 3 decimals of precision from pygrib whereas cfgrib is having 4 (or at least the old ingest only had four)
                        # abs_tol = 0.0001  # DewPoint only has 3 decimals of precision from pygrib whereas cfgrib is having 4 (or at least the old ingest only had four)
                    elif (
                        _dk == "RH"
                    ):  # RH only has one decimal of resolution from the grib file
                        abs_tol = 1.00001  # not really sure why math.isclose compares out to 5 places but not 6
                        # abs_tol = 0.00001  # not really sure why math.isclose compares out to 5 places but not 6
                        # There are no unusual math transformations in the RH handler.
                    else:
                        abs_tol = 0.001  # most fields validate between pygrib and cfgrib precisely

                    assert math.isclose(
                        result["data"][_k][_dk],
                        _json["data"][_k][_dk],
                        abs_tol=abs_tol,
                    ), f"""TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus failure data not close within {abs_tol}
                    {_k}.{_dk} {result["data"][_k][_dk]} != {_json["data"][_k][_dk]} within {abs_tol} decimal places."""


@pytest.mark.integration
def test_grib_builder_one_thread_file_pattern_mpas(tmp_path):
    """test gribBuilder with one thread for mpas.
    This test verifies the resulting data file against the one that is in couchbase already
    in order to make sure the calculations are proper."""
    credentials_file = os.environ["CREDENTIALS"]
    # remove possible existing DF test documents
    connect_cb()["cluster"].query("""DELETE
            FROM `vxdata`._default.METAR
            WHERE subset='METAR'
            AND type='DF'
            AND url LIKE "/opt/data/%""")

    log_queue = Queue()
    vx_ingest = VXIngest()
    vx_ingest.runit(
        {
            "job_id": "JOB-TEST:V01:METAR:GRIB2:MODEL:MPAS_physics_dev1",
            "credentials_file": credentials_file,
            "file_name_mask": "mpas_phys_dev1_two_%y%j%H_f%f.grib2",
            "output_dir": f"{tmp_path}",
            "threads": 1,
        },
        log_queue,
        stub_worker_log_configurer,
    )
    # check the output files to see if they match the documents that were
    # previously created by the real ingest process
    for _f in tmp_path.glob("*.json"):
        # read in the output file
        _json = None
        with _f.open(encoding="utf-8") as json_file:
            _json = json.load(json_file)[0]
        _id = _json["id"]
        if _id.startswith("LJ"):
            for _k in _json:
                assert _k in [
                    "id",
                    "subset",
                    "type",
                    "lineageId",
                    "script",
                    "scriptVersion",
                    "loadSpec",
                    "note",
                ], (
                    f"TestGribBuilderV01.test_grib_builder_one_thread_file_pattern_mpas LJ failure key {_k} not in {_json.keys()}"
                )
            continue
        _statement = f"select METAR.* from `{connect_cb()['bucket']}`._default.METAR where meta().id = '{_id}'"
        _qresult = connect_cb()["cluster"].query(_statement)
        result_rows = list(_qresult.rows())
        assert len(result_rows) > 0, (
            f"TestGribBuilderV01.test_grib_builder_one_thread_file_pattern_mpas failure test document {_id} not found in couchbase"
        )

        result = result_rows[0]
        # assert top level fields
        keys = _json.keys()
        for _k in result:
            assert _k in keys, (
                f"TestGribBuilderV01.test_grib_builder_one_thread_file_pattern_mpas failure top level key {_k} not in {_json.keys()}"
            )
        # assert the units
        assert result["units"] == _json["units"], (
            f"TestGribBuilderV01.test_grib_builder_one_thread_file_pattern_mpas failure units {result['units']} != {_json['units']}"
        )
        # assert the data
        for _k in result["data"]:
            assert _k in _json["data"], (
                f"TestGribBuilderV01.test_grib_builder_one_thread_file_pattern_mpas failure data key {_k} not in {_json['data'].keys()}"
            )
            for _dk in result["data"][_k]:
                assert _dk in _json["data"][_k], (
                    f"TestGribBuilderV01.test_grib_builder_one_thread_file_pattern_mpas failure data key {_k}.{_dk} not in {_json['data'][_k].keys()}"
                )
                # assert data field matches to 2 decimal places
                if _dk == "name":
                    # string compare
                    assert result["data"][_k][_dk] == _json["data"][_k][_dk], (
                        f"TestGribBuilderV01.test_grib_builder_one_thread_file_pattern_mpas failure name {result['data'][_k][_dk]} != {_json['data'][_k][_dk]}"
                    )
                else:
                    # math compare
                    # print(f"result {_k} {_dk} ", result["data"][_k][_dk])
                    abs_tol = 0.0
                    if _dk == "Ceiling":
                        abs_tol = 0.002  # ceiling values don't always have four decimals of resolution
                    elif _dk == "DewPoint":
                        abs_tol = 1.0001  # DewPoint only has 3 decimals of precision from pygrib whereas cfgrib is having 4 (or at least the old ingest only had four)
                        # abs_tol = 0.0001  # DewPoint only has 3 decimals of precision from pygrib whereas cfgrib is having 4 (or at least the old ingest only had four)
                    elif (
                        _dk == "RH"
                    ):  # RH only has one decimal of resolution from the grib file
                        abs_tol = 1.00001  # not really sure why math.isclose compares out to 5 places but not 6
                        # abs_tol = 0.00001  # not really sure why math.isclose compares out to 5 places but not 6
                        # There are no unusual math transformations in the RH handler.
                    else:
                        abs_tol = 0.001  # most fields validate between pygrib and cfgrib precisely

                    assert math.isclose(
                        result["data"][_k][_dk],
                        _json["data"][_k][_dk],
                        abs_tol=abs_tol,
                    ), f"""TestGribBuilderV01.test_grib_builder_one_thread_file_pattern_mpas failure data not close within {abs_tol}
                    {_k}.{_dk} {result["data"][_k][_dk]} != {_json["data"][_k][_dk]} within {abs_tol} decimal places."""


@pytest.mark.integration
def test_grib_builder_two_threads_file_pattern_hrrr_ops_conus(tmp_path):
    """test gribBuilder multi-threaded
    Not going to qualify the data on this one, just make sure it runs two threads properly
    """
    # 1632412800 fcst_len 1 -> 1632412800 - 1 * 3600 -> 1632409200 September 23, 2021 15:00:00 -> 2126615000001
    # 1632412800 fcst_len 3 -> 1632412800 - 3 * 3600 -> 1632402000 September 23, 2021 13:00:00 -> 2126613000003
    # 1632412800 fcst_len 15 -> 1632412800 - 15 * 3600 -> 1632358800 September 22, 2021 19:00:00  ->  (missing)
    # 1632412800 fcst_len 18 -> 1632412800 - 18 * 3600 -> 1632348000 September 22, 2021 22:00:00 -> 2126522000018 (missing)
    # 1632420000 September 23, 2021 18:00:00  2126616000018
    # 1632423600  September 23, 2021 19:00:00 2126617000001
    # first_epoch = 1634252400 - 10
    # last_epoch = 1634252400 + 10
    credentials_file = os.environ["CREDENTIALS"]
    # remove possible existing DF test documents
    connect_cb()["cluster"].query("""DELETE
            FROM `vxdata`._default.METAR
            WHERE subset='METAR'
            AND type='DF'
            AND url LIKE "/opt/data/%""")

    # remove output files
    log_queue = Queue()
    vx_ingest = VXIngest()
    # NOTE: the input file path is defined by the job document
    vx_ingest.runit(
        {
            "job_id": "JOB-TEST:V01:METAR:GRIB2:MODEL:HRRR",
            "credentials_file": credentials_file,
            "file_name_mask": "%y%j%H%f",
            "output_dir": f"{tmp_path}",
            "threads": 2,
            "file_pattern": "21287230000[0123456789]?",
        },
        log_queue,
        stub_worker_log_configurer,
    )


@pytest.mark.integration
def test_grib_builder_two_threads_file_pattern_rap_ops_130_conus(tmp_path):
    """test gribBuilder multi-threaded
    Not going to qualify the data on this one, just make sure it runs two threads properly
    """
    credentials_file = os.environ["CREDENTIALS"]
    # remove possible existing DF test documents
    connect_cb()["cluster"].query("""DELETE
            FROM `vxdata`._default.METAR
            WHERE subset='METAR'
            AND type='DF'
            AND url LIKE "/opt/data/%""")

    # remove output files
    log_queue = Queue()
    vx_ingest = VXIngest()
    # NOTE: the input file path is defined by the job document
    vx_ingest.runit(
        {
            "job_id": "JOB-TEST:V01:METAR:GRIB2:MODEL:RAP_OPS_130",
            "credentials_file": credentials_file,
            "file_name_mask": "%y%j%H%f",
            "output_dir": f"{tmp_path}",
            "threads": 2,
            "file_pattern": "23332080000[0123456789]?",
        },
        log_queue,
        stub_worker_log_configurer,
    )
