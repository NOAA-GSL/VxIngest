"""
integration tests for grib builder
This test expects to find a valid grib file in the local directory /opt/public/data/grids/hrrr/conus/wrfprs/grib2.
This test expects to write to the local output directory /opt/data/grib_to_cb/output so that directory should exist.
For files except rrfs_a the filenames are like 21 196 14 000018 %y %j %H %f  treating the last 6 decimals as microseconds even though they are not.
these files are two digit year, day of year, hour, and forecast lead time (6 digit ??)
"""

import json
import math
import numbers
import os
from multiprocessing import Queue
from pathlib import Path

import pytest
from couchbase.options import QueryOptions

from vxingest.grib2_to_cb.run_ingest_threads import VXIngest


def stub_worker_log_configurer(queue: Queue):
    """A stub to replace log_config.worker_log_configurer"""
    pass


def setup_connection():
    """test setup"""
    _vx_ingest = VXIngest()
    # Ensure credentials_file is a string, not a tuple
    credentials = os.environ["CREDENTIALS"]
    _vx_ingest.credentials_file = credentials
    _vx_ingest.cb_credentials = _vx_ingest.get_credentials(_vx_ingest.load_spec)
    _vx_ingest.connect_cb()
    try:
        # Clean up any previous test data - this data came from /opt/data/% so we know it is test data
        vx_ingest = setup_connection(_vx_ingest)
        id_query = """DELETE
                FROM `vxdata`.`_default`.`METAR` f
                WHERE f.subset = 'METAR'
                AND f.type = 'DF'
                AND f.url LIKE '/opt/data/%' RETURNING f.id AS id;"""
        row_iter = vx_ingest.cluster.query(
            id_query, QueryOptions(metrics=True, read_only=False)
        )
        for row in row_iter:
            print(f"Deleted {row['id']}")
    except Exception as e:
        print(f"Error occurred: {e}")
    return _vx_ingest


@pytest.mark.integration
def test_grib_builder_one_thread_file_pattern_hrrr_ops_conus(tmp_path: Path):
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
    # remove possible existing DF test documents
    vx_ingest = setup_connection()
    log_queue = Queue()
    job = vx_ingest.common_collection.get(
        "JOB-TEST:V01:METAR:GRIB2:MODEL:HRRR"
    ).content_as[dict]
    ingest_document_ids = job["ingest_document_ids"]
    collection = job["subset"]
    input_data_path = job["input_data_path"]
    file_mask = job["file_mask"]

    vx_ingest.runit(
        {
            "job_id": "JOB-TEST:V01:METAR:GRIB2:MODEL:HRRR",
            "credentials_file": os.environ["CREDENTIALS"],
            "collection": collection,
            "file_mask": file_mask,
            "input_data_path": input_data_path,
            "ingest_document_ids": ingest_document_ids,
            "output_dir": f"{tmp_path}",
            "file_pattern": "21287230000[0123456789]?",
            "threads": 1,
        },
        log_queue,
        stub_worker_log_configurer,
    )
    # check the output files to see if they match the documents that were
    # previously created by the real ingest process
    # check the number of files created
    if len(list(tmp_path.glob("*.json"))) < 2:
        pytest.fail("Not enough output files created")
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
        _statement = f"select METAR.* from `{vx_ingest.cb_credentials['bucket']}`._default.METAR where meta().id = '{_id}'"
        _qresult = vx_ingest.cluster.query(_statement)
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
                if _dk == "name" or _dk == "Vegetation Type":
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

                    assert result["data"][_k][_dk] is not None, (
                        f"""result {_k + "." + _dk}  is None """
                    )
                    assert _json["data"][_k][_dk] is not None, (
                        f"""_json {_k + "." + _dk} is None """
                    )
                    # Only compare with math.isclose if both are numbers
                    if isinstance(result["data"][_k][_dk], (int, float)) and isinstance(
                        _json["data"][_k][_dk], (int, float)
                    ):
                        assert math.isclose(
                            result["data"][_k][_dk],
                            _json["data"][_k][_dk],
                            abs_tol=abs_tol,
                        ), f"""TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus failure data not close within {abs_tol}
                        {_k}.{_dk} {result["data"][_k][_dk]} != {_json["data"][_k][_dk]} within {abs_tol} decimal places."""
                    else:
                        assert result["data"][_k][_dk] == _json["data"][_k][_dk], (
                            f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus failure non-numeric data {result['data'][_k][_dk]} != {_json['data'][_k][_dk]}"
                        )


@pytest.mark.integration
def test_grib_builder_one_thread_file_pattern_rrfs_a_conus(tmp_path: Path):
    """test gribBuilder with one thread.
    This test verifies the resulting data file against the one that is in couchbase already
    in order to make sure the calculations are proper."""
    vx_ingest = setup_connection()
    log_queue = Queue()
    job = vx_ingest.common_collection.get(
        "JOB-TEST:V01:METAR:GRIB2:MODEL:RRFS_A"
    ).content_as[dict]
    ingest_document_ids = job["ingest_document_ids"]
    collection = job["subset"]
    input_data_path = job["input_data_path"]
    file_mask = job["file_mask"]

    # rrfs_a 3km conus files do not have a filename pattern
    # that matches a date time in the file name so it relies
    # on the file_pattern to limit the files that are processed
    # These are 3km conus pressure level files
    vx_ingest.runit(
        {
            "job_id": "JOB-TEST:V01:METAR:GRIB2:MODEL:RRFS_A",
            "credentials_file": os.environ["CREDENTIALS"],
            "collection": collection,
            "file_mask": file_mask,
            "input_data_path": input_data_path,
            "ingest_document_ids": ingest_document_ids,
            "output_dir": f"{tmp_path}",
            "file_pattern": "rrfs.*/*/rrfs.t*z.prslev.3km.f*.conus.grib2",
            "threads": 1,
        },
        log_queue,
        stub_worker_log_configurer,
    )
    # check the output files to see if they match the documents that were
    # previously created by the real ingest process
    # check the number of files created
    if len(list(tmp_path.glob("*.json"))) < 2:
        pytest.fail("Not enough output files created")
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
        _statement = f"select METAR.* from `{vx_ingest.cb_credentials['bucket']}`._default.METAR where meta().id = '{_id}'"
        _qresult = vx_ingest.cluster.query(_statement)
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
                try:
                    assert _dk in _json["data"][_k], (
                        f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus failure data key {_k}.{_dk} not in {_json['data'][_k].keys()}"
                    )
                    # assert data field matches to 2 decimal places
                    if _dk == "name" or _dk == "Vegetation Type":
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

                        assert result["data"][_k][_dk] is not None, (
                            f"""result {_k + "." + _dk}  is None """
                        )
                        assert _json["data"][_k][_dk] is not None, (
                            f"""_json {_k + "." + _dk} is None """
                        )
                        assert math.isclose(
                            result["data"][_k][_dk],
                            _json["data"][_k][_dk],
                            abs_tol=abs_tol,
                        ), f"""TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus failure data not close within {abs_tol}
                        {_k}.{_dk} {result["data"][_k][_dk]} != {_json["data"][_k][_dk]} within {abs_tol} decimal places."""
                except Exception as e:
                    print(f"KeyError {_k} {_dk} in {_json['data'][_k].keys()}")
                    raise e


@pytest.mark.integration
def test_grib_builder_one_thread_file_pattern_mpas(tmp_path: Path):
    """test gribBuilder with one thread for mpas.
    This test verifies the resulting data file against the one that is in couchbase already
    in order to make sure the calculations are proper."""
    vx_ingest = setup_connection()
    log_queue = Queue()
    job = vx_ingest.common_collection.get(
        "JOB-TEST:V01:METAR:GRIB2:MODEL:MPAS_physics_dev1"
    ).content_as[dict]
    ingest_document_ids = job["ingest_document_ids"]
    collection = job["subset"]
    input_data_path = job["input_data_path"]
    file_mask = job["file_mask"]
    vx_ingest.runit(
        {
            "job_id": "JOB-TEST:V01:METAR:GRIB2:MODEL:RRFS_A",
            "credentials_file": os.environ["CREDENTIALS"],
            "collection": collection,
            "file_mask": file_mask,
            "input_data_path": input_data_path,
            "ingest_document_ids": ingest_document_ids,
            "output_dir": f"{tmp_path}",
            "threads": 1,
        },
        log_queue,
        stub_worker_log_configurer,
    )
    # check the output files to see if they match the documents that were
    # previously created by the real ingest process
    # check the number of files created
    if len(list(tmp_path.glob("*.json"))) < 2:
        pytest.fail("Not enough output files created")
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
        _statement = f"select METAR.* from `{vx_ingest.cb_credentials['bucket']}`._default.METAR where meta().id = '{_id}'"
        _qresult = vx_ingest.cluster.query(_statement)
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
                    if (
                        result["data"][_k][_dk] is not None
                        and _json["data"][_k][_dk] is None
                    ):
                        pytest.fail(
                            f"""_json {_k + "." + _dk} is None when result is not None"""
                        )
                    if (
                        _json["data"][_k][_dk] is not None
                        and result["data"][_k][_dk] is None
                    ):
                        pytest.fail(
                            f"""result {_k + "." + _dk} is None when _json is not None"""
                        )
                    try:
                        if isinstance(result["data"][_k][_dk], (numbers.Number)) and (
                            _json["data"][_k][_dk],
                            (numbers.Number),
                        ):
                            assert math.isclose(
                                result["data"][_k][_dk],
                                _json["data"][_k][_dk],
                                abs_tol=abs_tol,
                            ), (
                                f"""TestGribBuilderV01.test_grib_builder_one_thread_file_pattern_mpas failure data not close within {str(abs_tol)} {str(_k)}.{str(_dk)} {str(result["data"][_k][_dk])} != {str(_json["data"][_k][_dk])} within {str(abs_tol)} decimal places."""
                            )
                        else:
                            assert result["data"][_k][_dk] == _json["data"][_k][_dk], (
                                f"TestGribBuilderV01.test_grib_builder_one_thread_file_pattern_mpas failure non-numeric data {result['data'][_k][_dk]} != {_json['data'][_k][_dk]}"
                            )
                    except Exception as e:
                        print(f"KeyError {_k} {_dk} in {_json['data'][_k].keys()}")
                        raise e


@pytest.mark.integration
def test_grib_builder_two_threads_file_pattern_hrrr_ops_conus(tmp_path: Path):
    """test gribBuilder multi-threaded"""
    # 1632412800 fcst_len 1 -> 1632412800 - 1 * 3600 -> 1632409200 September 23, 2021 15:00:00 -> 2126615000001
    # 1632412800 fcst_len 3 -> 1632412800 - 3 * 3600 -> 1632402000 September 23, 2021 13:00:00 -> 2126613000003
    # 1632412800 fcst_len 15 -> 1632412800 - 15 * 3600 -> 1632358800 September 22, 2021 19:00:00  ->  (missing)
    # 1632412800 fcst_len 18 -> 1632412800 - 18 * 3600 -> 1632348000 September 22, 2021 22:00:00 -> 2126522000018 (missing)
    # 1632420000 September 23, 2021 18:00:00  2126616000018
    # 1632423600  September 23, 2021 19:00:00 2126617000001
    # first_epoch = 1634252400 - 10
    # last_epoch = 1634252400 + 10
    vx_ingest = setup_connection()
    log_queue = Queue()
    job = vx_ingest.common_collection.get(
        "JOB-TEST:V01:METAR:GRIB2:MODEL:HRRR"
    ).content_as[dict]
    ingest_document_ids = job["ingest_document_ids"]
    collection = job["subset"]
    input_data_path = job["input_data_path"]
    file_mask = job["file_mask"]
    vx_ingest.runit(
        {
            "job_id": "JOB-TEST:V01:METAR:GRIB2:MODEL:HRRR",
            "credentials_file": os.environ["CREDENTIALS"],
            "collection": collection,
            "file_mask": file_mask,
            "file_pattern": "21287230000[0123456789]?",
            "input_data_path": input_data_path,
            "ingest_document_ids": ingest_document_ids,
            "output_dir": f"{tmp_path}",
            "threads": 2,
        },
        log_queue,
        stub_worker_log_configurer,
    )
    # check the output files to see if they match the documents that were
    # previously created by the real ingest process
    # check the number of files created
    if len(list(tmp_path.glob("*.json"))) < 2:
        pytest.fail("Not enough output files created")
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
                    f"TestGribBuilderV01.test_grib_builder_two_threads_file_pattern_hrrr_ops_conus LJ failure key {_k} not in {_json.keys()}"
                )
            continue
        _statement = f"select METAR.* from `{vx_ingest.cb_credentials['bucket']}`._default.METAR where meta().id = '{_id}'"
        _qresult = vx_ingest.cluster.query(_statement)
        result_rows = list(_qresult.rows())
        assert len(result_rows) > 0, (
            f"TestGribBuilderV01.test_grib_builder_two_threads_file_pattern_hrrr_ops_conus failure test document {_id} not found in couchbase"
        )

        result = result_rows[0]
        # assert top level fields
        keys = _json.keys()
        for _k in result:
            assert _k in keys, (
                f"TestGribBuilderV01.test_grib_builder_two_threads_file_pattern_hrrr_ops_conus failure top level key {_k} not in {_json.keys()}"
            )
        # assert the units
        assert result["units"] == _json["units"], (
            f"TestGribBuilderV01.test_grib_builder_two_threads_file_pattern_hrrr_ops_conus failure units {result['units']} != {_json['units']}"
        )
        # assert the data
        for _k in result["data"]:
            assert _k in _json["data"], (
                f"TestGribBuilderV01.test_grib_builder_two_threads_file_pattern_hrrr_ops_conus failure data key {_k} not in {_json['data'].keys()}"
            )
            for _dk in result["data"][_k]:
                assert _dk in _json["data"][_k], (
                    f"TestGribBuilderV01.test_grib_builder_two_threads_file_pattern_hrrr_ops_conus failure data key {_k}.{_dk} not in {_json['data'][_k].keys()}"
                )
                # assert data field matches to 2 decimal places
                if _dk == "name":
                    # string compare
                    assert result["data"][_k][_dk] == _json["data"][_k][_dk], (
                        f"TestGribBuilderV01.test_grib_builder_two_threads_file_pattern_hrrr_ops_conus failure name {result['data'][_k][_dk]} != {_json['data'][_k][_dk]}"
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
                    if (
                        result["data"][_k][_dk] is not None
                        and _json["data"][_k][_dk] is None
                    ):
                        pytest.fail(
                            f"""_json {_k + "." + _dk} is None when result is not None"""
                        )
                    if (
                        _json["data"][_k][_dk] is not None
                        and result["data"][_k][_dk] is None
                    ):
                        pytest.fail(
                            f"""result {_k + "." + _dk} is None when _json is not None"""
                        )
                    try:
                        if isinstance(result["data"][_k][_dk], (numbers.Number)) and (
                            _json["data"][_k][_dk],
                            (numbers.Number),
                        ):
                            assert math.isclose(
                                result["data"][_k][_dk],
                                _json["data"][_k][_dk],
                                abs_tol=abs_tol,
                            ), (
                                f"""TestGribBuilderV01.test_grib_builder_two_threads_file_pattern_hrrr_ops_conus failure data not close within {str(abs_tol)} {str(_k)}.{str(_dk)} {str(result["data"][_k][_dk])} != {str(_json["data"][_k][_dk])} within {str(abs_tol)} decimal places."""
                            )
                        else:
                            assert result["data"][_k][_dk] == _json["data"][_k][_dk], (
                                f"TestGribBuilderV01.test_grib_builder_two_threads_file_pattern_hrrr_ops_conus failure non-numeric data {result['data'][_k][_dk]} != {_json['data'][_k][_dk]}"
                            )
                    except Exception as e:
                        print(f"KeyError {_k} {_dk} in {_json['data'][_k].keys()}")
                        raise e


@pytest.mark.integration
def test_grib_builder_two_threads_file_pattern_rap_ops_130_conus(tmp_path: Path):
    """test gribBuilder multi-threaded
    Not going to qualify the data on this one, just make sure it runs two threads properly
    """
    vx_ingest = setup_connection()
    log_queue = Queue()
    job = vx_ingest.common_collection.get(
        "JOB-TEST:V01:METAR:GRIB2:MODEL:RAP_OPS_130"
    ).content_as[dict]
    ingest_document_ids = job["ingest_document_ids"]
    collection = job["subset"]
    input_data_path = job["input_data_path"]
    file_mask = job["file_mask"]
    vx_ingest.runit(
        {
            "job_id": "JOB-TEST:V01:METAR:GRIB2:MODEL:RAP_OPS_130",
            "credentials_file": os.environ["CREDENTIALS"],
            "collection": collection,
            "file_mask": file_mask,
            "file_pattern": "233320800000*",
            "input_data_path": input_data_path,
            "ingest_document_ids": ingest_document_ids,
            "output_dir": f"{tmp_path}",
            "threads": 1,
        },
        log_queue,
        stub_worker_log_configurer,
    )
    # check the output files to see if they match the documents that were
    # previously created by the real ingest process
    # check the number of files created
    if len(list(tmp_path.glob("*.json"))) < 2:
        pytest.fail("Not enough output files created")
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
                    f"TestGribBuilderV01.test_grib_builder_two_threads_file_pattern_rap_ops_130_conus LJ failure key {_k} not in {_json.keys()}"
                )
            continue
        _statement = f"select METAR.* from `{vx_ingest.cb_credentials['bucket']}`._default.METAR where meta().id = '{_id}'"
        _qresult = vx_ingest.cluster.query(_statement)
        result_rows = list(_qresult.rows())
        assert len(result_rows) > 0, (
            f"TestGribBuilderV01.test_grib_builder_two_threads_file_pattern_rap_ops_130_conus failure test document {_id} not found in couchbase"
        )

        result = result_rows[0]
        # assert top level fields
        keys = _json.keys()
        for _k in result:
            assert _k in keys, (
                f"TestGribBuilderV01.test_grib_builder_two_threads_file_pattern_rap_ops_130_conus failure top level key {_k} not in {_json.keys()}"
            )
        # assert the units
        assert result["units"] == _json["units"], (
            f"TestGribBuilderV01.test_grib_builder_two_threads_file_pattern_rap_ops_130_conus failure units {result['units']} != {_json['units']}"
        )
        # assert the data
        for _k in result["data"]:
            assert _k in _json["data"], (
                f"TestGribBuilderV01.test_grib_builder_two_threads_file_pattern_rap_ops_130_conus failure data key {_k} not in {_json['data'].keys()}"
            )
            for _dk in result["data"][_k]:
                assert _dk in _json["data"][_k], (
                    f"TestGribBuilderV01.test_grib_builder_two_threads_file_pattern_rap_ops_130_conus failure data key {_k}.{_dk} not in {_json['data'][_k].keys()}"
                )
                # assert data field matches to 2 decimal places
                if _dk == "name":
                    # string compare
                    assert result["data"][_k][_dk] == _json["data"][_k][_dk], (
                        f"TestGribBuilderV01.test_grib_builder_two_threads_file_pattern_rap_ops_130_conus failure name {result['data'][_k][_dk]} != {_json['data'][_k][_dk]}"
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
                    if (
                        result["data"][_k][_dk] is not None
                        and _json["data"][_k][_dk] is None
                    ):
                        pytest.fail(
                            f"""_json {_k + "." + _dk} is None when result is not None"""
                        )
                    if (
                        _json["data"][_k][_dk] is not None
                        and result["data"][_k][_dk] is None
                    ):
                        pytest.fail(
                            f"""result {_k + "." + _dk} is None when _json is not None"""
                        )
                    try:
                        if isinstance(result["data"][_k][_dk], (numbers.Number)) and (
                            _json["data"][_k][_dk],
                            (numbers.Number),
                        ):
                            assert math.isclose(
                                result["data"][_k][_dk],
                                _json["data"][_k][_dk],
                                abs_tol=abs_tol,
                            ), (
                                f"""TestGribBuilderV01.test_grib_builder_two_threads_file_pattern_rap_ops_130_conus failure data not close within {str(abs_tol)} {str(_k)}.{str(_dk)} {str(result["data"][_k][_dk])} != {str(_json["data"][_k][_dk])} within {str(abs_tol)} decimal places."""
                            )
                        else:
                            assert result["data"][_k][_dk] == _json["data"][_k][_dk], (
                                f"TestGribBuilderV01.test_grib_builder_two_threads_file_pattern_rap_ops_130_conus failure non-numeric data {result['data'][_k][_dk]} != {_json['data'][_k][_dk]}"
                            )
                    except Exception as e:
                        print(f"KeyError {_k} {_dk} in {_json['data'][_k].keys()}")
                        raise e
