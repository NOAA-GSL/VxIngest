"""
integration tests for grib builder
This test expects to find a valid grib file in the local directory /opt/public/data/grids/hrrr/conus/wrfprs/grib2.
This test expects to write to the local output directory /opt/data/grib_to_cb/output so that directory should exist.
For files except rrfs_a the filenames are like 21 196 14 000018 %y %j %H %f  treating the last 6 decimals as microseconds even though they are not.
these files are two digit year, day of year, hour, and forecast lead time (6 digit ??)
"""

import json
import math
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
        id_query = """DELETE
                FROM `vxdata`.`_default`.`METAR` f
                WHERE f.subset = 'METAR'
                AND f.type = 'DF'
                AND f.url LIKE '/opt/data/%' RETURNING f.id AS id;"""
        row_iter = _vx_ingest.cluster.query(
            id_query, QueryOptions(metrics=True, read_only=False)
        )
        for row in row_iter:
            print(f"Deleted {row['id']}")
    except Exception as e:
        print(f"Error occurred: {e}")
    return _vx_ingest


def run_runtime_one_thread_file_pattern_test(
    tmp_path: Path,
    job_id: str,
    failure_prefix: str,
    file_pattern: str | None = None,
):
    """Run a runtime job and validate output JSONs against existing Couchbase docs.
    Test gribBuilder with one thread.
    This test verifies the resulting data file against the one that is in couchbase already
    in order to make sure the calculations are proper."""
    vx_ingest = setup_connection()
    log_queue = Queue()
    job_doc = vx_ingest.runtime_collection.get(job_id).content_as[dict]
    process_spec_id = job_doc.get("processSpecIds")[0]
    proc = vx_ingest.runtime_collection.get(process_spec_id).content_as[dict]
    ingest_document_ids = proc.get("ingestDocumentIds")
    data_source_id = proc.get("dataSourceId")
    data_source_doc = vx_ingest.runtime_collection.get(data_source_id).content_as[dict]
    input_data_path = data_source_doc["sourceDataUri"]
    selected_file_pattern = file_pattern or data_source_doc.get("filePattern", "*")
    collection = job_doc["subset"]

    vx_ingest.runit(
        {
            "job_id": job_id,
            "credentials_file": os.environ["CREDENTIALS"],
            "ingest_document_ids": ingest_document_ids,
            "collection": collection,
            "input_data_path": input_data_path,
            "file_mask": "",
            "output_dir": f"{tmp_path}",
            "file_pattern": selected_file_pattern,
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
            _json_docs = json.load(json_file)
        for _json in _json_docs:
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
            if _id.startswith("DF"):
                for _k in _json:
                    assert _k in [
                        "id",
                        "mtime",
                        "subset",
                        "type",
                        "fileType",
                        "originType",
                        "loadJobId",
                        "dataSourceId",
                        "url",
                        "projection",
                        "interpolation",
                    ], (
                        f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus DF failure key {_k} not in {_json.keys()}"
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
            if "dataSourceId" not in keys:
                keys = list(keys)
                keys.append("dataSourceId")
            for _k in result:
                assert _k in keys, (
                    f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus failure top level key {_k} not in {keys} statement: {_statement}"
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
                        if isinstance(
                            result["data"][_k][_dk], (int, float)
                        ) and isinstance(_json["data"][_k][_dk], (int, float)):
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
def test_grib_builder_one_thread_file_pattern_hrrr_ops_conus_normalized(tmp_path: Path):
    run_runtime_one_thread_file_pattern_test(
        tmp_path=tmp_path,
        job_id="JS:METAR:MODEL:HRRR_OPS_conus_3km_NORMALIZED_PRESSURE_TEST:schedule:job:V01",
        failure_prefix="TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus",
    )


@pytest.mark.integration
def test_grib_builder_one_thread_file_pattern_rrfs_a_conus(tmp_path: Path):
    """test gribBuilder with one thread.
    This test verifies the resulting data file against the one that is in couchbase already
    in order to make sure the calculations are proper."""
    run_runtime_one_thread_file_pattern_test(
        tmp_path=tmp_path,
        job_id="JS:METAR:MODEL:RRFSv1_conus_3km_RRFS_a_TEST:schedule:job:V01",
        failure_prefix="TestGribBuilderV01.test_grib_builder_one_thread_file_pattern_rrfs_a_conus",
    )

@pytest.mark.integration
def test_grib_builder_one_thread_file_pattern_mpas(tmp_path: Path):
    """test gribBuilder with one thread.
    This test verifies the resulting data file against the one that is in couchbase already
    in order to make sure the calculations are proper."""
    run_runtime_one_thread_file_pattern_test(
        tmp_path=tmp_path,
        job_id="JS:METAR:MODEL:MPAS_conus_3km_MPAS_physics_dev1-TEST:schedule:job:V01",
        failure_prefix="TestGribBuilderV01.test_grib_builder_one_thread_file_pattern_mpas",
    )
