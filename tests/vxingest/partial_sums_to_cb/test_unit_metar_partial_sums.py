import os
from multiprocessing import JoinableQueue

import pytest

from vxingest.partial_sums_to_cb.run_ingest_threads import VXIngest
from vxingest.partial_sums_to_cb.vx_ingest_manager import VxIngestManager


def setup_ingest():
    """test setup"""
    _vx_ingest = VXIngest()
    _vx_ingest.credentials_file = os.environ["CREDENTIALS"]
    _vx_ingest.load_spec = {}
    _vx_ingest.cb_credentials = _vx_ingest.get_credentials(_vx_ingest.load_spec)
    _vx_ingest.connect_cb()
    _vx_ingest.load_spec["ingest_document_ids"] = _vx_ingest.collection.get(
        "JOB-TEST:V01:METAR:CTC:CEILING:MODEL:OPS"
    ).content_as[dict]["ingest_document_ids"]
    _vx_ingest.load_spec["ingest_documents"] = {}
    for _id in _vx_ingest.load_spec["ingest_document_ids"]:
        _vx_ingest.load_spec["ingest_documents"][_id] = _vx_ingest.collection.get(
            _id
        ).content_as[dict]

    vx_ingest_manager = VxIngestManager(
        "test", _vx_ingest.load_spec, JoinableQueue(), "/tmp", None, None
    )
    assert vx_ingest_manager is not None, "vx_ingest_manager is None and should not be"
    return _vx_ingest, vx_ingest_manager


@pytest.mark.integration
def test_cb_connect_disconnect():
    """test the cb connect and close"""
    vx_ingest_manager = None
    try:
        vx_ingest, vx_ingest_manager = setup_ingest()
        vx_ingest_manager.connect_cb()
        result = vx_ingest_manager.cluster.query("SELECT raw CLOCK_LOCAL() as time")
        local_time = [list(result)[0]]
        assert vx_ingest is not None, "vx_ingest is None"
        assert local_time is not None, "local_time from CB should not be None"
        vx_ingest_manager.close_cb()
    except Exception as _e:
        pytest.fail(f"test_cb_connect_disconnect Exception failure: {_e}")
    finally:
        if vx_ingest_manager is not None:
            vx_ingest_manager.close_cb()


@pytest.mark.integration
def test_credentials_and_load_spec():
    """test the get_credentials and load_spec"""
    vx_ingest_manager = None
    try:
        vx_ingest, vx_ingest_manager = setup_ingest()
        assert vx_ingest.load_spec["cb_connection"]["user"] == "avid"
        vx_ingest_manager.close_cb()
    except Exception as _e:
        pytest.fail(f"test_credentials_and_load_spec Exception failure: {_e}")
    finally:
        if vx_ingest_manager is not None:
            vx_ingest_manager.close_cb()


@pytest.mark.integration
def test_write_load_job_to_files(tmp_path):
    """test write the load job"""
    vx_ingest_manager = None
    try:
        vx_ingest, vx_ingest_manager = setup_ingest()
        vx_ingest.load_job_id = "test_id"
        vx_ingest.output_dir = tmp_path
        vx_ingest.load_spec["load_job_doc"] = {"test": "a line of text"}
        vx_ingest.write_load_job_to_files()
    except Exception as _e:
        pytest.fail(f"test_write_load_job_to_files Exception failure: {_e}")
    finally:
        if vx_ingest_manager is not None:
            vx_ingest_manager.close_cb()


@pytest.mark.integration
def test_build_load_job_doc(tmp_path):
    """test the build load job"""
    vx_ingest_manager = None
    try:
        vx_ingest, vx_ingest_manager = setup_ingest()
        vx_ingest.load_job_id = "test_id"
        vx_ingest.path = tmp_path
        vx_ingest.load_spec["load_job_doc"] = {"test": "a line of text"}
        ljd = vx_ingest.build_load_job_doc("partial_sums")
        assert ljd["id"].startswith(
            "LJ:METAR:vxingest.partial_sums_to_cb.run_ingest_threads:VXIngest"
        ), (
            f"load_job ID is wrong: {ljd['id']} does not start with 'LJ:METAR:vxingest.partial_sums_to_cb.run_ingest_threads:VXIngest'"
        )
    except Exception as _e:
        pytest.fail(f"test_build_load_job_doc Exception failure: {_e}")
    finally:
        if vx_ingest_manager is not None:
            vx_ingest_manager.close_cb()
