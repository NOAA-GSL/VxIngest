# pylint: disable=missing-module-docstring
import os
from multiprocessing import JoinableQueue
from ctc_to_cb.run_ingest_threads import VXIngest
from ctc_to_cb.vx_ingest_manager import VxIngestManager
from builder_common.load_spec_yaml import LoadYamlSpecFile


def setup_ingest():
    """test setup"""
    try:
        cwd = os.getcwd()
        _vx_ingest = VXIngest()
        _vx_ingest.spec_file = cwd + "/ctc_to_cb/test/test_load_spec_metar_ctc_V01.yaml"
        _vx_ingest.credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
        _load_spec_file = LoadYamlSpecFile({"spec_file": _vx_ingest.spec_file})
        # read in the load_spec file
        _vx_ingest.load_spec = dict(_load_spec_file.read())
        _vx_ingest.cb_credentials = _vx_ingest.get_credentials(_vx_ingest.load_spec)
        _vx_ingest.connect_cb()
        vx_ingest_manager = VxIngestManager(
            "test", _vx_ingest.load_spec, JoinableQueue(), "/tmp"
        )
        assert (
            vx_ingest_manager is not None
        ), "vx_ingest_manager is None and should not be"
        return _vx_ingest, vx_ingest_manager
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"test_credentials_and_load_spec Exception failure: {_e}"


def test_cb_connect_disconnect():
    """test the cb connect and close"""
    try:
        vx_ingest, vx_ingest_manager = setup_ingest()
        vx_ingest_manager.connect_cb()
        result = vx_ingest_manager.cluster.query("SELECT raw CLOCK_LOCAL() as time")
        local_time = [list(result)[0]]
        assert vx_ingest is not None, "vx_ingest is None"
        assert local_time is not None, "local_time from CB should not be None"
        vx_ingest_manager.close_cb()
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"test_cb_connect_disconnect Exception failure: {_e}"
    finally:
        vx_ingest_manager.close_cb()


def test_credentials_and_load_spec():
    """test the get_credentials and load_spec"""
    try:
        vx_ingest, vx_ingest_manager = setup_ingest()
        assert vx_ingest.load_spec["cb_connection"]["user"] == "avid"
        vx_ingest_manager.close_cb()
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"test_credentials_and_load_spec Exception failure: {_e}"
    finally:
        vx_ingest_manager.close_cb()


def test_write_load_job_to_files():
    """test write the load job"""
    try:
        vx_ingest, vx_ingest_manager = setup_ingest()
        vx_ingest.load_job_id = "test_id"
        vx_ingest.output_dir = "/tmp"
        vx_ingest.load_spec["load_job_doc"] = {"test": "a line of text"}
        vx_ingest.write_load_job_to_files()
        os.remove("/tmp/test_id.json")
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"test_write_load_job_to_files Exception failure: {_e}"
    finally:
        vx_ingest_manager.close_cb()


def test_build_load_job_doc():
    """test the build load job"""
    try:
        vx_ingest, vx_ingest_manager = setup_ingest()
        vx_ingest.load_job_id = "test_id"
        vx_ingest.path = "/tmp"
        vx_ingest.load_spec["load_job_doc"] = {"test": "a line of text"}
        vx_ingest.spec_file = "/tmp/test_file"
        ljd = vx_ingest.build_load_job_doc("ctc")
        assert ljd["id"].startswith(
            "LJ:METAR:ctc_to_cb.run_ingest_threads:VXIngest"
        ), f"load_job ID is wrong: {ljd['id']} does not start with 'LJ:METAR:ctc_to_cb.run_ingest_threads:VXIngest'"
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"test_build_load_job_doc Exception failure: {_e}"
    finally:
        vx_ingest_manager.close_cb()
