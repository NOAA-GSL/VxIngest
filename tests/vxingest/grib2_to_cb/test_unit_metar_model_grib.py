# pylint: disable=missing-module-docstring
import os
import shutil
from pathlib import Path

from vxingest.grib2_to_cb.run_ingest_threads import VXIngest


def setup_connection_multiple_ingest_ids():
    """test setup - used to test multiple ingest_document_ids"""
    try:
        _vx_ingest = VXIngest()
        _vx_ingest.credentials_file = os.environ["CREDENTIALS"]
        _vx_ingest.cb_credentials = _vx_ingest.get_credentials(_vx_ingest.load_spec)
        _vx_ingest.connect_cb()
        _vx_ingest.load_spec["ingest_document_ids"] = _vx_ingest.collection.get(
            "JOB-TEST:V01:METAR:GRIB2:MODEL:HRRR"
        ).content_as[dict]["ingest_document_ids"]
        return _vx_ingest
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"test_credentials_and_load_spec Exception failure: {_e}"
        return None


def setup_connection():
    """test setup"""
    try:
        _vx_ingest = VXIngest()
        _vx_ingest.credentials_file = os.environ["CREDENTIALS"]
        _vx_ingest.cb_credentials = _vx_ingest.get_credentials(_vx_ingest.load_spec)
        _vx_ingest.connect_cb()
        _vx_ingest.load_spec["ingest_document_ids"] = _vx_ingest.collection.get(
            "JOB-TEST:V01:METAR:GRIB2:MODEL:HRRR"
        ).content_as[dict]["ingest_document_ids"]
        return _vx_ingest
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"test_credentials_and_load_spec Exception failure: {_e}"
        return None


def test_credentials_and_load_spec():
    """test the get_credentials and load_spec"""
    vx_ingest = None
    try:
        vx_ingest = setup_connection()
        assert True, vx_ingest.load_spec["cb_connection"]["user"] == "cb_user"
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"test_credentials_and_load_spec Exception failure: {_e}"
    finally:
        vx_ingest.close_cb()


def test_credentials_and_load_spec_multiple_ingest_ids():
    """test the get_credentials and load_spec"""
    vx_ingest = None
    try:
        vx_ingest = setup_connection_multiple_ingest_ids()
        assert True, vx_ingest.load_spec["cb_connection"]["user"] == "cb_user"
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"test_credentials_and_load_spec Exception failure: {_e}"
    finally:
        vx_ingest.close_cb()


def test_cb_connect_disconnect():
    """test the cb connect and close"""
    vx_ingest = None
    try:
        vx_ingest = setup_connection()
        result = vx_ingest.cluster.query("SELECT raw CLOCK_LOCAL() as time")
        local_time = [list(result)[0]]
        assert True, local_time is not None
        vx_ingest.close_cb()
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"test_cb_connect_disconnect Exception failure: {_e}"
    finally:
        vx_ingest.close_cb()


def test_write_load_job_to_files():
    """test write the load job"""
    vx_ingest = None
    try:
        vx_ingest = setup_connection()
        vx_ingest.load_job_id = "test_id"
        vx_ingest.output_dir = "/tmp"
        vx_ingest.load_spec["load_job_doc"] = {"test": "a line of text"}
        vx_ingest.write_load_job_to_files()
        os.remove("/tmp/test_id.json")
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"test_write_load_job_to_files Exception failure: {_e}"
    finally:
        vx_ingest.close_cb()


def test_build_load_job_doc():
    """test the build load job"""
    vx_ingest = None
    try:
        vx_ingest = setup_connection()
        vx_ingest.load_job_id = "test_id"
        vx_ingest.path = "/tmp"
        vx_ingest.load_spec["load_job_doc"] = {"test": "a line of text"}
        lineage = "CTC"
        ljd = vx_ingest.build_load_job_doc(lineage)
        assert True, ljd["id"].startswith(
            "LJ:METAR:vxingest.grib2_to_cb.run_ingest_threads:VXIngest"
        )
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"test_build_load_job_doc Exception failure: {_e}"
    finally:
        vx_ingest.close_cb()


def test_vxingest_get_file_list():
    """test the vxingest get_file_list"""
    vx_ingest = None
    try:
        vx_ingest = setup_connection()
        vx_ingest.load_job_id = "test_id"
        if os.path.exists("/tmp/test"):
            shutil.rmtree("/tmp/test")
        os.mkdir("/tmp/test")
        # order is important to see if the files are getting returned sorted by mtime
        Path("/tmp/test/f_fred_01").touch()
        Path("/tmp/test/f_fred_02").touch()
        Path("/tmp/test/f_fred_04").touch()
        Path("/tmp/test/f_fred_05").touch()
        Path("/tmp/test/f_fred_03").touch()
        Path("/tmp/test/f_1_fred_01").touch()
        Path("/tmp/test/f_2_fred_01").touch()
        Path("/tmp/test/f_3_fred_01").touch()
        query = f""" SELECT url, mtime
            From `{vx_ingest.cb_credentials['bucket']}`.{vx_ingest.cb_credentials['scope']}.{vx_ingest.cb_credentials['collection']}
            WHERE
            subset='metar'
            AND type='DF'
            AND fileType='grib2'
            AND originType='model'
            AND model='HRRR_OPS' order by url;"""
        files = vx_ingest.get_file_list(query, "/tmp/test", "f_fred_*")
        assert True, files == [
            "/tmp/test/f_fred_01",
            "/tmp/test/f_fred_02",
            "/tmp/test/f_fred_04",
            "/tmp/test/f_fred_05",
            "/tmp/test/f_fred_03",
        ]
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"test_build_load_job_doc Exception failure: {_e}"
    finally:
        shutil.rmtree("/tmp/test")
        vx_ingest.close_cb()
