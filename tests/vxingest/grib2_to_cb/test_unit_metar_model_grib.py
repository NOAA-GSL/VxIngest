import os
from pathlib import Path

import pytest
from vxingest.grib2_to_cb.run_ingest_threads import VXIngest


def setup_connection_multiple_ingest_ids():
    """test setup - used to test multiple ingest_document_ids"""
    _vx_ingest = VXIngest()
    _vx_ingest.credentials_file = os.environ["CREDENTIALS"]
    _vx_ingest.cb_credentials = _vx_ingest.get_credentials(_vx_ingest.load_spec)
    _vx_ingest.connect_cb()
    _vx_ingest.load_spec["ingest_document_ids"] = _vx_ingest.collection.get(
        "JOB-TEST:V01:METAR:GRIB2:MODEL:HRRR"
    ).content_as[dict]["ingest_document_ids"]
    return _vx_ingest


def setup_connection():
    """test setup"""
    _vx_ingest = VXIngest()
    _vx_ingest.credentials_file = os.environ["CREDENTIALS"]
    _vx_ingest.cb_credentials = _vx_ingest.get_credentials(_vx_ingest.load_spec)
    _vx_ingest.connect_cb()
    _vx_ingest.load_spec["ingest_document_ids"] = _vx_ingest.collection.get(
        "JOB-TEST:V01:METAR:GRIB2:MODEL:HRRR"
    ).content_as[dict]["ingest_document_ids"]
    return _vx_ingest


@pytest.mark.integration()
def test_credentials_and_load_spec():
    """test the get_credentials and load_spec"""
    vx_ingest = None
    try:
        vx_ingest = setup_connection()
        assert True, vx_ingest.load_spec["cb_connection"]["user"] == "cb_user"
    except Exception as _e:
        pytest.fail(f"test_credentials_and_load_spec Exception failure: {_e}")
    finally:
        vx_ingest.close_cb()


@pytest.mark.integration()
def test_credentials_and_load_spec_multiple_ingest_ids():
    """test the get_credentials and load_spec"""
    vx_ingest = None
    try:
        vx_ingest = setup_connection_multiple_ingest_ids()
        assert True, vx_ingest.load_spec["cb_connection"]["user"] == "cb_user"
    except Exception as _e:
        pytest.fail(f"test_credentials_and_load_spec Exception failure: {_e}")
    finally:
        vx_ingest.close_cb()


@pytest.mark.integration()
def test_cb_connect_disconnect():
    """test the cb connect and close"""
    vx_ingest = None
    try:
        vx_ingest = setup_connection()
        result = vx_ingest.cluster.query("SELECT raw CLOCK_LOCAL() as time")
        local_time = [list(result)[0]]
        assert True, local_time is not None
        vx_ingest.close_cb()
    except Exception as _e:
        pytest.fail(f"test_cb_connect_disconnect Exception failure: {_e}")
    finally:
        vx_ingest.close_cb()


@pytest.mark.integration()
def test_write_load_job_to_files(tmp_path):
    """test write the load job"""
    vx_ingest = None
    try:
        vx_ingest = setup_connection()
        vx_ingest.load_job_id = "test_id"
        vx_ingest.output_dir = tmp_path
        vx_ingest.load_spec["load_job_doc"] = {"test": "a line of text"}
        vx_ingest.write_load_job_to_files()
    except Exception as _e:
        pytest.fail(f"test_write_load_job_to_files Exception failure: {_e}")
    finally:
        vx_ingest.close_cb()


@pytest.mark.integration()
def test_build_load_job_doc(tmp_path):
    """test the build load job"""
    vx_ingest = None
    try:
        vx_ingest = setup_connection()
        vx_ingest.load_job_id = "test_id"
        vx_ingest.path = tmp_path
        vx_ingest.load_spec["load_job_doc"] = {"test": "a line of text"}
        lineage = "CTC"
        ljd = vx_ingest.build_load_job_doc(lineage)
        assert True, ljd["id"].startswith(
            "LJ:METAR:vxingest.grib2_to_cb.run_ingest_threads:VXIngest"
        )
    except Exception as _e:
        pytest.fail(f"test_build_load_job_doc Exception failure: {_e}")
    finally:
        vx_ingest.close_cb()


@pytest.mark.integration()
def test_vxingest_get_file_list(tmp_path):
    """test the vxingest get_file_list"""
    vx_ingest = None
    try:
        vx_ingest = setup_connection()
        vx_ingest.load_job_id = "test_id"
        pattern = "%Y%m%d_%H%M"
        # order is important to see if the files are getting returned sorted by mtime, not name
        Path(tmp_path / "20210920_1701").touch()
        Path(tmp_path / "20210920_1702").touch()
        Path(tmp_path / "20210920_1704").touch()
        Path(tmp_path / "20210920_1705").touch()
        Path(tmp_path / "20210920_1703").touch()
        Path(tmp_path / "20210921_1701").touch()
        Path(tmp_path / "20210922_1701").touch()
        Path(tmp_path / "20210923_1701").touch()
        query = f""" SELECT url, mtime
            From `{vx_ingest.cb_credentials['bucket']}`.{vx_ingest.cb_credentials['scope']}.{vx_ingest.cb_credentials['collection']}
            WHERE
            subset='metar'
            AND type='DF'
            AND fileType='grib2'
            AND originType='model'
            AND model='HRRR_OPS' order by url;"""
        files = vx_ingest.get_file_list(query, tmp_path, "20210920_17*", pattern)
        assert True, files == [
            tmp_path / "20210920_1701",
            tmp_path / "20210920_1702",
            tmp_path / "20210920_1704",
            tmp_path / "20210920_1705",
            tmp_path / "20210920_1703",
        ]
    except Exception as _e:
        pytest.fail(f"test_build_load_job_doc Exception failure: {_e}")
    finally:
        vx_ingest.close_cb()
