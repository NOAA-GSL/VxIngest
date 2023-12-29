""""
    integration tests for netcdf
"""
import os
from multiprocessing import Queue
from pathlib import Path

from vxingest.netcdf_to_cb.run_ingest_threads import VXIngest


def stub_worker_log_configurer(queue: Queue):
    """A stub to replace log_config.worker_log_configurer"""
    pass


def setup_connection():
    """test setup"""
    _vx_ingest = VXIngest()
    _vx_ingest.credentials_file = (os.environ["CREDENTIALS"],)
    _vx_ingest.cb_credentials = _vx_ingest.get_credentials(_vx_ingest.load_spec)
    _vx_ingest.connect_cb()
    _vx_ingest.load_spec["ingest_document_ids"] = _vx_ingest.collection.get(
        "JOB-TEST:V01:METAR:NETCDF:OBS"
    ).content_as[dict]["ingest_document_ids"]
    return _vx_ingest


def test_one_thread_specify_file_pattern(tmp_path):
    log_queue = Queue()
    vx_ingest = VXIngest()
    vx_ingest.runit(
        {
            "job_id": "JOB-TEST:V01:METAR:NETCDF:OBS",
            "credentials_file": os.environ["CREDENTIALS"],
            "file_name_mask": "%Y%m%d_%H%M",
            "output_dir": f"{tmp_path}",
            "threads": 1,
            "file_pattern": "20211108_0000",
        },
        log_queue,
        stub_worker_log_configurer,
    )
    assert (
        len(list(tmp_path.glob("[0123456789]???????_[0123456789]???.json"))) > 0
    ), "There are no output files"

    lj_doc_regex = "LJ:METAR:vxingest.netcdf_to_cb.run_ingest_threads:VXIngest:*.json"
    assert (
        len(list(tmp_path.glob(lj_doc_regex))) == 1
    ), "there is no load job output file"

    # use file globbing to see if we got one output file for each input file plus one load job file
    input_path = Path("/opt/data/netcdf_to_cb/input_files")
    assert len(list(tmp_path.glob("20211108*.json"))) == len(
        list(input_path.glob("20211108_0000"))
    ), "number of output files is incorrect"


def test_two_threads_spedicfy_file_pattern(tmp_path):
    """
    integration test for testing multithreaded capability
    """
    log_queue = Queue()
    vx_ingest = VXIngest()
    vx_ingest.runit(
        {
            "job_id": "JOB-TEST:V01:METAR:NETCDF:OBS",
            "credentials_file": os.environ["CREDENTIALS"],
            "file_name_mask": "%Y%m%d_%H%M",
            "output_dir": f"{tmp_path}",
            "threads": 2,
            "file_pattern": "20211105*",
        },
        log_queue,
        stub_worker_log_configurer,
    )
    assert (
        len(list(tmp_path.glob("[0123456789]???????_[0123456789]???.json"))) > 0
    ), "There are no output files"

    lj_doc_regex = "LJ:METAR:vxingest.netcdf_to_cb.run_ingest_threads:VXIngest:*.json"
    assert (
        len(list(tmp_path.glob(lj_doc_regex))) == 1
    ), "there is no load job output file"

    # use file globbing to see if we got one output file for each input file plus one load job file
    input_path = Path("/opt/data/netcdf_to_cb/input_files")
    assert len(list(tmp_path.glob("20211105*.json"))) == len(
        list(input_path.glob("20211105*"))
    ), "number of output files is incorrect"


def test_one_thread_default(tmp_path):
    """This test will start one thread of the ingestManager and simply make sure it runs with no Exceptions.
    It will attempt to process any files that are in the input directory that match the file_name_mask.
    TIP: you might want to use local credentials to a local couchbase. If you do
    you will need to run the scripts in the matsmetadata directory to load the local metadata.
    """
    log_queue = Queue()
    vx_ingest = VXIngest()
    vx_ingest.runit(
        {
            "job_id": "JOB-TEST:V01:METAR:NETCDF:OBS",
            "credentials_file": os.environ["CREDENTIALS"],
            "file_name_mask": "%Y%m%d_%H%M",
            "output_dir": f"{tmp_path}",
            "file_pattern": "[0123456789]???????_[0123456789]???",
            "threads": 1,
        },
        log_queue,
        stub_worker_log_configurer,
    )
    assert (
        len(list(tmp_path.glob("[0123456789]???????_[0123456789]???.json"))) > 0
    ), "There are no output files"

    lj_doc_regex = "LJ:METAR:vxingest.netcdf_to_cb.run_ingest_threads:VXIngest:*.json"
    assert (
        len(list(tmp_path.glob(lj_doc_regex))) >= 1
    ), "there is no load job output file"

    # use file globbing to see if we got one output file for each input file plus one load job file
    input_path = Path("/opt/data/netcdf_to_cb/input_files")
    assert len(list(tmp_path.glob("[0123456789]???????_[0123456789]???.json"))) == len(
        list(input_path.glob("[0123456789]???????_[0123456789]???"))
    ), "number of output files is incorrect"


def test_two_threads_default(tmp_path):
    """This test will start one thread of the ingestManager and simply make sure it runs with no Exceptions.
    It will attempt to process any files that are in the input directory that atch the file_name_mask.
    TIP: you might want to use local credentials to a local couchbase. If you do
    you will need to run the scripts in the matsmetadata directory to load the local metadata.
    """
    log_queue = Queue()
    vx_ingest = VXIngest()
    vx_ingest.runit(
        {
            "job_id": "JOB-TEST:V01:METAR:NETCDF:OBS",
            "credentials_file": os.environ["CREDENTIALS"],
            "file_name_mask": "%Y%m%d_%H%M",
            "output_dir": f"{tmp_path}",
            "threads": 2,
        },
        log_queue,
        stub_worker_log_configurer,
    )
    assert (
        len(list(tmp_path.glob("[0123456789]???????_[0123456789]???.json"))) > 0
    ), "There are no output files"

    lj_doc_regex = "LJ:METAR:vxingest.netcdf_to_cb.run_ingest_threads:VXIngest:*.json"
    assert (
        len(list(tmp_path.glob(lj_doc_regex))) >= 1
    ), "there is no load job output file"

    # use file globbing to see if we got one output file for each input file plus one load job file
    input_path = Path("/opt/data/netcdf_to_cb/input_files")
    assert len(list(tmp_path.glob("[0123456789]???????_[0123456789]???.json"))) == len(
        list(input_path.glob("[0123456789]???????_[0123456789]???"))
    ), "number of output files is incorrect"
