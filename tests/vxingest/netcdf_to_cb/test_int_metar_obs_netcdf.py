""""
    integration tests for netcdf
"""
import os
from glob import glob
from multiprocessing import Queue

from vxingest.netcdf_to_cb.run_ingest_threads import VXIngest


def stub_worker_log_configurer(queue: Queue):
    """A stub to replace log_config.worker_log_configurer"""
    pass


def setup_connection():
    """test setup"""
    try:
        _vx_ingest = VXIngest()
        _vx_ingest.credentials_file = (os.environ["CREDENTIALS"],)
        _vx_ingest.cb_credentials = _vx_ingest.get_credentials(_vx_ingest.load_spec)
        _vx_ingest.connect_cb()
        _vx_ingest.load_spec["ingest_document_ids"] = _vx_ingest.collection.get(
            "JOB-TEST:V01:METAR:NETCDF:OBS"
        ).content_as[dict]["ingest_document_ids"]
        return _vx_ingest
    except Exception as _e:
        assert False, f"test_credentials_and_load_spec Exception failure: {_e}"
        return None


def test_one_thread_specify_file_pattern(tmp_path):
    try:
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
            len(glob(f"{tmp_path}/[0123456789]???????_[0123456789]???.json")) > 0
        ), "There are no output files"

        assert (
            len(
                glob(
                    f"{tmp_path}/LJ:METAR:vxingest.netcdf_to_cb.run_ingest_threads:VXIngest:*.json"
                )
            )
            == 1
        ), "there is no load job output file"

        # use file globbing to see if we got one output file for each input file plus one load job file
        assert len(glob(f"{tmp_path}/20211108*.json")) == len(
            glob("/opt/data/netcdf_to_cb/input_files/20211108_0000")
        ), "number of output files is incorrect"
    except Exception as _e:
        assert False, f"TestGsdIngestManager Exception failure: {_e}"


def test_two_threads_spedicfy_file_pattern(tmp_path):
    """
    integration test for testing multithreaded capability
    """
    try:
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
            len(glob(f"{tmp_path}/[0123456789]???????_[0123456789]???.json")) > 0
        ), "There are no output files"

        assert (
            len(
                glob(
                    f"{tmp_path}/LJ:METAR:vxingest.netcdf_to_cb.run_ingest_threads:VXIngest:*.json"
                )
            )
            == 1
        ), "there is no load job output file"

        # use file globbing to see if we got one output file for each input file plus one load job file
        assert len(glob(f"{tmp_path}/20211105*.json")) == len(
            glob("/opt/data/netcdf_to_cb/input_files/20211105*")
        ), "number of output files is incorrect"
    except Exception as _e:
        assert False, f"TestGsdIngestManager Exception failure: {_e}"


def test_one_thread_default(tmp_path):
    """This test will start one thread of the ingestManager and simply make sure it runs with no Exceptions.
    It will attempt to process any files that are in the input directory that match the file_name_mask.
    TIP: you might want to use local credentials to a local couchbase. If you do
    you will need to run the scripts in the matsmetadata directory to load the local metadata.
    """
    try:
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
            len(glob(f"{tmp_path}/[0123456789]???????_[0123456789]???.json")) > 0
        ), "There are no output files"

        assert (
            len(
                glob(
                    f"{tmp_path}/LJ:METAR:vxingest.netcdf_to_cb.run_ingest_threads:VXIngest:*.json"
                )
            )
            >= 1
        ), "there is no load job output file"

        # use file globbing to see if we got one output file for each input file plus one load job file
        assert len(glob(f"{tmp_path}/[0123456789]???????_[0123456789]???.json")) == len(
            glob(
                "/opt/data/netcdf_to_cb/input_files/[0123456789]???????_[0123456789]???"
            )
        ), "number of output files is incorrect"

    except Exception as _e:
        assert False, f"TestGsdIngestManager Exception failure: {_e}"


def test_two_threads_default(tmp_path):
    """This test will start one thread of the ingestManager and simply make sure it runs with no Exceptions.
    It will attempt to process any files that are in the input directory that atch the file_name_mask.
    TIP: you might want to use local credentials to a local couchbase. If you do
    you will need to run the scripts in the matsmetadata directory to load the local metadata.
    """
    try:
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
            len(glob(f"{tmp_path}/[0123456789]???????_[0123456789]???.json")) > 0
        ), "There are no output files"

        assert (
            len(
                glob(
                    f"{tmp_path}/LJ:METAR:vxingest.netcdf_to_cb.run_ingest_threads:VXIngest:*.json"
                )
            )
            >= 1
        ), "there is no load job output file"

        # use file globbing to see if we got one output file for each input file plus one load job file
        assert len(glob(f"{tmp_path}/[0123456789]???????_[0123456789]???.json")) == len(
            glob(
                "/opt/data/netcdf_to_cb/input_files/[0123456789]???????_[0123456789]???"
            )
        ), "number of output files is incorrect"
    except Exception as _e:
        assert False, f"TestGsdIngestManager Exception failure: {_e}"


def check_mismatched_fcst_valid_epoch_to_id():
    """This is a simple ultility test that can be used to see if there are
    any missmatched fcstValidEpoch values among the observations i.e. the fcstValidEpoch in the id
    does not match the fcstValidEpoch in the top level fcstValidEpoch field"""
    try:
        vx_ingest = setup_connection()
        cluster = vx_ingest.cluster
        result = cluster.query(
            f"""
            select METAR.fcstValidEpoch, METAR.id
            FROM `{vx_ingest.cb_credentials['bucket']}`.{vx_ingest.cb_credentials['scope']}.{vx_ingest.cb_credentials['collection']}
            WHERE
                docType = "obs"
                AND subset = "METAR"
                AND type = "DD"
                AND version = "V01"
                AND NOT CONTAINS(id,to_string(fcstValidEpoch)) """
        )
        for row in result:
            assert False, f"These do not have the same fcstValidEpoch: {str(row['fcstValidEpoch']) + row['id']}"
    except Exception as _e:
        assert False, f"TestGsdIngestManager Exception failure: {_e}"
