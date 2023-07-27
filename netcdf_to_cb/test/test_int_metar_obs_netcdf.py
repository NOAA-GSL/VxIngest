""""
    integration tests for netcdf
"""
import os
from glob import glob
from netcdf_to_cb.run_ingest_threads import VXIngest

def setup_connection():
    """test setup"""
    try:
        _vx_ingest = VXIngest()
        _vx_ingest.credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
        _vx_ingest.cb_credentials = _vx_ingest.get_credentials(_vx_ingest.load_spec)
        _vx_ingest.connect_cb()
        _vx_ingest.load_spec['ingest_document_ids'] = _vx_ingest.collection.get("JOB:V01:METAR:NETCDF:OBS").content_as[dict]["ingest_document_ids"]
        return _vx_ingest
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"test_credentials_and_load_spec Exception failure: {_e}"
        return None


def test_one_thread_spedicfy_file_pattern():  # pylint:disable=missing-function-docstring
    try:
        # setup - remove output files
        for _f in glob("/opt/data/netcdf_to_cb/output/test1/*.json"):
            os.remove(_f)
        vx_ingest = VXIngest()
        vx_ingest.runit(
            {
                "job_id": "JOB:V01:METAR:NETCDF:OBS",
                "credentials_file": os.environ["HOME"] + "/adb-cb1-credentials",
                "path": "/opt/data/netcdf_to_cb/input_files",
                "file_name_mask": "%Y%m%d_%H%M",
                "output_dir": "/opt/data/netcdf_to_cb/output/test1",
                "threads": 1,
                "file_pattern": "20211108_0000",
            }
        )
        assert (
            len(
                glob(
                    "/opt/data/netcdf_to_cb/output/test1/[0123456789]???????_[0123456789]???.json"
                )
            )
            > 0
        ), "There are no output files"

        assert (
            len(
                glob(
                    "/opt/data/netcdf_to_cb/output/test1/LJ:METAR:netcdf_to_cb.run_ingest_threads:VXIngest:*.json"
                )
            )
            == 1
        ), "there is no load job output file"

        # use file globbing to see if we got one output file for each input file plus one load job file
        assert len(glob("/opt/data/netcdf_to_cb/output/test1/20211108*.json")) == len(
            glob("/opt/data/netcdf_to_cb/input_files/20211108_0000")
        ), "number of output files is incorrect"
        # teardown remove output files
        for _f in glob("/opt/data/netcdf_to_cb/output/test1/*.json"):
            os.remove(_f)
    except Exception as _e:  # pylint: disable=broad-except
        assert False, f"TestGsdIngestManager Exception failure: {_e}"


def test_two_threads_spedicfy_file_pattern():
    """
    integration test for testing multithreaded capability
    """
    try:
        # setup - remove output files
        for _f in glob("/opt/data/netcdf_to_cb/output/test2/*.json"):
            os.remove(_f)
        vx_ingest = VXIngest()
        vx_ingest.runit(
            {
                "job_id": "JOB:V01:METAR:NETCDF:OBS",
                "credentials_file": os.environ["HOME"] + "/adb-cb1-credentials",
                "path": "/opt/data/netcdf_to_cb/input_files",
                "file_name_mask": "%Y%m%d_%H%M",
                "output_dir": "/opt/data/netcdf_to_cb/output/test2",
                "threads": 2,
                "file_pattern": "20210919*",
            }
        )
        assert (
            len(
                glob(
                    "/opt/data/netcdf_to_cb/output/test2/[0123456789]???????_[0123456789]???.json"
                )
            )
            > 0
        ), "There are no output files"

        assert (
            len(
                glob(
                    "/opt/data/netcdf_to_cb/output/test2/LJ:METAR:netcdf_to_cb.run_ingest_threads:VXIngest:*.json"
                )
            )
            == 1
        ), "there is no load job output file"

        # use file globbing to see if we got one output file for each input file plus one load job file
        assert len(glob("/opt/data/netcdf_to_cb/output/test2/20210919*.json")) == len(
            glob("/opt/data/netcdf_to_cb/input_files/20210919*")
        ), "number of output files is incorrect"

        # teardown remove output files
        for _f in glob("/opt/data/netcdf_to_cb/output/test2/*.json"):
            os.remove(_f)
    except Exception as _e:  # pylint: disable=broad-except
        assert False, f"TestGsdIngestManager Exception failure: {_e}"


def test_one_thread_default():
    """This test will start one thread of the ingestManager and simply make sure it runs with no Exceptions.
    It will attempt to process any files that are in the input directory that atch the file_name_mask.
    TIP: you might want to use local credentials to a local couchbase. If you do
    you will need to run the scripts in the matsmetadata directory to load the local metadata.
    Remove any documents type DD prior to using this test."""
    try:
        # setup - remove output files
        for _f in glob("/opt/data/netcdf_to_cb/output/test3/*.json"):
            os.remove(_f)
        vx_ingest = VXIngest()
        vx_ingest.runit(
            {
                "job_id": "JOB:V01:METAR:NETCDF:OBS",
                "credentials_file": os.environ["HOME"] + "/adb-cb1-credentials",
                "path": "/opt/data/netcdf_to_cb/input_files",
                "file_name_mask": "%Y%m%d_%H%M",
                "output_dir": "/opt/data/netcdf_to_cb/output/test3",
                "file_pattern": "[0123456789]???????_[0123456789]???",
                "threads": 1,
            }
        )
        assert (
            len(
                glob(
                    "/opt/data/netcdf_to_cb/output/test3/[0123456789]???????_[0123456789]???.json"
                )
            )
            > 0
        ), "There are no output files"

        assert (
            len(
                glob(
                    "/opt/data/netcdf_to_cb/output/test3/LJ:METAR:netcdf_to_cb.run_ingest_threads:VXIngest:*.json"
                )
            )
            >= 1
        ), "there is no load job output file"

        # use file globbing to see if we got one output file for each input file plus one load job file
        assert len(
            glob(
                "/opt/data/netcdf_to_cb/output/test3/[0123456789]???????_[0123456789]???.json"
            )
        ) == len(
            glob(
                "/opt/data/netcdf_to_cb/input_files/[0123456789]???????_[0123456789]???"
            )
        ), "number of output files is incorrect"

        # teardown remove output files
        for _f in glob("/opt/data/netcdf_to_cb/output/test3/*.json"):
            os.remove(_f)
    except Exception as _e:  # pylint: disable=broad-except
        assert False, f"TestGsdIngestManager Exception failure: {_e}"


def test_two_threads_default():
    """This test will start one thread of the ingestManager and simply make sure it runs with no Exceptions.
    It will attempt to process any files that are in the input directory that atch the file_name_mask.
    TIP: you might want to use local credentials to a local couchbase. If you do
    you will need to run the scripts in the matsmetadata directory to load the local metadata.
    Remove any documents type DD prior to using this test."""
    try:
        # setup - remove output files
        for _f in glob("/opt/data/netcdf_to_cb/output/test4/*.json"):
            os.remove(_f)
        vx_ingest = VXIngest()
        vx_ingest.runit(
            {
                "job_id": "JOB:V01:METAR:NETCDF:OBS",
                "credentials_file": os.environ["HOME"] + "/adb-cb1-credentials",
                "path": "/opt/data/netcdf_to_cb/input_files",
                "file_name_mask": "%Y%m%d_%H%M",
                "output_dir": "/opt/data/netcdf_to_cb/output/test4",
                "threads": 2,
            }
        )
        assert (
            len(
                glob(
                    "/opt/data/netcdf_to_cb/output/test4/[0123456789]???????_[0123456789]???.json"
                )
            )
            > 0
        ), "There are no output files"

        assert (
            len(
                glob(
                    "/opt/data/netcdf_to_cb/output/test4/LJ:METAR:netcdf_to_cb.run_ingest_threads:VXIngest:*.json"
                )
            )
            >= 1
        ), "there is no load job output file"

        # use file globbing to see if we got one output file for each input file plus one load job file
        assert len(
            glob(
                "/opt/data/netcdf_to_cb/output/test4/[0123456789]???????_[0123456789]???.json"
            )
        ) == len(
            glob(
                "/opt/data/netcdf_to_cb/input_files/[0123456789]???????_[0123456789]???"
            )
        ), "number of output files is incorrect"

        # teardown remove output files
        for _f in glob("/opt/data/netcdf_to_cb/output/test4/*.json"):
            os.remove(_f)
    except Exception as _e:  # pylint: disable=broad-except
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
    except Exception as _e:  # pylint: disable=broad-except
        assert False, f"TestGsdIngestManager Exception failure: {_e}"
