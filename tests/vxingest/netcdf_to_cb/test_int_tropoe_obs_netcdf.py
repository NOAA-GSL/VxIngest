""" "
integration tests for netcdf tropoe
This test derives a tropoe observation from a netcdf file and then compares the derived
observation to the observation that is stored in the couchbase database.
Special note on test data:
The test data is located in the directory /opt/data/fireweather/input_files/sgptropoeC1.c1.20210605.000502.nc
and this data does exist in the couchbase database. The document id of the obs document
is "DD-TEST:" and the data set also includes many validTime documents for the station sgp.
The data set aslo includes a data file document "DF:METAR:netcdf:tropoe:..." which is used
by the ingest manager to determine if the data has already been ingested.
If you want to re-import the
data ***you will need to delete the DF document from the couchbase database*** in order
for the data to be re-processed.
If you re-import the data and forget to delete the DF document you will get an error like...
"AssertionError: There are no output files".
"""

# import json
import json
import os
from multiprocessing import Queue
from pathlib import Path

import pytest

from vxingest.netcdf_to_cb.run_ingest_threads import VXIngest


def stub_worker_log_configurer(queue: Queue):
    """A stub to replace log_config.worker_log_configurer"""
    pass


def setup_connection():
    """test setup"""
    _vx_ingest = VXIngest()
    _vx_ingest.credentials_file = (os.environ["CREDENTIALS"],)
    _vx_ingest.cb_credentials = _vx_ingest.get_credentials(_vx_ingest.load_spec)
    # override the collection to TROPOE
    _vx_ingest.load_spec["cb_connection"]["collection"] = "TROPOE"
    _vx_ingest.connect_cb()
    _vx_ingest.load_spec["ingest_document_ids"] = _vx_ingest.collection.get(
        "JOB-TEST:V01:TROPOE:NETCDF:OBS"
    ).content_as[dict]["ingest_document_ids"]
    return _vx_ingest


def assert_dicts_almost_equal(dict1, dict2, rel_tol=1e-09):
    """Utility function to compare potentially nested dictionaries containing floats"""
    assert set(dict1.keys()) == set(dict2.keys()), (
        "Dictionaries do not have the same keys"
    )
    for key in dict1:
        if isinstance(dict1[key], dict):
            assert_dicts_almost_equal(dict1[key], dict2[key], rel_tol)
        else:
            assert dict1[key] == pytest.approx(dict2[key], rel=rel_tol), (
                f"Values for {key} do not match"
            )


@pytest.mark.integration
def test_one_thread_specify_file_pattern(tmp_path):
    log_queue = Queue()
    vx_ingest = VXIngest()
    vx_ingest.runit(
        {
            "job_id": "JOB-TEST:V01:TROPOE:NETCDF:OBS",
            "credentials_file": os.environ["CREDENTIALS"],
            "file_name_mask": "sgptropoeC1.c1.%Y%m%d.%H%M%S.nc",
            "output_dir": f"{tmp_path}",
            "threads": 1,
            "file_pattern": "*.nc",
        },
        log_queue,
        stub_worker_log_configurer,
    )

    # Test that we have one or more output files
    output_file_list = list(tmp_path.glob("*.json"))
    assert len(output_file_list) > 0, "There are no output files"

    # Test that we have one "load job" ("LJ") document
    lj_doc_regex = "LJ:TROPOE:vxingest.netcdf_to_cb.run_ingest_threads:VXIngest:*.json"
    num_load_job_files = len(list(tmp_path.glob(lj_doc_regex)))
    assert num_load_job_files == 1, "there is no load job output file"

    # Test that we have one output file per input file
    input_path = Path("/opt/data/fireweather/input_files")
    num_input_files = len(list(input_path.glob("*.nc")))
    output_files = list(tmp_path.glob("*nc.json"))
    num_output_files = len(output_files)
    assert num_output_files == num_input_files, "number of output files is incorrect"

    # Test that the output file matches the content in the database
    try:
        derived_data = json.load((output_files[0]).open(encoding="utf-8"))
        obs_id = derived_data[0]["id"]
        derived_record = [d for d in derived_data if d["id"] == obs_id]
        retrieved_record = vx_ingest.collection.get(obs_id).content_as[dict]
        assert derived_record[0]["validTime"] == retrieved_record["validTime"], (
            "derived and retrieved validTime do not match"
        )
        assert_dicts_almost_equal(derived_record[0], retrieved_record)
    except Exception as _e:
        print(f"*** test_one_thread_specify_file_pattern: Exception: {str(_e)}")
        pytest.fail(f"*** test_one_thread_specify_file_pattern: Exception: {str(_e)}")
    finally:
        # cleanup
        for output_file in output_file_list:
            output_file.unlink()
