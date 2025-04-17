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
            "file_pattern": "sgptropoeC1.c1.20210605.000502.nc",
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
    num_input_files = len(list(input_path.glob("sgptropoeC1.c1.*.nc")))
    num_output_files = len(list(tmp_path.glob("sgptropoeC1.c1.*.json")))
    assert num_output_files == num_input_files, "number of output files is incorrect"

    # Test that the output file matches the content in the database
    # derived_data = json.load(
    #     (tmp_path / "sgptropoeC1.c1.20210605.000502.nc.json").open(encoding="utf-8")
    # )
    # station_id = ""
    # derived_station = {}
    # obs_id = ""
    # derived_obs = {}
    # for item in derived_data:
    #     if item["docType"] == "station" and item["name"] == "KDEN":
    #         station_id = item["id"]
    #         derived_station = item
    #     else:
    #         if item["docType"] == "obs":
    #             obs_id = item["id"]
    #             derived_obs = item
    #     if derived_station and derived_obs:
    #         break
    # # retrieved_station = vx_ingest.collection.get(station_id).content_as[dict]
    # # retrieved_obs = vx_ingest.collection.get(obs_id).content_as[dict]
    # # # make sure the updateTime is the same in both the derived and retrieved station
    # # retrieved_station["updateTime"] = derived_station["updateTime"]
    # # # make sure the firstTime and lastTime are the same in both the derived and retrieved station['geo']
    # # retrieved_station["geo"][0]["firstTime"] = derived_station["geo"][0]["firstTime"]
    # # retrieved_station["geo"][0]["lastTime"] = derived_station["geo"][0]["lastTime"]

    # assert derived_station == retrieved_station

    # assert_dicts_almost_equal(derived_obs, retrieved_obs)
