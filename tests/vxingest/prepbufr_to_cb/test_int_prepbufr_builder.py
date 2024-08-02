"""
integration tests for prepbufr RAOB's
This test derives a RAOB observations from a prepbufr file and then compares the derived
observation to the observation that is stored in the couchbase database.
Special note on test data:
The test data is located in the directory /opt/data/prepbufr_to_cb/input_files/...
and this data does exist in the couchbase database. The document id of the obs document
is "..." and the data set also includes many station documents.
The data set also includes a data file document "" which is used
by the ingest manager to determine if the data has already been ingested.
If you want to re-import the
data ***you will need to delete the DF document from the couchbase database*** in order
for the data to be re-processed.
If you re-import the data and forget to delete the DF document you will get an error like...
"AssertionError: There are no output files". This integration test is best used for debugging
and development purposes of the PrepbufrRaobsObsBuilderV01 class.
"""

import json
import os
import pathlib
from multiprocessing import Queue
from pathlib import Path

import pytest
import yaml
from vxingest.prepbufr_to_cb.run_ingest_threads import VXIngest


def stub_worker_log_configurer(queue: Queue):
    """A stub to replace log_config.worker_log_configurer"""
    pass


def setup_connection():
    """test setup"""
    _vx_ingest = VXIngest()
    _vx_ingest.credentials_file = os.environ["CREDENTIALS"]
    _vx_ingest.cb_credentials = _vx_ingest.get_credentials(_vx_ingest.load_spec)
    _vx_ingest.cb_credentials["collection"] = "RAOB"
    _vx_ingest.connect_cb()
    _vx_ingest.load_spec["ingest_document_ids"] = _vx_ingest.collection.get(
        "JOB-TEST:V01:RAOB:PREPBUFR:OBS"
    ).content_as[dict]["ingest_document_ids"]
    # load additional mysql configuration
    with pathlib.Path(_vx_ingest.credentials_file).open(encoding="utf-8") as _f:
        _yaml_data = yaml.load(_f, yaml.SafeLoader)
        _vx_ingest.load_spec["_mysql_host"] = _yaml_data["mysql_host"]
        _vx_ingest.load_spec["_mysql_user"] = _yaml_data["mysql_user"]
        _vx_ingest.load_spec["_mysql_pwd"] = _yaml_data["mysql_password"]
    return _vx_ingest


def assert_dicts_almost_equal(dict1, dict2, rel_tol=1e-09):
    """Utility function to compare potentially nested dictionaries containing floats"""
    assert set(dict1.keys()) == set(
        dict2.keys()
    ), "Dictionaries do not have the same keys"
    for key in dict1:
        if isinstance(dict1[key], dict):
            assert_dicts_almost_equal(dict1[key], dict2[key], rel_tol)
        else:
            try:
                assert dict1[key] == pytest.approx(
                    dict2[key], rel=rel_tol, nan_ok=True
                ), (
                    "Derived and retrieved values for key: "
                    + str(key)
                    + " do not match - "
                    + str(dict1[key])
                    + " does not match "
                    + str(dict2[key])
                )
            except Exception as e:
                print("failed:" + e)


@pytest.mark.integration()
def test_one_thread_specify_file_pattern(tmp_path: Path):
    """Note: this test takes a long time to run (few minutes)"""
    log_queue = Queue()
    vx_ingest = setup_connection()
    stations = [
        "70026",
        "72393",
        "74794",
        "71119",
        "76225",
        "76256",
        "76458",
        "76526",
        "76595",
        "76612",
        "76644",
        "76654",
        "76679",
        "76692",
        "76743",
        "76903",
        "78384",
        "78397",
        "78486",
        "78526",
        "78583",
        "78954",
        "78970",
        "82022",
        "82026",
        "82099",
        "82107",
        "82193",
        "82244",
        "82332",
        "82411",
        "82532",
        "82599",
        "82705",
    ]
    print("Testing stations: ", stations)
    print(f"output path is: {tmp_path}")
    vx_ingest.write_data_for_station_list = stations
    vx_ingest.write_data_for_levels = [200, 300, 500, 700, 900]
    vx_ingest.runit(
        {
            "job_id": "JOB-TEST:V01:RAOB:PREPBUFR:OBS",
            "credentials_file": os.environ["CREDENTIALS"],
            "file_name_mask": "%y%j%H%M",  # only tests the first part of the file name i.e. 241011200.gdas.t12z.prepbufr.nr -> 241011200
            "output_dir": f"{tmp_path}",
            "threads": 1,
            "file_pattern": "242130000*",  # specifically /opt/data/prepbufr_to_cb/input_files/242130000.gdas.t00z.prepbufr.nr,
            # "file_pattern": "242131200*",  # specifically /opt/data/prepbufr_to_cb/input_files/242131200.gdas.t00z.prepbufr.nr,
            # "file_pattern": "242121800*",  # specifically /opt/data/prepbufr_to_cb/input_files/242121800.gdas.t00z.prepbufr.nr,
            # "file_pattern": "241570000*",  # specifically /opt/data/prepbufr_to_cb/input_files/241570000.gdas.t00z.prepbufr.nr,
        },
        log_queue,
        stub_worker_log_configurer,
    )

    # Test that we have one or more output files
    output_file_list = list(
        tmp_path.glob(
            "[0123456789]????????.gdas.t[0123456789][0123456789]z.prepbufr.nr.json"
        )
    )

    # Test that we have one "load job" ("LJ") document
    lj_doc_regex = "LJ:RAOB:vxingest.prepbufr_to_cb.run_ingest_threads:VXIngest:*.json"
    num_load_job_files = len(list(tmp_path.glob(lj_doc_regex)))
    assert (
        num_load_job_files >= 1
    ), f"Number of load job files is incorrect {num_load_job_files} is not >= 1"

    # Test that we have one output file per input file
    input_path = Path("/opt/data/prepbufr_to_cb/input_files")
    num_input_files = len(list(input_path.glob("242130000*")))
    #num_input_files = len(list(input_path.glob("242131200*")))
    #num_input_files = len(list(input_path.glob("242121800*")))
    #num_input_files = len(list(input_path.glob("241011200*")))
    num_output_files = len(output_file_list)
    assert (
        num_output_files == num_input_files
    ), f"number of output files is incorrect {num_output_files} != {num_input_files}"

    # Test that the output file matches the content in the database

    derived_data = json.load((output_file_list[0]).open(encoding="utf-8"))
    station_id = ""
    derived_station = {}
    obs_id = ""
    derived_obs = {}
    for item in derived_data:
        try:
            if "docType" not in item:
                continue
            if item["docType"] == "station":
                station_id = item["id"]
                derived_station = item
                retrieved_station = vx_ingest.collection.get(station_id).content_as[dict]
                # make sure the updateTime is the same in both the derived and retrieved station
                retrieved_station["updateTime"] = derived_station["updateTime"]
                # make sure the firstTime and lastTime are the same in both the derived and retrieved station['geo']
                retrieved_station["geo"][0]["firstTime"] = derived_station["geo"][0][
                    "firstTime"
                ]
                retrieved_station["geo"][0]["lastTime"] = derived_station["geo"][0][
                    "lastTime"
                ]
                assert f"derived station{station_id} does not equal retrieved station", (
                    derived_station == retrieved_station
                )
        except Exception as e:
            print("failed:" + str(e))
            print("station_id", station_id)
            raise e
        else:
            try:
                if item["docType"] == "obs":
                    obs_id = item["id"]
                    derived_obs = item
                    retrieved_obs = vx_ingest.collection.get(obs_id).content_as[dict]
                    assert_dicts_almost_equal(derived_obs, retrieved_obs)
            except Exception as e:
                print("failed:" + str(e))
                print("obs_id", obs_id)
                raise e
            else:
                continue
