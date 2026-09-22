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
    # load additional mysql configuration
    with pathlib.Path(_vx_ingest.credentials_file).open(encoding="utf-8") as _f:
        _yaml_data = yaml.load(_f, yaml.SafeLoader)
        _vx_ingest.load_spec["_mysql_host"] = _yaml_data["mysql_host"]
        _vx_ingest.load_spec["_mysql_user"] = _yaml_data["mysql_user"]
        _vx_ingest.load_spec["_mysql_pwd"] = _yaml_data["mysql_password"]
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
                print("failed:" + str(e))


def run_ingest_case(
    tmp_path: Path,
    job_id: str,
):
    """Run one ingest case and perform common output assertions.

    Args:
        tmp_path (Path): pytest temporary directory for generated files.
        job_id (str): RUNTIME job document id.

    Returns:
        tuple: (vx_ingest, input_data_path, file_mask, file_pattern, output_file_list)
    """
    log_queue = Queue()
    vx_ingest = setup_connection()

    runtime_collection = (
        vx_ingest.cluster.bucket("vxdata").scope("_default").collection("RUNTIME")
    )
    job_spec = runtime_collection.get(job_id).content_as[dict]
    process_id = job_spec["processSpecIds"][0]
    process_spec = runtime_collection.get(process_id).content_as[dict]
    ingest_document_ids = process_spec["ingestDocumentIds"]
    data_source_id = process_spec["dataSourceId"]
    data_source_spec = runtime_collection.get(data_source_id).content_as[dict]
    collection = process_spec["subset"]
    input_data_path = data_source_spec["sourceDataUri"]
    file_pattern = data_source_spec.get("filePattern", "*")
    file_mask = data_source_spec.get("fileMask", None)
    output_path_str = f"{tmp_path}"
    vx_ingest.runit(
        {
            "job_id": job_id,
            "credentials_file": os.environ["CREDENTIALS"],
            "collection": collection,
            "file_mask": file_mask,
            "input_data_path": input_data_path,
            "ingest_document_ids": ingest_document_ids,
            "output_dir": output_path_str,
            "threads": 1,
            "file_pattern": file_pattern,
        },
        log_queue,
        stub_worker_log_configurer,
    )
    try:
        output_file_list = list(tmp_path.glob("*.json"))
        assert len(output_file_list) > 0, "There are no output files"
        num_load_job_files = len(list(tmp_path.glob("LJ*.json")))
        assert num_load_job_files == 1, "there is no load job output file"
        return vx_ingest, input_data_path, file_mask, file_pattern, output_file_list
    except Exception as _e:
        raise AssertionError(f"Exception: {_e}") from _e


@pytest.mark.integration
def test_one_thread_specify_file_pattern(tmp_path: Path):
    """Note: this test takes a long time to run (few minutes)"""
    try:
        vx_ingest = setup_connection()
        try:
            job_id = "JS:RAOB:OBS:PREPBUFR-TEST:schedule:job:V01"
            (
                vx_ingest,
                _input_data_path,
                _file_mask,
                file_pattern,
                output_file_list,
            ) = run_ingest_case(
                tmp_path=tmp_path,
                job_id=job_id,
            )
        except Exception as e:
            raise AssertionError(f"Exception: {e}") from e
        # Test that we have one or more output files
        output_file_list = list(
            tmp_path.glob(
                "__opt__data__prepbufr_to_cb__input_files__[0123456789]????????.gdas.t[0123456789][0123456789]z.prepbufr.nr.json"
            )
        )

        # Test that we have one "load job" ("LJ") document
        lj_doc_regex = "LJ:RAOB:vxingest.prepbufr_to_cb.run_ingest_threads:VXIngest:[0123456789]*.json"
        num_load_job_files = len(list(tmp_path.glob(lj_doc_regex)))
        assert num_load_job_files >= 1, (
            f"Number of load job files is incorrect {num_load_job_files} is not >= 1"
        )

        # Test that we have one output file per input file
        input_path = Path(_input_data_path.removeprefix("file://"))
        num_input_files = len(list(input_path.glob(file_pattern + "*")))
        num_output_files = len(output_file_list)
        assert num_output_files == num_input_files, (
            f"number of output files is incorrect {num_output_files} != {num_input_files}"
        )

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
                    retrieved_station = vx_ingest.collection.get(station_id).content_as[
                        dict
                    ]
                    # make sure the updateTime is the same in both the derived and retrieved station
                    retrieved_station["updateTime"] = derived_station["updateTime"]
                    # make sure the firstTime and lastTime are the same in both the derived and retrieved station['geo']
                    retrieved_station["geo"][0]["firstTime"] = derived_station["geo"][
                        0
                    ]["firstTime"]
                    retrieved_station["geo"][0]["lastTime"] = derived_station["geo"][0][
                        "lastTime"
                    ]
                    assert (
                        f"derived station{station_id} does not equal retrieved station"
                    ), derived_station == retrieved_station
            except Exception as e1:
                print("failed:" + str(e1))
                print("station_id", station_id)
                raise e1
            else:
                try:
                    if item["docType"] == "obs":
                        obs_id = item["id"]
                        derived_obs = item
                        retrieved_obs = vx_ingest.collection.get(obs_id).content_as[
                            dict
                        ]
                        assert_dicts_almost_equal(derived_obs, retrieved_obs)
                except Exception as e2:
                    print("failed:" + str(e2))
                    print("obs_id", obs_id)
                    raise e2
                else:
                    continue
    except Exception as e3:
        raise AssertionError(f"Exception: {e3}") from e3
