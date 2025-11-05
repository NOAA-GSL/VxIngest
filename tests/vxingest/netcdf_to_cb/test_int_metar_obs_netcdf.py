""" "
integration tests for netcdf
This test derives a METAR observation from a netcdf file and then compares the derived
observation to the observation that is stored in the couchbase database.
Special note on test data:
The test data is located in the directory /opt/data/netcdf_to_cb/input_files/20211108_0000
and this data does exist in the couchbase database. The document id of the obs document
is "DD-TEST:V01:METAR:obs:1636329600" and the data set also includes many station documents.
The data set aslo includes a data file document "DF:METAR:netcdf:madis:20211108_0000" which is used
by the ingest manager to determine if the data has already been ingested.
If you want to re-import the
data ***you will need to delete the DF document from the couchbase database*** in order
for the data to be re-processed.
If you re-import the data and forget to delete the DF document you will get an error like...
"AssertionError: There are no output files".
"""

import json
import os
from multiprocessing import Queue
from pathlib import Path

import pytest
from couchbase.options import QueryOptions

from vxingest.netcdf_to_cb.netcdf_builder_parent import NetcdfBuilder  # noqa: F401
from vxingest.netcdf_to_cb.run_ingest_threads import VXIngest


def stub_worker_log_configurer(queue: Queue):
    """A stub to replace log_config.worker_log_configurer"""
    pass


def setup_connection():
    """test setup"""
    _vx_ingest = VXIngest()
    # Ensure credentials_file is a string, not a tuple
    credentials = os.environ["CREDENTIALS"]
    if isinstance(credentials, tuple):
        credentials = credentials[0]
    _vx_ingest.credentials_file = credentials
    _vx_ingest.cb_credentials = _vx_ingest.get_credentials(_vx_ingest.load_spec)
    _vx_ingest.connect_cb()
    try:
        vx_ingest = setup_connection(_vx_ingest)
        id_query = """DELETE
                FROM `vxdata`.`_default`.`METAR` f
                WHERE f.subset = 'METAR'
                AND f.type = 'DF'
                AND f.url LIKE '/opt/data/%' RETURNING f.id AS id;"""
        row_iter = vx_ingest.cluster.query(
            id_query, QueryOptions(metrics=True, read_only=False)
        )
        for row in row_iter:
            print(f"Deleted {row['id']}")
    except Exception as e:
        print(f"Error occurred: {e}")

    return _vx_ingest


def assert_dicts_almost_equal(dict1, dict2, rel_tol=1e-09):
    """Utility function to compare potentially nested dictionaries containing floats"""
    assert set(dict1.keys()) == set(dict2.keys()), (
        f"Dictionaries do not have the same keys {dict1.keys()} vs {dict2.keys()}"
    )
    for key in dict1:
        if isinstance(dict1[key], dict):
            assert_dicts_almost_equal(dict1[key], dict2[key], rel_tol)
        else:
            assert dict1[key] == pytest.approx(dict2[key], rel=rel_tol), (
                f"Values for station {dict1['name']} {key} do not match dict1[key]: {dict1[key]} dict2[key]: {dict2[key]}"
            )


@pytest.mark.integration
def test_one_thread_specify_file_pattern_job_spec_rt(tmp_path: Path):
    log_queue = Queue()
    vx_ingest = setup_connection()
    # these normally come from the jobSpec->ProcessSpec->DataSourceSpec
    runtime_collection = (
        vx_ingest.cluster.bucket("vxdata").scope("_default").collection("RUNTIME")
    )
    job_spec = runtime_collection.get(
        "JS:METAR:OBS:NETCDF-TEST:schedule:job:V01"
    ).content_as[dict]
    process_id = job_spec["processSpecIds"][0]
    process_spec = runtime_collection.get(process_id).content_as[dict]
    ingest_document_ids = process_spec["ingestDocumentIds"]
    data_source_id = process_spec["dataSourceId"]
    data_source_spec = runtime_collection.get(data_source_id).content_as[dict]
    collection = process_spec["subset"]
    input_data_path = data_source_spec["sourceDataUri"]
    file_mask = data_source_spec["fileMask"]

    # file_pattern is optional and is used to specify a subset of files to process
    vx_ingest.runit(
        {
            "credentials_file": os.environ["CREDENTIALS"],
            "collection": collection,
            "file_mask": file_mask,
            "input_data_path": input_data_path,
            "ingest_document_ids": ingest_document_ids,
            "output_dir": f"{tmp_path}",
            "threads": 1,
            "file_pattern": "20250911_1500",
        },
        log_queue,
        stub_worker_log_configurer,
    )

    # Test that we have one or more output files
    output_file_list = list(tmp_path.glob("[0123456789]???????_[0123456789]???.json"))
    assert len(output_file_list) > 0, "There are no output files"

    # Test that we have one "load job" ("LJ") document
    lj_doc_regex = "LJ:METAR:vxingest.netcdf_to_cb.run_ingest_threads:VXIngest:*.json"
    num_load_job_files = len(list(tmp_path.glob(lj_doc_regex)))
    assert num_load_job_files == 1, "there is no load job output file"

    # Test that we have one output file per input file
    input_path = Path("/opt/data/netcdf_to_cb/input_files")
    num_input_files = len(list(input_path.glob("20250911_1500")))
    num_output_files = len(list(tmp_path.glob("20250911_1500.json")))
    assert num_output_files == num_input_files, "number of output files is incorrect"

    # Test that the output file matches the content in the database
    derived_data = json.load((tmp_path / "20250911_1500.json").open(encoding="utf-8"))
    station_id = ""
    derived_station = {}
    obs_id = ""
    derived_obs = {}
    for item in derived_data:
        try:
            if item["type"] == "DF":
                continue
            if item["docType"] == "station" and item["name"] == "KDEN":
                station_id = item["id"]
                derived_station = item
            else:
                if item["docType"] == "obs":
                    obs_id = item["id"]
                    derived_obs = item
            if derived_station and derived_obs:
                break
        except Exception as e:
            pytest.fail(f"Error processing derived data item: {e}")
    try:
        retrieved_station = vx_ingest.collection.get(station_id).content_as[dict]
        retrieved_obs = vx_ingest.collection.get(obs_id).content_as[dict]
        # make sure the updateTime is the same in both the derived and retrieved station
        retrieved_station["updateTime"] = derived_station["updateTime"]
        # make sure the firstTime and lastTime are the same in both the derived and retrieved station['geo']
        retrieved_station["geo"][0]["firstTime"] = derived_station["geo"][0][
            "firstTime"
        ]
        retrieved_station["geo"][0]["lastTime"] = derived_station["geo"][0]["lastTime"]
    except Exception as e:
        pytest.fail(f"Error retrieving documents from database: {e}")
    assert derived_station == retrieved_station
    assert_dicts_almost_equal(derived_obs, retrieved_obs)


@pytest.mark.integration
def test_one_thread_specify_file_pattern(tmp_path: Path):
    log_queue = Queue()
    vx_ingest = setup_connection()
    job = vx_ingest.common_collection.get("JOB-TEST:V01:METAR:NETCDF:OBS").content_as[
        dict
    ]
    ingest_document_ids = job["ingest_document_ids"]
    collection = job["subset"]
    input_data_path = job["input_data_path"]
    file_mask = job["file_mask"]
    vx_ingest.runit(
        {
            "credentials_file": os.environ["CREDENTIALS"],
            "collection": collection,
            "file_mask": file_mask,
            "input_data_path": input_data_path,
            "ingest_document_ids": ingest_document_ids,
            "output_dir": f"{tmp_path}",
            "threads": 1,
            "file_pattern": "20211108_0000",
        },
        log_queue,
        stub_worker_log_configurer,
    )

    # Test that we have one or more output files
    output_file_list = list(tmp_path.glob("[0123456789]???????_[0123456789]???.json"))
    assert len(output_file_list) > 0, "There are no output files"

    # Test that we have one "load job" ("LJ") document
    lj_doc_regex = "LJ:METAR:vxingest.netcdf_to_cb.run_ingest_threads:VXIngest:*.json"
    num_load_job_files = len(list(tmp_path.glob(lj_doc_regex)))
    assert num_load_job_files == 1, "there is no load job output file"

    # Test that we have one output file per input file
    input_path = Path("/opt/data/netcdf_to_cb/input_files")
    num_input_files = len(list(input_path.glob("20211108_0000")))
    num_output_files = len(list(tmp_path.glob("20211108*.json")))
    assert num_output_files == num_input_files, "number of output files is incorrect"

    # Test that the output file matches the content in the database
    derived_data = json.load((tmp_path / "20211108_0000.json").open(encoding="utf-8"))
    station_id = ""
    derived_station = {}
    obs_id = ""
    derived_obs = {}
    for item in derived_data:
        if item["docType"] == "station" and item["name"] == "KDEN":
            station_id = item["id"]
            derived_station = item
        else:
            if item["docType"] == "obs":
                obs_id = item["id"]
                derived_obs = item
        if derived_station and derived_obs:
            break
    retrieved_station = vx_ingest.collection.get(station_id).content_as[dict]
    retrieved_obs = vx_ingest.collection.get(obs_id).content_as[dict]
    # make sure the updateTime is the same in both the derived and retrieved station
    retrieved_station["updateTime"] = derived_station["updateTime"]
    # make sure the firstTime and lastTime are the same in both the derived and retrieved station['geo']
    retrieved_station["geo"][0]["firstTime"] = derived_station["geo"][0]["firstTime"]
    retrieved_station["geo"][0]["lastTime"] = derived_station["geo"][0]["lastTime"]

    assert derived_station == retrieved_station

    assert_dicts_almost_equal(derived_obs, retrieved_obs)


@pytest.mark.integration
def test_two_threads_spedicfy_file_pattern(tmp_path: Path):
    """
    integration test for testing multithreaded capability
    """
    log_queue = Queue()
    vx_ingest = setup_connection()
    job = vx_ingest.common_collection.get("JOB-TEST:V01:METAR:NETCDF:OBS").content_as[
        dict
    ]
    ingest_document_ids = job["ingest_document_ids"]
    collection = job["subset"]
    input_data_path = job["input_data_path"]

    vx_ingest.runit(
        {
            "credentials_file": os.environ["CREDENTIALS"],
            "collection": collection,
            "file_mask": "%Y%m%d_%H%M",
            "input_data_path": input_data_path,
            "ingest_document_ids": ingest_document_ids,
            "output_dir": f"{tmp_path}",
            "threads": 2,
            "file_pattern": "20211105*",
        },
        log_queue,
        stub_worker_log_configurer,
    )
    assert len(list(tmp_path.glob("[0123456789]???????_[0123456789]???.json"))) > 0, (
        "There are no output files"
    )

    lj_doc_regex = "LJ:METAR:vxingest.netcdf_to_cb.run_ingest_threads:VXIngest:*.json"
    assert len(list(tmp_path.glob(lj_doc_regex))) == 1, (
        "there is no load job output file"
    )

    # use file globbing to see if we got one output file for each input file plus one load job file
    input_path = Path("/opt/data/netcdf_to_cb/input_files")
    assert len(list(tmp_path.glob("20211105*.json"))) == len(
        list(input_path.glob("20211105*"))
    ), "number of output files is incorrect"


@pytest.mark.integration
def test_one_thread_default(tmp_path: Path):
    """This test will start one thread of the ingestManager and simply make sure it runs with no Exceptions.
    It will attempt to process any files that are in the input directory that match the file_name_mask.
    TIP: you might want to use local credentials to a local couchbase. If you do
    you will need to run the scripts in the matsmetadata directory to load the local metadata.
    """
    log_queue = Queue()
    vx_ingest = setup_connection()
    job = vx_ingest.common_collection.get("JOB-TEST:V01:METAR:NETCDF:OBS").content_as[
        dict
    ]
    ingest_document_ids = job["ingest_document_ids"]
    collection = job["subset"]
    input_data_path = job["input_data_path"]
    vx_ingest.runit(
        {
            "credentials_file": os.environ["CREDENTIALS"],
            "collection": collection,
            "file_mask": "%Y%m%d_%H%M",
            "input_data_path": input_data_path,
            "ingest_document_ids": ingest_document_ids,
            "output_dir": f"{tmp_path}",
            "file_pattern": "[0123456789]???????_[0123456789]???",
            "threads": 1,
        },
        log_queue,
        stub_worker_log_configurer,
    )
    assert len(list(tmp_path.glob("[0123456789]???????_[0123456789]???.json"))) > 0, (
        "There are no output files"
    )

    lj_doc_regex = "LJ:METAR:vxingest.netcdf_to_cb.run_ingest_threads:VXIngest:*.json"
    assert len(list(tmp_path.glob(lj_doc_regex))) >= 1, (
        "there is no load job output file"
    )

    # use file globbing to see if we got one output file for each input file plus one load job file
    input_path = Path("/opt/data/netcdf_to_cb/input_files")
    assert len(list(tmp_path.glob("[0123456789]???????_[0123456789]???.json"))) == len(
        list(input_path.glob("[0123456789]???????_[0123456789]???"))
    ), "number of output files is incorrect"


@pytest.mark.integration
def test_two_threads_default(tmp_path: Path):
    """This test will start one thread of the ingestManager and simply make sure it runs with no Exceptions.
    It will attempt to process any files that are in the input directory that atch the file_name_mask.
    TIP: you might want to use local credentials to a local couchbase. If you do
    you will need to run the scripts in the matsmetadata directory to load the local metadata.
    """
    log_queue = Queue()
    vx_ingest = setup_connection()
    job = vx_ingest.common_collection.get("JOB-TEST:V01:METAR:NETCDF:OBS").content_as[
        dict
    ]
    ingest_document_ids = job["ingest_document_ids"]
    collection = job["subset"]
    input_data_path = job["input_data_path"]
    vx_ingest.runit(
        {
            "credentials_file": os.environ["CREDENTIALS"],
            "collection": collection,
            "file_mask": "%Y%m%d_%H%M",
            "input_data_path": input_data_path,
            "ingest_document_ids": ingest_document_ids,
            "output_dir": f"{tmp_path}",
            "file_pattern": "[0123456789]???????_[0123456789]???",
            "threads": 2,
        },
        log_queue,
        stub_worker_log_configurer,
    )
    assert len(list(tmp_path.glob("[0123456789]???????_[0123456789]???.json"))) > 0, (
        "There are no output files"
    )

    lj_doc_regex = "LJ:METAR:vxingest.netcdf_to_cb.run_ingest_threads:VXIngest:*.json"
    assert len(list(tmp_path.glob(lj_doc_regex))) >= 1, (
        "there is no load job output file"
    )

    # use file globbing to see if we got one output file for each input file plus one load job file
    input_path = Path("/opt/data/netcdf_to_cb/input_files")
    assert len(list(tmp_path.glob("[0123456789]???????_[0123456789]???.json"))) == len(
        list(input_path.glob("[0123456789]???????_[0123456789]???"))
    ), "number of output files is incorrect"
