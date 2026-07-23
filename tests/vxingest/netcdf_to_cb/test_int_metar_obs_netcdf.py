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
    _vx_ingest.credentials_file = credentials
    _vx_ingest.cb_credentials = _vx_ingest.get_credentials(_vx_ingest.load_spec)
    _vx_ingest.connect_cb()
    try:
        # Clean up any previous test data - this data came from /opt/data/% so we know it is test data
        id_query = """DELETE
                FROM `vxdata`.`_default`.`METAR` f
                WHERE f.subset = 'METAR'
                AND f.type = 'DF'
                AND f.url LIKE '/opt/data/%' RETURNING f.id AS id;"""
        row_iter = _vx_ingest.cluster.query(
            id_query, QueryOptions(metrics=True, read_only=False)
        )
        for row in row_iter:
            print(f"Deleted {row['id']}")
    except Exception as e:
        print(f"Error occurred: {e}")
    return _vx_ingest


def assert_dicts_almost_equal(dict1, dict2, rel_tol=1e-09):
    """Utility function to compare potentially nested dictionaries containing floats"""
    if (
        ("Altimeter Pressure" in dict1)
        and ("Altimeter Pressure" not in dict2)
        and dict1["Surface Pressure"] is not None
    ):
        pytest.fail(
            f"Altimeter Pressure is not None in dict1 but not in dict2, and Surface Pressure in dict1 is not None for station {dict1['name']}"
        )
    if (
        "Altimeter Pressure" in dict1
        and dict1["Altimeter Pressure"] is None
        and "Altimeter Pressure" not in dict2
    ):
        dict2["Altimeter Pressure"] = None
    assert set(dict1.keys()) == set(dict2.keys()), (
        f"Dictionaries do not have the same keys {dict1.keys()} vs {dict2.keys()}"
    )
    for key in dict1:
        if isinstance(dict1[key], dict):
            assert_dicts_almost_equal(dict1[key], dict2[key], rel_tol)
        else:
            if (
                key == "validTimeISO"
                and dict1[key].endswith("+00:00")
                and not dict2[key].endswith("+00:00")
            ):
                # hadle ISO time string comparison for naive and timezone aware datetimes
                # some of the older data was not timezone aware (naive) so if it is not, we make both
                # seem timezone aware by adding the +00:00
                # This is only for testing purposes - the current code always produces timezone aware datetimes
                dict2[key] = dict2[key] + "+00:00"
            assert dict1[key] == pytest.approx(dict2[key], rel=rel_tol), (
                f"Values for station {dict1['name']} {key} do not match dict1[key]: {dict1[key]} dict2[key]: {dict2[key]}"
            )


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
    file_mask = data_source_spec["fileMask"]

    vx_ingest.runit(
        {
            "job_id": job_id,
            "credentials_file": os.environ["CREDENTIALS"],
            "collection": collection,
            "file_mask": file_mask,
            "input_data_path": input_data_path,
            "ingest_document_ids": ingest_document_ids,
            "output_dir": f"{tmp_path}",
            "threads": 1,
            "file_pattern": file_pattern,
        },
        log_queue,
        stub_worker_log_configurer,
    )

    output_path_str = str(input_data_path).replace("file://", "")
    output_path_str = output_path_str.replace(os.sep, "__")
    output_file_list = list(tmp_path.glob(output_path_str + "*.json"))
    assert len(output_file_list) > 0, "There are no output files"
    num_load_job_files = len(list(tmp_path.glob("LJ*.json")))
    assert num_load_job_files == 1, "there is no load job output file"
    return vx_ingest, input_data_path, file_mask, file_pattern, output_file_list


@pytest.mark.integration
def test_metar_one_thread_specify_file_pattern_job_spec_rt(tmp_path: Path):
    job_id = "JS:METAR:OBS:NETCDF-TEST:schedule:job:V01"
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

    # Test that we have one output file per input file
    input_path = Path(_input_data_path[7:])
    num_input_files = len(list(input_path.glob(file_pattern)))
    num_output_files = len(output_file_list)
    assert num_output_files == num_input_files, "number of output files is incorrect"

    # Test that the output files match the content in the database
    matching_output_files = list(tmp_path.glob(f"*{file_pattern}.json"))
    assert matching_output_files, (
        f"No derived output files matched pattern '*{file_pattern}.json'"
    )
    station_id = ""
    derived_station = {}
    obs_id = ""
    derived_obs = {}
    for output_file in matching_output_files:
        with output_file.open(encoding="utf-8") as f:
            derived_data = json.load(f)
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
        if derived_station and derived_obs:
            break

    assert derived_station, (
        f"Failed to find station record in files matching '*{file_pattern}.json'"
    )
    assert derived_obs, (
        f"Failed to find obs record in files matching '*{file_pattern}.json'"
    )
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
def test_tropoe_one_thread_specify_file_pattern(tmp_path: Path):
    job_id = "JS:TROPOE-TEST:OBS:NETCDF:schedule:job:V01"
    vx_ingest, input_data_path, file_mask, file_pattern, output_file_list = (
        run_ingest_case(
            tmp_path=tmp_path,
            job_id=job_id,
        )
    )

    # Test that we have one output file per input file
    input_path = Path(input_data_path[7:])
    num_input_files = len(list(input_path.glob(file_pattern)))
    num_output_files = len(output_file_list)
    assert num_output_files == num_input_files, "number of output files is incorrect"

    # Test that the output files match the content in the database
    matching_output_files = list(tmp_path.glob(f"*{file_pattern}.json"))
    assert matching_output_files, (
        f"No derived output files matched pattern '*{file_pattern}.json'"
    )
    for output_file in matching_output_files:
        try:
            with output_file.open(encoding="utf-8") as f:
                derived_data = json.load(f)
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

