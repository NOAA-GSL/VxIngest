"""
_test for VxIngest SUMS builders
"""

import json
import math
import os
import time
from datetime import datetime, timedelta
from multiprocessing import Queue
from pathlib import Path

import pytest
import yaml
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import (
    ClusterOptions,
    ClusterTimeoutOptions,
    QueryOptions,
)

from vxingest.partial_sums_to_cb import partial_sums_builder
from vxingest.partial_sums_to_cb.run_ingest_threads import VXIngest

# This test expects to find obs data and model data
# in the local directory /opt/data/partial_sums_to_cb/input
# This test expects to write to the local output directory /opt/data/ctc_to_cb/output
# so that directory should exist.

# /public/data/grib/hrrr_wrfsfc/7/0/83/0_1905141_30/2125112000000
# "DD:V01:METAR:HRRR_OPS:1631102400:0
# DD:V01:METAR:obs:1631102400
# wd 87.92309758021554

cb_model_obs_data = []
mysql_model_obs_data = []
stations = []


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
        pytest.fail(f"Error during setup connection: {e}")
    return _vx_ingest


def get_latest_model_obs_epoch(_vx_ingest, subset, model):
    # try to find the first and last epoch from the data in couchbase.
    # The ctc_builder won't build any data that has already been built (except for the very last one).
    # The test will process the CTC documents but not upsert them.
    stmnt = f"""SELECT MAX(fve.fcstValidEpoch)
            FROM vxdata._default.{subset} fve
            WHERE fve.type='DD'
            AND fve.docType='obs'
            AND fve.version='V01'
            AND fve.subset='{subset}';"""
    result = _vx_ingest.cluster.query(stmnt, QueryOptions(metrics=True, read_only=True))
    max_obs = list(result.rows())[0]["$1"]
    stmnt = f"""SELECT MAX(fve.fcstValidEpoch)
            FROM vxdata._default.{subset} fve
            WHERE fve.type='DD'
            AND fve.docType='model'
            AND fve.model='{model}'
            AND fve.version='V01'
            AND fve.subset='{subset}';"""
    result = _vx_ingest.cluster.query(stmnt, QueryOptions(metrics=True, read_only=True))
    max_model = list(result.rows())[0]["$1"]
    max_epoch = min(max_obs, max_model)
    return max_epoch


def stub_worker_log_configurer(queue: Queue):
    """A stub to replace log_config.worker_log_configurer"""
    pass


@pytest.mark.integration
def test_check_fcst_valid_epoch_fcst_valid_iso():
    """
    integration test to check fcst_valid_epoch is derived correctly
    """
    credentials_file = os.environ["CREDENTIALS"]
    assert Path(credentials_file).is_file(), "credentials_file Does not exist"
    with Path(credentials_file).open(encoding="utf-8") as _f:
        yaml_data = yaml.load(_f, yaml.SafeLoader)
    _host = yaml_data["cb_host"]
    _user = yaml_data["cb_user"]
    _password = yaml_data["cb_password"]
    _bucket = yaml_data["cb_bucket"]
    _collection = yaml_data["cb_collection"]
    _scope = yaml_data["cb_scope"]

    timeout_options = ClusterTimeoutOptions(
        kv_timeout=timedelta(seconds=25), query_timeout=timedelta(seconds=120)
    )
    options = ClusterOptions(
        PasswordAuthenticator(_user, _password), timeout_options=timeout_options
    )
    cluster = Cluster(_host, options)
    options = ClusterOptions(PasswordAuthenticator(_user, _password))
    cluster = Cluster(_host, options)
    stmnt = f"""SELECT m0.fcstValidEpoch fve, fcstValidISO fvi
        FROM `{_bucket}`.{_scope}.{_collection} m0
        WHERE
            m0.type='DD'
            AND m0.docType='PARTAILSUMS'
            AND m0.subset='{_collection}'
            AND m0.version='V01'
            AND m0.model='HRRR_OPS'
            AND m0.region='ALL_HRRR'
    """
    result = cluster.query(stmnt)
    for row in result:
        fve = row["fve"]
        utc_time = datetime.strptime(row["fvi"], "%Y-%m-%dT%H:%M:%S")
        epoch_time = int((utc_time - datetime(1970, 1, 1)).total_seconds())
        assert fve == epoch_time, (
            "fcstValidEpoch and fcstValidIso are not the same time"
        )
        assert (fve % 3600) == 0, "fcstValidEpoch is not at top of hour"


@pytest.mark.integration
def test_get_stations_geo_search():
    """
    Currently we know that there are differences between the geo search stations list and the legacy
    stations list. This test does show those differences. The assertion is commented out.
    """
    credentials_file = os.environ["CREDENTIALS"]
    assert Path(credentials_file).is_file(), "credentials_file Does not exist"
    with Path(credentials_file).open(encoding="utf-8") as _f:
        yaml_data = yaml.load(_f, yaml.SafeLoader)
    _host = yaml_data["cb_host"]
    _user = yaml_data["cb_user"]
    _password = yaml_data["cb_password"]
    _bucket = yaml_data["cb_bucket"]
    _collection = yaml_data["cb_collection"]
    _scope = yaml_data["cb_scope"]

    timeout_options = ClusterTimeoutOptions(
        kv_timeout=timedelta(seconds=25), query_timeout=timedelta(seconds=120)
    )
    options = ClusterOptions(
        PasswordAuthenticator(_user, _password), timeout_options=timeout_options
    )
    cluster = Cluster(_host, options)
    collection = cluster.bucket(_bucket).scope(_scope).collection(_collection)
    load_spec = {}
    load_spec["cluster"] = cluster
    load_spec["collection"] = collection
    load_spec["ingest_document_ids"] = [
        f"MD:V01:{_collection}:HRRR_OPS:ALL_HRRR:SUMS:SURFACE:ingest"
    ]
    # get the ingest document id.
    ingest_document_result = collection.get(
        f"MD-TEST:V01:{_collection}:HRRR_OPS:ALL_HRRR:SUMS:SURFACE:ingest"
    )
    ingest_document = ingest_document_result.content_as[dict]
    # instantiate a partialsumsBuilder so we can use its get_station methods
    builder_class = partial_sums_builder.PartialSumsSurfaceModelObsBuilderV01
    builder = builder_class(load_spec, ingest_document)
    # usually these would get assigned in build_document
    builder.bucket = _bucket
    builder.scope = _scope
    builder.collection = _collection
    builder.subset = _collection

    result = cluster.query(
        f"""
        SELECT name,
            geo.bottom_right.lat AS br_lat,
            geo.bottom_right.lon AS br_lon,
            geo.top_left.lat AS tl_lat,
            geo.top_left.lon AS tl_lon
        FROM `{_bucket}`.{_scope}.{_collection}
        WHERE type='MD'
            AND docType='region'
            AND subset='COMMON'
            AND version='V01'
        """
    )
    for row in result:
        # use the builder geosearch to get the station list - just use current epoch
        stations = sorted(
            # builder.get_stations_for_region_by_geosearch(row["name"],round(time.time()))
            builder.get_stations_for_region_by_sort(row["name"], round(time.time()))
        )
        # get the legacy station list from the test document (this came from mysql)
        # classic_station_id = "MD-TEST:V01:CLASSIC_STATIONS:" + row["name"]
        # doc = collection.get(classic_station_id.strip())
        # classic_stations = sorted(doc.content_as[dict]["stations"])
        classic_stations = builder.get_legacy_stations_for_region(row["name"])
        stations_difference = [
            i
            for i in classic_stations + stations
            if i not in classic_stations or i not in stations
        ]
        print(
            "region "
            + row["name"]
            + "difference length is "
            + str(len(stations_difference))
            + " stations symmetric_difference is "
            + str(stations_difference)
        )
        assert len(stations_difference) < 1000, (
            "difference between expected and actual greater than 100"
        )


@pytest.mark.integration
def test_ps_builder_surface_hrrr_ops_all_hrrr():
    """
    This test verifies that data is returned for each fcstLen.
    It can be used to debug the builder by putting a specific epoch for first_epoch.
    By default it will build all unbuilt SUMS objects and put them into the output folder.
    Then it takes the last output json file and loads that file.
    Then the test derives the same SUMS.
    It calculates the Partial using couchbase data for input.
    Then the couchbase SUMS fcstValidEpochs are compared and asserted against the derived SUMS.
    """

    global cb_model_obs_data
    global stations

    credentials_file = os.environ["CREDENTIALS"]
    id = "HRRR_OPS_conus_3km_TEST"
    job_id = "JS:METAR:SUMS:HRRR_OPS_conus_3km_TEST:schedule:job:V01"
    outdir = Path("/opt/data/test/partial_sums_to_cb/hrrr_ops/sums/output")
    model = "HRRR_OPS"
    return _run_sums_builder_test(
        credentials_file=credentials_file,
        id=id,
        job_id=job_id,
        outdir=outdir,
        model=model,
        region="ALL_HRRR",
    )


@pytest.mark.integration
def test_ps_builder_surface_mpas_physics_dev1_all_hrrr():
    """
    This test verifies that data is returned for each fcstLen.
    It can be used to debug the builder by putting a specific epoch for first_epoch.
    By default it will build all unbuilt SUMS objects and put them into the output folder.
    Then it takes the last output json file and loads that file.
    Then the test derives the same SUMS.
    It calculates the Partial Sums using couchbase data for input.
    Then the couchbase SUMS fcstValidEpochs are compared and asserted against the derived SUMS.
    """

    global cb_model_obs_data
    global stations

    credentials_file = os.environ["CREDENTIALS"]
    job_id = "JS:METAR:SUMS:MPAS_conus_3km_MPAS_physics_dev1-TEST:schedule:job:V01"
    outdir = Path("/opt/data/test/partial_sums_to_cb/mpas_physics_dev1/sums/output")
    id = "mpas_physics_dev1"
    model = "MPAS_physics_dev1"

    return _run_sums_builder_test(
        credentials_file=credentials_file,
        id=id,
        job_id=job_id,
        outdir=outdir,
        model=model,
        region="ALL_HRRR",
    )


def _run_sums_builder_test(credentials_file, id, job_id, outdir, model, region):
    if not outdir.exists():
        outdir.mkdir(parents=True)
    for _f in outdir.glob("*.json"):
        Path(_f).unlink()
    print(f"Running SUMS builder test with ID: {id}")
    log_queue = Queue()
    vx_ingest = setup_connection()
    with Path(credentials_file).open(encoding="utf-8") as _f:
        yaml_data = yaml.load(_f, yaml.SafeLoader)
    _host = yaml_data["cb_host"]
    _user = yaml_data["cb_user"]
    _password = yaml_data["cb_password"]
    _bucket = yaml_data["cb_bucket"]
    _collection = yaml_data["cb_collection"]
    _scope = yaml_data["cb_scope"]

    timeout_options = ClusterTimeoutOptions(
        kv_timeout=timedelta(seconds=25), query_timeout=timedelta(seconds=120)
    )
    options = ClusterOptions(
        PasswordAuthenticator(_user, _password), timeout_options=timeout_options
    )
    cluster = Cluster(_host, options)

    result = cluster.query(
        f"""SELECT RAW fcstValidEpoch
        FROM `{_bucket}`.{_scope}.{_collection}
        WHERE type="DD"
            AND docType="SUMS"
            AND subDocType = "SURFACE"
            AND model='{model}'
            AND region='{region}'
            AND version='V01'
            AND subset='{_collection}'"""
    )
    ps_fcst_valid_epochs = list(result)
    assert ps_fcst_valid_epochs, (
        f"No SUMS fcstValidEpoch values found for model={model}, region={region}"
    )
    fcst_valid_epoch = ps_fcst_valid_epochs[-1]

    job = vx_ingest.runtime_collection.get(job_id).content_as[dict]

    ingest_document_sets = []
    job_doc = vx_ingest.runtime_collection.get(job_id).content_as[dict]
    for process_spec_id in job_doc.get("processSpecIds"):
        proc = vx_ingest.runtime_collection.get(process_spec_id).content_as[dict]
        ingest_document_sets.append(proc.get("ingestDocumentIds"))

    for ingest_document_ids in ingest_document_sets:
        config = {
            "job_id": job_id,
            "credentials_file": credentials_file,
            "ingest_document_ids": ingest_document_ids,
            "collection": job["subset"],
            "input_data_path": "",
            "file_mask": "",
            "output_dir": str(outdir),
            "file_pattern": "",
            "start_epoch": fcst_valid_epoch,
            "end_epoch": fcst_valid_epoch,
            "threads": 1,
        }
        print(f"Running ingest with config: {config}")
        vx_ingest.runit(config, log_queue, stub_worker_log_configurer)
        print(f"Processed ingest document IDs: {ingest_document_ids}")
    list_of_output_files = outdir.glob("*")
    # latest_output_file = max(list_of_output_files, key=os.path.getctime)
    latest_output_file = min(list_of_output_files, key=os.path.getctime)

    # Opening JSON file
    with Path(latest_output_file).open(encoding="utf-8") as output_file:
        # returns JSON object as a dictionary
        vx_ingest_output_data = json.load(output_file)
    # if this is an LJ document then the SUMS's were already ingested
    # and the test should stop here
    if vx_ingest_output_data[0]["type"] == "LJ":
        return
    # get the last fcstValidEpochs
    fcst_valid_epochs = {doc["fcstValidEpoch"] for doc in vx_ingest_output_data}
    # take a fcstValidEpoch in the middle of the list
    fcst_valid_epoch = list(fcst_valid_epochs)[int(len(fcst_valid_epochs) / 2)]
    # get all the documents that have the chosen fcstValidEpoch
    docs = [
        _doc
        for _doc in vx_ingest_output_data
        if _doc["fcstValidEpoch"] == fcst_valid_epoch
    ]
    # get all the fcstLens for those docs
    fcst_lens = []
    for _elem in docs:
        fcst_lens.append(_elem["fcstLen"])

    for _i in fcst_lens:
        _elem = None
        # find the document for this fcst_len
        for _elem in docs:
            if _elem["fcstLen"] == _i:
                break
        assert _elem is not None, "fcstLen not found in output"


@pytest.mark.integration
def test_ps_surface_data_hrrr_ops_all_hrrr():
    """
    This test is a comprehensive test of the partialSumsBuilder data. It will retrieve SUMS documents
    for a specific fcstValidEpoch from couchbase and calculate the SUM's for the same fcstValidEpoch.
    It then compares the data with assertions. The intent is to
    demonstrate that the data transformation from input model obs pairs is being done
    corrctly.
    """

    _calculate_and_compare_sums(
        job_id="JS:METAR:SUMS:RRFSv1_conus_3km_ret_RRFS_jul2024:schedule:job:V01",
        region="ALL_HRRR",
        model="RRFSv1_conus_3km_ret_RRFS_jul2024",
    )


def _calculate_and_compare_sums(job_id, region, model):
    # Implement the logic to calculate and compare the partial sums with the couchbase data
    # Generate partial sums data using the builder.build_document directly. This data should match what is in
    # the database already unless something has changed in the builder code.
    try:
        credentials_file = os.environ["CREDENTIALS"]
        assert Path(credentials_file).is_file(), "credentials_file Does not exist"
        with Path(credentials_file).open(encoding="utf-8") as _f:
            yaml_data = yaml.load(_f, yaml.SafeLoader)
        _host = yaml_data["cb_host"]
        _user = yaml_data["cb_user"]
        _password = yaml_data["cb_password"]
        _bucket = yaml_data["cb_bucket"]
        _collection = yaml_data["cb_collection"]
        _scope = yaml_data["cb_scope"]

        timeout_options = ClusterTimeoutOptions(
            kv_timeout=timedelta(seconds=25), query_timeout=timedelta(seconds=120)
        )
        options = ClusterOptions(
            PasswordAuthenticator(_user, _password), timeout_options=timeout_options
        )
        cluster = Cluster(_host, options)

        result = cluster.query(
            f"""SELECT fcstValidEpoch AS fcstValidEpoch
            FROM `{_bucket}`.{_scope}.{_collection}
            WHERE type="DD"
                AND docType="SUMS"
                AND subDocType = "SURFACE"
                AND model='{model}'
                AND region='{region}'
                AND version='V01'
                AND subset='{_collection}'"""
        )
        ps_fcst_valid_data = list(result)
        assert ps_fcst_valid_data, (
            f"No SUMS ps_fcst_valid_data values found for model={model}, region={region}"
        )
        fcst_valid_epoch = ps_fcst_valid_data[(len(ps_fcst_valid_data) - 1) // 2][
            "fcstValidEpoch"
        ]
        result = cluster.query(
            f"""SELECT fcstLen AS fcstLen
            FROM `{_bucket}`.{_scope}.{_collection}
            WHERE type="DD"
                AND docType="SUMS"
                AND subDocType = "SURFACE"
                AND model='{model}'
                AND region='{region}'
                AND version='V01'
                AND subset='{_collection}'
                AND fcstValidEpoch = {fcst_valid_epoch}"""
        )
        ps_fcst_valid_data = list(result)
        fcstLen = ps_fcst_valid_data[(len(ps_fcst_valid_data) - 1) // 2]["fcstLen"]

        result = cluster.query(
            f"""
            SELECT m.id AS id, m.data AS data
            FROM `{_bucket}`.{_scope}.{_collection} m
            WHERE m.type='DD'
                AND m.docType = "SUMS"
                AND m.subDocType = "SURFACE"
                AND m.model='{model}'
                AND m.region='{region}'
                AND m.version='V01'
                AND m.subset='{_collection}'
                AND m.fcstValidEpoch = {fcst_valid_epoch}
                AND m.fcstLen = {fcstLen}
                ORDER BY m.fcstLen;
            """
        )
        cb_results = list(result)
        assert cb_results, (
            f"No SUMS documents found for model={model}, region={region}, fcstValidEpoch={fcst_valid_epoch}"
        )

        vx_ingest = setup_connection()
        ingest_document_ids = []
        job_doc = vx_ingest.runtime_collection.get(job_id).content_as[dict]
        for process_spec_id in job_doc.get("processSpecIds"):
            proc = vx_ingest.runtime_collection.get(process_spec_id).content_as[dict]
            ingest_document_ids.extend(proc.get("ingestDocumentIds"))
        ingest_documents = {}
        for ingest_document_id in ingest_document_ids:
            ingest_documents[ingest_document_id] = vx_ingest.runtime_collection.get(
                ingest_document_id
            ).content_as[dict]
        load_spec = {
            "cb_credentials": vx_ingest.cb_credentials,
            "first_last_params": {
                "first_epoch": fcst_valid_epoch,
                "last_epoch": fcst_valid_epoch,
            },
            "ingest_document_ids": ingest_document_ids,
            "ingest_documents": ingest_documents,
            "fmask": None,
            "input_data_path": None,
            "load_job_doc": None,
            "cb_connection": {
                "bucket": vx_ingest.cb_credentials["bucket"],
                "scope": vx_ingest.cb_credentials["scope"],
                "collection": vx_ingest.cb_credentials["collection"],
            },
            "cluster": vx_ingest.cluster,
            "collection": vx_ingest.collection,
            "common_collection": vx_ingest.common_collection,
        }
        builder = partial_sums_builder.PartialSumsSurfaceModelObsBuilderV01(
            load_spec, ingest_documents[ingest_document_ids[0]]
        )
        generated_data = builder.build_document(ingest_document_ids[0])
        if not generated_data:
            pytest.fail(
                f"No generated partial sums data found for ingest_document_id={ingest_document_ids[0]}"
            )
        for element in cb_results:
            cb_data = element["data"]
            cb_id = element["id"]
            generated_element_data = generated_data[cb_id]["data"]
            _compare_partial_sums(generated_element_data, cb_data)
    except Exception as _e:
        pytest.fail(f"Exception occurred while processing partial sums: {str(_e)}")


def _compare_partial_sums(generated_data, cb_ps):
    """Compare generated partial sums data with couchbase data"""
    assert generated_data is not None, "Generated partial sums data should not be None"
    assert cb_ps is not None, "Couchbase partial sums data should not be None"

    def _assert_same_structure_and_values(expected, actual, path="root"):
        if isinstance(expected, dict):
            assert isinstance(actual, dict), (
                f"Type mismatch at {path}: expected dict, got {type(actual).__name__}"
            )
            assert set(expected.keys()) == set(actual.keys()), (
                f"Key mismatch at {path}: expected keys={sorted(expected.keys())}, "
                f"actual keys={sorted(actual.keys())}"
            )
            for key in expected:
                _assert_same_structure_and_values(
                    expected[key], actual[key], f"{path}.{key}"
                )
            return

        if isinstance(expected, list):
            assert isinstance(actual, list), (
                f"Type mismatch at {path}: expected list, got {type(actual).__name__}"
            )
            assert len(expected) == len(actual), (
                f"Length mismatch at {path}: expected={len(expected)} actual={len(actual)}"
            )
            for idx, expected_item in enumerate(expected):
                _assert_same_structure_and_values(
                    expected_item, actual[idx], f"{path}[{idx}]"
                )
            return

        if isinstance(expected, float) or isinstance(actual, float):
            assert math.isclose(expected, actual, abs_tol=0.001), (
                "Generated partial sums do not match Couchbase partial sums "
                f"at {path} within tolerance 0.001: "
                f"couchbase={expected!r} generated={actual!r}"
            )
            return

        assert expected == actual, (
            "Generated partial sums do not match Couchbase partial sums "
            f"at {path}: couchbase={expected!r} generated={actual!r}"
        )

    _assert_same_structure_and_values(cb_ps, generated_data)
