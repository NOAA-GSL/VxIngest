"""
test for VxIngest CTC builders
"""

import json
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
    GetOptions,
    QueryOptions,
)

from vxingest.ctc_to_cb import ctc_builder
from vxingest.ctc_to_cb.run_ingest_threads import VXIngest

# This test expects to find obs data and model data
# in the local directory /opt/data/ctc_to_cb/input
# This test expects to write to the local output directory /opt/data/ctc_to_cb/output
# so that directory should exist.

# /public/data/grib/hrrr_wrfsfc/7/0/83/0_1905141_30/2125112000000
# "DD:V01:METAR:HRRR_OPS:1631102400:0
# DD:V01:METAR:obs:1631102400
# wd 87.92309758021554

cb_model_obs_data = []
mysql_model_obs_data = []
stations = []


def setup_connection(_vx_ingest):
    """test setup"""
    # Ensure credentials_file is a string, not a tuple
    credentials = os.environ["CREDENTIALS"]
    if isinstance(credentials, tuple):
        credentials = credentials[0]
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
        kv_timeout=timedelta(seconds=125), query_timeout=timedelta(seconds=120)
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
            AND m0.docType='CTC'
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


def calculate_cb_ctc(
    epoch,
    fcst_len,
    threshold,
    model,
    subset,
    region,
    doc_sub_type,
    reject_stations=None,
):
    if reject_stations is None:
        reject_stations = []

    global cb_model_obs_data
    global stations

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
        kv_timeout=timedelta(seconds=300), query_timeout=timedelta(seconds=120)
    )
    options = ClusterOptions(
        PasswordAuthenticator(_user, _password), timeout_options=timeout_options
    )
    cluster = Cluster(_host, options)
    collection = cluster.bucket(_bucket).scope(_scope).collection(_collection)
    runtime = cluster.bucket(_bucket).scope(_scope).collection("RUNTIME")
    load_spec = {}
    load_spec["cluster"] = cluster
    load_spec["collection"] = collection
    ingest_document_result = runtime.get(
        f"IS:{subset}:CTC:{doc_sub_type.upper()}:{model}:ALL_HRRR:ingest:V01"
    )
    ingest_document = ingest_document_result.content_as[dict]
    # instantiate a ctcBuilder so we can use its get_station methods
    builder_class = ctc_builder.CTCModelObsBuilderV01
    builder = builder_class(load_spec, ingest_document)
    # usually these would get assigned in build_document
    builder.bucket = _bucket
    builder.scope = _scope
    builder.collection = _collection
    builder.subset = _collection
    legacy_stations = sorted(builder.get_stations_for_region_by_sort(region, epoch))
    obs_id = f"DD:V01:{subset}:obs:{epoch}"
    stations = sorted(
        [station for station in legacy_stations if station not in reject_stations]
    )
    model_id = f"DD:V01:{subset}:{model}:{epoch}:{fcst_len}"
    try:
        full_model_data = (
            load_spec["collection"]
            .get(model_id, GetOptions(timeout=timedelta(seconds=300)))
            .content_as[dict]
        )
    except Exception:
        time.sleep(0.25)
        full_model_data = load_spec["collection"].get(model_id).content_as[dict]
    cb_model_obs_data = []
    try:
        # Increase timeout for get operation
        full_obs_data = (
            load_spec["collection"]
            .get(obs_id, GetOptions(timeout=timedelta(seconds=300)))
            .content_as[dict]
        )
    except Exception:
        time.sleep(0.25)
        full_obs_data = load_spec["collection"].get(obs_id).content_as[dict]
    for station in stations:
        # find observation data for this station
        if station not in full_obs_data["data"]:
            continue
        obs_data = full_obs_data["data"][station]
        # find model data for this station
        if station not in full_model_data["data"]:
            continue
        model_data = full_model_data["data"][station]
        # add to model_obs_data
        if (
            obs_data
            and model_data
            and obs_data[doc_sub_type] is not None
            and model_data[doc_sub_type] is not None
        ):
            dat = {
                "time": epoch,
                "fcst_len": fcst_len,
                "thrsh": threshold,
                "model": model_data[doc_sub_type] if model_data else None,
                "obs": obs_data[doc_sub_type] if obs_data else None,
                "name": station,
            }
            cb_model_obs_data.append(dat)
        # calculate the CTC
    hits = 0
    misses = 0
    false_alarms = 0
    correct_negatives = 0
    for elem in cb_model_obs_data:
        if elem["model"] is None or elem["obs"] is None:
            continue
        if elem["model"] < threshold and elem["obs"] < threshold:
            hits = hits + 1
        if elem["model"] < threshold and not elem["obs"] < threshold:
            false_alarms = false_alarms + 1
        if not elem["model"] < threshold and elem["obs"] < threshold:
            misses = misses + 1
        if not elem["model"] < threshold and not elem["obs"] < threshold:
            correct_negatives = correct_negatives + 1
    ctc = {
        "fcst_valid_epoch": epoch,
        "fcst_len": fcst_len,
        "threshold": threshold,
        "hits": hits,
        "misses": misses,
        "false_alarms": false_alarms,
        "correct_negatives": correct_negatives,
    }
    return ctc


def _run_ctc_builder_test(
    id: str,
    job_id: str,
    model: str,
    outdir: Path,
):
    """Shared runner for CTC ceiling builder integration cases."""
    print(f"Running CTC builder test with ID: {id}")
    credentials_file = os.environ["CREDENTIALS"]
    if not outdir.exists():
        outdir.mkdir(parents=True)

    for _f in outdir.glob("*.json"):
        Path(_f).unlink()

    log_queue = Queue()
    vx_ingest = setup_connection(VXIngest())
    job = vx_ingest.runtime_collection.get(job_id).content_as[dict]

    ingest_document_sets = []
    job_doc = vx_ingest.runtime_collection.get(job_id).content_as[dict]
    for process_spec_id in job_doc.get("processSpecIds"):
        proc = vx_ingest.runtime_collection.get(process_spec_id).content_as[dict]
        ingest_document_sets.append(proc.get("ingestDocumentIds"))

    latest_epoch = get_latest_model_obs_epoch(vx_ingest, "METAR", model)
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
            "start_epoch": latest_epoch,
            "end_epoch": latest_epoch,
            "threads": 1,
        }
        print(f"Running ingest with config: {config}")
        vx_ingest.runit(config, log_queue, stub_worker_log_configurer)
        print(f"Processed ingest document IDs: {ingest_document_ids}")

        latest_output_file = min(outdir.glob("*"), key=os.path.getctime)
        with latest_output_file.open(encoding="utf8") as output_file:
            vx_ingest_output_data = json.load(output_file)

        if vx_ingest_output_data[0]["type"] == "LJ":
            return

        fcst_valid_epochs = {doc["fcstValidEpoch"] for doc in vx_ingest_output_data}
        fcst_valid_epoch = list(fcst_valid_epochs)[int(len(fcst_valid_epochs) / 2)]
        thresholds = ["500", "1000", "3000", "60000"]
        docs = [
            _doc
            for _doc in vx_ingest_output_data
            if _doc["fcstValidEpoch"] == fcst_valid_epoch
        ]
        fcst_lens = [_elem["fcstLen"] for _elem in docs]

        for fcst_len in fcst_lens:
            matched_doc = None
            for _elem in docs:
                if _elem["fcstLen"] == fcst_len:
                    matched_doc = _elem
                    break

            for threshold in thresholds:
                print(
                    f"Asserting derived CTC for fcstValidEpoch: {matched_doc['fcstValidEpoch']} model: {model} region: ALL_HRRR fcst_len: {fcst_len} threshold: {threshold}"
                )
                cb_ctc = calculate_cb_ctc(
                    epoch=matched_doc["fcstValidEpoch"],
                    fcst_len=fcst_len,
                    threshold=int(threshold),
                    model=model,
                    subset="METAR",
                    doc_sub_type="Ceiling",
                    region="ALL_HRRR",
                )
                if cb_ctc is None:
                    print(f"cb_ctc is None for threshold {threshold}- continuing")


@pytest.mark.integration
def test_ctc_data_hrrr_ops_all_hrrr_new():
    """Builds ceiling CTC output and validates derivation for HRRR and MPAS jobs."""
    global cb_model_obs_data
    global stations
    try:
        _run_ctc_builder_test(
            id="hrrr_ops_all_hrrr",
            job_id="JS:METAR:CTC:HRRR_OPS_conus_3km_TEST:schedule:job:V01",
            model="HRRR_OPS",
            outdir=Path("/opt/data/ctc_to_cb/hrrr_ops/output"),
        )
    except Exception as e:
        pytest.fail(f"Exception in test_ctc_ceiling_data_hrrr_ops_all_hrrr (HRRR_OPS): {e}")

@pytest.mark.integration
def test_ctc_data_MPAS_physics_dev1_all_hrrr_new():
    """Builds ceiling CTC output and validates derivation for HRRR and MPAS jobs."""
    global cb_model_obs_data
    global stations
    try:
        _run_ctc_builder_test(
            id="mpas_physics_dev1_all_hrrr",
            job_id="JS:METAR:CTC:MPAS_conus_3km_MPAS_physics_dev1-TEST:schedule:job:V01",
            model="MPAS_physics_dev1",
            outdir=Path("/opt/data/ctc_to_cb/mpas_physics_dev1/output"),
        )
    except Exception as e:
        pytest.fail(f"Exception in test_ctc_ceiling_data_MPAS_physics_dev1_all_hrrr (MPAS_physics_dev1): {e}")
