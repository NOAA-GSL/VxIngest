# pylint: disable=too-many-lines
"""
test for VxIngest CTC builders
"""
import glob
import json
import os
import time
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import yaml
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions, ClusterTimeoutOptions
from ctc_to_cb import ctc_builder
from ctc_to_cb.run_ingest_threads import VXIngest


# This test expects to find obs data and model data for hrrr_ops.
# /public/data/grib/hrrr_wrfsfc/7/0/83/0_1905141_30/2125112000000
# "DD:V01:METAR:HRRR_OPS:1631102400:0
# DD:V01:METAR:obs:1631102400
# wd 87.92309758021554

cb_model_obs_data = []
mysql_model_obs_data = []
stations = []


def test_check_fcst_valid_epoch_fcst_valid_iso():
    """
    integration test to check fcst_valid_epoch is derived correctly
    """
    try:
        credentials_file = os.environ["CREDENTIALS"]
        assert Path(credentials_file).is_file(), "credentials_file Does not exist"
        _f = open(credentials_file, encoding="utf-8")
        yaml_data = yaml.load(_f, yaml.SafeLoader)
        _host = yaml_data["cb_host"]
        _user = yaml_data["cb_user"]
        _password = yaml_data["cb_password"]
        _bucket = yaml_data["cb_bucket"]
        _collection = yaml_data["cb_collection"]
        _scope = yaml_data["cb_scope"]
        _f.close()

        timeout_options = ClusterTimeoutOptions(
            kv_timeout=timedelta(seconds=25), query_timeout=timedelta(seconds=120)
        )
        options = ClusterOptions(
            PasswordAuthenticator(_user, _password), timeout_options=timeout_options
        )
        cluster = Cluster("couchbase://" + _host, options)
        options = ClusterOptions(PasswordAuthenticator(_user, _password))
        cluster = Cluster("couchbase://" + _host, options)
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
            assert (
                fve == epoch_time
            ), "fcstValidEpoch and fcstValidIso are not the same time"
            assert (fve % 3600) == 0, "fcstValidEpoch is not at top of hour"
    except Exception as _e:  # pylint: disable=broad-except, disable=broad-except
        assert (
            False
        ), f"TestGsdIngestManager.test_check_fcstValidEpoch_fcstValidIso Exception failure:  {_e}"


def test_get_stations_geo_search():
    """
    Currently we know that there are differences between the geo search stations list and the legacy
    stations list. This test does show those differences. The assertion is commented out.
    """
    try:
        credentials_file = os.environ["CREDENTIALS"]
        assert Path(credentials_file).is_file(), "credentials_file Does not exist"
        _f = open(credentials_file, encoding="utf-8")
        yaml_data = yaml.load(_f, yaml.SafeLoader)
        _host = yaml_data["cb_host"]
        _user = yaml_data["cb_user"]
        _password = yaml_data["cb_password"]
        _bucket = yaml_data["cb_bucket"]
        _collection = yaml_data["cb_collection"]
        _scope = yaml_data["cb_scope"]
        _f.close()

        timeout_options = ClusterTimeoutOptions(
            kv_timeout=timedelta(seconds=25), query_timeout=timedelta(seconds=120)
        )
        options = ClusterOptions(
            PasswordAuthenticator(_user, _password), timeout_options=timeout_options
        )
        cluster = Cluster("couchbase://" + _host, options)
        collection = cluster.bucket(_bucket).scope(_scope).collection(_collection)
        load_spec = {}
        load_spec["cluster"] = cluster
        load_spec["collection"] = collection
        load_spec["ingest_document_ids"] = [
            f"MD:V01:{_collection}:HRRR_OPS:ALL_HRRR:CTC:CEILING:ingest"
        ]
        # get the ingest document id.
        ingest_document_result = collection.get(
            f"MD-TEST:V01:{_collection}:HRRR_OPS:ALL_HRRR:CTC:CEILING:ingest"
        )
        ingest_document = ingest_document_result.content_as[dict]
        # instantiate a ctcBuilder so we can use its get_station methods
        builder_class = getattr(ctc_builder, "CTCModelObsBuilderV01")
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
            stations = sorted(  # pylint: disable=redefined-outer-name
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
            assert (
                len(stations_difference) < 1000
            ), "difference between expected and actual greater than 100"
    except Exception as _e:  # pylint: disable=broad-except
        assert False, f"TestGsdIngestManager Exception failure:  {_e}"


def calculate_cb_ctc(  # pylint: disable=dangerous-default-value,missing-function-docstring
    epoch,
    fcst_len,
    threshold,
    model,
    subset,
    region,
    reject_stations=[],
):
    global cb_model_obs_data
    global stations

    credentials_file = os.environ["CREDENTIALS"]
    assert Path(credentials_file).is_file(), "credentials_file Does not exist"
    _f = open(credentials_file, encoding="utf-8")
    yaml_data = yaml.load(_f, yaml.SafeLoader)
    _host = yaml_data["cb_host"]
    _user = yaml_data["cb_user"]
    _password = yaml_data["cb_password"]
    _bucket = yaml_data["cb_bucket"]
    _collection = yaml_data["cb_collection"]
    _scope = yaml_data["cb_scope"]
    _f.close()

    timeout_options = ClusterTimeoutOptions(
        kv_timeout=timedelta(seconds=25), query_timeout=timedelta(seconds=120)
    )
    options = ClusterOptions(
        PasswordAuthenticator(_user, _password), timeout_options=timeout_options
    )
    cluster = Cluster("couchbase://" + _host, options)
    collection = cluster.bucket(_bucket).scope(_scope).collection(_collection)
    load_spec = {}
    load_spec["cluster"] = cluster
    load_spec["collection"] = collection
    ingest_document_result = load_spec["collection"].get(
        f"MD:V01:{subset}:{model}:ALL_HRRR:CTC:CEILING:ingest"
    )
    ingest_document = ingest_document_result.content_as[dict]
    # instantiate a ctcBuilder so we can use its get_station methods
    builder_class = getattr(ctc_builder, "CTCModelObsBuilderV01")
    builder = builder_class(load_spec, ingest_document)
    # usually these would get assigned in build_document
    builder.bucket = _bucket
    builder.scope = _scope
    builder.collection = _collection
    builder.subset = _collection
    legacy_stations = sorted(
        #                builder.get_stations_for_region_by_geosearch(region, epoch)
        builder.get_stations_for_region_by_sort(region, epoch)
    )
    obs_id = f"DD:V01:{subset}:obs:{epoch}"
    stations = sorted(  # pylint: disable=redefined-outer-name
        [station for station in legacy_stations if station not in reject_stations]
    )
    model_id = f"DD:V01:{subset}:{model}:{epoch}:{fcst_len}"
    print("cb_ctc model_id:", model_id, " obs_id:", obs_id)
    try:
        full_model_data = load_spec["collection"].get(model_id).content_as[dict]
    except:  # pylint: disable=bare-except
        time.sleep(0.25)
        full_model_data = load_spec["collection"].get(model_id).content_as[dict]
    cb_model_obs_data = []  # pylint: disable=redefined-outer-name
    try:
        full_obs_data = load_spec["collection"].get(obs_id).content_as[dict]
    except:  # pylint: disable=bare-except
        time.sleep(0.25)
        full_obs_data = load_spec["collection"].get(obs_id).content_as[dict]
    for station in stations:
        # find observation data for this station
        if not station in full_obs_data["data"].keys():
            continue
        obs_data = full_obs_data["data"][station]
        # find model data for this station
        if not station in full_model_data["data"].keys():
            continue
        model_data = full_model_data["data"][station]
        # add to model_obs_data
        if obs_data and model_data and obs_data["Ceiling"] and model_data["Ceiling"]:
            dat = {
                "time": epoch,
                "fcst_len": fcst_len,
                "thrsh": threshold,
                "model": model_data["Ceiling"] if model_data else None,
                "obs": obs_data["Ceiling"] if obs_data else None,
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


def test_ctc_builder_hrrr_ops_all_hrrr():  # pylint: disable=too-many-locals
    """
    This test verifies that data is returned for each fcstLen and each threshold.
    It can be used to debug the builder by putting a specific epoch for first_epoch.
    By default it will build all unbuilt CTC objects and put them into the output folder.
    Then it takes the last output json file and loads that file.
    Then the test derives the same CTC.
    It calculates the CTC using couchbase data for input.
    Then the couchbase CTC fcstValidEpochs are compared and asserted against the derived CTC.
    """
    # noinspection PyBroadException
    global cb_model_obs_data
    global stations

    try:
        if "data" not in os.environ:
            os.environ["data"] = "/opt/data"
        credentials_file = os.environ["CREDENTIALS"]
        job_id = "JOB-TEST:V01:METAR:CTC:SUM:MODEL:HRRR_RAP_130"
        outdir = os.environ["data"] + "/ctc_to_cb/hrrr_ops/output"
        filepaths = outdir + "/*.json"
        files = glob.glob(filepaths)
        for _f in files:
            try:
                os.remove(_f)
            except OSError as _e:
                assert False, f"Error:  {_e}"
        vx_ingest = VXIngest()
        # These CTC's might already have been ingested in which case this won't do anything.
        vx_ingest.runit(
            {
                "job_id": job_id,
                "credentials_file": credentials_file,
                "output_dir": outdir,
                "threads": 1,
                "first_epoch": 1638489600,
                "last_epoch": 1638496800,
            }
        )

        list_of_output_files = glob.glob(outdir + "/*")
        # latest_output_file = max(list_of_output_files, key=os.path.getctime)
        latest_output_file = min(list_of_output_files, key=os.path.getctime)
        try:
            # Opening JSON file
            output_file = open(latest_output_file, encoding="utf8")
            # returns JSON object as a dictionary
            vx_ingest_output_data = json.load(output_file)
            # if this is an LJ document then the CTC's were already ingested
            # and the test should stop here
            if vx_ingest_output_data[0]['type'] == "LJ":
                return
            # get the last fcstValidEpochs
            fcst_valid_epochs = {doc["fcstValidEpoch"] for doc in vx_ingest_output_data}
            # take a fcstValidEpoch in the middle of the list
            fcst_valid_epoch = list(fcst_valid_epochs)[int(len(fcst_valid_epochs) / 2)]
            _thresholds = ["500", "1000", "3000", "60000"]
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
            output_file.close()
        except Exception as _e:  # pylint: disable=broad-except
            assert False, f"TestCTCBuilderV01 Exception failure opening output: {_e}"
        for _i in fcst_lens:
            _elem = None
            # find the document for this fcst_len
            for _elem in docs:
                if _elem["fcstLen"] == _i:
                    break
            # process all the thresholds
            for _t in _thresholds:
                print(
                    f"Asserting derived CTC for fcstValidEpoch: {_elem['fcstValidEpoch']} model: HRRR_OPS region: ALL_HRRR fcst_len: {_i} threshold: {_t}"
                )
                cb_ctc = calculate_cb_ctc(
                    epoch=_elem["fcstValidEpoch"],
                    fcst_len=_i,
                    threshold=int(_t),
                    model="HRRR_OPS",
                    subset="METAR",
                    region="ALL_HRRR",
                )
                if cb_ctc is None:
                    print(
                        f"cb_ctc is None for threshold {str(_t)}- contunuing"
                    )
                    continue
    except Exception as _e:  # pylint: disable=broad-except
        assert False, f"TestCTCBuilderV01 Exception failure: {_e}"


def test_ctc_data_hrrr_ops_all_hrrr():  # pylint: disable=too-many-locals
    # noinspection PyBroadException
    """
    This test is a comprehensive test of the ctcBuilder data. It will retrieve CTC documents
    for a specific fcstValidEpoch from couchbase and calculate the CTC's for the same fcstValidEpoch.
    It then compares the data with assertions. The intent is to
    demonstrate that the data transformation from input model obs pairs is being done
    corrctly.
    """

    credentials_file = os.environ["CREDENTIALS"]
    assert Path(credentials_file).is_file(), "credentials_file Does not exist"
    _f = open(credentials_file, encoding="utf8")
    yaml_data = yaml.load(_f, yaml.SafeLoader)
    _host = yaml_data["cb_host"]
    _user = yaml_data["cb_user"]
    _password = yaml_data["cb_password"]
    _bucket = yaml_data["cb_bucket"]
    _collection = yaml_data["cb_collection"]
    _scope = yaml_data["cb_scope"]
    _f.close()

    timeout_options = ClusterTimeoutOptions(
        kv_timeout=timedelta(seconds=25), query_timeout=timedelta(seconds=120)
    )
    options = ClusterOptions(
        PasswordAuthenticator(_user, _password), timeout_options=timeout_options
    )
    cluster = Cluster("couchbase://" + _host, options)
    # get available fcstValidEpochs for couchbase
    try:
        result = cluster.query(
            f"""SELECT RAW fcstValidEpoch
            FROM `{_bucket}`.{_scope}.{_collection}
            WHERE type="DD"
                AND docType="CTC"
                AND subDocType = "CEILING"
                AND model='HRRR_OPS'
                AND region='ALL_HRRR'
                AND version='V01'
                AND subset='{_collection}'"""
        )
        cb_fcst_valid_epochs = list(result)
        if len(cb_fcst_valid_epochs) == 0:
            assert False, "There is no data"
        # choose the last one
        #fcst_valid_epoch = cb_fcst_valid_epochs[-1]
        fcst_valid_epoch = cb_fcst_valid_epochs[round(len(cb_fcst_valid_epochs) / 2)]
        # get all the cb fcstLen values
        result = cluster.query(
            f"""SELECT raw fcstLen
            FROM `{_bucket}`.{_scope}.{_collection}
            WHERE type='DD'
                AND docType = "CTC"
                AND subDocType = "CEILING"
                AND model='HRRR_OPS'
                AND region='ALL_HRRR'
                AND version='V01'
                AND subset='{_collection}'
                AND fcstValidEpoch = {fcst_valid_epoch}
                order by fcstLen
            """
        )
        cb_fcst_valid_lens = list(result)
        # get the thesholdDescriptions from the couchbase metadata
        result = cluster.query(
            f"""
            SELECT RAW thresholdDescriptions
            FROM `{_bucket}`.{_scope}.{_collection}
            WHERE type="MD"
                AND docType="matsAux"
            """,
            read_only=True,
        )
        # get the associated couchbase ceiling model data
        # get the associated couchbase obs
        # get the ctc couchbase data
        result = cluster.query(
            f"""
            SELECT *
            FROM `{_bucket}`.{_scope}.{_collection}
            WHERE type='DD'
                AND docType = "CTC"
                AND subDocType = "CEILING"
                AND model='HRRR_OPS'
                AND region='ALL_HRRR'
                AND version='V01'
                AND subset='{_collection}'
                AND fcstValidEpoch = {fcst_valid_epoch}
                AND fcstLen IN {cb_fcst_valid_lens}
                order by fcstLen;
            """
        )
        cb_results = list(result)
        # print the couchbase statement
        print(
            "cb statement is:"
            + f"""
            SELECT *
            FROM `{_bucket}`.{_scope}.{_collection}
            WHERE type='DD'
                AND docType = "CTC"
                AND subDocType = "CEILING"
                AND model='HRRR_OPS'
                AND region='ALL_HRRR'
                AND version='V01'
                AND subset='{_collection}'
                AND fcstValidEpoch = {fcst_valid_epoch}
                AND fcstLen IN {cb_fcst_valid_lens}
                order by fcstLen;"""
        )
        for _cb_ctc in cb_results:
            fcstln = _cb_ctc["METAR"]["fcstLen"]
            for _threshold in _cb_ctc["METAR"]["data"].keys():
                _ctc = calculate_cb_ctc(
                    fcst_valid_epoch,
                    fcstln,
                    int(float(_threshold)),
                    "HRRR_OPS",
                    _collection,
                    "ALL_HRRR",
                )
                # assert ctc values
                fields = ["hits", "misses", "false_alarms", "correct_negatives"]
                for field in fields:
                    _ctc_value = _ctc[field]
                    _cb_ctc_value = _cb_ctc[_collection]["data"][_threshold][field]
                    assert (
                        _ctc_value == _cb_ctc_value
                    ), f"""
                    For epoch : {_ctc['fcst_valid_epoch']}
                    and fstLen: {_ctc['fcst_len']}
                    and threshold: {_threshold}
                    the derived CTC {field}: {_ctc_value} and caclulated CTC {field}: {_cb_ctc_value} values do not match"""
    except Exception as _e:  # pylint: disable=broad-except
        assert False, f"TestCTCBuilderV01 Exception failure:  {_e}"
    return
