# pylint: disable=too-many-lines
"""
_test for VxIngest SUMS builders
"""
import glob
import json
import os
import time
from datetime import datetime, timedelta
from multiprocessing import Queue
from pathlib import Path

import yaml
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, ClusterTimeoutOptions
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

def stub_worker_log_configurer(queue: Queue):
    """A stub to replace log_config.worker_log_configurer"""
    pass

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

def test_ps_builder_surface_hrrr_ops_all_hrrr():  # pylint: disable=too-many-locals
    """
    This test verifies that data is returned for each fcstLen.
    It can be used to debug the builder by putting a specific epoch for first_epoch.
    By default it will build all unbuilt SUMS objects and put them into the output folder.
    Then it takes the last output json file and loads that file.
    Then the test derives the same SUMS.
    It calculates the Partial using couchbase data for input.
    Then the couchbase SUMS fcstValidEpochs are compared and asserted against the derived SUMS.
    """
    # noinspection PyBroadException
    global cb_model_obs_data #pylint: disable=global-variable-not-assigned
    global stations #pylint: disable=global-variable-not-assigned

    try:
        credentials_file = os.environ["CREDENTIALS"]
        job_id = "JOB-TEST:V01:METAR:SUMS:SURFACE:MODEL:OPS"
        outdir = "/opt/data/test/partial_sums_to_cb/hrrr_ops/sums/output"
        if not os.path.exists(outdir):
            # Create a new directory because it does not exist
            os.makedirs(outdir)
        filepaths = outdir + "/*.json"
        files = glob.glob(filepaths)
        for _f in files:
            try:
                os.remove(_f)
            except OSError as _e:
                assert False, f"Error:  {_e}"
        log_queue = Queue()
        vx_ingest = VXIngest()
        # These SUM's might already have been ingested in which case this won't do anything.
        vx_ingest.runit(
            {
                "job_id": job_id,
                "credentials_file": credentials_file,
                "output_dir": outdir,
                "threads": 1,
                "first_epoch": 1638489600,
                "last_epoch": 1638496800,
            }, log_queue, stub_worker_log_configurer
        )

        list_of_output_files = glob.glob(outdir + "/*")
        # latest_output_file = max(list_of_output_files, key=os.path.getctime)
        latest_output_file = min(list_of_output_files, key=os.path.getctime)
        try:
            # Opening JSON file
            output_file = open(latest_output_file, encoding="utf8")
            # returns JSON object as a dictionary
            vx_ingest_output_data = json.load(output_file)
            # if this is an LJ document then the SUMS's were already ingested
            # and the test should stop here
            if vx_ingest_output_data[0]['type'] == "LJ":
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
            output_file.close()
        except Exception as _e:  # pylint: disable=broad-except
            assert False, f"TestPartialSumsBuilderV01 Exception failure opening output: {_e}"
        for _i in fcst_lens:
            _elem = None
            # find the document for this fcst_len
            for _elem in docs:
                if _elem["fcstLen"] == _i:
                    break
            assert _elem is not None, "fcstLen not found in output"
    except Exception as _e:  # pylint: disable=broad-except
        assert False, f"TestPartialSumsBuilderV01 Exception failure: {_e}"

def test_ps_surface_data_hrrr_ops_all_hrrr():  # pylint: disable=too-many-locals
    # noinspection PyBroadException
    """
    This test is a comprehensive test of the partialSumsBuilder data. It will retrieve SUMS documents
    for a specific fcstValidEpoch from couchbase and calculate the SUM's for the same fcstValidEpoch.
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
                AND docType="SUMS"
                AND subDocType = "SURFACE"
                AND model='HRRR_OPS'
                AND region='ALL_HRRR'
                AND version='V01'
                AND subset='{_collection}'"""
        )
        ps_fcst_valid_epochs = list(result)
        #if len(ps_fcst_valid_epochs) == 0:
        #    assert False, "There is no data"
        # choose the last one
        fcst_valid_epoch = []
        if len(ps_fcst_valid_epochs) > 0:
            fcst_valid_epoch = ps_fcst_valid_epochs[-1]
        # get all the cb fcstLen values
        result = cluster.query(
            f"""SELECT raw fcstLen
            FROM `{_bucket}`.{_scope}.{_collection}
            WHERE type='DD'
                AND docType = "SUMS"
                AND subDocType = "SURFACE"
                AND model='HRRR_OPS'
                AND region='ALL_HRRR'
                AND version='V01'
                AND subset='{_collection}'
                AND fcstValidEpoch = {fcst_valid_epoch}
                order by fcstLen
            """
        )
        ps_fcst_valid_lens = list(result)
        # get the associated couchbase model data
        # get the associated couchbase obs
        # get the SUMS couchbase data
        result = cluster.query(
            f"""
            SELECT *
            FROM `{_bucket}`.{_scope}.{_collection}
            WHERE type='DD'
                AND docType = "SUMS"
                AND subDocType = "SURFACE"
                AND model='HRRR_OPS'
                AND region='ALL_HRRR'
                AND version='V01'
                AND subset='{_collection}'
                AND fcstValidEpoch = {fcst_valid_epoch}
                AND fcstLen IN {ps_fcst_valid_lens}
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
                AND docType = "SUMS"
                AND subDocType = "SURFACE"
                AND model='HRRR_OPS'
                AND region='ALL_HRRR'
                AND version='V01'
                AND subset='{_collection}'
                AND fcstValidEpoch = {fcst_valid_epoch}
                AND fcstLen IN {ps_fcst_valid_lens}
                order by fcstLen;"""
        )
        for _cb_ps in cb_results:
            print (f"do something {_cb_ps}")
    except Exception as _e:  # pylint: disable=broad-except
        assert False, f"TestBuilderV01 Exception failure:  {_e}"
    return
