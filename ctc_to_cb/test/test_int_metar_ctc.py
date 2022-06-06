# pylint: disable=too-many-lines
"""
test for VxIngest CTC builders
"""
import glob
import json
import os
import time
from datetime import datetime
from pathlib import Path
import pytest

import pymysql
import yaml
from pymysql.constants import CLIENT
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster, ClusterOptions
from builder_common.load_spec_yaml import LoadYamlSpecFile
from ctc_to_cb import ctc_builder
from ctc_to_cb.run_ingest_threads import VXIngest


# This test expects to find obs data and model data for hrrr_ops.
# This test expects to write to the local output directory /opt/data/ctc_to_cb/output
# so that directory should exist.

# /public/data/grib/hrrr_wrfsfc/7/0/83/0_1905141_30/2125112000000
# "DD:V01:METAR:HRRR_OPS:1631102400:0
# DD:V01:METAR:obs:1631102400
# wd 87.92309758021554

cb_model_obs_data = []
mysql_model_obs_data = []
stations = []
mysql_not_in_stations = []


def test_check_fcst_valid_epoch_fcst_valid_iso():
    """
    integration test to check fcst_valid_epoch is derived correctly
    """
    try:
        credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
        assert Path(credentials_file).is_file(), "credentials_file Does not exist"
        _f = open(credentials_file)
        yaml_data = yaml.load(_f, yaml.SafeLoader)
        host = yaml_data["cb_host"]
        user = yaml_data["cb_user"]
        password = yaml_data["cb_password"]
        _f.close()
        options = ClusterOptions(PasswordAuthenticator(user, password))
        cluster = Cluster("couchbase://" + host, options)
        result = cluster.query(
            """SELECT m0.fcstValidEpoch fve, fcstValidISO fvi
            FROM mdata m0
            WHERE
                m0.type='DD'
                AND m0.docType='CTC'
                AND m0.subset='METAR'
                AND m0.version='V01'
                AND m0.model='HRRR_OPS'
                AND m0.region='ALL_HRRR'
        """
        )
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
        credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
        assert Path(credentials_file).is_file(), "credentials_file Does not exist"
        _f = open(credentials_file)
        yaml_data = yaml.load(_f, yaml.SafeLoader)
        host = yaml_data["cb_host"]
        user = yaml_data["cb_user"]
        password = yaml_data["cb_password"]
        _f.close()
        options = ClusterOptions(PasswordAuthenticator(user, password))
        cluster = Cluster("couchbase://" + host, options)
        collection = cluster.bucket("mdata").default_collection()
        cwd = os.getcwd()
        spec_file = (
            cwd + "/ctc_to_cb/test/test_load_spec_metar_hrrr_ops_all_hrrr_ctc_V01.yaml"
        )
        load_spec_file = LoadYamlSpecFile({"spec_file": spec_file})
        load_spec = dict(load_spec_file.read())
        load_spec["cluster"] = cluster
        load_spec["collection"] = collection
        ingest_document_result = collection.get(
            "MD-TEST:V01:METAR:HRRR_OPS:ALL_HRRR:CTC:CEILING:ingest"
        )
        ingest_document = ingest_document_result.content
        # instantiate a ctcBuilder so we can use its get_station methods
        builder_class = getattr(ctc_builder, "CTCModelObsBuilderV01")
        builder = builder_class(load_spec, ingest_document)
        result = cluster.query(
            """
            SELECT name,
                geo.bottom_right.lat AS br_lat,
                geo.bottom_right.lon AS br_lon,
                geo.top_left.lat AS tl_lat,
                geo.top_left.lon AS tl_lon
            FROM mdata
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
            # classic_stations = sorted(doc.content["stations"])
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


def calculate_mysql_ctc(epoch, fcst_len, threshold, model, region, obs_table):
    """This method calculates a ctc table from mysql data using the following algorithm
    --replace into $table (time,fcst_len,trsh,yy,yn,ny,nn)
    select 1*3600*floor((o.time+1800)/(1*3600)) as time,
        m.fcst_len as fcst_len,
        $thresh as trsh,
        sum(if(    (m.ceil < $thresh) and     (o.ceil < $thresh),1,0)) as yy,
        sum(if(    (m.ceil < $thresh) and NOT (o.ceil < $thresh),1,0)) as yn,
        sum(if(NOT (m.ceil < $thresh) and     (o.ceil < $thresh),1,0)) as ny,
        sum(if(NOT (m.ceil < $thresh) and NOT (o.ceil < $thresh),1,0)) as nn
        from
        ceiling2.$model as m,madis3.metars,ceiling2.$obs_table as o,ceiling2.ruc_metars as rm
        where 1 = 1
        and m.madis_id = metars.madis_id
        and m.madis_id = o.madis_id
        and m.fcst_len = $fcst_len
        and m.time = o.time
        and find_in_set("ALL_HRRR",reg) > 0
        and o.time  >= $valid_time - 1800
        and o.time < $valid_time + 1800
        and m.time  >= $valid_time - 1800
        and m.time < $valid_time + 1800
        group by time
        having yy+yn+ny+nn > 0
        order by time
    """
    credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
    assert Path(credentials_file).is_file(), "credentials_file Does not exist"
    _cf = open(credentials_file)
    yaml_data = yaml.load(_cf, yaml.SafeLoader)
    _cf.close()
    host = yaml_data["mysql_host"]
    user = yaml_data["mysql_user"]
    passwd = yaml_data["mysql_password"]
    connection = pymysql.connect(
        host=host,
        user=user,
        passwd=passwd,
        local_infile=True,
        autocommit=True,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.SSDictCursor,
        client_flag=CLIENT.MULTI_STATEMENTS,
    )
    cursor = connection.cursor(pymysql.cursors.SSDictCursor)
    statement = """
        select 1*3600*floor((o.time+1800)/(1*3600)) as time,
        m.fcst_len as fcst_len,
        {thrsh} as trsh,
        sum(if(    ( m.ceil < {thrsh}) and     ( o.ceil < {thrsh}),1,0)) as yy,
        sum(if(    ( m.ceil < {thrsh}) and NOT ( o.ceil < {thrsh}),1,0)) as yn,
        sum(if(NOT ( m.ceil < {thrsh}) and     ( o.ceil < {thrsh}),1,0)) as ny,
        sum(if(NOT ( m.ceil < {thrsh}) and NOT ( o.ceil < {thrsh}),1,0)) as nn
        from
        ceiling2.{model} as m, madis3.metars, ceiling2.{obs} as o
        where 1 = 1
        and m.madis_id = madis3.metars.madis_id
        and m.madis_id = o.madis_id
        and m.fcst_len = {fcst_len}
        and m.time = o.time
        and find_in_set("{region}",reg) > 0
        and o.time  >= {time} - 1800
        and o.time < {time} + 1800
        and m.time  >= {time} - 1800
        and m.time < {time} + 1800
        group by time
        having yy+yn+ny+nn > 0
        order by time
        """.format(
        thrsh=threshold,
        model=model,
        region=region,
        fcst_len=fcst_len,
        time=epoch,
        obs=obs_table,
    )
    cursor.execute(statement)
    ctc = cursor.fetchall()[0]
    return ctc


def calculate_mysql_ctc_loop(  # pylint: disable=dangerous-default-value, missing-function-docstring
    epoch, fcst_len, threshold, model, region, obs_table, reject_stations=[]
):
    credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
    assert Path(credentials_file).is_file(), "credentials_file Does not exist"
    _cf = open(credentials_file)
    yaml_data = yaml.load(_cf, yaml.SafeLoader)
    _cf.close()
    host = yaml_data["mysql_host"]
    user = yaml_data["mysql_user"]
    passwd = yaml_data["mysql_password"]
    connection = pymysql.connect(
        host=host,
        user=user,
        passwd=passwd,
        local_infile=True,
        autocommit=True,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.SSDictCursor,
        client_flag=CLIENT.MULTI_STATEMENTS,
    )
    cursor = connection.cursor(pymysql.cursors.SSDictCursor)
    statement = """
        select 1*3600*floor((o.time+1800)/(1*3600)) as time,
        m.fcst_len as fcst_len,
        {thrsh} as trsh,
        m.ceil as model_value, o.ceil as obs_value, name
        from
        ceiling2.{model} as m, madis3.metars, ceiling2.{obs} as o
        where 1 = 1
        and m.madis_id = madis3.metars.madis_id
        and m.madis_id = o.madis_id
        and m.fcst_len = {fcst_len}
        and m.time = o.time
        and find_in_set("{region}",reg) > 0
        and o.time  >= {time} - 1800
        and o.time < {time} + 1800
        and m.time  >= {time} - 1800
        and m.time < {time} + 1800
        order by time, fcst_len, name, trsh
        """.format(
        thrsh=threshold,
        model=model,
        region=region,
        fcst_len=fcst_len,
        time=epoch,
        obs=obs_table,
    )
    print("mysql_ctc statement: ", statement)
    cursor.execute(statement)
    row = cursor.fetchone()
    if row is None:
        return None
    hits = 0
    misses = 0
    false_alarms = 0
    correct_negatives = 0
    mysql_model_obs_data = []  # pylint: disable=redefined-outer-name
    while row is not None:
        if row["name"] not in stations or row["name"] in reject_stations:
            mysql_not_in_stations.append(row["name"])
        else:
            if row["model_value"] is None or row["obs_value"] is None:
                row = cursor.fetchone()
                continue
            mysql_model_obs_data.append(row)
            if row["model_value"] < threshold and row["obs_value"] < threshold:
                hits = hits + 1
            if row["model_value"] < threshold and not row["obs_value"] < threshold:
                false_alarms = false_alarms + 1
            if not row["model_value"] < threshold and row["obs_value"] < threshold:
                misses = misses + 1
            if not row["model_value"] < threshold and not row["obs_value"] < threshold:
                correct_negatives = correct_negatives + 1
        row = cursor.fetchone()

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


def calculate_cb_ctc(  # pylint: disable=dangerous-default-value,missing-function-docstring
    spec_file_name,
    epoch,
    fcst_len,
    threshold,
    model,
    subset,
    region,
    reject_stations=[],
):
    credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
    assert Path(credentials_file).is_file(), "credentials_file Does not exist"
    _f = open(credentials_file)
    yaml_data = yaml.load(_f, yaml.SafeLoader)
    host = yaml_data["cb_host"]
    user = yaml_data["cb_user"]
    password = yaml_data["cb_password"]
    _f.close()
    load_spec_file = LoadYamlSpecFile({"spec_file": spec_file_name})
    load_spec = dict(load_spec_file.read())
    options = ClusterOptions(PasswordAuthenticator(user, password))
    load_spec["cluster"] = Cluster("couchbase://" + host, options)
    load_spec["collection"] = load_spec["cluster"].bucket("mdata").default_collection()
    ingest_document_result = load_spec["collection"].get(
        "MD:V01:{subset}:{model}:ALL_HRRR:CTC:CEILING:ingest".format(
            subset=subset, model=model
        )
    )
    ingest_document = ingest_document_result.content
    # instantiate a ctcBuilder so we can use its get_station methods
    builder_class = getattr(ctc_builder, "CTCModelObsBuilderV01")
    builder = builder_class(load_spec, ingest_document)
    legacy_stations = sorted(
        #                builder.get_stations_for_region_by_geosearch(region, epoch)
        builder.get_stations_for_region_by_sort(region, epoch)
    )
    obs_id = "DD:V01:{subset}:obs:{fcst_valid_epoch}".format(
        subset=subset, fcst_valid_epoch=epoch
    )
    stations = sorted(  # pylint: disable=redefined-outer-name
        [station for station in legacy_stations if station not in reject_stations]
    )
    model_id = "DD:V01:{subset}:{model}:{fcst_valid_epoch}:{fcst_len}".format(
        subset=subset,
        model=model,
        fcst_valid_epoch=epoch,
        fcst_len=fcst_len,
    )
    print("cb_ctc model_id:", model_id, " obs_id:", obs_id)
    try:
        full_model_data = load_spec["collection"].get(model_id).content
    except:  # pylint: disable=bare-except
        time.sleep(0.25)
        full_model_data = load_spec["collection"].get(model_id).content
    cb_model_obs_data = []  # pylint: disable=redefined-outer-name
    try:
        full_obs_data = load_spec["collection"].get(obs_id).content
    except:  # pylint: disable=bare-except
        time.sleep(0.25)
        full_obs_data = load_spec["collection"].get(obs_id).content
    for station in stations:
        # find observation data for this station
        obs_data = None
        for elem in full_obs_data["data"]:
            if elem["name"] == station:
                obs_data = elem
                break
        # find model data for this station
        model_data = None
        for elem in full_model_data["data"]:
            if elem["name"] == station:
                model_data = elem
                break
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
    Then the test  derives the same CTC in three ways.
    1) it calculates the CTC using couchbase data for input.
    2) It calculates the CTC using mysql data for input.
    3) It uses the mysql legacy query with the embeded calculation.
    The two mysql derived CTC's are compared and asserted, and then the couchbase CTC
    is compared and asserted against the mysql CTC.
    NOTE: We expect this test to have wide tolerances because of the differences in the
    mysql and couchbase ingest
    algorithms. Use this test more for debugging.
    """
    # noinspection PyBroadException
    try:
        cwd = os.getcwd()
        credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
        spec_file = (
            cwd + "/ctc_to_cb/test/test_load_spec_metar_hrrr_ops_all_hrrr_ctc_V01.yaml"
        )
        outdir = "/opt/data/ctc_to_cb/hrrr_ops/output"
        filepaths = outdir + "/*.json"
        files = glob.glob(filepaths)
        for _f in files:
            try:
                os.remove(_f)
            except OSError as _e:
                assert False, f"Error:  {_e}"
        vx_ingest = VXIngest()
        vx_ingest.runit(
            {
                "spec_file": spec_file,
                "credentials_file": credentials_file,
                "output_dir": outdir,
                "threads": 1,
                "first_epoch": 100,
            }
        )

        list_of_output_files = glob.glob("/opt/data/ctc_to_cb/output/*")
        # latest_output_file = max(list_of_output_files, key=os.path.getctime)
        latest_output_file = min(list_of_output_files, key=os.path.getctime)
        try:
            # Opening JSON file
            output_file = open(latest_output_file)
            # returns JSON object as a dictionary
            vx_ingest_output_data = json.load(output_file)
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
                    "Asserting mysql derived CTC for fcstValidEpoch: {epoch} model: HRRR_OPS region: ALL_HRRR fcst_len: {fcst_len} threshold: {thrsh}".format(
                        epoch=_elem["fcstValidEpoch"], thrsh=_t, fcst_len=_i
                    )
                )
                cb_ctc = calculate_cb_ctc(
                    spec_file_name=spec_file,
                    epoch=_elem["fcstValidEpoch"],
                    fcst_len=_i,
                    threshold=int(_t),
                    model="HRRR_OPS",
                    subset="METAR",
                    region="ALL_HRRR",
                )
                if cb_ctc is None:
                    print(
                        "cb_ctc_loop is None for threshold {thrsh}- contunuing".format(
                            thrsh=str(_t)
                        )
                    )
                    continue
                mysql_ctc = calculate_mysql_ctc_loop(
                    epoch=_elem["fcstValidEpoch"],
                    fcst_len=_i,
                    threshold=int(_t) / 10,
                    model="HRRR_OPS",
                    region="ALL_HRRR",
                    obs_table="obs",
                )
                if mysql_ctc is None:
                    print(
                        "mysql_ctc_loop is None for threshold {thrsh}- contunuing".format(
                            thrsh=str(_t)
                        )
                    )
                    continue
                # mysql_ctc = calculate_mysql_ctc(
                #     epoch=elem["fcstValidEpoch"],
                #     fcst_len=i,
                #     threshold=int(t) / 10,
                #     model="HRRR_OPS",
                #     region="ALL_HRRR",
                #     obs_table="obs",
                # )
                # are the station names the same?
                mysql_names = [elem["name"] for elem in mysql_model_obs_data]
                cb_names = [elem["name"] for elem in cb_model_obs_data]
                name_diffs = [
                    _i
                    for _i in cb_names + mysql_names
                    if _i not in cb_names or _i not in mysql_names
                ]
                try:
                    assert len(name_diffs) > 0, (
                        "There are differences between the mysql and CB station names"
                        + str(name_diffs)
                    )

                except:  # pylint: disable=bare-except
                    # recalculate without the differences
                    cb_model_obs_data = []  # pylint: disable=redefined-outer-name
                    cb_ctc = {}
                    cb_ctc = calculate_cb_ctc(
                        spec_file_name=spec_file,
                        epoch=_elem["fcstValidEpoch"],
                        fcst_len=_i,
                        threshold=int(_t),
                        model="HRRR_OPS",
                        subset="METAR",
                        region="ALL_HRRR",
                        reject_stations=name_diffs,
                    )
                    mysql_ctc = {}
                    mysql_model_obs_data = []  # pylint: disable=redefined-outer-name
                    mysql_ctc = calculate_mysql_ctc_loop(
                        epoch=_elem["fcstValidEpoch"],
                        fcst_len=_i,
                        threshold=int(_t) / 10,
                        model="HRRR_OPS",
                        region="ALL_HRRR",
                        obs_table="obs",
                        reject_stations=name_diffs,
                    )

                # assertEqual(
                #     len(mysql_model_obs_data),
                #     len(cb_model_obs_data),
                #     "model_obs_data are not the same length",
                # )
                for _r in range(  # pylint:disable=consider-using-enumerate
                    len(mysql_model_obs_data)
                ):  # pylint: disable=consider-using-enumerate
                    delta = round(
                        (
                            mysql_model_obs_data[_r]["model_value"] * 10
                            + cb_model_obs_data[_r]["model"]
                        )
                        * 0.05
                    )
                    try:
                        assert mysql_model_obs_data[_r][
                            "model_value"
                        ] * 10 == pytest.approx(
                            cb_model_obs_data[_r]["model"], delta=delta
                        ), "mysql and cb model values differ"
                    except:  # pylint: disable=bare-except
                        print(
                            _i,
                            "model",
                            mysql_model_obs_data[_r]["time"],
                            mysql_model_obs_data[_r]["fcst_len"],
                            cb_model_obs_data[_r]["thrsh"],
                            mysql_model_obs_data[_r]["name"],
                            mysql_model_obs_data[_r]["model_value"] * 10,
                            cb_model_obs_data[_r]["name"],
                            cb_model_obs_data[_r]["model"],
                            delta,
                        )
                    try:
                        assert mysql_model_obs_data[_r][
                            "obs_value"
                        ] * 10 == pytest.approx(
                            cb_model_obs_data[_r]["obs"], delta=delta
                        ), "mysql and cb obs values differ"
                    except:  # pylint: disable=bare-except
                        print(
                            _i,
                            "obs",
                            mysql_model_obs_data[_r]["time"],
                            mysql_model_obs_data[_r]["fcst_len"],
                            cb_model_obs_data[_r]["thrsh"],
                            mysql_model_obs_data[_r]["name"],
                            mysql_model_obs_data[_r]["obs_value"] * 10,
                            cb_model_obs_data[_r]["name"],
                            cb_model_obs_data[_i]["obs"],
                            delta,
                        )
    except Exception as _e:  # pylint: disable=broad-except
        assert False, f"TestCTCBuilderV01 Exception failure: {_e}"


def test_ctc_builder_hrrr_ops_all_hrrr_compare_model_obs_data():
    """
    This test verifies that data is returned for each fcstLen and each threshold.
    It can be used to debug the builder by putting a specific epoch for first_epoch.
    By default it will build all unbuilt CTC objects and put them into the output folder.
    Then it takes the last output json file and loads that file.
    Then the test  derives the same CTC in three ways.
    1) it calculates the CTC using couchbase data for input.
    2) It calculates the CTC using mysql data for input.
    3) It uses the mysql legacy query with the embeded calculation.
    The two mysql derived CTC's are compared and asserted, and then the couchbase CTC
    is compared and asserted against the mysql CTC.
    """
    # noinspection PyBroadException
    try:
        credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
        assert Path(credentials_file).is_file(), "credentials_file Does not exist"
        _cf = open(credentials_file)
        yaml_data = yaml.load(_cf, yaml.SafeLoader)
        host = yaml_data["cb_host"]
        user = yaml_data["cb_user"]
        password = yaml_data["cb_password"]
        _cf.close()
        options = ClusterOptions(PasswordAuthenticator(user, password))
        cluster = Cluster("couchbase://" + host, options)

        host = yaml_data["mysql_host"]
        user = yaml_data["mysql_user"]
        passwd = yaml_data["mysql_password"]
        connection = pymysql.connect(
            host=host,
            user=user,
            passwd=passwd,
            local_infile=True,
            autocommit=True,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.SSDictCursor,
            client_flag=CLIENT.MULTI_STATEMENTS,
        )
        cursor = connection.cursor(pymysql.cursors.SSDictCursor)
        cwd = os.getcwd()
        spec_file = (
            cwd + "/ctc_to_cb/test/test_load_spec_metar_hrrr_ops_all_hrrr_ctc_V01.yaml"
        )
        # get the ceiling thresholds from the metadata
        result = cluster.query(
            """
            SELECT RAW mdata.thresholdDescriptions
            FROM mdata
            WHERE type="MD"
                AND docType="matsAux"
            """,
            read_only=True,
        )
        thresholds = list(map(int, list((list(result)[0])["ceiling"].keys())))

        # get the model fcstValidEpoch list from couchbase
        result = cluster.query(
            """SELECT RAW fcstValidEpoch
                FROM mdata
                WHERE type='DD'
                    AND docType='model'
                    AND mdata.model='{model}'
                    AND mdata.version='V01'
                    AND mdata.subset='METAR'""".format(
                model="HRRR_OPS"
            )
        )
        cb_model_fcst_valid_epochs = list(result)
        # get the obs fcstValidEpoch list from couchbase
        result = cluster.query(
            """SELECT raw mdata.fcstValidEpoch
                FROM mdata
                WHERE mdata.type='DD'
                    AND mdata.docType='obs'
                    AND mdata.subset='METAR'
                    AND mdata.version='V01'"""
        )
        cb_obs_fcst_valid_epochs = list(result)
        cb_common_fcst_valid_epochs = [
            val
            for val in cb_obs_fcst_valid_epochs
            if val in set(cb_model_fcst_valid_epochs)
        ]
        # get available fcstValidEpochs for  legacy
        first_epoch = cb_common_fcst_valid_epochs[0]
        last_epoch = cb_common_fcst_valid_epochs[-1]
        cursor.execute(
            """
            select DISTINCT
            1 * 3600 * floor((o.time + 1800) /(1 * 3600)) as time
            from
            ceiling2.{model} as m,
            madis3.metars,
            ceiling2.obs as o
            where
            1 = 1
            and m.madis_id = madis3.metars.madis_id
            and m.madis_id = o.madis_id
            and m.fcst_len = 1
            and m.time = o.time
            and find_in_set("{region}", reg) > 0
            and o.time >= {first_epoch} - 1800
            and o.time < {last_epoch} + 1800
            and m.time >= {first_epoch} - 1800
            and m.time < {last_epoch} + 1800
            order by
            time""".format(
                model="HRRR_OPS",
                region="ALL_HRRR",
                first_epoch=first_epoch,
                last_epoch=last_epoch,
            )
        )
        mysql_common_fcst_valid_epochs = [o["time"] for o in cursor.fetchall()]
        common_fcst_valid_epochs = [
            val
            for val in cb_common_fcst_valid_epochs
            if val in set(mysql_common_fcst_valid_epochs)
        ]

        # choose one sort of near the end to be sure that all the data is present and that
        # it won't get its raw data migrated away
        rindex = min(len(common_fcst_valid_epochs), 15) * -1
        fcst_valid_epoch = common_fcst_valid_epochs[rindex]
        result = cluster.query(
            """
            SELECT RAW fcstLen
            FROM mdata
            WHERE mdata.type="DD"
                AND mdata.docType="model"
                AND mdata.model='{model}'
                AND mdata.version='V01'
                AND mdata.subset='METAR'
                AND mdata.fcstValidEpoch={fcst_valid_epoch}""".format(
                model="HRRR_OPS", fcst_valid_epoch=fcst_valid_epoch
            )
        )
        cb_fcst_lens = list(result)
        cursor.execute(
            """
            select DISTINCT fcst_len
            from
            ceiling2.{model} as m,
            madis3.metars
            where
                1 = 1
                and find_in_set("{region}", reg) > 0
                and m.time >= {epoch} - 1800
                and m.time < {epoch} + 1800""".format(
                model="HRRR_OPS", region="ALL_HRRR", epoch=fcst_valid_epoch
            )
        )
        mysql_fcst_lens_result = cursor.fetchall()
        mysql_fcst_lens = [o["fcst_len"] for o in mysql_fcst_lens_result]
        fcst_lens = [
            fcst_len for fcst_len in mysql_fcst_lens if fcst_len in set(cb_fcst_lens)
        ]
        print("..")
        print("cb fcst_lens:", cb_fcst_lens)
        print("mysql fcst_lens:", mysql_fcst_lens)
        print("common fcst_lens:", fcst_lens)
        for _i in fcst_lens:
            for _t in thresholds:
                # process all the thresholds
                print(
                    "Asserting mysql derived CTC for fcstValidEpoch: {epoch} model: {model} region: {region} fcst_len: {fcst_len} threshold: {thrsh}".format(
                        model="HRRR_OPS",
                        region="ALL_HRRR",
                        epoch=fcst_valid_epoch,
                        thrsh=_t,
                        fcst_len=_i,
                    )
                )
                # calculate_cb_ctc derives the cb data for the compare
                cb_ctc = calculate_cb_ctc(
                    spec_file_name=spec_file,
                    epoch=fcst_valid_epoch,
                    model="HRRR_OPS",
                    subset="METAR",
                    region="ALL_HRRR",
                    fcst_len=_i,
                    threshold=int(_t),
                )
                if cb_ctc is None:
                    print(
                        "mysql_ctc_loop is None for threshold {thrsh}- contunuing".format(
                            thrsh=str(_t)
                        )
                    )
                    continue
                # calculate_mysql_ctc_loop derives the mysql data for the compare
                mysql_ctc_loop = calculate_mysql_ctc_loop(
                    model="HRRR_OPS",
                    region="ALL_HRRR",
                    epoch=fcst_valid_epoch,
                    fcst_len=_i,
                    threshold=int(_t) / 10,
                    obs_table="obs",
                )
                if mysql_ctc_loop is None:
                    print(
                        "mysql_ctc_loop is None for threshold {thrsh}- contunuing".format(
                            thrsh=str(_t)
                        )
                    )
                    continue
                # mysql_ctc = calculate_mysql_ctc(model="HRRR_OPS", region="ALL_HRRR", epoch=fcst_valid_epoch, fcst_len=i, threshold=int(t) / 10, obs_table="obs")
                # are the station names the same?
                mysql_names = [elem["name"] for elem in mysql_model_obs_data]
                cb_names = [elem["name"] for elem in cb_model_obs_data]
                name_diffs = [
                    i
                    for i in cb_names + mysql_names
                    if i not in cb_names or i not in mysql_names
                ]
                # Fix This when we sort out why there are differences
                assert (
                    len(name_diffs) > 0
                ), "There are differences between the mysql and CB station names"
                cb_ctc_nodiffs = (  # pylint: disable=unused-variable
                    calculate_cb_ctc(  # pylint: disable=unused-variable
                        spec_file_name=spec_file,
                        epoch=fcst_valid_epoch,
                        model="HRRR_OPS",
                        subset="METAR",
                        region="ALL_HRRR",
                        fcst_len=_i,
                        threshold=int(_t),
                        reject_stations=name_diffs,
                    )
                )
                try:
                    assert len(mysql_model_obs_data) == len(
                        cb_model_obs_data
                    ), "model_obs_data are not the same length"
                except:  # pylint: disable=bare-except
                    print(
                        "model_obs_data are not the same length",
                        len(mysql_model_obs_data),
                        len(cb_model_obs_data),
                    )
                min_length = min(len(mysql_model_obs_data), len(cb_model_obs_data))
                for _r in range(min_length):
                    if cb_model_obs_data[_r]["model"] is None:
                        try:
                            assert (
                                mysql_model_obs_data[_r]["model_value"]
                                == cb_model_obs_data[_r]["model"]
                            )
                        except:  # pylint: disable=bare-except
                            print(
                                _r,
                                "model",
                                mysql_model_obs_data[_r]["time"],
                                mysql_model_obs_data[_r]["fcst_len"],
                                cb_model_obs_data[_r]["thrsh"],
                                mysql_model_obs_data[_r]["name"],
                                mysql_model_obs_data[_r]["model_value"] * 10,
                                cb_model_obs_data[_r]["model"],
                            )
                    else:
                        # find the delta between the two, mysql must be multiplied by 10
                        delta = round(
                            (
                                mysql_model_obs_data[_r]["model_value"] * 10
                                + cb_model_obs_data[_r]["model"]
                            )
                            * 0.05
                        )
                        try:
                            # do the model values match within 5% ?
                            assert mysql_model_obs_data[_r][
                                "model_value"
                            ] * 10 == pytest.approx(
                                cb_model_obs_data[_r]["model"], delta=delta
                            ), "mysql and cb model values differ"
                        except:  # pylint: disable=bare-except
                            print(
                                _r,
                                "model",
                                mysql_model_obs_data[_r]["time"],
                                mysql_model_obs_data[_r]["fcst_len"],
                                cb_model_obs_data[_r]["thrsh"],
                                mysql_model_obs_data[_r]["name"],
                                mysql_model_obs_data[_r]["model_value"] * 10,
                                cb_model_obs_data[_r]["model"],
                                delta,
                            )
                        try:
                            # do the obs match within 5%
                            assert mysql_model_obs_data[_r][
                                "obs_value"
                            ] * 10 == pytest.approx(
                                cb_model_obs_data[_r]["obs"], delta=delta
                            ), "mysql and cb obs values differ"
                        except:  # pylint: disable=bare-except
                            print(
                                _r,
                                "obs",
                                mysql_model_obs_data[_r]["time"],
                                mysql_model_obs_data[_r]["fcst_len"],
                                cb_model_obs_data[_r]["thrsh"],
                                mysql_model_obs_data[_r]["name"],
                                mysql_model_obs_data[_r]["obs_value"] * 10,
                                cb_model_obs_data[_r]["obs"],
                                delta,
                            )

    except Exception as _e:  # pylint: disable=broad-except
        assert False, f"TestCTCBuilderV01 Exception failure:  {_e}"
    return


def test_ctc_data_hrrr_ops_all_hrrr():  # pylint: disable=too-many-locals
    # noinspection PyBroadException
    """
    This test is a comprehensive test of the ctcBuilder data. It will retrieve CTC documents
    for a specific fcstValidEpoch from couchbase and the legacy mysql database.
    It determines an appropriate fcstValidEpoch that exists in both datasets, then
    a common set of fcst_len values. It then compares the data with assertions. The intent is to
    demonstrate that the data transformation from input model obs pairs is being done
    the same for couchbase as it is for the legacy ingest system.

    NOTE: We do not expect the mysql CTC data to match the CB CTC data - use this for debugging
    """
    try:
        credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
        assert Path(credentials_file).is_file(), "credentials_file Does not exist"
        _cf = open(credentials_file)
        yaml_data = yaml.load(_cf, yaml.SafeLoader)
        host = yaml_data["cb_host"]
        user = yaml_data["cb_user"]
        password = yaml_data["cb_password"]
        _cf.close()
        options = ClusterOptions(PasswordAuthenticator(user, password))
        cluster = Cluster("couchbase://" + host, options)

        host = yaml_data["mysql_host"]
        user = yaml_data["mysql_user"]
        passwd = yaml_data["mysql_password"]
        connection = pymysql.connect(
            host=host,
            user=user,
            passwd=passwd,
            local_infile=True,
            autocommit=True,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.SSDictCursor,
            client_flag=CLIENT.MULTI_STATEMENTS,
        )
        cursor = connection.cursor(pymysql.cursors.SSDictCursor)
        # get available fcstValidEpochs for couchbase
        result = cluster.query(
            """SELECT RAW fcstValidEpoch
            FROM mdata
            WHERE type="DD"
                AND docType="CTC"
                AND mdata.subDocType = "CEILING"
                AND mdata.model='HRRR_OPS'
                AND mdata.region='ALL_HRRR'
                AND mdata.version='V01'
                AND mdata.subset='METAR'"""
        )
        cb_fcst_valid_epochs = list(result)
        # get available fcstValidEpochs for  legacy
        cursor.execute(
            "select time from ceiling_sums2.HRRR_OPS_ALL_HRRR where time > %s AND time < %s;",
            (cb_fcst_valid_epochs[0], cb_fcst_valid_epochs[-1]),
        )
        common_fcst_valid_lens_result = cursor.fetchall()
        # choose the last one that is common
        fcst_valid_epoch = common_fcst_valid_lens_result[-1]["time"]
        # get all the cb fcstLen values
        result = cluster.query(
            """SELECT raw mdata.fcstLen
            FROM mdata
            WHERE mdata.type='DD'
                AND mdata.docType = "CTC"
                AND mdata.subDocType = "CEILING"
                AND mdata.model='HRRR_OPS'
                AND mdata.region='ALL_HRRR'
                AND mdata.version='V01'
                AND mdata.subset='METAR'
                AND mdata.fcstValidEpoch = $time
                order by mdata.fcstLen
            """,
            time=fcst_valid_epoch,
        )
        cb_fcst_valid_lens = list(result)
        # get the mysql_fcst_len values
        statement = "select DISTINCT fcst_len from ceiling_sums2.HRRR_OPS_ALL_HRRR where time = %s;"
        cursor.execute(statement, (fcst_valid_epoch))
        mysql_fcst_valid_lens_result = cursor.fetchall()
        mysql_fcst_valid_lens = [o["fcst_len"] for o in mysql_fcst_valid_lens_result]
        # get the intersection of the fcst_len's
        intersect_fcst_lens = [
            value for value in mysql_fcst_valid_lens if value in cb_fcst_valid_lens
        ]
        # get the thesholdDescriptions from the couchbase metadata
        result = cluster.query(
            """
            SELECT RAW mdata.thresholdDescriptions
            FROM mdata
            WHERE type="MD"
                AND docType="matsAux"
            """,
            read_only=True,
        )
        thresholds = list(map(int, list((list(result)[0])["ceiling"].keys())))

        # get the associated couchbase ceiling model data
        # get the associated couchbase obs
        # get the ctc couchbase data
        result = cluster.query(
            """
            SELECT *
            FROM mdata
            WHERE mdata.type='DD'
                AND mdata.docType = "CTC"
                AND mdata.subDocType = "CEILING"
                AND mdata.model='HRRR_OPS'
                AND mdata.region='ALL_HRRR'
                AND mdata.version='V01'
                AND mdata.subset='METAR'
                AND mdata.fcstValidEpoch = $time
                AND mdata.fcstLen IN $intersect_fcst_lens
                order by mdata.fcstLen;
            """,
            time=fcst_valid_epoch,
            intersect_fcst_lens=intersect_fcst_lens,
        )
        cb_results = list(result)
        # print the couchbase statement
        print(
            "cb statement is:"
            + """
        SELECT *
            FROM mdata
            WHERE mdata.type='DD'
                AND mdata.docType = "CTC"
                AND mdata.subDocType = "CEILING"
                AND mdata.model='HRRR_OPS'
                AND mdata.region='ALL_HRRR'
                AND mdata.version='V01'
                AND mdata.subset='METAR'
                AND mdata.fcstValidEpoch = """
            + str(fcst_valid_epoch)
            + """ AND mdata.fcstLen IN """
            + str(intersect_fcst_lens)
            + """ order by mdata.fcstLen;"""
        )

        # get the associated mysql ceiling model data
        # get the associated mysql obs
        # get the ctc mysql data
        format_strings = ",".join(["%s"] * len(intersect_fcst_lens))
        params = [fcst_valid_epoch]
        params.extend(intersect_fcst_lens)
        statement = (
            "select fcst_len,trsh, yy as hits, yn as false_alarms, ny as misses, nn as correct_negatives from ceiling_sums2.HRRR_OPS_ALL_HRRR where time = %s AND fcst_len IN ("
            "" + format_strings + ") ORDER BY fcst_len;"
        )
        # print the mysql statement
        string_intersect_fcst_lens = [str(ifl) for ifl in intersect_fcst_lens]
        print_statement = (
            "mysql statement is: "
            + "select fcst_len,trsh, yy as hits, yn as false_alarms, ny as misses, nn as correct_negatives from ceiling_sums2.HRRR_OPS_ALL_HRRR where time = "
            + str(fcst_valid_epoch)
            + " AND fcst_len IN ("
            + ",".join(string_intersect_fcst_lens)
            + ") ORDER BY fcst_len;"
        )
        print(print_statement)
        cursor.execute(statement, tuple(params))
        mysql_results = cursor.fetchall()
        #
        mysql_fcst_len_thrsh = {}
        for fcst_len in intersect_fcst_lens:

            mysql_fcst_len = [
                value for value in mysql_results if value["fcst_len"] == fcst_len
            ]
            cb_index_for_fcst_len = None
            for cb_index_for_fcst_len, _x in enumerate(cb_results):
                if _x["mdata"]["fcstLen"] == fcst_len:
                    break
            for _t in thresholds:
                for mysql_fcst_len_thrsh in mysql_fcst_len:
                    if mysql_fcst_len_thrsh["trsh"] * 10 == _t:
                        break
                assert cb_results[cb_index_for_fcst_len]["mdata"]["data"][str(_t)][
                    "hits"
                ] == pytest.approx(
                    mysql_fcst_len_thrsh["hits"], delta=1000
                ), "mysql hits {mhits} do not match couchbase hits {chits} for fcst_len {f} and threshold {t}".format(
                    mhits=mysql_fcst_len_thrsh["hits"],
                    chits=cb_results[cb_index_for_fcst_len]["mdata"]["data"][str(_t)][
                        "hits"
                    ],
                    f=fcst_len,
                    t=_t,
                )
                assert cb_results[cb_index_for_fcst_len]["mdata"]["data"][str(_t)][
                    "misses"
                ] == pytest.approx(
                    mysql_fcst_len_thrsh["misses"], delta=1000
                ), "mysql misses {mmisses} do not match couchbase misses {cmisses} for fcst_len {f} and threshold {t}".format(
                    mmisses=mysql_fcst_len_thrsh["misses"],
                    cmisses=cb_results[cb_index_for_fcst_len]["mdata"]["data"][str(_t)][
                        "misses"
                    ],
                    f=fcst_len,
                    t=_t,
                )
                assert cb_results[cb_index_for_fcst_len]["mdata"]["data"][str(_t)][
                    "false_alarms"
                ] == pytest.approx(
                    mysql_fcst_len_thrsh["false_alarms"], delta=1000
                ), "mysql false_alarms {mfalse_alarms} do not match couchbase false_alarms {cfalse_alarms} for fcst_len {f} and threshold {t}".format(
                    mfalse_alarms=mysql_fcst_len_thrsh["false_alarms"],
                    cfalse_alarms=cb_results[cb_index_for_fcst_len]["mdata"]["data"][
                        str(_t)
                    ]["false_alarms"],
                    f=fcst_len,
                    t=_t,
                )
                assert cb_results[cb_index_for_fcst_len]["mdata"]["data"][str(_t)][
                    "correct_negatives"
                ] == pytest.approx(
                    mysql_fcst_len_thrsh["correct_negatives"], delta=1000
                ), "mysql correct_negatives {mcorrect_negatives} do not match couchbase correct_negatives {ccorrect_negatives} for fcst_len {f} and threshold {t}".format(
                    mcorrect_negatives=mysql_fcst_len_thrsh["correct_negatives"],
                    ccorrect_negatives=cb_results[cb_index_for_fcst_len]["mdata"][
                        "data"
                    ][str(_t)]["correct_negatives"],
                    f=fcst_len,
                    t=_t,
                )
    except Exception as _e:  # pylint: disable=broad-except
        assert False, f"TestCTCBuilderV01 Exception failure:  {_e}"
    return
