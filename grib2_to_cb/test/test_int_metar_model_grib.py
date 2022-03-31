import copy
import glob
import json
import math
import os
import sys
import datetime as DT
import unittest
from glob import glob
import grib2_to_cb.get_grid as gg
import pygrib
import pymysql
from pymysql.constants import CLIENT
import pyproj
import yaml
import numpy as np
from couchbase.cluster import Cluster, ClusterOptions
from couchbase_core.cluster import PasswordAuthenticator
from grib2_to_cb.run_ingest_threads import VXIngest


class TestGribBuilderV01(unittest.TestCase):
    """
    This test expects to find a valid grib file in the local directory /opt/public/data/grids/hrrr/conus/wrfprs/grib2.
    This test expects to write to the local output directory /opt/data/grib_to_cb/output so that directory should exist.
    """
    # 21 196 14 000018 %y %j %H %f  treating the last 6 decimals as microseconds even though they are not.
    # these files are two digit year, day of year, hour, and forecast lead time (6 digit ??)
    def get_geo_index(self, fcst_valid_epoch, geo):
        latest_time = 0
        latest_index = 0
        try:
            for geo_index in range(len(geo)):
                if geo[geo_index]['lastTime'] > latest_time:
                    latest_time = geo[geo_index]['lastTime']
                    latest_index = geo_index
                found = False
                if geo[geo_index]['firstTime'] >= fcst_valid_epoch and fcst_valid_epoch <= geo[geo_index]['lastTime']:
                    found = True
                    break
            if found:
                return geo_index
            else:
                return latest_index
        except Exception as _e:  # pylint: disable=bare-except, disable=broad-except
                print("GribBuilder.get_geo_index: Exception  error: %s", str(_e))
                return 0

    def test_compare_model_to_mysql(self):
        """This test attempts to find recent models that match in both the mysql and the CB
        databases and compare them. This test isn't likely to succeed unless both the legacy
        ingest and the VxIngest have recently run.
        """
        try:
            station = "KPDX"
            credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
            self.assertTrue(
               os.path.isfile(credentials_file), "credentials_file Does not exist"
            )

            f = open(credentials_file)
            yaml_data = yaml.load(f, yaml.SafeLoader)
            host = yaml_data["cb_host"]
            user = yaml_data["cb_user"]
            password = yaml_data["cb_password"]
            options = ClusterOptions(PasswordAuthenticator(user, password))
            cluster = Cluster("couchbase://" + host, options)

            result = cluster.query(
                """SELECT DISTINCT RAW fcstValidEpoch
                FROM mdata
                WHERE type='DD'
                AND docType="model"
                AND model="HRRR_OPS"
                AND version='V01'
                AND subset='METAR'
                ORDER BY fcstValidEpoch DESC;"""
            )
            cb_hrrr_ops_fcst_valid_epochs = list(result)

            result = cluster.query(
                """SELECT DISTINCT RAW fcstValidEpoch
                FROM mdata
                WHERE type='DD'
                AND docType="obs"
                AND version='V01'
                AND subset='METAR'
                ORDER BY fcstValidEpoch DESC;"""
            )
            cb_obs_fcst_valid_epochs = list(result)

            # find the intersection of the cb models and the obs
            intersect_cb_times = [
                value
                for value in cb_hrrr_ops_fcst_valid_epochs
                if value in cb_obs_fcst_valid_epochs
            ]

            # find the mysql hrrr_ops and obs

            host = yaml_data["mysql_host"]
            user = yaml_data["mysql_user"]
            passwd = yaml_data["mysql_password"]
            local_infile = True
            connection = pymysql.connect(
                host=host,
                user=user,
                passwd=passwd,
                local_infile=local_infile,
                autocommit=True,
                charset="utf8mb4",
                cursorclass=pymysql.cursors.SSDictCursor,
                client_flag=CLIENT.MULTI_STATEMENTS,
            )
            cursor = connection.cursor(pymysql.cursors.SSDictCursor)
            statement = """SELECT floor((m0.time+1800)/(3600))*3600 AS time
                            FROM   madis3.HRRR_OPSqp AS m0,
                            madis3.obs AS o,
                            madis3.metars AS s
                            WHERE  s.name = %s
                            AND m0.time = o.time
                            AND s.madis_id = m0.sta_id
                            AND o.sta_id = m0.sta_id
                            AND m0.fcst_len = 0
                            AND m0.time <= %s
                            AND m0.time >= %s;"""
            cursor.execute(statement, (station, intersect_cb_times[0], intersect_cb_times[-1]))
            intersect_mysql_times_tmp = cursor.fetchall()
            intersect_mysql_times = [t["time"] for t in intersect_mysql_times_tmp]
            valid_times = [
                value for value in intersect_mysql_times if value in intersect_cb_times
            ]

            result = cluster.query(
                """SELECT raw mdata.units
                    FROM mdata
                    UNNEST mdata.data AS data_item
                    WHERE mdata.type='DD'
                        AND mdata.docType="obs"
                        AND mdata.version='V01'
                        AND mdata.subset='METAR'
                        AND data_item.name=$station
                        AND mdata.fcstValidEpoch=$time""",
                        time=valid_times[0], station=station)
            units = list(result)[0]

            for time in valid_times:
                # get the common fcst lengths
                result = cluster.query(
                    """SELECT raw mdata.fcstLen
                        FROM mdata
                        UNNEST mdata.data AS data_item
                        WHERE mdata.type='DD'
                            AND mdata.docType="model"
                            AND mdata.model="HRRR_OPS"
                            AND mdata.version='V01'
                            AND mdata.subset='METAR'
                            AND data_item.name=$station
                            AND mdata.fcstValidEpoch=$time
                            ORDER BY mdata.fcstLen""",
                            time=time, station=station)
                cb_model_fcst_lens = list(result)

                statement = """SELECT DISTINCT m0.fcst_len
                        FROM   madis3.metars AS s,
                            ceiling2.HRRR_OPS AS m0
                        WHERE  1 = 1
                            AND s.madis_id = m0.madis_id
                            AND s.NAME = "kpdx"
                            AND m0.time >= %s - 1800
                            AND m0.time < %s + 1800
                        ORDER  BY m0.fcst_len; """
                cursor.execute(statement, (time, time))
                mysql_model_fcst_lens_dict = cursor.fetchall()
                mysql_model_fcst_lens = [v["fcst_len"] for v in mysql_model_fcst_lens_dict]
                intersect_fcst_len = [
                    value for value in mysql_model_fcst_lens if value in cb_model_fcst_lens
                ]
                if len(intersect_fcst_len) == 0:
                    # no fcst_len in common
                    continue

                result = cluster.query(
                    """SELECT DISTINCT mdata.fcstValidEpoch,
                        mdata.fcstLen, data_item
                        FROM mdata
                        UNNEST mdata.data AS data_item
                        WHERE mdata.type='DD'
                            AND mdata.docType="model"
                            AND mdata.model="HRRR_OPS"
                            AND mdata.version='V01'
                            AND mdata.subset='METAR'
                            AND data_item.name=$station
                            AND mdata.fcstValidEpoch=$time
                            AND mdata.fcstLen IN $fcst_lens
                            ORDER BY mdata.fcstLen""",
                    station=station, time=time, fcst_lens=intersect_fcst_len)
                cb_model_values = list(result)

                format_strings = ','.join(['%s'] * len(intersect_fcst_len))
                params = [station,time,time]
                params.extend(intersect_fcst_len)
                statement = """select m0.*
                from  madis3.metars as s, madis3.HRRR_OPSqp as m0
                WHERE 1=1
                AND s.madis_id = m0.sta_id
                AND s.name = %s
                AND  m0.time >= %s - 1800 and m0.time < %s + 1800
                AND m0.fcst_len IN (""" + format_strings + ") ORDER BY m0.fcst_len;"
                cursor.execute(statement, tuple(params))
                mysql_model_values_tmp = cursor.fetchall()
                mysql_model_fcst_len = [v["fcst_len"] for v in mysql_model_values_tmp]
                mysql_model_press = [v["press"] / 10 for v in mysql_model_values_tmp]
                mysql_model_temp = [v["temp"] / 10 for v in mysql_model_values_tmp]
                mysql_model_dp = [v["dp"] / 10 for v in mysql_model_values_tmp]
                mysql_model_wd = [v["wd"] for v in mysql_model_values_tmp]
                mysql_model_ws = [v["ws"] for v in mysql_model_values_tmp]
                mysql_model_rh = [v["rh"] / 10 for v in mysql_model_values_tmp]

                statement = """select m0.*
                from  madis3.metars as s, ceiling2.HRRR_OPS as m0
                WHERE 1=1
                AND s.madis_id = m0.madis_id
                AND s.name = %s
                AND  m0.time >= %s - 1800 and m0.time < %s + 1800
                AND m0.fcst_len IN (""" + format_strings + ") ORDER BY m0.fcst_len;"
                cursor.execute(statement, tuple(params))
                mysql_model_ceiling_values_tmp = cursor.fetchall()
                mysql_model_ceiling = (
                    [v["ceil"] * 10 for v in mysql_model_ceiling_values_tmp]
                    if len(mysql_model_ceiling_values_tmp) > 0
                    else None
                )

                statement = """select m0.*
                from  madis3.metars as s, visibility.HRRR_OPS as m0
                WHERE 1=1
                AND s.madis_id = m0.madis_id
                AND s.name = %s
                AND  m0.time >= %s - 1800 and m0.time < %s + 1800
                AND m0.fcst_len IN (""" + format_strings + ") ORDER BY m0.fcst_len;"
                cursor.execute(statement, tuple(params))
                mysql_model_visibility_values_tmp = cursor.fetchall()
                mysql_model_visibility = (
                    [v["vis100"] / 100 for v in mysql_model_visibility_values_tmp]
                    if len(mysql_model_visibility_values_tmp) > 0
                    else None
                )

                # now we have values for this time for each fcst_len, iterate the fcst_len and assert each value
                intersect_data_dict = {}
                for i in intersect_fcst_len:
                    intersect_data_dict[i] = {}
                    mysql_index = mysql_model_fcst_len.index(i)
                    cb = next(item for item in cb_model_values if item["fcstLen"] == i)
                    intersect_data_dict[i]["cb"] = {}
                    intersect_data_dict[i]["cb"]["fcstLen"] = cb["fcstLen"]
                    intersect_data_dict[i]["cb"].update(cb["data_item"])
                    intersect_data_dict[i]["mysql"] = {}
                    intersect_data_dict[i]["mysql"]["fcst_len"] = mysql_model_fcst_len[mysql_index]
                    intersect_data_dict[i]["mysql"]["press"] = mysql_model_press[mysql_index]
                    intersect_data_dict[i]["mysql"]["temp"] = mysql_model_temp[mysql_index]
                    intersect_data_dict[i]["mysql"]["dp"] = mysql_model_dp[mysql_index]
                    intersect_data_dict[i]["mysql"]["rh"] = mysql_model_rh[mysql_index]
                    intersect_data_dict[i]["mysql"]["ws"] = mysql_model_ws[mysql_index]
                    intersect_data_dict[i]["mysql"]["wd"] = mysql_model_wd[mysql_index]

                    try:
                        if mysql_model_ceiling is None or mysql_index >= len(mysql_model_ceiling) or mysql_model_ceiling[mysql_index] is None:
                            intersect_data_dict[i]["mysql"]["ceiling"] = None
                        else:
                            intersect_data_dict[i]["mysql"]["ceiling"] = mysql_model_ceiling[mysql_index]

                        if mysql_model_visibility is None or mysql_index >= len(mysql_model_visibility) or mysql_model_visibility[mysql_index] is None:
                            intersect_data_dict[i]["mysql"]["visibility"] = None
                        else:
                            intersect_data_dict[i]["mysql"]["visibility"] = mysql_model_visibility[mysql_index]
                    except:
                        self.fail(
                           "TestGsdIngestManager Exception failure for ceiling or visibility: " + str(sys.exc_info()[0])
                        )
                    print(
                        "time: {0}\t\tfcst_len: {1}\t\tstation:{2}".format(
                            time, i, station
                        )
                    )
                    print("field\t\tmysql\t\tcb\t\t\t\tdelta\t\t\tunits")

                    if intersect_data_dict[i]["mysql"]["press"] is not None and intersect_data_dict[i]["cb"]["Surface Pressure"] is not None:
                        delta = intersect_data_dict[i]["mysql"]["press"] - intersect_data_dict[i]["cb"]["Surface Pressure"]
                    else:
                        delta = None
                    print(
                        "'press'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                            intersect_data_dict[i]["mysql"]["press"],
                            intersect_data_dict[i]["cb"]["Surface Pressure"],
                            delta, units['Surface Pressure']
                        )
                    )

                    if intersect_data_dict[i]["mysql"]["temp"] is not None and intersect_data_dict[i]["cb"]["Temperature"] is not None:
                        delta = intersect_data_dict[i]["mysql"]["temp"] - intersect_data_dict[i]["cb"]["Temperature"]
                    else:
                        delta = None
                    print(
                        "'temp'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                            intersect_data_dict[i]["mysql"]["temp"],
                            intersect_data_dict[i]["cb"]["Temperature"],
                            delta, units['Temperature']
                        )
                    )

                    if intersect_data_dict[i]["mysql"]["dp"] is not None and intersect_data_dict[i]["cb"]["DewPoint"] is not None:
                        delta = intersect_data_dict[i]["mysql"]["dp"] - intersect_data_dict[i]["cb"]["DewPoint"]
                    else:
                        delta = None
                    print(
                        "'dp'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                            intersect_data_dict[i]["mysql"]["dp"],
                            intersect_data_dict[i]["cb"]["DewPoint"],
                            delta, units['DewPoint']
                        )
                    )

                    if intersect_data_dict[i]["mysql"]["rh"] is not None and intersect_data_dict[i]["cb"]["RH"] is not None:
                        delta = intersect_data_dict[i]["mysql"]["rh"] - intersect_data_dict[i]["cb"]["RH"]
                    else:
                        delta = None
                    print(
                        "'rh'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                            intersect_data_dict[i]["mysql"]["rh"],
                            intersect_data_dict[i]["cb"]["RH"],
                            delta, units['RH']
                        )
                    )

                    if intersect_data_dict[i]["mysql"]["ws"] is not None and intersect_data_dict[i]["cb"]["WS"] is not None:
                        delta = intersect_data_dict[i]["mysql"]["ws"] - intersect_data_dict[i]["cb"]["WS"]
                    else:
                        delta = None
                    print(
                        "'ws'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                            intersect_data_dict[i]["mysql"]["ws"],
                            intersect_data_dict[i]["cb"]["WS"],
                            delta, units['WS']
                        )
                    )

                    if intersect_data_dict[i]["mysql"]["wd"] is not None and intersect_data_dict[i]["cb"]["WD"] is not None:
                        delta = intersect_data_dict[i]["mysql"]["wd"] - intersect_data_dict[i]["cb"]["WD"]
                        if delta > 180:
                            delta = 360 - delta
                        if delta < -180:
                            delta = 360 + delta
                    else:
                        delta = None
                    print(
                        "'wd'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                            intersect_data_dict[i]["mysql"]["wd"],
                            intersect_data_dict[i]["cb"]["WD"],
                            delta, units['WD']
                        )
                    )

                    if intersect_data_dict[i]["mysql"]["ceiling"] is not None and intersect_data_dict[i]["cb"]["Ceiling"] is not None:
                        delta = intersect_data_dict[i]["mysql"]["ceiling"] - intersect_data_dict[i]["cb"]["Ceiling"]
                    else:
                        delta = None
                    print(
                        "'ceiling'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                            intersect_data_dict[i]["mysql"]["ceiling"],
                            intersect_data_dict[i]["cb"]["Ceiling"],
                            delta, units['Ceiling']
                        )
                    )

                    if intersect_data_dict[i]["mysql"]["visibility"] is not None and intersect_data_dict[i]["cb"]["Visibility"] is not None:
                        delta = intersect_data_dict[i]["mysql"]["visibility"] - intersect_data_dict[i]["cb"]["Visibility"]
                    else:
                        delta = None
                    print(
                        "'visibility'\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                            intersect_data_dict[i]["mysql"]["visibility"],
                            intersect_data_dict[i]["cb"]["Visibility"],
                            delta, units['Visibility']
                        )
                    )
                    print("--")

                for i in intersect_fcst_len:
                    self.assertEqual(
                        intersect_data_dict[i]["mysql"]["fcst_len"],
                        intersect_data_dict[i]["cb"]["fcstLen"],
                        msg="MYSQL fcst_len and CB fcstLen are not equal",
                    )

                    if intersect_data_dict[i]["mysql"]["press"] is not None and intersect_data_dict[i]["cb"]["Surface Pressure"] is not None:
                        np.testing.assert_allclose(
                            intersect_data_dict[i]["mysql"]["press"],
                            intersect_data_dict[i]["cb"]["Surface Pressure"],
                            atol=1.5,
                            rtol=0,
                            err_msg="MYSQL Pressure and CB Surface Pressure are not approximately equal",
                            verbose=True,
                        )
                    if intersect_data_dict[i]["mysql"]["temp"] is not None and intersect_data_dict[i]["cb"]["Temperature"] is not None:
                        np.testing.assert_allclose(
                            intersect_data_dict[i]["mysql"]["temp"],
                            intersect_data_dict[i]["cb"]["Temperature"],
                            atol=3,
                            rtol=0,
                            err_msg="MYSQL temp and CB Temperature are not approximately equal",
                            verbose=True,
                        )
                    if intersect_data_dict[i]["mysql"]["dp"] is not None and intersect_data_dict[i]["cb"]["DewPoint"] is not None:
                        np.testing.assert_allclose(
                            intersect_data_dict[i]["mysql"]["dp"],
                            intersect_data_dict[i]["cb"]["DewPoint"],
                            atol=5,
                            rtol=0,
                            err_msg="MYSQL dp and CB Dew Point are not approximately equal",
                            verbose=True,
                        )
                    if intersect_data_dict[i]["mysql"]["rh"] is not None and intersect_data_dict[i]["cb"]["RH"] is not None:
                        np.testing.assert_allclose(
                            intersect_data_dict[i]["mysql"]["rh"],
                            intersect_data_dict[i]["cb"]["RH"],
                            atol=20,
                            rtol=0,
                            err_msg="MYSQL rh and CB RH are not approximately equal",
                            verbose=True,
                        )
                    if intersect_data_dict[i]["mysql"]["wd"] is not None and intersect_data_dict[i]["cb"]["WD"] is not None:
                        np.testing.assert_allclose(
                            intersect_data_dict[i]["mysql"]["wd"],
                            intersect_data_dict[i]["cb"]["WD"],
                            atol=9999999,
                            rtol=0,
                            err_msg="MYSQL wd and CB WD are not approximately equal",
                            verbose=True,
                        )
                    if intersect_data_dict[i]["mysql"]["ws"] is not None and intersect_data_dict[i]["cb"]["WS"] is not None:
                        np.testing.assert_allclose(
                            intersect_data_dict[i]["mysql"]["ws"],
                            intersect_data_dict[i]["cb"]["WS"],
                            atol=3,
                            rtol=0,
                            err_msg="MYSQL ws and CB WS are not approximately equal",
                            verbose=True,
                        )
                    if intersect_data_dict[i]["mysql"]["visibility"] is not None and intersect_data_dict[i]["cb"]["Visibility"] is not None:
                        np.testing.assert_allclose(
                            intersect_data_dict[i]["mysql"]["visibility"],
                            intersect_data_dict[i]["cb"]["Visibility"],
                            atol=99999,
                            rtol=0,
                            err_msg="MYSQL Visibility and CB Visibility are not approximately equal",
                            verbose=True,
                        )
                    if intersect_data_dict[i]["mysql"]["ceiling"] is not None and intersect_data_dict[i]["cb"]["Ceiling"] is not None:
                        np.testing.assert_allclose(
                            intersect_data_dict[i]["mysql"]["ceiling"],
                            intersect_data_dict[i]["cb"]["Ceiling"],
                            atol=99999,
                            rtol=0,
                            err_msg="MYSQL Ceiling and CB Ceiling are not approximately equal",
                            verbose=True,
                        )
        except:
            print(str(sys.exc_info()))
            self.fail("TestGsdIngestManager Exception failure: " + str(sys.exc_info()))
    def test_compare_model_to_mysql_HRRR_OPS_LEGACY_retro(self):
        """This test attempts to find recent models that match in both the mysql and the CB
        databases and compare them. This test isn't likely to succeed unless both the legacy
        ingest and the VxIngest have recently run.
        """
        try:
            station = ""
            credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
            self.assertTrue(
               os.path.isfile(credentials_file), "credentials_file Does not exist"
            )

            f = open(credentials_file)
            yaml_data = yaml.load(f, yaml.SafeLoader)
            host = yaml_data["cb_host"]
            user = yaml_data["cb_user"]
            password = yaml_data["cb_password"]
            options = ClusterOptions(PasswordAuthenticator(user, password))
            cluster = Cluster("couchbase://" + host, options)

            result = cluster.query(
                """SELECT DISTINCT RAW fcstValidEpoch
                FROM mdata
                WHERE type='DD'
                AND docType="model"
                AND model="HRRR_OPS"
                AND version='V01'
                AND subset='METAR'
                AND fcstValidEpoch BETWEEN  1625097600 AND 1639526400
                ORDER BY fcstValidEpoch DESC;"""
            )
            cb_hrrr_ops_fcst_valid_epochs = list(result)
            # The METAR LEGACY RETRO went from JULY 1 through 14 2021 and DECEMBER 1 through 14 2021
            # so BETWEEN  1625097600 AND 1639526400
            result = cluster.query(
                """SELECT DISTINCT RAW fcstValidEpoch
                FROM mdata
                WHERE type='DD'
                AND docType="obs"
                AND version='V01'
                AND subset='METAR_LEGACY_RETRO'
                AND fcstValidEpoch BETWEEN  1625097600 AND 1639526400
                ORDER BY fcstValidEpoch DESC;"""
            )
            cb_obs_fcst_valid_epochs = list(result)

            # find the intersection of the cb models and the obs
            intersect_cb_times = [
                value
                for value in cb_hrrr_ops_fcst_valid_epochs
                if value in cb_obs_fcst_valid_epochs
            ]

            # find the mysql hrrr_ops and obs

            host = yaml_data["mysql_host"]
            user = yaml_data["mysql_user"]
            passwd = yaml_data["mysql_password"]
            local_infile = True
            connection = pymysql.connect(
                host=host,
                user=user,
                passwd=passwd,
                local_infile=local_infile,
                autocommit=True,
                charset="utf8mb4",
                cursorclass=pymysql.cursors.SSDictCursor,
                client_flag=CLIENT.MULTI_STATEMENTS,
            )
            cursor = connection.cursor(pymysql.cursors.SSDictCursor)
            for station in ["KPDX", "KDEN", "KFST", "KENW", "KBOS"]:
                # this will change from madis3.HRRR_OPSqp to madis3.HRRR_OPS_LEGACY_retroqp
                statement = """SELECT floor((m0.time+1800)/(3600))*3600 AS time
                                FROM madis3.HRRR_OPSqp AS m0,
                                madis3.obs_retro AS o,
                                madis3.metars AS s
                                WHERE  s.name = %s
                                AND m0.time = o.time
                                AND s.madis_id = m0.sta_id
                                AND o.sta_id = m0.sta_id
                                AND m0.fcst_len = 0
                                AND m0.time <= %s
                                AND m0.time >= %s;"""
                cursor.execute(statement, (station, intersect_cb_times[0], intersect_cb_times[-1]))
                intersect_mysql_times_tmp = cursor.fetchall()
                intersect_mysql_times = [t["time"] for t in intersect_mysql_times_tmp]
                valid_times = [
                    value for value in intersect_mysql_times if value in intersect_cb_times
                ]
                # units are always the same for metars
                result = cluster.query(
                    """SELECT raw mdata.units
                        FROM mdata
                        UNNEST mdata.data AS data_item
                        WHERE mdata.type='DD'
                            AND mdata.docType="obs"
                            AND mdata.version='V01'
                            AND mdata.subset='METAR_LEGACY_RETRO'
                            LIMIT 1""",
                            time=valid_times[0], station=station)
                units = list(result)[0]

                for time in valid_times:
                    # get the common fcst lengths
                    result = cluster.query(
                        """SELECT raw mdata.fcstLen
                            FROM mdata
                            UNNEST mdata.data AS data_item
                            WHERE mdata.type='DD'
                                AND mdata.docType="model"
                                AND mdata.model="HRRR_OPS"
                                AND mdata.version='V01'
                                AND mdata.subset='METAR'
                                AND data_item.name=$station
                                AND mdata.fcstValidEpoch=$time
                                ORDER BY mdata.fcstLen""",
                                time=time, station=station)
                    cb_model_fcst_lens = list(result)

                    statement = """SELECT DISTINCT m0.fcst_len
                            FROM   madis3.metars AS s,
                                ceiling2.HRRR_OPS_LEGACY_retro AS m0
                            WHERE  1 = 1
                                AND s.madis_id = m0.madis_id
                                AND s.NAME = "{station}"
                                AND m0.time >= {time} - 1800
                                AND m0.time < {time} + 1800
                            ORDER  BY m0.fcst_len; """.format(station=station,time=time)
                    cursor.execute(statement)
                    mysql_model_fcst_lens_dict = cursor.fetchall()
                    mysql_model_fcst_lens = [v["fcst_len"] for v in mysql_model_fcst_lens_dict]
                    intersect_fcst_len = [
                        value for value in mysql_model_fcst_lens if value in cb_model_fcst_lens
                    ]
                    if len(intersect_fcst_len) == 0:
                        # no fcst_len in common
                        continue

                    result = cluster.query(
                        """SELECT DISTINCT mdata.fcstValidEpoch,
                            mdata.fcstLen, data_item
                            FROM mdata
                            UNNEST mdata.data AS data_item
                            WHERE mdata.type='DD'
                                AND mdata.docType="model"
                                AND mdata.model="HRRR_OPS"
                                AND mdata.version='V01'
                                AND mdata.subset='METAR'
                                AND data_item.name=$station
                                AND mdata.fcstValidEpoch=$time
                                AND mdata.fcstLen IN $fcst_lens
                                ORDER BY mdata.fcstLen""",
                        station=station, time=time, fcst_lens=intersect_fcst_len)
                    cb_model_values = list(result)

                    format_strings = ','.join(['%s'] * len(intersect_fcst_len))
                    params = [station,time,time]
                    params.extend(intersect_fcst_len)
                    # this will change from madis3.HRRR_OPSqp to madis3.HRRR_OPS_LEGACY_retroqp
                    statement = """select m0.*
                    from  madis3.metars as s, madis3.HRRR_OPSqp as m0
                    WHERE 1=1
                    AND s.madis_id = m0.sta_id
                    AND s.name = %s
                    AND  m0.time >= %s - 1800 and m0.time < %s + 1800
                    AND m0.fcst_len IN (""" + format_strings + ") ORDER BY m0.fcst_len;"
                    cursor.execute(statement, tuple(params))
                    mysql_model_values_tmp = cursor.fetchall()
                    mysql_model_fcst_len = [v["fcst_len"] for v in mysql_model_values_tmp]
                    mysql_model_press = [v["press"] / 10 for v in mysql_model_values_tmp]
                    mysql_model_temp = [v["temp"] / 10 for v in mysql_model_values_tmp]
                    mysql_model_dp = [v["dp"] / 10 for v in mysql_model_values_tmp]
                    mysql_model_wd = [v["wd"] for v in mysql_model_values_tmp]
                    mysql_model_ws = [v["ws"] for v in mysql_model_values_tmp]
                    mysql_model_rh = [v["rh"] / 10 for v in mysql_model_values_tmp]

                    statement = """select m0.*
                    from  madis3.metars as s, ceiling2.HRRR_OPS_LEGACY_retro as m0
                    WHERE 1=1
                    AND s.madis_id = m0.madis_id
                    AND s.name = %s
                    AND  m0.time >= %s - 1800 and m0.time < %s + 1800
                    AND m0.fcst_len IN (""" + format_strings + ") ORDER BY m0.fcst_len;"
                    cursor.execute(statement, tuple(params))
                    mysql_model_ceiling_values_tmp = cursor.fetchall()
                    mysql_model_ceiling = (
                        [v["ceil"] * 10 for v in mysql_model_ceiling_values_tmp]
                        if len(mysql_model_ceiling_values_tmp) > 0
                        else None
                    )
                    # this will change from visibility.HRRR_OPS to visibility.HRRR_OPS_LEGACY_retro
                    statement = """select m0.*
                    from  madis3.metars as s, visibility.HRRR_OPS as m0
                    WHERE 1=1
                    AND s.madis_id = m0.madis_id
                    AND s.name = %s
                    AND  m0.time >= %s - 1800 and m0.time < %s + 1800
                    AND m0.fcst_len IN (""" + format_strings + ") ORDER BY m0.fcst_len;"
                    cursor.execute(statement, tuple(params))
                    mysql_model_visibility_values_tmp = cursor.fetchall()
                    mysql_model_visibility = (
                        [v["vis100"] / 100 for v in mysql_model_visibility_values_tmp]
                        if len(mysql_model_visibility_values_tmp) > 0
                        else None
                    )

                    # now we have values for this time for each fcst_len, iterate the fcst_len and assert each value
                    intersect_data_dict = {}
                    for i in intersect_fcst_len:
                        intersect_data_dict[i] = {}
                        # there is a possibility that a fcst length isn't available (if the retor isn't finished)
                        # so ignore that case.
                        try:
                            mysql_index = mysql_model_fcst_len.index(i)
                        except:
                            continue
                        cb = next(item for item in cb_model_values if item["fcstLen"] == i)
                        intersect_data_dict[i]["cb"] = {}
                        intersect_data_dict[i]["cb"]["fcstLen"] = cb["fcstLen"]
                        intersect_data_dict[i]["cb"].update(cb["data_item"])
                        intersect_data_dict[i]["mysql"] = {}
                        intersect_data_dict[i]["mysql"]["fcst_len"] = mysql_model_fcst_len[mysql_index]
                        intersect_data_dict[i]["mysql"]["press"] = mysql_model_press[mysql_index]
                        intersect_data_dict[i]["mysql"]["temp"] = mysql_model_temp[mysql_index]
                        intersect_data_dict[i]["mysql"]["dp"] = mysql_model_dp[mysql_index]
                        intersect_data_dict[i]["mysql"]["rh"] = mysql_model_rh[mysql_index]
                        intersect_data_dict[i]["mysql"]["ws"] = mysql_model_ws[mysql_index]
                        intersect_data_dict[i]["mysql"]["wd"] = mysql_model_wd[mysql_index]

                        try:
                            if mysql_model_ceiling is None or mysql_index >= len(mysql_model_ceiling) or mysql_model_ceiling[mysql_index] is None:
                                intersect_data_dict[i]["mysql"]["ceiling"] = None
                            else:
                                intersect_data_dict[i]["mysql"]["ceiling"] = mysql_model_ceiling[mysql_index]

                            if mysql_model_visibility is None or mysql_index >= len(mysql_model_visibility) or mysql_model_visibility[mysql_index] is None:
                                intersect_data_dict[i]["mysql"]["visibility"] = None
                            else:
                                intersect_data_dict[i]["mysql"]["visibility"] = mysql_model_visibility[mysql_index]
                        except:
                            self.fail(
                            "TestGsdIngestManager Exception failure for ceiling or visibility: " + str(sys.exc_info()[0])
                            )
                        print(
                            "time: {0}\t\tfcst_len: {1}\t\tstation:{2}".format(
                                time, i, station
                            )
                        )
                        print("field\t\tmysql\t\tcb\t\t\t\tdelta\t\t\tunits")

                        if intersect_data_dict[i]["mysql"]["press"] is not None and intersect_data_dict[i]["cb"]["Surface Pressure"] is not None:
                            delta = intersect_data_dict[i]["mysql"]["press"] - intersect_data_dict[i]["cb"]["Surface Pressure"]
                        else:
                            delta = None
                        print(
                            "var - 'press'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                                intersect_data_dict[i]["mysql"]["press"],
                                intersect_data_dict[i]["cb"]["Surface Pressure"],
                                delta, units['Surface Pressure']
                            )
                        )

                        if intersect_data_dict[i]["mysql"]["temp"] is not None and intersect_data_dict[i]["cb"]["Temperature"] is not None:
                            delta = intersect_data_dict[i]["mysql"]["temp"] - intersect_data_dict[i]["cb"]["Temperature"]
                        else:
                            delta = None
                        print(
                            "var - 'temp'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                                intersect_data_dict[i]["mysql"]["temp"],
                                intersect_data_dict[i]["cb"]["Temperature"],
                                delta, units['Temperature']
                            )
                        )

                        if intersect_data_dict[i]["mysql"]["dp"] is not None and intersect_data_dict[i]["cb"]["DewPoint"] is not None:
                            delta = intersect_data_dict[i]["mysql"]["dp"] - intersect_data_dict[i]["cb"]["DewPoint"]
                        else:
                            delta = None
                        print(
                            "var - 'dp'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                                intersect_data_dict[i]["mysql"]["dp"],
                                intersect_data_dict[i]["cb"]["DewPoint"],
                                delta, units['DewPoint']
                            )
                        )

                        if intersect_data_dict[i]["mysql"]["rh"] is not None and intersect_data_dict[i]["cb"]["RH"] is not None:
                            delta = intersect_data_dict[i]["mysql"]["rh"] - intersect_data_dict[i]["cb"]["RH"]
                        else:
                            delta = None
                        print(
                            "var - 'rh'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                                intersect_data_dict[i]["mysql"]["rh"],
                                intersect_data_dict[i]["cb"]["RH"],
                                delta, units['RH']
                            )
                        )

                        if intersect_data_dict[i]["mysql"]["ws"] is not None and intersect_data_dict[i]["cb"]["WS"] is not None:
                            delta = intersect_data_dict[i]["mysql"]["ws"] - intersect_data_dict[i]["cb"]["WS"]
                        else:
                            delta = None
                        print(
                            "var - 'ws'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                                intersect_data_dict[i]["mysql"]["ws"],
                                intersect_data_dict[i]["cb"]["WS"],
                                delta, units['WS']
                            )
                        )

                        if intersect_data_dict[i]["mysql"]["wd"] is not None and intersect_data_dict[i]["cb"]["WD"] is not None:
                            delta = intersect_data_dict[i]["mysql"]["wd"] - intersect_data_dict[i]["cb"]["WD"]
                            if delta > 180:
                                delta = 360 - delta
                            if delta < -180:
                                delta = 360 + delta
                        else:
                            delta = None
                        print(
                            "var - 'wd'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                                intersect_data_dict[i]["mysql"]["wd"],
                                intersect_data_dict[i]["cb"]["WD"],
                                delta, units['WD']
                            )
                        )

                        if intersect_data_dict[i]["mysql"]["ceiling"] is not None and intersect_data_dict[i]["cb"]["Ceiling"] is not None:
                            delta = intersect_data_dict[i]["mysql"]["ceiling"] - intersect_data_dict[i]["cb"]["Ceiling"]
                        else:
                            delta = None
                        print(
                            "var - 'ceiling'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                                intersect_data_dict[i]["mysql"]["ceiling"],
                                intersect_data_dict[i]["cb"]["Ceiling"],
                                delta, units['Ceiling']
                            )
                        )

                        if intersect_data_dict[i]["mysql"]["visibility"] is not None and intersect_data_dict[i]["cb"]["Visibility"] is not None:
                            delta = intersect_data_dict[i]["mysql"]["visibility"] - intersect_data_dict[i]["cb"]["Visibility"]
                        else:
                            delta = None
                        print(
                            "var - 'visibility'\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                                intersect_data_dict[i]["mysql"]["visibility"],
                                intersect_data_dict[i]["cb"]["Visibility"],
                                delta, units['Visibility']
                            )
                        )
                        print("--")

                    for i in intersect_fcst_len:
                        if i not in intersect_data_dict.keys() or "mysql" not in intersect_data_dict[i].keys() or "cb" not in intersect_data_dict[i].keys():
                            continue
                        self.assertEqual(
                            intersect_data_dict[i]["mysql"]["fcst_len"],
                            intersect_data_dict[i]["cb"]["fcstLen"],
                            msg="MYSQL fcst_len and CB fcstLen are not equal",
                        )

                        if intersect_data_dict[i]["mysql"]["press"] is not None and intersect_data_dict[i]["cb"]["Surface Pressure"] is not None:
                            np.testing.assert_allclose(
                                intersect_data_dict[i]["mysql"]["press"],
                                intersect_data_dict[i]["cb"]["Surface Pressure"],
                                atol=1.5,
                                rtol=0,
                                err_msg="MYSQL Pressure and CB Surface Pressure are not approximately equal",
                                verbose=True,
                            )
                        if intersect_data_dict[i]["mysql"]["temp"] is not None and intersect_data_dict[i]["cb"]["Temperature"] is not None:
                            np.testing.assert_allclose(
                                intersect_data_dict[i]["mysql"]["temp"],
                                intersect_data_dict[i]["cb"]["Temperature"],
                                atol=3,
                                rtol=0,
                                err_msg="MYSQL temp and CB Temperature are not approximately equal",
                                verbose=True,
                            )
                        if intersect_data_dict[i]["mysql"]["dp"] is not None and intersect_data_dict[i]["cb"]["DewPoint"] is not None:
                            np.testing.assert_allclose(
                                intersect_data_dict[i]["mysql"]["dp"],
                                intersect_data_dict[i]["cb"]["DewPoint"],
                                atol=5,
                                rtol=0,
                                err_msg="MYSQL dp and CB Dew Point are not approximately equal",
                                verbose=True,
                            )
                        if intersect_data_dict[i]["mysql"]["rh"] is not None and intersect_data_dict[i]["cb"]["RH"] is not None:
                            np.testing.assert_allclose(
                                intersect_data_dict[i]["mysql"]["rh"],
                                intersect_data_dict[i]["cb"]["RH"],
                                atol=20,
                                rtol=0,
                                err_msg="MYSQL rh and CB RH are not approximately equal",
                                verbose=True,
                            )
                        if intersect_data_dict[i]["mysql"]["wd"] is not None and intersect_data_dict[i]["cb"]["WD"] is not None:
                            np.testing.assert_allclose(
                                intersect_data_dict[i]["mysql"]["wd"],
                                intersect_data_dict[i]["cb"]["WD"],
                                atol=9999999,
                                rtol=0,
                                err_msg="MYSQL wd and CB WD are not approximately equal",
                                verbose=True,
                            )
                        if intersect_data_dict[i]["mysql"]["ws"] is not None and intersect_data_dict[i]["cb"]["WS"] is not None:
                            np.testing.assert_allclose(
                                intersect_data_dict[i]["mysql"]["ws"],
                                intersect_data_dict[i]["cb"]["WS"],
                                atol=3,
                                rtol=0,
                                err_msg="MYSQL ws and CB WS are not approximately equal",
                                verbose=True,
                            )
                        if intersect_data_dict[i]["mysql"]["visibility"] is not None and intersect_data_dict[i]["cb"]["Visibility"] is not None:
                            np.testing.assert_allclose(
                                intersect_data_dict[i]["mysql"]["visibility"],
                                intersect_data_dict[i]["cb"]["Visibility"],
                                atol=99999,
                                rtol=0,
                                err_msg="MYSQL Visibility and CB Visibility are not approximately equal",
                                verbose=True,
                            )
                        if intersect_data_dict[i]["mysql"]["ceiling"] is not None and intersect_data_dict[i]["cb"]["Ceiling"] is not None:
                            np.testing.assert_allclose(
                                intersect_data_dict[i]["mysql"]["ceiling"],
                                intersect_data_dict[i]["cb"]["Ceiling"],
                                atol=99999,
                                rtol=0,
                                err_msg="MYSQL Ceiling and CB Ceiling are not approximately equal",
                                verbose=True,
                            )
        except:
            print(str(sys.exc_info()))
            self.fail("TestGsdIngestManager Exception failure: " + str(sys.exc_info()))

    def test_compare_model_to_mysql_all_stations(self):
        """This test attempts to find recent models that match in both the mysql and the CB
        databases and compare them for all stations for one fcstValidEpoch.
        This test isn't likely to succeed unless both the legacy
        ingest and the VxIngest have recently run.
        """
        try:
            station = "KPDX"
            credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
            self.assertTrue(
                os.path.isfile(credentials_file), "credentials_file Does not exist"
            )

            f = open(credentials_file)
            yaml_data = yaml.load(f, yaml.SafeLoader)
            host = yaml_data["cb_host"]
            user = yaml_data["cb_user"]
            password = yaml_data["cb_password"]
            options = ClusterOptions(PasswordAuthenticator(user, password))
            cluster = Cluster("couchbase://" + host, options)

            result = cluster.query(
                """SELECT DISTINCT RAW fcstValidEpoch
                FROM mdata
                WHERE type='DD'
                AND docType="model"
                AND model="HRRR_OPS"
                AND version='V01'
                AND subset='METAR'
                ORDER BY fcstValidEpoch DESC;"""
            )
            cb_hrrr_ops_fcst_valid_epochs = list(result)

            result = cluster.query(
                """SELECT DISTINCT RAW fcstValidEpoch
                FROM mdata
                WHERE type='DD'
                AND docType="obs"
                AND version='V01'
                AND subset='METAR'
                ORDER BY fcstValidEpoch DESC;"""
            )
            cb_obs_fcst_valid_epochs = list(result)

            # find the intersection of the cb models and the obs
            intersect_cb_times = [
                value
                for value in cb_hrrr_ops_fcst_valid_epochs
                if value in cb_obs_fcst_valid_epochs
            ]

            # find the mysql hrrr_ops and obs

            host = yaml_data["mysql_host"]
            user = yaml_data["mysql_user"]
            passwd = yaml_data["mysql_password"]
            local_infile = True
            connection = pymysql.connect(
                host=host,
                user=user,
                passwd=passwd,
                local_infile=local_infile,
                autocommit=True,
                charset="utf8mb4",
                cursorclass=pymysql.cursors.SSDictCursor,
                client_flag=CLIENT.MULTI_STATEMENTS,
            )
            cursor = connection.cursor(pymysql.cursors.SSDictCursor)
            statement = """SELECT floor((m0.time+1800)/(3600))*3600 AS time
                            FROM   madis3.HRRR_OPSqp AS m0,
                            madis3.obs AS o,
                            madis3.metars AS s
                            WHERE  s.name = %s
                            AND m0.time = o.time
                            AND s.madis_id = m0.sta_id
                            AND o.sta_id = m0.sta_id
                            AND m0.fcst_len = 0
                            AND m0.time <= %s
                            AND m0.time >= %s;"""
            cursor.execute(statement, (station, intersect_cb_times[0], intersect_cb_times[-1]))
            intersect_mysql_times_tmp = cursor.fetchall()
            intersect_mysql_times = [t["time"] for t in intersect_mysql_times_tmp]
            valid_times = [
                value for value in intersect_mysql_times if value in intersect_cb_times
            ]

            result = cluster.query(
                """SELECT raw mdata.units
                    FROM mdata
                    UNNEST mdata.data AS data_item
                    WHERE mdata.type='DD'
                        AND mdata.docType="obs"
                        AND mdata.version='V01'
                        AND mdata.subset='METAR'
                        AND data_item.name=$station
                        AND mdata.fcstValidEpoch=$time""",
                        time=valid_times[0], station=station)
            units = list(result)[0]

            for time in valid_times:
                # get the common fcst lengths
                result = cluster.query(
                    """SELECT raw mdata.fcstLen
                        FROM mdata
                        UNNEST mdata.data AS data_item
                        WHERE mdata.type='DD'
                            AND mdata.docType="model"
                            AND mdata.model="HRRR_OPS"
                            AND mdata.version='V01'
                            AND mdata.subset='METAR'
                            AND data_item.name=$station
                            AND mdata.fcstValidEpoch=$time
                            ORDER BY mdata.fcstLen""",
                            time=time, station=station)
                cb_model_fcst_lens = list(result)

                statement = """SELECT DISTINCT m0.fcst_len
                        FROM   madis3.metars AS s,
                            ceiling2.HRRR_OPS AS m0
                        WHERE  1 = 1
                            AND s.madis_id = m0.madis_id
                            AND s.NAME = "%s"
                            AND m0.time >= %s - 1800
                            AND m0.time < %s + 1800
                        ORDER  BY m0.fcst_len; """
                cursor.execute(statement, (station, time, time))
                mysql_model_fcst_lens_dict = cursor.fetchall()
                mysql_model_fcst_lens = [v["fcst_len"] for v in mysql_model_fcst_lens_dict]
                intersect_fcst_len = [
                    value for value in mysql_model_fcst_lens if value in cb_model_fcst_lens
                ]
                if len(intersect_fcst_len) == 0:
                    # no fcst_len in common
                    continue

                result = cluster.query(
                    """SELECT DISTINCT mdata.fcstValidEpoch,
                        mdata.fcstLen, data_item
                        FROM mdata
                        UNNEST mdata.data AS data_item
                        WHERE mdata.type='DD'
                            AND mdata.docType="model"
                            AND mdata.model="HRRR_OPS"
                            AND mdata.version='V01'
                            AND mdata.subset='METAR'
                            AND data_item.name=$station
                            AND mdata.fcstValidEpoch=$time
                            AND mdata.fcstLen IN $fcst_lens
                            ORDER BY mdata.fcstLen""",
                    station=station, time=time, fcst_lens=intersect_fcst_len)
                cb_model_values = list(result)

                format_strings = ','.join(['%s'] * len(intersect_fcst_len))
                params = [station,time,time]
                params.extend(intersect_fcst_len)
                statement = """select m0.*
                from  madis3.metars as s, madis3.HRRR_OPSqp as m0
                WHERE 1=1
                AND s.madis_id = m0.sta_id
                AND s.name = %s
                AND  m0.time >= %s - 1800 and m0.time < %s + 1800
                AND m0.fcst_len IN (""" + format_strings + ") ORDER BY m0.fcst_len;"
                cursor.execute(statement, tuple(params))
                mysql_model_values_tmp = cursor.fetchall()
                mysql_model_fcst_len = [v["fcst_len"] for v in mysql_model_values_tmp]
                mysql_model_press = [v["press"] / 10 for v in mysql_model_values_tmp]
                mysql_model_temp = [v["temp"] / 10 for v in mysql_model_values_tmp]
                mysql_model_dp = [v["dp"] / 10 for v in mysql_model_values_tmp]
                mysql_model_wd = [v["wd"] for v in mysql_model_values_tmp]
                mysql_model_ws = [v["ws"] for v in mysql_model_values_tmp]
                mysql_model_rh = [v["rh"] / 10 for v in mysql_model_values_tmp]

                statement = """select m0.*
                from  madis3.metars as s, ceiling2.HRRR_OPS as m0
                WHERE 1=1
                AND s.madis_id = m0.madis_id
                AND s.name = %s
                AND  m0.time >= %s - 1800 and m0.time < %s + 1800
                AND m0.fcst_len IN (""" + format_strings + ") ORDER BY m0.fcst_len;"
                cursor.execute(statement, tuple(params))
                mysql_model_ceiling_values_tmp = cursor.fetchall()
                mysql_model_ceiling = (
                    [v["ceil"] * 10 for v in mysql_model_ceiling_values_tmp]
                    if len(mysql_model_ceiling_values_tmp) > 0
                    else None
                )

                statement = """select m0.*
                from  madis3.metars as s, visibility.HRRR_OPS as m0
                WHERE 1=1
                AND s.madis_id = m0.madis_id
                AND s.name = %s
                AND  m0.time >= %s - 1800 and m0.time < %s + 1800
                AND m0.fcst_len IN (""" + format_strings + ") ORDER BY m0.fcst_len;"
                cursor.execute(statement, tuple(params))
                mysql_model_visibility_values_tmp = cursor.fetchall()
                mysql_model_visibility = (
                    [v["vis100"] / 100 for v in mysql_model_visibility_values_tmp]
                    if len(mysql_model_visibility_values_tmp) > 0
                    else None
                )

                # now we have values for this time for each fcst_len, iterate the fcst_len and assert each value
                intersect_data_dict = {}
                for i in intersect_fcst_len:
                    intersect_data_dict[i] = {}
                    mysql_index = mysql_model_fcst_len.index(i)
                    cb = next(item for item in cb_model_values if item["fcstLen"] == i)
                    intersect_data_dict[i]["cb"] = {}
                    intersect_data_dict[i]["cb"]["fcstLen"] = cb["fcstLen"]
                    intersect_data_dict[i]["cb"].update(cb["data_item"])
                    intersect_data_dict[i]["mysql"] = {}
                    intersect_data_dict[i]["mysql"]["fcst_len"] = mysql_model_fcst_len[mysql_index]
                    intersect_data_dict[i]["mysql"]["press"] = mysql_model_press[mysql_index]
                    intersect_data_dict[i]["mysql"]["temp"] = mysql_model_temp[mysql_index]
                    intersect_data_dict[i]["mysql"]["dp"] = mysql_model_dp[mysql_index]
                    intersect_data_dict[i]["mysql"]["rh"] = mysql_model_rh[mysql_index]
                    intersect_data_dict[i]["mysql"]["ws"] = mysql_model_ws[mysql_index]
                    intersect_data_dict[i]["mysql"]["wd"] = mysql_model_wd[mysql_index]

                    try:
                        if mysql_model_ceiling is None or mysql_index >= len(mysql_model_ceiling) or mysql_model_ceiling[mysql_index] is None:
                            intersect_data_dict[i]["mysql"]["ceiling"] = None
                        else:
                            intersect_data_dict[i]["mysql"]["ceiling"] = mysql_model_ceiling[mysql_index]

                        if mysql_model_visibility is None or mysql_index >= len(mysql_model_visibility) or mysql_model_visibility[mysql_index] is None:
                            intersect_data_dict[i]["mysql"]["visibility"] = None
                        else:
                            intersect_data_dict[i]["mysql"]["visibility"] = mysql_model_visibility[mysql_index]
                    except:
                        self.fail(
                           "TestGsdIngestManager Exception failure for ceiling or visibility: " + str(sys.exc_info()[0])
                        )
                    print(
                        "time: {0}\t\tfcst_len: {1}\t\tstation:{2}".format(
                            time, i, station
                        )
                    )
                    print("field\t\tmysql\t\tcb\t\t\t\tdelta\t\t\tunits")

                    if intersect_data_dict[i]["mysql"]["press"] is not None and intersect_data_dict[i]["cb"]["Surface Pressure"] is not None:
                        delta = intersect_data_dict[i]["mysql"]["press"] - intersect_data_dict[i]["cb"]["Surface Pressure"]
                    else:
                        delta = None
                    print(
                        "var - 'press'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                            intersect_data_dict[i]["mysql"]["press"],
                            intersect_data_dict[i]["cb"]["Surface Pressure"],
                            delta, units['Surface Pressure']
                        )
                    )

                    if intersect_data_dict[i]["mysql"]["temp"] is not None and intersect_data_dict[i]["cb"]["Temperature"] is not None:
                        delta = intersect_data_dict[i]["mysql"]["temp"] - intersect_data_dict[i]["cb"]["Temperature"]
                    else:
                        delta = None
                    print(
                        "var - 'temp'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                            intersect_data_dict[i]["mysql"]["temp"],
                            intersect_data_dict[i]["cb"]["Temperature"],
                            delta, units['Temperature']
                        )
                    )

                    if intersect_data_dict[i]["mysql"]["dp"] is not None and intersect_data_dict[i]["cb"]["DewPoint"] is not None:
                        delta = intersect_data_dict[i]["mysql"]["dp"] - intersect_data_dict[i]["cb"]["DewPoint"]
                    else:
                        delta = None
                    print(
                        "var - 'dp'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                            intersect_data_dict[i]["mysql"]["dp"],
                            intersect_data_dict[i]["cb"]["DewPoint"],
                            delta, units['DewPoint']
                        )
                    )

                    if intersect_data_dict[i]["mysql"]["rh"] is not None and intersect_data_dict[i]["cb"]["RH"] is not None:
                        delta = intersect_data_dict[i]["mysql"]["rh"] - intersect_data_dict[i]["cb"]["RH"]
                    else:
                        delta = None
                    print(
                        "var - 'rh'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                            intersect_data_dict[i]["mysql"]["rh"],
                            intersect_data_dict[i]["cb"]["RH"],
                            delta, units['RH']
                        )
                    )

                    if intersect_data_dict[i]["mysql"]["ws"] is not None and intersect_data_dict[i]["cb"]["WS"] is not None:
                        delta = intersect_data_dict[i]["mysql"]["ws"] - intersect_data_dict[i]["cb"]["WS"]
                    else:
                        delta = None
                    print(
                        "var - 'ws'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                            intersect_data_dict[i]["mysql"]["ws"],
                            intersect_data_dict[i]["cb"]["WS"],
                            delta, units['WS']
                        )
                    )

                    if intersect_data_dict[i]["mysql"]["wd"] is not None and intersect_data_dict[i]["cb"]["WD"] is not None:
                        delta = intersect_data_dict[i]["mysql"]["wd"] - intersect_data_dict[i]["cb"]["WD"]
                        if delta > 180:
                            delta = 360 - delta
                        if delta < -180:
                            delta = 360 + delta
                    else:
                        delta = None
                    print(
                        "var - 'wd'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                            intersect_data_dict[i]["mysql"]["wd"],
                            intersect_data_dict[i]["cb"]["WD"],
                            delta, units['WD']
                        )
                    )

                    if intersect_data_dict[i]["mysql"]["ceiling"] is not None and intersect_data_dict[i]["cb"]["Ceiling"] is not None:
                        delta = intersect_data_dict[i]["mysql"]["ceiling"] - intersect_data_dict[i]["cb"]["Ceiling"]
                    else:
                        delta = None
                    print(
                        "var - 'ceiling'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                            intersect_data_dict[i]["mysql"]["ceiling"],
                            intersect_data_dict[i]["cb"]["Ceiling"],
                            delta, units['Ceiling']
                        )
                    )

                    if intersect_data_dict[i]["mysql"]["visibility"] is not None and intersect_data_dict[i]["cb"]["Visibility"] is not None:
                        delta = intersect_data_dict[i]["mysql"]["visibility"] - intersect_data_dict[i]["cb"]["Visibility"]
                    else:
                        delta = None
                    print(
                        "'visibility'\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                            intersect_data_dict[i]["mysql"]["visibility"],
                            intersect_data_dict[i]["cb"]["Visibility"],
                            delta, units['Visibility']
                        )
                    )
                    print("--")

                for i in intersect_fcst_len:
                    self.assertEqual(
                        intersect_data_dict[i]["mysql"]["fcst_len"],
                        intersect_data_dict[i]["cb"]["fcstLen"],
                        msg="MYSQL fcst_len and CB fcstLen are not equal",
                    )

                    if intersect_data_dict[i]["mysql"]["press"] is not None and intersect_data_dict[i]["cb"]["Surface Pressure"] is not None:
                        np.testing.assert_allclose(
                            intersect_data_dict[i]["mysql"]["press"],
                            intersect_data_dict[i]["cb"]["Surface Pressure"],
                            atol=1.5,
                            rtol=0,
                            err_msg="MYSQL Pressure and CB Surface Pressure are not approximately equal",
                            verbose=True,
                        )
                    if intersect_data_dict[i]["mysql"]["temp"] is not None and intersect_data_dict[i]["cb"]["Temperature"] is not None:
                        np.testing.assert_allclose(
                            intersect_data_dict[i]["mysql"]["temp"],
                            intersect_data_dict[i]["cb"]["Temperature"],
                            atol=3,
                            rtol=0,
                            err_msg="MYSQL temp and CB Temperature are not approximately equal",
                            verbose=True,
                        )
                    if intersect_data_dict[i]["mysql"]["dp"] is not None and intersect_data_dict[i]["cb"]["DewPoint"] is not None:
                        np.testing.assert_allclose(
                            intersect_data_dict[i]["mysql"]["dp"],
                            intersect_data_dict[i]["cb"]["DewPoint"],
                            atol=5,
                            rtol=0,
                            err_msg="MYSQL dp and CB Dew Point are not approximately equal",
                            verbose=True,
                        )
                    if intersect_data_dict[i]["mysql"]["rh"] is not None and intersect_data_dict[i]["cb"]["RH"] is not None:
                        np.testing.assert_allclose(
                            intersect_data_dict[i]["mysql"]["rh"],
                            intersect_data_dict[i]["cb"]["RH"],
                            atol=20,
                            rtol=0,
                            err_msg="MYSQL rh and CB RH are not approximately equal",
                            verbose=True,
                        )
                    if intersect_data_dict[i]["mysql"]["wd"] is not None and intersect_data_dict[i]["cb"]["WD"] is not None:
                        np.testing.assert_allclose(
                            intersect_data_dict[i]["mysql"]["wd"],
                            intersect_data_dict[i]["cb"]["WD"],
                            atol=9999999,
                            rtol=0,
                            err_msg="MYSQL wd and CB WD are not approximately equal",
                            verbose=True,
                        )
                    if intersect_data_dict[i]["mysql"]["ws"] is not None and intersect_data_dict[i]["cb"]["WS"] is not None:
                        np.testing.assert_allclose(
                            intersect_data_dict[i]["mysql"]["ws"],
                            intersect_data_dict[i]["cb"]["WS"],
                            atol=3,
                            rtol=0,
                            err_msg="MYSQL ws and CB WS are not approximately equal",
                            verbose=True,
                        )
                    if intersect_data_dict[i]["mysql"]["visibility"] is not None and intersect_data_dict[i]["cb"]["Visibility"] is not None:
                        np.testing.assert_allclose(
                            intersect_data_dict[i]["mysql"]["visibility"],
                            intersect_data_dict[i]["cb"]["Visibility"],
                            atol=99999,
                            rtol=0,
                            err_msg="MYSQL Visibility and CB Visibility are not approximately equal",
                            verbose=True,
                        )
                    if intersect_data_dict[i]["mysql"]["ceiling"] is not None and intersect_data_dict[i]["cb"]["Ceiling"] is not None:
                        np.testing.assert_allclose(
                            intersect_data_dict[i]["mysql"]["ceiling"],
                            intersect_data_dict[i]["cb"]["Ceiling"],
                            atol=99999,
                            rtol=0,
                            err_msg="MYSQL Ceiling and CB Ceiling are not approximately equal",
                            verbose=True,
                        )
        except:
            print(str(sys.exc_info()))
            self.fail("TestGsdIngestManager Exception failure: " + str(sys.exc_info()))

    def test_gribBuilder_one_thread_file_pattern_hrrr_ops_conus(self):
        try:
            #1632412800 fcst_len 1 -> 1632412800 - 1 * 3600 -> 1632409200 September 23, 2021 15:00:00 -> 2126615000001
            #1632412800 fcst_len 3 -> 1632412800 - 3 * 3600 -> 1632402000 September 23, 2021 13:00:00 -> 2126613000003
            #1632412800 fcst_len 15 -> 1632412800 - 15 * 3600 -> 1632358800 September 22, 2021 19:00:00  ->  (missing)
            #1632412800 fcst_len 18 -> 1632412800 - 18 * 3600 -> 1632348000 September 22, 2021 22:00:00 -> 2126522000018 (missing)
            #1632420000 September 23, 2021 18:00:00  2126616000018
            #1632423600  September 23, 2021 19:00:00 2126617000001
            #first_epoch = 1634252400 - 10
            #last_epoch = 1634252400 + 10
            cwd = os.getcwd()
            self.credentials_file = os.environ['HOME'] + '/adb-cb1-credentials'
            self.spec_file = cwd + '/grib2_to_cb/test/test_load_spec_grib_metar_hrrr_ops_V01.yaml'
            # remove output files
            for _f in glob('/opt/data/grib2_to_cb/output/test1/*.json'):
                os.remove(_f)
            vxIngest = VXIngest()
            vxIngest.runit({'spec_file': self.spec_file,
                            'credentials_file': self.credentials_file,
                            'path': '/opt/public/data/grids/hrrr/conus/wrfprs/grib2',
                            'file_name_mask': '%y%j%H%f',
                            'output_dir': '/opt/data/grib2_to_cb/output/test1',
                            'threads': 1,
                            'file_pattern': '21287230000[0123456789]?'
                            })

        except:
            self.fail("TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus Exception failure: " +
                      str(sys.exc_info()[0]))
        finally:
            # remove output files
            for _f in glob('/opt/data/grib2_to_cb/output/test1/*.json'):
                os.remove(_f)

    def test_gribBuilder_two_threads_file_pattern_hrrr_ops_conus(self):
        try:
            #1632412800 fcst_len 1 -> 1632412800 - 1 * 3600 -> 1632409200 September 23, 2021 15:00:00 -> 2126615000001
            #1632412800 fcst_len 3 -> 1632412800 - 3 * 3600 -> 1632402000 September 23, 2021 13:00:00 -> 2126613000003
            #1632412800 fcst_len 15 -> 1632412800 - 15 * 3600 -> 1632358800 September 22, 2021 19:00:00  ->  (missing)
            #1632412800 fcst_len 18 -> 1632412800 - 18 * 3600 -> 1632348000 September 22, 2021 22:00:00 -> 2126522000018 (missing)
            #1632420000 September 23, 2021 18:00:00  2126616000018
            #1632423600  September 23, 2021 19:00:00 2126617000001
            #first_epoch = 1634252400 - 10
            #last_epoch = 1634252400 + 10
            cwd = os.getcwd()
            self.credentials_file = os.environ['HOME'] + '/adb-cb1-credentials'
            self.spec_file = cwd + '/grib2_to_cb/test/test_load_spec_grib_metar_hrrr_ops_V01.yaml'
            # remove output files
            for _f in glob('/opt/data/grib2_to_cb/output/test2/*.json'):
                os.remove(_f)
            vxIngest = VXIngest()
            vxIngest.runit({'spec_file': self.spec_file,
                            'credentials_file': self.credentials_file,
                            'path': '/opt/public/data/grids/hrrr/conus/wrfprs/grib2',
                            'file_name_mask': '%y%j%H%f',
                            'output_dir': '/opt/data/grib2_to_cb/output/test2',
                            'threads': 2,
                            'file_pattern': '21287230000[0123456789]?'
                            })

        except:
            self.fail("TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus Exception failure: " +
                      str(sys.exc_info()[0]))
        finally:
            # remove output files
            for _f in glob('/opt/data/grib2_to_cb/output/test2/*.json'):
                os.remove(_f)


    def test_gribBuilder_verses_script(self):
        # noinspection PyBroadException
        try:
                        # remove output files
            for _f in glob('/opt/data/grib2_to_cb/output/test3/*.json'):
                os.remove(_f)
            #list_of_input_files = glob('/opt/public/data/grids/hrrr/conus/wrfprs/grib2/*')
            #latest_input_file = max(list_of_input_files, key=os.path.getctime)
            #file_utc_time = datetime.datetime.strptime(os.path.basename(latest_input_file), '%y%j%H%f')
            cwd = os.getcwd()
            self.credentials_file = os.environ['HOME'] + '/adb-cb1-credentials'
            self.spec_file = cwd + '/grib2_to_cb/test/test_load_spec_grib_metar_hrrr_ops_V01.yaml'
            vxIngest = VXIngest()
            vxIngest.runit({'spec_file': self.spec_file,
                            'credentials_file': self.credentials_file,
                            'path': '/opt/public/data/grids/hrrr/conus/wrfprs/grib2',
                            'file_name_mask': '%y%j%H%f',
                            'output_dir': '/opt/data/grib2_to_cb/output/test3',
                            'threads': 1,
                            'file_pattern': '212872300000[012]'
                            })
                            #'file_pattern': '21287230000[0123456789]?'

            list_of_output_files = glob('/opt/data/grib2_to_cb/output/test3/[0123456789]????????????.json')
            latest_output_file = max(
                list_of_output_files, key=os.path.getctime)
            # Opening JSON file
            f = open(latest_output_file)
            # returns JSON object as
            # a dictionary
            vxIngest_output_data = json.load(f)
            # Closing file
            f.close()
            expected_station_data = {}

            f = open(self.credentials_file)
            yaml_data = yaml.load(f, yaml.SafeLoader)
            host = yaml_data['cb_host']
            user = yaml_data['cb_user']
            password = yaml_data['cb_password']
            options = ClusterOptions(PasswordAuthenticator(user, password))
            self.cluster = Cluster('couchbase://' + host, options)
            self.collection = self.cluster.bucket("mdata").default_collection()

            # Grab the projection information from the test file
            latest_input_file = "/opt/public/data/grids/hrrr/conus/wrfprs/grib2/" + os.path.basename("/opt/data/grib2_to_cb/output/test3/2128723000002.json").split('.')[0]
            self.projection = gg.getGrid(latest_input_file)
            self.grbs = pygrib.open(latest_input_file)
            self.grbm = self.grbs.message(1)
            fcst_valid_epoch = round(self.grbm.validDate.timestamp())
            self.spacing, max_x, max_y = gg.getAttributes(latest_input_file)

            self.assertEqual(self.projection.description, 'PROJ-based coordinate operation',
                             "projection description: is Not corrrect")
            # Set the two projections to be used during the transformation (nearest neighbor method, what we use for everything with METARS)
            self.in_proj = pyproj.Proj(proj='latlon')
            self.out_proj = self.projection
            self.transformer = pyproj.Transformer.from_proj(
                proj_from=self.in_proj, proj_to=self.out_proj)
            self.transformer_reverse = pyproj.Transformer.from_proj(
                proj_from=self.out_proj, proj_to=self.in_proj)
            self.domain_stations = []
            for i in vxIngest_output_data[0]['data']:
                station_name = i['name']
                result = self.cluster.query(
                    "SELECT mdata.geo from mdata where type='MD' and docType='station' and subset='METAR' and version='V01' and mdata.name = $name",
                    name=station_name)
                row = result.get_single_result()
                geo_index = self.get_geo_index(fcst_valid_epoch,row['geo'])
                i['lat'] = row['geo'][geo_index]['lat']
                i['lon'] = row['geo'][geo_index]['lon']
                x, y = self.transformer.transform(
                    row['geo'][geo_index]['lon'], row['geo'][geo_index]['lat'], radians=False)
                x_gridpoint, y_gridpoint = x/self.spacing, y/self.spacing
                if x_gridpoint < 0 or x_gridpoint > max_x or y_gridpoint < 0 or y_gridpoint > max_y:
                    continue
                station = copy.deepcopy(row)
                station['geo'][geo_index]['x_gridpoint'] = x_gridpoint
                station['geo'][geo_index]['y_gridpoint'] = y_gridpoint
                station['name'] = station_name
                self.domain_stations.append(station)

            expected_station_data['fcstValidEpoch'] = fcst_valid_epoch
            self.assertEqual(expected_station_data['fcstValidEpoch'], vxIngest_output_data[0]['fcstValidEpoch'],
                             "expected fcstValidEpoch and derived fcstValidEpoch are not the same")
            expected_station_data['fcstValidISO'] = DT.datetime.fromtimestamp(fcst_valid_epoch).isoformat()
            self.assertEqual(expected_station_data['fcstValidISO'], vxIngest_output_data[0]['fcstValidISO'],
                             "expected fcstValidISO and derived fcstValidISO are not the same")
            expected_station_data['id'] = "DD-TEST:V01:METAR:HRRR_OPS:" + str(expected_station_data['fcstValidEpoch']) + ":" + str(
                self.grbm.forecastTime)
            self.assertEqual(expected_station_data['id'], vxIngest_output_data[0]['id'],
                             "expected id and derived id are not the same")

            # Ceiling
            message = self.grbs.select(name='Orography')[0]
            surface_hgt_values = message['values']

            message = self.grbs.select(
                name='Geopotential Height', typeOfFirstFixedSurface='215')[0]
            ceil_values = message['values']

            for i in range(len(self.domain_stations)):
                station = self.domain_stations[i]
                geo_index = self.get_geo_index(fcst_valid_epoch,station['geo'])
                surface = surface_hgt_values[round(
                    station['geo'][geo_index]['y_gridpoint']), round(station['geo'][geo_index]['x_gridpoint'])]
                ceil_msl = ceil_values[round(
                    station['geo'][geo_index]['y_gridpoint']), round(station['geo'][geo_index]['x_gridpoint'])]
                # Convert to ceiling AGL and from meters to tens of feet (what is currently inside SQL, we'll leave it as just feet in CB)
                ceil_agl = (ceil_msl - surface) * 3.281

                # lazy initialization of _expected_station_data
                if 'data' not in expected_station_data.keys():
                    expected_station_data['data'] = []
                if len(expected_station_data['data']) <= i:
                    expected_station_data['data'].append({})

                expected_station_data['data'][i]['Ceiling'] = ceil_agl if not np.ma.is_masked(ceil_agl) else None

            # Surface Pressure
            message = self.grbs.select(name='Surface pressure')[0]
            values = message['values']
            for i in range(len(self.domain_stations)):
                station = self.domain_stations[i]
                geo_index = self.get_geo_index(fcst_valid_epoch,station['geo'])
                value = values[round(station['geo'][geo_index]['y_gridpoint']), round(
                    station['geo'][geo_index]['x_gridpoint'])]
                # interpolated gridpoints cannot be rounded
                interpolated_value = gg.interpGridBox(
                    values, station['geo'][geo_index]['y_gridpoint'], station['geo'][geo_index]['x_gridpoint'])
                pres_mb = interpolated_value / 100
                expected_station_data['data'][i]['Surface Pressure'] = pres_mb if not np.ma.is_masked(pres_mb) else None

            # Temperature
            message = self.grbs.select(name='2 metre temperature')[0]
            values = message['values']
            for i in range(len(self.domain_stations)):
                station = self.domain_stations[i]
                geo_index = self.get_geo_index(fcst_valid_epoch,station['geo'])
                tempk = gg.interpGridBox(
                    values, station['geo'][geo_index]['y_gridpoint'], station['geo'][geo_index]['x_gridpoint'])
                tempf = ((tempk-273.15)*9)/5 + 32
                expected_station_data['data'][i]['Temperature'] = tempf if not np.ma.is_masked(tempf) else None

            # Dewpoint
            message = self.grbs.select(name='2 metre dewpoint temperature')[0]
            values = message['values']
            for i in range(len(self.domain_stations)):
                station = self.domain_stations[i]
                geo_index = self.get_geo_index(fcst_valid_epoch,station['geo'])
                dpk = gg.interpGridBox(
                    values, station['geo'][geo_index]['y_gridpoint'], station['geo'][geo_index]['x_gridpoint'])
                dpf = ((dpk-273.15)*9)/5 + 32
                expected_station_data['data'][i]['DewPoint'] = dpf if not np.ma.is_masked(dpf) else None

            # Relative Humidity
            message = self.grbs.select(name='2 metre relative humidity')[0]
            values = message['values']
            for i in range(len(self.domain_stations)):
                station = self.domain_stations[i]
                geo_index = self.get_geo_index(fcst_valid_epoch,station['geo'])
                rh = gg.interpGridBox(
                    values, station['geo'][geo_index]['y_gridpoint'], station['geo'][geo_index]['x_gridpoint'])
                expected_station_data['data'][i]['RH'] = rh if not np.ma.is_masked(rh) else None

            # Wind Speed
            message = self.grbs.select(name='10 metre U wind component')[0]
            uwind_values = message['values']

            vwind_message = self.grbs.select(
                name='10 metre V wind component')[0]
            vwind_values = vwind_message['values']

            for i in range(len(self.domain_stations)):
                station = self.domain_stations[i]
                geo_index = self.get_geo_index(fcst_valid_epoch,station['geo'])
                uwind_ms = gg.interpGridBox(
                    uwind_values, station['geo'][geo_index]['y_gridpoint'], station['geo'][geo_index]['x_gridpoint'])
                vwind_ms = gg.interpGridBox(
                    vwind_values, station['geo'][geo_index]['y_gridpoint'], station['geo'][geo_index]['x_gridpoint'])
                # Convert from U-V components to speed and direction (requires rotation if grid is not earth relative)
                # wind speed then convert to mph
                ws_ms = math.sqrt((uwind_ms*uwind_ms)+(vwind_ms*vwind_ms))
                ws_mph = (ws_ms/0.447) + 0.5
                expected_station_data['data'][i]['WS'] = ws_mph if not np.ma.is_masked(ws_mph) else None

                # wind direction   - lon is the lon of the station
                station = self.domain_stations[i]
                theta = gg.getWindTheta(vwind_message, station['geo'][geo_index]['lon'])
                radians = math.atan2(uwind_ms, vwind_ms)
                wd = (radians*57.2958) + theta + 180
                # adjust for outliers
                if wd < 0:
                    wd = wd + 360
                if wd > 360:
                    wd = wd - 360

                expected_station_data['data'][i]['WD'] = wd if not np.ma.is_masked(wd) else None

            # Visibility
            message = self.grbs.select(name='Visibility')[0]
            values = message['values']
            for i in range(len(self.domain_stations)):
                station = self.domain_stations[i]
                geo_index = self.get_geo_index(fcst_valid_epoch,station['geo'])
                value = values[round(station['geo'][geo_index]['y_gridpoint']), round(
                    station['geo'][geo_index]['x_gridpoint'])]
                expected_station_data['data'][i]['Visibility'] = value / 1609.344 if not np.ma.is_masked(value) else None
            self.grbs.close()

            for i in range(len(self.domain_stations)):
                if expected_station_data['data'][i]['Ceiling'] is not None:
                    self.assertAlmostEqual(expected_station_data['data'][i]['Ceiling'],
                                       vxIngest_output_data[0]['data'][i]['Ceiling'], msg="Expected Ceiling and derived Ceiling are not equal")

                if expected_station_data['data'][i]['Surface Pressure'] is not None:
                    self.assertAlmostEqual(expected_station_data['data'][i]['Surface Pressure'],
                                       vxIngest_output_data[0]['data'][i]['Surface Pressure'], msg="Expected Surface Pressure and derived Surface Pressure are not equal")

                if expected_station_data['data'][i]['Temperature'] is not None:
                    self.assertAlmostEqual(expected_station_data['data'][i]['Temperature'],
                                       vxIngest_output_data[0]['data'][i]['Temperature'], msg="Expected Temperature and derived Temperature are not equal")

                if expected_station_data['data'][i]['DewPoint'] is not None:
                    self.assertAlmostEqual(expected_station_data['data'][i]['DewPoint'],
                                       vxIngest_output_data[0]['data'][i]['DewPoint'], msg="Expected DewPoint and derived DewPoint are not equal")

                if expected_station_data['data'][i]['RH'] is not None:
                    self.assertAlmostEqual(expected_station_data['data'][i]['RH'],
                                       vxIngest_output_data[0]['data'][i]['RH'], msg="Expected RH and derived RH are not equal")

                if expected_station_data['data'][i]['WS'] is not None:
                    self.assertAlmostEqual(expected_station_data['data'][i]['WS'],
                                       vxIngest_output_data[0]['data'][i]['WS'], msg="Expected WS and derived WS are not equal")

                if expected_station_data['data'][i]['WD'] is not None:
                    self.assertAlmostEqual(expected_station_data['data'][i]['WD'],
                                       vxIngest_output_data[0]['data'][i]['WD'], msg="Expected WD and derived WD are not equal")

                if expected_station_data['data'][i]['Visibility'] is not None:
                    self.assertAlmostEqual(expected_station_data['data'][i]['Visibility'],
                                       vxIngest_output_data[0]['data'][i]['Visibility'], msg="Expected Visibility and derived Visibility are not equal")

        except:
            self.fail("TestGribBuilderV01 Exception failure: " +
                      str(sys.exc_info()[0]))
        return
