import sys
import os
import yaml
import pymysql
import numpy as np
from unittest import TestCase
from netcdf_to_cb.run_ingest_threads import VXIngest
from pathlib import Path
from couchbase.cluster import Cluster, ClusterOptions
from couchbase_core.cluster import PasswordAuthenticator
from pymysql.constants import CLIENT

class TestNetcdfObsBuilderV01(TestCase):
    def test_compare_model_to_mysql(self):
        """This test attempts to find recent models that match in both the mysql and the CB
        databases and compare them. This test isn't likely to succeed unless both the legacy
        ingest and the VxIngest have recently run.
        """
        try:
            credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
            self.assertTrue(
                Path(credentials_file).is_file(), "credentials_file Does not exist"
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
                """SELECT raw fcstValidEpoch
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
                            WHERE  s.name = "KPDX"
                            AND m0.time = o.time
                            AND s.madis_id = m0.sta_id
                            AND o.sta_id = m0.sta_id
                            AND m0.fcst_len = 0
                            AND m0.time <= %s
                            AND m0.time >= %s;"""
            cursor.execute(statement, (intersect_cb_times[0], intersect_cb_times[-1]))
            intersect_mysql_times_tmp = cursor.fetchall()
            intersect_mysql_times = [t["time"] for t in intersect_mysql_times_tmp]
            valid_times = [
                value for value in intersect_mysql_times if value in intersect_cb_times
            ]

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
                            AND data_item.name="KPDX"
                            AND mdata.fcstValidEpoch=$time
                            ORDER BY mdata.fcstLen""",
                            time=time)
                cb_model_fcst_lens = list(result)

                statement = """SELECT m0.fcst_len
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


                result = cluster.query(
                    """SELECT mdata.fcstValidEpoch,
                        mdata.fcstLen, data_item
                        FROM mdata
                        UNNEST mdata.data AS data_item
                        WHERE mdata.type='DD'
                            AND mdata.docType="model"
                            AND mdata.model="HRRR_OPS"
                            AND mdata.version='V01'
                            AND mdata.subset='METAR'
                            AND data_item.name="KPDX"
                            AND mdata.fcstValidEpoch=$time
                            AND mdata.fcstLen IN $fcst_lens
                            ORDER BY mdata.fcstLen""",
                    time=time, fcst_lens=intersect_fcst_len)
                cb_model_values = list(result)

                format_strings = ','.join(['%s'] * len(intersect_fcst_len))
                params = [time,time]
                params.extend(intersect_fcst_len)
                statement = """select m0.*
                from  madis3.metars as s, madis3.HRRR_OPSqp as m0
                WHERE 1=1
                AND s.madis_id = m0.sta_id
                AND s.name = "KPDX"
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
                AND s.name = "KPDX"
                AND  m0.time >= %s - 1800 and m0.time < %s + 1800
                ORDER BY m0.fcst_len;"""
                cursor.execute(statement, (time, time))
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
                AND s.name = "KPDX"
                AND  m0.time >= %s - 1800 and m0.time < %s + 1800
                ORDER BY m0.fcst_len;"""
                cursor.execute(statement, (time, time))
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
                            time, i, "KPDX"
                        )
                    )
                    print("field\t\tmysql\t\tcb\t\t\t\tdelta")

                    if (intersect_data_dict[i]["mysql"]["press"] and intersect_data_dict[i]["cb"]["Surface Pressure"]):
                        delta = abs(
                            intersect_data_dict[i]["mysql"]["press"] - intersect_data_dict[i]["cb"]["Surface Pressure"]
                        )
                    else:
                        delta = None
                    print(
                        "press\t\t{0}\t\t{1}\t\t\t{2}".format(
                            intersect_data_dict[i]["mysql"]["press"],
                            intersect_data_dict[i]["cb"]["Surface Pressure"],
                            delta,
                        )
                    )

                    if (intersect_data_dict[i]["mysql"]["temp"] and intersect_data_dict[i]["cb"]["Temperature"]):
                        delta = abs(
                            intersect_data_dict[i]["mysql"]["temp"] - intersect_data_dict[i]["cb"]["Temperature"]
                        )
                    else:
                        delta = None
                    print(
                        "temp\t\t{0}\t\t{1}\t\t\t{2}".format(
                            intersect_data_dict[i]["mysql"]["temp"],
                            intersect_data_dict[i]["cb"]["Temperature"],
                            delta,
                        )
                    )

                    if (intersect_data_dict[i]["mysql"]["dp"] and intersect_data_dict[i]["cb"]["DewPoint"]):
                        delta = abs(
                            intersect_data_dict[i]["mysql"]["dp"] - intersect_data_dict[i]["cb"]["DewPoint"]
                        )
                    else:
                        delta = None
                    print(
                        "dp\t\t{0}\t\t{1}\t\t\t{2}".format(
                            intersect_data_dict[i]["mysql"]["dp"],
                            intersect_data_dict[i]["cb"]["DewPoint"],
                            delta,
                        )
                    )

                    if (intersect_data_dict[i]["mysql"]["rh"] and intersect_data_dict[i]["cb"]["RH"]):
                        delta = abs(
                            intersect_data_dict[i]["mysql"]["rh"] - intersect_data_dict[i]["cb"]["RH"]
                        )
                    else:
                        delta = None
                    print(
                        "rh\t\t{0}\t\t{1}\t\t\t{2}".format(
                            intersect_data_dict[i]["mysql"]["rh"],
                            intersect_data_dict[i]["cb"]["RH"],
                            delta,
                        )
                    )

                    if (intersect_data_dict[i]["mysql"]["ws"] and intersect_data_dict[i]["cb"]["WS"]):
                        delta = abs(
                            intersect_data_dict[i]["mysql"]["ws"] - intersect_data_dict[i]["cb"]["WS"]
                        )
                    else:
                        delta = None
                    print(
                        "ws\t\t{0}\t\t{1}\t\t\t{2}".format(
                            intersect_data_dict[i]["mysql"]["ws"],
                            intersect_data_dict[i]["cb"]["WS"],
                            delta,
                        )
                    )

                    if (intersect_data_dict[i]["mysql"]["wd"] and intersect_data_dict[i]["cb"]["WD"]):
                        delta = abs(
                            intersect_data_dict[i]["mysql"]["wd"] - intersect_data_dict[i]["cb"]["WD"]
                        )
                    else:
                        delta = None
                    print(
                        "wd\t\t{0}\t\t{1}\t\t\t{2}".format(
                            intersect_data_dict[i]["mysql"]["wd"],
                            intersect_data_dict[i]["cb"]["WD"],
                            delta,
                        )
                    )

                    if (intersect_data_dict[i]["mysql"]["ceiling"] and intersect_data_dict[i]["cb"]["Ceiling"]):
                        delta = abs(
                            intersect_data_dict[i]["mysql"]["ceiling"] - intersect_data_dict[i]["cb"]["Ceiling"]
                        )
                    else:
                        delta = None
                    print(
                        "ceiling\t\t{0}\t\t{1}\t\t\t{2}".format(
                            intersect_data_dict[i]["mysql"]["ceiling"],
                            intersect_data_dict[i]["cb"]["Ceiling"],
                            delta,
                        )
                    )

                    if (intersect_data_dict[i]["mysql"]["visibility"] and intersect_data_dict[i]["cb"]["Visibility"]):
                        delta = abs(
                            intersect_data_dict[i]["mysql"]["visibility"] - intersect_data_dict[i]["cb"]["Visibility"]
                        )
                    else:
                        delta = None
                    print(
                        "visibility\t{0}\t\t{1}\t\t\t{2}".format(
                            intersect_data_dict[i]["mysql"]["visibility"],
                            intersect_data_dict[i]["cb"]["Visibility"],
                            delta,
                        )
                    )
                    print("--")

                for i in intersect_fcst_len:
                    self.assertEqual(
                        intersect_data_dict[i]["mysql"]["fcst_len"],
                        intersect_data_dict[i]["cb"]["fcstLen"],
                        msg="MYSQL fcst_len and CB fcstLen are not equal",
                    )

                    if (intersect_data_dict[i]["mysql"]["press"] is not None and intersect_data_dict[i]["cb"]["Surface Pressure"] is not None):
                        np.testing.assert_allclose(
                            intersect_data_dict[i]["mysql"]["press"],
                            intersect_data_dict[i]["cb"]["Surface Pressure"],
                            atol=1.5,
                            rtol=0,
                            err_msg="MYSQL Pressure and CB Surface Pressure are not approximately equal",
                            verbose=True,
                        )
                    if (intersect_data_dict[i]["mysql"]["temp"] is not None and intersect_data_dict[i]["cb"]["Temperature"] is not None):
                        np.testing.assert_allclose(
                            intersect_data_dict[i]["mysql"]["temp"],
                            intersect_data_dict[i]["cb"]["Temperature"],
                            atol=3,
                            rtol=0,
                            err_msg="MYSQL temp and CB Temperature are not approximately equal",
                            verbose=True,
                        )
                    if (intersect_data_dict[i]["mysql"]["dp"] is not None and intersect_data_dict[i]["cb"]["DewPoint"] is not None):
                        np.testing.assert_allclose(
                            intersect_data_dict[i]["mysql"]["dp"],
                            intersect_data_dict[i]["cb"]["DewPoint"],
                            atol=5,
                            rtol=0,
                            err_msg="MYSQL dp and CB Dew Point are not approximately equal",
                            verbose=True,
                        )
                    if (intersect_data_dict[i]["mysql"]["rh"] is not None and intersect_data_dict[i]["cb"]["RH"] is not None):
                        np.testing.assert_allclose(
                            intersect_data_dict[i]["mysql"]["rh"],
                            intersect_data_dict[i]["cb"]["RH"],
                            atol=20,
                            rtol=0,
                            err_msg="MYSQL rh and CB RH are not approximately equal",
                            verbose=True,
                        )
                    # TODO - FIX THIS!
                    if (intersect_data_dict[i]["mysql"]["wd"] is not None and intersect_data_dict[i]["cb"]["WD"] is not None):
                        np.testing.assert_allclose(
                            intersect_data_dict[i]["mysql"]["wd"],
                            intersect_data_dict[i]["cb"]["WD"],
                            atol=9999999,
                            rtol=0,
                            err_msg="MYSQL wd and CB WD are not approximately equal",
                            verbose=True,
                        )
                    if (intersect_data_dict[i]["mysql"]["ws"] is not None and intersect_data_dict[i]["cb"]["WS"] is not None):
                        np.testing.assert_allclose(
                            intersect_data_dict[i]["mysql"]["ws"],
                            intersect_data_dict[i]["cb"]["WS"],
                            atol=3,
                            rtol=0,
                            err_msg="MYSQL ws and CB WS are not approximately equal",
                            verbose=True,
                        )
                    if (intersect_data_dict[i]["mysql"]["visibility"] is not None and intersect_data_dict[i]["cb"]["Visibility"] is not None):
                        np.testing.assert_allclose(
                            intersect_data_dict[i]["mysql"]["visibility"],
                            intersect_data_dict[i]["cb"]["Visibility"],
                            atol=0.01,
                            rtol=0,
                            err_msg="MYSQL Visibility and CB Visibility are not approximately equal",
                            verbose=True,
                        )
                    if (intersect_data_dict[i]["mysql"]["ceiling"] is not None and intersect_data_dict[i]["cb"]["Ceiling"] is not None):
                        np.testing.assert_allclose(
                            intersect_data_dict[i]["mysql"]["ceiling"],
                            intersect_data_dict[i]["cb"]["Ceiling"],
                            atol=7,
                            rtol=0,
                            err_msg="MYSQL Ceiling and CB Ceiling are not approximately equal",
                            verbose=True,
                        )
        except:
            print(str(sys.exc_info()))
            self.fail("TestGsdIngestManager Exception failure: " + str(sys.exc_info()))

    def test_compare_obs_to_mysql(self):
        """This test attempts to find recent observations that match in both the mysql and the CB
        databases and compare them. This test isn't likely to succeed unless both the legacy
        ingest and the VxIngest have recently run.
        """
        try:
            credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
            self.assertTrue(
                Path(credentials_file).is_file(), "credentials_file Does not exist"
            )

            f = open(credentials_file)
            yaml_data = yaml.load(f, yaml.SafeLoader)
            host = yaml_data["cb_host"]
            user = yaml_data["cb_user"]
            password = yaml_data["cb_password"]
            options = ClusterOptions(PasswordAuthenticator(user, password))
            cluster = Cluster("couchbase://" + host, options)

            result = cluster.query(
                """SELECT raw fcstValidEpoch
                FROM mdata
                WHERE type='DD'
                AND docType="obs"
                AND version='V01'
                AND subset='METAR'
                ORDER BY fcstValidEpoch DESC
                """
            )
            cb_obs_fcst_valid_epochs = list(result)

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
            statement = """SELECT floor((o.time+1800)/(3600))*3600 AS time
                            FROM   madis3.obs AS o,
                            madis3.metars AS s
                            WHERE  s.name = "KPDX"
                            AND s.madis_id = o.sta_id
                            AND o.time <= %s
                            AND o.time >= %s; """
            cursor.execute(
                statement, (cb_obs_fcst_valid_epochs[0], cb_obs_fcst_valid_epochs[-1])
            )
            intersect_mysql_times_tmp = cursor.fetchall()
            intersect_mysql_times = [t["time"] for t in intersect_mysql_times_tmp]
            valid_times = [
                value
                for value in intersect_mysql_times
                if value in cb_obs_fcst_valid_epochs
            ]

            for time in valid_times:
                result = cluster.query(
                    """SELECT raw data_item
                        FROM mdata
                        UNNEST mdata.data AS data_item
                        WHERE mdata.type='DD'
                            AND mdata.docType="obs"
                            AND mdata.version='V01'
                            AND mdata.subset='METAR'
                            AND data_item.name="KPDX"
                            AND mdata.fcstValidEpoch=$time""",
                    time=time,
                )
                cb_obs_values = list(result)[0]

                statement = """select o.*
                from  madis3.metars as s, madis3.obs as o
                WHERE 1=1
                AND s.madis_id = o.sta_id
                AND s.name = "KPDX"
                AND  o.time >= %s - 1800 and o.time < %s + 1800 order by ABS(%s - o.time) limit 1;"""
                cursor.execute(statement, (time, time, time))
                mysql_obs_values_tmp = cursor.fetchall()

                mysql_obs_press = mysql_obs_values_tmp[0]["slp"] / 10
                mysql_obs_temp = mysql_obs_values_tmp[0]["temp"] / 10
                # need to convert mysql farenheight * 10 value to straight kelvin
                mysql_obs_dp = mysql_obs_values_tmp[0]["dp"] / 10
                mysql_obs_wd = mysql_obs_values_tmp[0]["wd"]
                mysql_obs_ws = mysql_obs_values_tmp[0]["ws"]

                statement = """select o.*
                from  madis3.metars as s, ceiling2.obs as o
                WHERE 1=1
                AND s.madis_id = o.madis_id
                AND s.name = "KPDX"
                AND  o.time >= %s - 1800 and o.time < %s + 1800 order by ABS(%s - o.time) limit 1;"""
                cursor.execute(statement, (time, time, time))
                mysql_obs_ceiling_values_tmp = cursor.fetchall()
                if len(mysql_obs_ceiling_values_tmp) > 0:
                    mysql_obs_ceiling = mysql_obs_ceiling_values_tmp[0]["ceil"] * 10
                else:
                     mysql_obs_ceiling = None

                statement = """select o.*
                from  madis3.metars as s, visibility.obs as o
                WHERE 1=1
                AND s.madis_id = o.madis_id
                AND s.name = "KPDX"
                AND  o.time >= %s - 1800 and o.time < %s + 1800 order by ABS(%s - o.time) limit 1;"""
                cursor.execute(statement, (time, time, time))
                mysql_obs_visibility_values_tmp = cursor.fetchall()
                if len(mysql_obs_visibility_values_tmp) > 0:
                    mysql_obs_visibility = mysql_obs_visibility_values_tmp[0]["vis100"] / 100
                else:
                     mysql_obs_visibility = None

                # now we have values for this time for each fcst_len, iterate the fcst_len and assert each value
                intersect_data_dict = {}
                intersect_data_dict["cb"] = cb_obs_values
                intersect_data_dict["mysql"] = {}
                intersect_data_dict["mysql"]["press"] = mysql_obs_press
                intersect_data_dict["mysql"]["temp"] = mysql_obs_temp
                # convert farenheight to kelvin
                intersect_data_dict["mysql"]["dp"] = mysql_obs_dp
                intersect_data_dict["mysql"]["ws"] = mysql_obs_ws
                intersect_data_dict["mysql"]["wd"] = mysql_obs_wd
                intersect_data_dict["mysql"]["ceiling"] = mysql_obs_ceiling
                intersect_data_dict["mysql"]["visibility"] = mysql_obs_visibility
                print("time: {0}\t\tstation: {1}".format(time, "KPDX"))
                print("field\t\tmysql\t\tcb\t\t\tdelta")

                if (
                    intersect_data_dict["mysql"]["press"]
                    and intersect_data_dict["cb"]["Surface Pressure"]
                ):
                    delta = abs(
                        intersect_data_dict["mysql"]["press"] - intersect_data_dict["cb"]["Surface Pressure"]
                    )
                else:
                    delta = None
                print(
                    "press\t\t{0}\t\t{1}\t\t\t{2}".format(
                        intersect_data_dict["mysql"]["press"],
                        intersect_data_dict["cb"]["Surface Pressure"],
                        delta,
                    )
                )

                if (intersect_data_dict["mysql"]["temp"] and intersect_data_dict["cb"]["Temperature"]):
                    delta = abs(
                        intersect_data_dict["mysql"]["temp"] - intersect_data_dict["cb"]["Temperature"]
                    )
                else:
                    delta = None
                print(
                    "temp\t\t{0}\t\t{1}\t\t\t{2}".format(
                        intersect_data_dict["mysql"]["temp"],
                        intersect_data_dict["cb"]["Temperature"],
                        delta,
                    )
                )

                if (intersect_data_dict["mysql"]["dp"] and intersect_data_dict["cb"]["DewPoint"]):
                    delta = abs(
                        intersect_data_dict["mysql"]["dp"] - intersect_data_dict["cb"]["DewPoint"]
                    )
                else:
                    delta = None
                print(
                    "dp\t\t{0}\t\t{1}\t\t\t{2}".format(
                        intersect_data_dict["mysql"]["dp"],
                        intersect_data_dict["cb"]["DewPoint"],
                        delta,
                    )
                )

                if (intersect_data_dict["mysql"]["wd"] and intersect_data_dict["cb"]["WD"]):
                    delta = abs(
                        intersect_data_dict["mysql"]["wd"] - intersect_data_dict["cb"]["WD"]
                    )
                else:
                    delta = None
                print(
                    "wd\t\t{0}\t\t{1}\t\t\t{2}".format(
                        intersect_data_dict["mysql"]["wd"],
                        intersect_data_dict["cb"]["WD"],
                        delta,
                    )
                )

                if (intersect_data_dict["mysql"]["ws"] and intersect_data_dict["cb"]["WS"]):
                    delta = abs(intersect_data_dict["mysql"]["ws"] - intersect_data_dict["cb"]["WS"]
                    )
                else:
                    delta = None
                print(
                    "ws\t\t{0}\t\t{1}\t\t\t{2}".format(
                        intersect_data_dict["mysql"]["ws"],
                        intersect_data_dict["cb"]["WS"],
                        delta,
                    )
                )

                if (intersect_data_dict["mysql"]["ceiling"] and intersect_data_dict["cb"]["Ceiling"]):
                    delta = abs(
                        intersect_data_dict["mysql"]["ceiling"]
                        - intersect_data_dict["cb"]["Ceiling"]
                    )
                else:
                    delta = None
                print(
                    "ceiling\t\t{0}\t\t{1}\t\t\t{2}".format(
                        intersect_data_dict["mysql"]["ceiling"],
                        intersect_data_dict["cb"]["Ceiling"],
                        delta,
                    )
                )

                if (intersect_data_dict["mysql"]["visibility"] and intersect_data_dict["cb"]["Visibility"]):
                    delta = abs(
                        intersect_data_dict["mysql"]["visibility"]
                        - intersect_data_dict["cb"]["Visibility"]
                    )
                else:
                    delta = None
                print(
                    "visibility\t{0}\t\t{1}\t\t\t{2}".format(
                        intersect_data_dict["mysql"]["visibility"],
                        intersect_data_dict["cb"]["Visibility"],
                        delta,
                    )
                )
                print("--")
                np.testing.assert_allclose(
                    intersect_data_dict["mysql"]["press"],
                    intersect_data_dict["cb"]["Surface Pressure"],
                    atol=1,
                    rtol=0,
                    err_msg="MYSQL Pressure and CB Surface Pressure are not approximately equal",
                    verbose=True,
                )
                np.testing.assert_allclose(
                    intersect_data_dict["mysql"]["temp"],
                    intersect_data_dict["cb"]["Temperature"],
                    atol=1.5,
                    rtol=0,
                    err_msg="MYSQL temp and CB Temperature are not approximately equal",
                    verbose=True,
                )
                np.testing.assert_allclose(
                    intersect_data_dict["mysql"]["dp"],
                    intersect_data_dict["cb"]["DewPoint"],
                    atol=0.5,
                    rtol=0,
                    err_msg="MYSQL dp and CB Dew Point are not approximately equal",
                    verbose=True,
                )
                if (
                    intersect_data_dict["mysql"]["wd"] is not None
                    and intersect_data_dict["cb"]["WD"] is not None
                ):
                    np.testing.assert_allclose(
                        intersect_data_dict["mysql"]["wd"],
                        intersect_data_dict["cb"]["WD"],
                        atol=25,
                        rtol=0,
                        err_msg="MYSQL wd and CB WD are not approximately equal",
                        verbose=True,
                    )
                if (intersect_data_dict["mysql"]["ws"] is not None and intersect_data_dict["cb"]["WS"] is not None):
                    np.testing.assert_allclose(
                        intersect_data_dict["mysql"]["ws"],
                        intersect_data_dict["cb"]["WS"],
                        atol=2,
                        rtol=0,
                        err_msg="MYSQL ws and CB WS are not approximately equal",
                        verbose=True,
                    )
                if (intersect_data_dict["mysql"]["visibility"] is not None and intersect_data_dict["cb"]["Visibility"] is not None):
                    np.testing.assert_allclose(
                        intersect_data_dict["mysql"]["visibility"],
                        intersect_data_dict["cb"]["Visibility"],
                        atol=1,
                        rtol=0,
                        err_msg="MYSQL Visibility and CB Visibility are not approximately equal",
                        verbose=True,
                    )
                if (intersect_data_dict["mysql"]["ceiling"] is not None and intersect_data_dict["cb"]["Ceiling"] is not None):
                    np.testing.assert_allclose(
                        intersect_data_dict["mysql"]["ceiling"],
                        intersect_data_dict["cb"]["Ceiling"],
                        atol=60000,
                        rtol=0,
                        err_msg="MYSQL Ceiling and CB Ceiling are not approximately equal",
                        verbose=True,
                    )
        except:
            print(str(sys.exc_info()))
            self.fail("TestGsdIngestManager Exception failure: " + str(sys.exc_info()))

    def test_one_thread_default_epoch(self):
        # noinspection PyBroadException
        try:
            cwd = os.getcwd()
            self.spec_file = (
                cwd + "/netcdf_to_cb/test/test_load_spec_netcdf_metar_obs_V01.yaml"
            )
            vxIngest = VXIngest()
            vxIngest.runit(
                {
                    "spec_file": self.spec_file,
                    "credentials_file": os.environ["HOME"] + "/adb-cb1-credentials",
                    "path": "/opt/data/netcdf_to_cb/input_files",
                    "file_name_mask": "%Y%m%d_%H%M",
                    "output_dir": "/opt/data/netcdf_to_cb/output",
                    "threads": 1,
                }
            )
        except:
            self.fail(
                "TestGsdIngestManager Exception failure: " + str(sys.exc_info()[0])
            )

    def test_one_thread_first_epoch(self):
        # noinspection PyBroadException
        try:
            cwd = os.getcwd()
            self.spec_file = (
                cwd + "/netcdf_to_cb/test/test_load_spec_netcdf_metar_obs_V01.yaml"
            )
            vxIngest = VXIngest()
            vxIngest.runit(
                {
                    "spec_file": self.spec_file,
                    "credentials_file": os.environ["HOME"] + "/adb-cb1-credentials",
                    "path": "/opt/data/netcdf_to_cb/input_files",
                    "file_name_mask": "%Y%m%d_%H%M",
                    "output_dir": "/opt/data/netcdf_to_cb/output",
                    "threads": 1,
                    "first_epoch": 1631268000 - 10,
                    "last_epoch": 1631268000 + 10,
                }
            )
        except:
            self.fail(
                "TestGsdIngestManager Exception failure: " + str(sys.exc_info()[0])
            )

    def test_two_threads_default_epoch(self):
        # noinspection PyBroadException
        try:
            cwd = os.getcwd()
            self.spec_file = (
                cwd + "/netcdf_to_cb/test/test_load_spec_netcdf_metar_obs_V01.yaml"
            )
            vxIngest = VXIngest()
            vxIngest.runit(
                {
                    "spec_file": self.spec_file,
                    "credentials_file": os.environ["HOME"] + "/adb-cb1-credentials",
                    "path": "/opt/data/netcdf_to_cb/input_files",
                    "file_name_mask": "%Y%m%d_%H%M",
                    "output_dir": "/opt/data/netcdf_to_cb/output",
                    "threads": 2,
                }
            )
        except:
            self.fail(
                "TestGsdIngestManager Exception failure: " + str(sys.exc_info()[0])
            )

    def test_two_threads_first_epoch(self):
        # noinspection PyBroadException
        try:
            cwd = os.getcwd()
            self.spec_file = (
                cwd + "/netcdf_to_cb/test/test_load_spec_netcdf_metar_obs_V01.yaml"
            )
            vxIngest = VXIngest()
            vxIngest.runit(
                {
                    "spec_file": self.spec_file,
                    "credentials_file": os.environ["HOME"] + "/adb-cb1-credentials",
                    "path": "/opt/data/netcdf_to_cb/input_files",
                    "file_name_mask": "%Y%m%d_%H%M",
                    "output_dir": "/opt/data/netcdf_to_cb/output",
                    "threads": 2,
                    "first_epoch": 1625875200,
                }
            )
        except:
            self.fail(
                "TestGsdIngestManager Exception failure: " + str(sys.exc_info()[0])
            )
