import sys
import os
from glob import glob
import yaml
import pymysql
from pymysql.constants import CLIENT
import numpy as np
from unittest import TestCase
from netcdf_to_cb.run_ingest_threads import VXIngest
from pathlib import Path
from couchbase.cluster import Cluster, ClusterOptions
from couchbase_core.cluster import PasswordAuthenticator
from ctc_to_cb.ctc_builder import CTCBuilder

class TestNetcdfMetarLegacyObsBuilderV01(TestCase):
    def test_legacy_one_thread_spedicfy_file_pattern(self):
        try:
            cwd = os.getcwd()
            self.spec_file = (
                cwd + "/netcdf_to_cb/test/test_load_spec_netcdf_metar_legacy_obs_V01.yaml"
            )
            # setup - remove output files
            for _f in glob('/opt/data/netcdf_to_cb_legacy/output/test1/*.json'):
                os.remove(_f)
            vx_ingest = VXIngest()
            vx_ingest.runit(
                {
                    "spec_file": self.spec_file,
                    "credentials_file": os.environ["HOME"] + "/adb-cb1-credentials",
                    "path": "/opt/data/netcdf_to_cb/input_files",
                    "file_name_mask": "%Y%m%d_%H%M",
                    "output_dir": "/opt/data/netcdf_to_cb_legacy/output/test1",
                    "threads": 1,
                    "file_pattern": "20211108_0000"
                }
            )
            self.assertTrue(len(glob("/opt/data/netcdf_to_cb_legacy/output/test1/[0123456789]???????_[0123456789]???.json")) > 0,msg="There are no output files")
            self.assertTrue(len(glob("/opt/data/netcdf_to_cb_legacy/output/test1/LJ:netcdf_to_cb.run_ingest_threads:VXIngest:*.json")) == 1, msg="there is no load job output file")
            # use file globbing to see if we got one output file for each input file plus one load job file
            self.assertTrue(len(glob("/opt/data/netcdf_to_cb_legacy/output/test1/20211108*.json")) ==
                len(glob("/opt/data/netcdf_to_cb/input_files/20211108_0000")), msg="number of output files is incorrect")
            # teardown remove output files
            for _f in glob('/opt/data/netcdf_to_cb_legacy/output/test1/*.json'):
                os.remove(_f)
        except Exception as e:
            self.fail("TestGsdIngestManager Exception failure: " + str(e))

    def test_compare_legacy_obs_to_mysql(self):
        """This test attempts to find recent metar-legacy observations that match in both the mysql and the CB
        databases and compare them. This test isn't likely to succeed unless both the legacy
        ingest and the VxIngest have recently run.
        """
        try:
            station = "KPDX"
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
                """SELECT RAW TONUMBER(split(meta().id,":")[4]) as fcstValidEpoch
                    FROM mdata
                    WHERE
                        type="DD"
                        AND docType="obs"
                        AND subset="METAR_LEGACY"
                        AND version = "V01"
                        ORDER BY fcstValidEpoch DESC"""
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
            statement = """SELECT DISTINCT floor((o.time+1800)/(3600))*3600 AS time
                            FROM   ceiling2.obs AS o,
                            madis3.metars AS s
                            WHERE  s.name = %s
                            AND s.madis_id = o.madis_id
                            AND o.time <= %s
                            AND o.time >= %s order by o.time; """
            cursor.execute(
                statement, (station, cb_obs_fcst_valid_epochs[0], cb_obs_fcst_valid_epochs[-1])
            )
            intersect_mysql_times_tmp = cursor.fetchall()
            intersect_mysql_times = [t["time"] for t in intersect_mysql_times_tmp]
            valid_times = [
                value
                for value in intersect_mysql_times
                if value in cb_obs_fcst_valid_epochs
            ]

            result = cluster.query(
                """SELECT raw mdata.units
                    FROM mdata
                    UNNEST mdata.data AS data_item
                    WHERE mdata.type='DD'
                        AND mdata.docType="obs"
                        AND mdata.version='V01'
                        AND mdata.subset='METAR_LEGACY'
                        AND data_item.name=$station
                        AND mdata.fcstValidEpoch=$time""",
                        time=valid_times[0], station=station)
            units = list(result)[0]


            for time in valid_times:
                result = cluster.query(
                    """SELECT raw data_item
                        FROM mdata
                        UNNEST mdata.data AS data_item
                        WHERE mdata.type='DD'
                            AND mdata.docType="obs"
                            AND mdata.version='V01'
                            AND mdata.subset='METAR_LEGACY'
                            AND data_item.name=$station
                            AND mdata.fcstValidEpoch=$time""",
                    time=time, station=station
                )
                cb_obs_values = list(result)[0]

                statement = """select o.*
                from  madis3.metars as s, madis3.obs as o
                WHERE 1=1
                AND s.madis_id = o.sta_id
                AND s.name = %s
                AND  o.time >= %s - 1800 and o.time < %s + 1800 order by abs(%s - o.time) limit 1;"""
                cursor.execute(statement, (station, time, time, time))
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
                AND s.name = %s
                AND  o.time >= %s - 1800 and o.time < %s + 1800 order by abs(%s - o.time) limit 1;"""
                cursor.execute(statement, (station, time, time, time))
                mysql_obs_ceiling_values_tmp = cursor.fetchall()
                if len(mysql_obs_ceiling_values_tmp) > 0:
                    mysql_obs_ceiling = mysql_obs_ceiling_values_tmp[0]["ceil"] * 10
                else:
                     mysql_obs_ceiling = None

                statement = """select o.*
                from  madis3.metars as s, visibility.obs as o
                WHERE 1=1
                AND s.madis_id = o.madis_id
                AND s.name = %s
                AND  o.time >= %s - 1800 and o.time < %s + 1800 order by abs(%s - o.time) limit 1;"""
                cursor.execute(statement, (station, time, time, time))
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
                print("time: {0}\t\tstation: {1}".format(time, station))
                print("field\t\tmysql\t\tcb\t\t\tdelta\t\t\tunits")

                if intersect_data_dict["mysql"]["press"] is not None and intersect_data_dict["cb"]["Surface Pressure"] is not None:
                    delta = intersect_data_dict["mysql"]["press"] - intersect_data_dict["cb"]["Surface Pressure"]
                else:
                    delta = None
                print(
                    "'press'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                        intersect_data_dict["mysql"]["press"],
                        intersect_data_dict["cb"]["Surface Pressure"],
                        delta, units['Surface Pressure']
                    )
                )

                if intersect_data_dict["mysql"]["temp"] is not None and intersect_data_dict["cb"]["Temperature"] is not None:
                    delta = abs(
                        intersect_data_dict["mysql"]["temp"] - intersect_data_dict["cb"]["Temperature"]
                    )
                else:
                    delta = None
                print(
                    "'temp'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                        intersect_data_dict["mysql"]["temp"],
                        intersect_data_dict["cb"]["Temperature"],
                        delta, units['Temperature']
                    )
                )

                if intersect_data_dict["mysql"]["dp"] is not None and intersect_data_dict["cb"]["DewPoint"] is not None:
                    delta = intersect_data_dict["mysql"]["dp"] - intersect_data_dict["cb"]["DewPoint"]
                else:
                    delta = None
                print(
                    "'dp'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                        intersect_data_dict["mysql"]["dp"],
                        intersect_data_dict["cb"]["DewPoint"],
                        delta, units['DewPoint']
                    )
                )

                if intersect_data_dict["mysql"]["wd"] is not None and intersect_data_dict["cb"]["WD"] is not None:
                    delta = intersect_data_dict["mysql"]["wd"] - intersect_data_dict["cb"]["WD"]
                    if delta > 180:
                        delta = 360 - delta
                    if delta < -180:
                        delta = 360 + delta
                else:
                    delta = None
                print(
                    "'wd'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                        intersect_data_dict["mysql"]["wd"],
                        intersect_data_dict["cb"]["WD"],
                        delta, units['WD']
                    )
                )

                if intersect_data_dict["mysql"]["ws"] is not None and intersect_data_dict["cb"]["WS"]  is not None:
                    delta =intersect_data_dict["mysql"]["ws"] - intersect_data_dict["cb"]["WS"]
                else:
                    delta = None
                print(
                    "'ws'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                        intersect_data_dict["mysql"]["ws"],
                        intersect_data_dict["cb"]["WS"],
                        delta, units['WS']
                    )
                )

                if intersect_data_dict["mysql"]["ceiling"] is not None and intersect_data_dict["cb"]["Ceiling"] is not None:
                    delta = intersect_data_dict["mysql"]["ceiling"] - intersect_data_dict["cb"]["Ceiling"]
                else:
                    delta = None
                print(
                    "'ceiling'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                        intersect_data_dict["mysql"]["ceiling"],
                        intersect_data_dict["cb"]["Ceiling"],
                        delta, units['Ceiling']
                    )
                )

                if intersect_data_dict["mysql"]["visibility"] is not None and intersect_data_dict["cb"]["Visibility"] is not None:
                    delta = intersect_data_dict["mysql"]["visibility"] - intersect_data_dict["cb"]["Visibility"]
                else:
                    delta = None
                print(
                    "'visibility'\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                        intersect_data_dict["mysql"]["visibility"],
                        intersect_data_dict["cb"]["Visibility"],
                        delta, units['Visibility']
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
                    atol=3,
                    rtol=0,
                    err_msg="MYSQL temp and CB Temperature are not approximately equal",
                    verbose=True,
                )
                np.testing.assert_allclose(
                    intersect_data_dict["mysql"]["dp"],
                    intersect_data_dict["cb"]["DewPoint"],
                    atol=2.0,
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
                        atol=10,
                        rtol=0,
                        err_msg="MYSQL wd and CB WD are not approximately equal",
                        verbose=True,
                    )
                if intersect_data_dict["mysql"]["ws"] is not None and intersect_data_dict["cb"]["WS"] is not None:
                    np.testing.assert_allclose(
                        intersect_data_dict["mysql"]["ws"],
                        intersect_data_dict["cb"]["WS"],
                        atol=10,
                        rtol=0,
                        err_msg="MYSQL ws and CB WS are not approximately equal",
                        verbose=True,
                    )
                if intersect_data_dict["mysql"]["visibility"] is not None and intersect_data_dict["cb"]["Visibility"] is not None:
                    np.testing.assert_allclose(
                        intersect_data_dict["mysql"]["visibility"],
                        intersect_data_dict["cb"]["Visibility"],
                        atol=10,
                        rtol=0,
                        err_msg="MYSQL Visibility and CB Visibility are not approximately equal",
                        verbose=True,
                    )
                if intersect_data_dict["mysql"]["ceiling"] is not None and intersect_data_dict["cb"]["Ceiling"] is not None:
                    np.testing.assert_allclose(
                        intersect_data_dict["mysql"]["ceiling"],
                        intersect_data_dict["cb"]["Ceiling"],
                        atol=30,
                        rtol=0,
                        err_msg="MYSQL Ceiling and CB Ceiling are not approximately equal",
                        verbose=True,
                    )
        except:
            print(str(sys.exc_info()))
            self.fail("TestGsdIngestManager Exception failure: " + str(sys.exc_info()))


    def test_compare_legacy_retro_obs_to_mysql(self):
        """This test attempts to find recent metar_legacy_retro observations that match in both the mysql and the CB
        databases and compare them. This test isn't likely to succeed unless both the legacy
        ingest and the VxIngest have recently run.
        """
        try:
            station = ""
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

            # For units all the stations are the same, just use KPDX
            result = cluster.query(
                """SELECT raw mdata.units
                    FROM mdata
                    UNNEST mdata.data AS data_item
                    WHERE mdata.type='DD'
                        AND mdata.docType="obs"
                        AND mdata.version='V01'
                        AND mdata.subset='METAR_LEGACY_RETRO'
                        AND data_item.name="KPDX"
                        LIMIT 1""")
            units = list(result)[0]

            result = cluster.query(
                """SELECT RAW TONUMBER(split(meta().id,":")[4]) as fcstValidEpoch
                    FROM mdata
                    WHERE
                        type="DD"
                        AND docType="obs"
                        AND subset="METAR_LEGACY_RETRO"
                        AND version = "V01"
                        ORDER BY fcstValidEpoch DESC"""
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

            for station in ["KDKR", "KPDX", "KDEN", "KFST", "KENW", "KBOS"]:
                statement = """SELECT DISTINCT floor((o.time+1800)/(3600))*3600 AS time
                                FROM   ceiling2.obs_retro AS o,
                                madis3.metars AS s
                                WHERE  s.name = %s
                                AND s.madis_id = o.madis_id
                                AND o.time <= %s
                                AND o.time >= %s order by o.time; """
                cursor.execute(
                    statement, (station, cb_obs_fcst_valid_epochs[0], cb_obs_fcst_valid_epochs[-1])
                )
                intersect_mysql_times_tmp = cursor.fetchall()
                intersect_mysql_times = [t["time"] for t in intersect_mysql_times_tmp]
                valid_times = [
                    value
                    for value in intersect_mysql_times
                    if value in cb_obs_fcst_valid_epochs
                ]

                #valid_times = [1639389600]
                for time in valid_times:
                    result = cluster.query(
                        """SELECT raw data_item
                            FROM mdata
                            UNNEST mdata.data AS data_item
                            WHERE mdata.type='DD'
                                AND mdata.docType="obs"
                                AND mdata.version='V01'
                                AND mdata.subset='METAR_LEGACY_RETRO'
                                AND data_item.name=$station
                                AND mdata.fcstValidEpoch=$time""",
                        time=time, station=station
                    )
                    if len (list(result)) == 0:
                        cb_obs_values = {
                            "Ceiling": None,
                            "Ceiling Reported Time": None,
                            "DewPoint": None,
                            "Reported Time": None,
                            "Surface Pressure": None,
                            "Temperature": None,
                            "Visibility": None,
                            "WD": None,
                            "WS": None,
                            "name": station}
                    else:
                        cb_obs_values = list(result)[0]

                    statement = """select o.*
                    from  madis3.metars as s, madis3.obs_retro as o
                    WHERE 1=1
                    AND s.madis_id = o.sta_id
                    AND s.name = %s
                    AND  o.time >= %s - 1800 and o.time < %s + 1800 order by abs(%s - o.time) limit 1;"""
                    cursor.execute(statement, (station, time, time, time))
                    mysql_obs_values_tmp = cursor.fetchall()
                    if len(mysql_obs_values_tmp) == 0:
                        mysql_obs_values_tmp = {
                            "slp": None,
                            "temp":None,
                            "dp":None,
                            "wd":None,
                            "ws":None
                        }
                    if mysql_obs_values_tmp[0]["slp"] is not None:
                        mysql_obs_press = mysql_obs_values_tmp[0]["slp"] / 10
                    else:
                        mysql_obs_press = None
                    if mysql_obs_values_tmp[0]["temp"] is not None:
                        mysql_obs_temp = mysql_obs_values_tmp[0]["temp"] / 10
                    else:
                        mysql_obs_temp = None
                    # need to convert mysql farenheight * 10 value to straight kelvin
                    if mysql_obs_values_tmp[0]["dp"] is not None:
                        mysql_obs_dp = mysql_obs_values_tmp[0]["dp"] / 10
                    else:
                        mysql_obs_dp = None
                    if mysql_obs_values_tmp[0]["wd"] is not None:
                        mysql_obs_wd = mysql_obs_values_tmp[0]["wd"]
                    else:
                        mysql_obs_wd = None
                    if mysql_obs_values_tmp[0]["ws"] is not None:
                        mysql_obs_ws = mysql_obs_values_tmp[0]["ws"]
                    else:
                        mysql_obs_ws = None

                    statement = """select o.*
                    from  madis3.metars as s, ceiling2.obs_retro as o
                    WHERE 1=1
                    AND s.madis_id = o.madis_id
                    AND s.name = %s
                    AND  o.time >= %s - 1800 and o.time < %s + 1800 order by abs(%s - o.time) limit 1;"""
                    cursor.execute(statement, (station, time, time, time))
                    mysql_obs_ceiling_values_tmp = cursor.fetchall()
                    if len(mysql_obs_ceiling_values_tmp) > 0 and mysql_obs_ceiling_values_tmp[0]["ceil"] is not None:
                        mysql_obs_ceiling = mysql_obs_ceiling_values_tmp[0]["ceil"] * 10
                    else:
                        mysql_obs_ceiling = None
                    statement = """select o.*
                    from  madis3.metars as s, visibility.obs as o
                    WHERE 1=1
                    AND s.madis_id = o.madis_id
                    AND s.name = %s
                    AND  o.time >= %s - 1800 and o.time < %s + 1800 order by abs(%s - o.time) limit 1;"""
                    cursor.execute(statement, (station, time, time, time))
                    mysql_obs_visibility_values_tmp = cursor.fetchall()
                    if len(mysql_obs_visibility_values_tmp) > 0 and mysql_obs_visibility_values_tmp[0]["vis100"] is not None:
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
                    print("time: {0}\t\tstation: {1}".format(time, station))
                    print("field\t\tmysql\t\tcb\t\t\tdelta\t\t\tunits")

                    if intersect_data_dict["mysql"]["press"] is not None and intersect_data_dict["cb"]["Surface Pressure"] is not None:
                        delta = intersect_data_dict["mysql"]["press"] - intersect_data_dict["cb"]["Surface Pressure"]
                    else:
                        delta = None
                    print(
                        "var - 'press'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                            intersect_data_dict["mysql"]["press"],
                            intersect_data_dict["cb"]["Surface Pressure"],
                            delta, units['Surface Pressure']
                        )
                    )

                    if intersect_data_dict["mysql"]["temp"] is not None and intersect_data_dict["cb"]["Temperature"] is not None:
                        delta = abs(
                            intersect_data_dict["mysql"]["temp"] - intersect_data_dict["cb"]["Temperature"]
                        )
                    else:
                        delta = None
                    print(
                        "var - 'temp'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                            intersect_data_dict["mysql"]["temp"],
                            intersect_data_dict["cb"]["Temperature"],
                            delta, units['Temperature']
                        )
                    )

                    if intersect_data_dict["mysql"]["dp"] is not None and intersect_data_dict["cb"]["DewPoint"] is not None:
                        delta = intersect_data_dict["mysql"]["dp"] - intersect_data_dict["cb"]["DewPoint"]
                    else:
                        delta = None
                    print(
                        "var - 'dp'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                            intersect_data_dict["mysql"]["dp"],
                            intersect_data_dict["cb"]["DewPoint"],
                            delta, units['DewPoint']
                        )
                    )

                    if intersect_data_dict["mysql"]["wd"] is not None and intersect_data_dict["cb"]["WD"] is not None:
                        delta = intersect_data_dict["mysql"]["wd"] - intersect_data_dict["cb"]["WD"]
                        if delta > 180:
                            delta = 360 - delta
                        if delta < -180:
                            delta = 360 + delta
                    else:
                        delta = None
                    print(
                        "var - 'wd'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                            intersect_data_dict["mysql"]["wd"],
                            intersect_data_dict["cb"]["WD"],
                            delta, units['WD']
                        )
                    )

                    if intersect_data_dict["mysql"]["ws"] is not None and intersect_data_dict["cb"]["WS"]  is not None:
                        delta =intersect_data_dict["mysql"]["ws"] - intersect_data_dict["cb"]["WS"]
                    else:
                        delta = None
                    print(
                        "var - 'ws'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                            intersect_data_dict["mysql"]["ws"],
                            intersect_data_dict["cb"]["WS"],
                            delta, units['WS']
                        )
                    )

                    if intersect_data_dict["mysql"]["ceiling"] is not None and intersect_data_dict["cb"]["Ceiling"] is not None:
                        delta = intersect_data_dict["mysql"]["ceiling"] - intersect_data_dict["cb"]["Ceiling"]
                    else:
                        delta = None
                    print(
                        "var - 'ceiling'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                            intersect_data_dict["mysql"]["ceiling"],
                            intersect_data_dict["cb"]["Ceiling"],
                            delta, units['Ceiling']
                        )
                    )

                    if intersect_data_dict["mysql"]["visibility"] is not None and intersect_data_dict["cb"]["Visibility"] is not None:
                        delta = intersect_data_dict["mysql"]["visibility"] - intersect_data_dict["cb"]["Visibility"]
                    else:
                        delta = None
                    print(
                        "var - 'visibility'\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                            intersect_data_dict["mysql"]["visibility"],
                            intersect_data_dict["cb"]["Visibility"],
                            delta, units['Visibility']
                        )
                    )
                    print("--")
                    if intersect_data_dict["mysql"]["press"] is None or intersect_data_dict["cb"]["Surface Pressure"] is None:
                        try:
                            self.assertEqual(intersect_data_dict["mysql"]["press"],
                                intersect_data_dict["cb"]["Surface Pressure"],
                                msg="MYSQL Pressure and CB Surface Pressure are not equal")
                        except:
                            print(str(sys.exc_info()))
                    else:
                        np.testing.assert_allclose(
                            intersect_data_dict["mysql"]["press"],
                            intersect_data_dict["cb"]["Surface Pressure"],
                            atol=0.5,
                            rtol=0,
                            err_msg="MYSQL Pressure and CB Surface Pressure are not approximately equal",
                            verbose=True,
                        )
                    if intersect_data_dict["mysql"]["temp"] is None or intersect_data_dict["cb"]["Temperature"] is None:
                        try:
                            self.assertEqual(intersect_data_dict["mysql"]["temp"],
                                intersect_data_dict["cb"]["Temperature"],
                                msg="MYSQL temp and CB Temperature are not equal:")
                        except:
                            print(str(sys.exc_info()))
                    else:
                        np.testing.assert_allclose(
                            intersect_data_dict["mysql"]["temp"],
                            intersect_data_dict["cb"]["Temperature"],
                            atol=0.5,
                            rtol=0,
                            err_msg="MYSQL temp and CB Temperature are not approximately equal",
                            verbose=True,
                        )
                    if intersect_data_dict["mysql"]["dp"] is None or intersect_data_dict["cb"]["DewPoint"] is None:
                        try:
                            self.assertEqual(intersect_data_dict["mysql"]["dp"],
                                intersect_data_dict["cb"]["DewPoint"],
                                msg="MYSQL dp and CB DewPoint are not equal:")
                        except:
                            print(str(sys.exc_info()))
                    else:
                        np.testing.assert_allclose(
                            intersect_data_dict["mysql"]["dp"],
                            intersect_data_dict["cb"]["DewPoint"],
                            atol=0.5,
                            rtol=0,
                            err_msg="MYSQL dp and CB Dew Point are not approximately equal",
                            verbose=True,
                        )
                    if intersect_data_dict["mysql"]["wd"] is None or intersect_data_dict["cb"]["WD"] is None:
                        try:
                            self.assertEqual(intersect_data_dict["mysql"]["wd"],
                                intersect_data_dict["cb"]["WD"],
                                msg="MYSQL wd and CB WD are not equal:")
                        except:
                            print(str(sys.exc_info()))

                    else:
                        np.testing.assert_allclose(
                            intersect_data_dict["mysql"]["wd"],
                            intersect_data_dict["cb"]["WD"],
                            atol=30,
                            rtol=0,
                            err_msg="MYSQL wd and CB WD are not approximately equal",
                            verbose=True,
                        )
                    if intersect_data_dict["mysql"]["ws"] is None or intersect_data_dict["cb"]["WS"] is None:
                        try:
                            self.assertEqual(intersect_data_dict["mysql"]["ws"],
                                intersect_data_dict["cb"]["WS"],
                                msg="MYSQL ws and CB WS are not equal:")
                        except:
                            print(str(sys.exc_info()))
                    else:
                        np.testing.assert_allclose(
                            intersect_data_dict["mysql"]["ws"],
                            intersect_data_dict["cb"]["WS"],
                            atol=1.0,
                            rtol=0,
                            err_msg="MYSQL ws and CB WS are not approximately equal",
                            verbose=True,
                        )
                    if intersect_data_dict["mysql"]["visibility"] is None or intersect_data_dict["cb"]["Visibility"] is None:
                        try:
                            self.assertEqual(intersect_data_dict["mysql"]["visibility"],
                                intersect_data_dict["cb"]["Visibility"],
                                msg="MYSQL visibility and CB Visibility are not equal:")
                        except:
                            print(str(sys.exc_info()))
                    else:
                        np.testing.assert_allclose(
                            intersect_data_dict["mysql"]["visibility"],
                            intersect_data_dict["cb"]["Visibility"],
                            atol=0.005,
                            rtol=0,
                            err_msg="MYSQL Visibility and CB Visibility are not approximately equal",
                            verbose=True,
                        )
                    try:
                        if intersect_data_dict["mysql"]["ceiling"] is None or intersect_data_dict["cb"]["Ceiling"] is None:
                            try:
                                self.assertEqual(intersect_data_dict["mysql"]["ceiling"],
                                    intersect_data_dict["cb"]["Ceiling"],
                                    msg="MYSQL ceiling and CB Ceiling are not equal:" + str(intersect_data_dict["mysql"]["ceiling"]) + " != " + str(intersect_data_dict["cb"]["Ceiling"]))
                            except:
                                print(str(sys.exc_info()))
                        else:
                            np.testing.assert_allclose(
                                intersect_data_dict["mysql"]["ceiling"],
                                intersect_data_dict["cb"]["Ceiling"],
                                atol=20,
                                rtol=0,
                                err_msg="MYSQL Ceiling and CB Ceiling are not approximately equal",
                                verbose=True,
                            )
                    except:
                        print(str(sys.exc_info()))
        except:
            print(str(sys.exc_info()))
            self.fail("TestGsdIngestManager Exception failure: " + str(sys.exc_info()))


    def print_retro_delta(self, variable, retro_vals, legacy_vals, units, station, time):
        if retro_vals[variable] is not None and legacy_vals[variable] is not None:
            delta = retro_vals[variable] - legacy_vals[variable]
        else:
            delta = None
        print(
            "var - {0}\t\t{1}\t\t{2}\t\t\t{3}\t\t\t{4}\t\t\t{5}\t\t\t{6}".format(
                variable.replace(' ','_'),
                retro_vals[variable],
                legacy_vals[variable],
                delta, units[variable].replace(' ','_'), station, time)
        )

    def test_compare_legacy_obs_to_metar_retro(self):
        """This test attempts to find recent metar_legacy and metar_legacy_retro observations that match
        in the couchbase database and compare them. This test isn't likely to succeed unless both the legacy
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
            collection = cluster.bucket("mdata").default_collection()
            ingest_document = collection.get("MD:V01:METAR_LEGACY:HRRR_OPS_LEGACY:ALL_HRRR:CTC:CEILING:ingest").content
            ctc_builder = CTCBuilder(None, ingest_document, cluster, collection)

            result = cluster.query(
                """SELECT RAW TONUMBER(split(meta().id,":")[4]) as fcstValidEpoch
                    FROM mdata
                    WHERE
                        type="DD"
                        AND docType="obs"
                        AND subset="METAR_LEGACY"
                        AND version = "V01"
                        ORDER BY fcstValidEpoch DESC"""
            )
            cb_legacy_obs_fcst_valid_epochs = list(result)

            result = cluster.query(
                """SELECT RAW TONUMBER(split(meta().id,":")[4]) as fcstValidEpoch
                    FROM mdata
                    WHERE
                        type="DD"
                        AND docType="obs"
                        AND subset="METAR_LEGACY_RETRO"
                        AND version = "V01"
                        ORDER BY fcstValidEpoch DESC"""
            )
            cb_retro_obs_fcst_valid_epochs = list(result)

            valid_times = [
                value
                for value in cb_retro_obs_fcst_valid_epochs
                if value in cb_legacy_obs_fcst_valid_epochs
            ]
            # units are the same for all the stations
            result = cluster.query(
                """SELECT raw mdata.units
                    FROM mdata
                    UNNEST mdata.data AS data_item
                    WHERE mdata.type='DD'
                        AND mdata.docType="obs"
                        AND mdata.version='V01'
                        AND mdata.subset='METAR_LEGACY'
                        AND data_item.name="KPDX" limit 1;""")
            units = list(result)[0]

            try:
                full_station_name_list = ctc_builder.get_stations_for_region_by_geosearch("ALL_HRRR")
                # get the reject station list - legacy station list has to have rejected stations removed.
                results = collection.get("MD-TEST:V01:LEGACY_REJECTED_STATIONS").content
                rejected_stations = results['stations']
                rejected_station_names = [s['name'] for s in rejected_stations]
                # prune out the rejected stations
                stations = [s for s in full_station_name_list if s not in rejected_station_names]
            except Exception as _e:  # pylint: disable=broad-except
                print(
                    "%s: Exception getting station list: error: %s",
                    self.__class__.__name__,
                    str(_e),
                )

            for time in valid_times:
                try:
                    retry = 0
                    while retry < 3:
                        try:
                            result = collection.get_multi(["""DD:V01:METAR_LEGACY:obs:{0}""".format(time),"""DD:V01:METAR_LEGACY_RETRO:obs:{0}""".format(time)])
                            retry = 3
                        except:
                            retry = retry + 1
                    legacy_data = result["""DD:V01:METAR_LEGACY:obs:{0}""".format(time)].content['data']
                    retro_data = result["""DD:V01:METAR_LEGACY_RETRO:obs:{0}""".format(time)].content['data']
                    for station in stations:
                        cb_legacy_obs_values = next((x for x in legacy_data if x['name'] == station), None)
                        cb_retro_obs_values = next((x for x in retro_data if x['name'] == station), None)
                        if cb_legacy_obs_values is None and cb_retro_obs_values is None:
                            continue
                        print("\n")
                        if cb_legacy_obs_values is None and cb_retro_obs_values is not None:
                            print ("no legacy value for time {0} and station {1}".format(time, station) + " when there are retro values")
                            continue
                        if cb_legacy_obs_values is not None and cb_retro_obs_values is None:
                            print ("no retro value for time {0} and station {1}".format(time, station) + " when there are legacy values")
                            continue
                        self.print_retro_delta("Surface Pressure",cb_retro_obs_values,cb_legacy_obs_values,units,station,time)
                        self.print_retro_delta("Temperature",cb_retro_obs_values,cb_legacy_obs_values,units,station,time)
                        self.print_retro_delta("DewPoint",cb_retro_obs_values,cb_legacy_obs_values,units,station,time)
                        self.print_retro_delta("WD",cb_retro_obs_values,cb_legacy_obs_values,units,station,time)
                        self.print_retro_delta("WS",cb_retro_obs_values,cb_legacy_obs_values,units,station,time)
                        self.print_retro_delta("Visibility",cb_retro_obs_values,cb_legacy_obs_values,units,station,time)
                        self.print_retro_delta("Ceiling",cb_retro_obs_values,cb_legacy_obs_values,units,station,time)
                        continue

                        # now we have values for this time for each fcst_len, iterate the fcst_len and assert each value
                        print("--")
                        if  cb_retro_obs_values["Surface Pressure"] is not None and cb_legacy_obs_values["Surface Pressure"] is not None:
                            np.testing.assert_allclose(
                                cb_retro_obs_values["Surface Pressure"],
                                cb_legacy_obs_values["Surface Pressure"],
                                atol=1,
                                rtol=0,
                                err_msg="RETRO Surface Pressure and CB Surface Pressure are not approximately equal",
                                verbose=True,
                            )
                        if  cb_retro_obs_values["Temperature"] is not None and cb_legacy_obs_values["Temperature"] is not None:
                            np.testing.assert_allclose(
                                cb_retro_obs_values["Temperature"],
                                cb_legacy_obs_values["Temperature"],
                                atol=3,
                                rtol=0,
                                err_msg="RETRO Temperature and Legacy Temperature are not approximately equal",
                                verbose=True,
                            )
                        if  cb_retro_obs_values["DewPoint"] is not None and cb_legacy_obs_values["DewPoint"] is not None:
                            np.testing.assert_allclose(
                                cb_retro_obs_values["DewPoint"],
                                cb_legacy_obs_values["DewPoint"],
                                atol=2.0,
                                rtol=0,
                                err_msg="RETRO Dew Point and Legacy Dew Point are not approximately equal",
                                verbose=True,
                            )
                        if  cb_retro_obs_values["WD"] is not None and cb_legacy_obs_values["WD"] is not None:
                            np.testing.assert_allclose(
                                cb_retro_obs_values["WD"],
                                cb_legacy_obs_values["WD"],
                                atol=10,
                                rtol=0,
                                err_msg="RETRO WD and CB WD are not approximately equal",
                                verbose=True,
                            )
                        if  cb_retro_obs_values["WS"] is not None and cb_legacy_obs_values["WS"] is not None:
                            np.testing.assert_allclose(
                                cb_retro_obs_values["WS"],
                                cb_legacy_obs_values["WS"],
                                atol=10,
                                rtol=0,
                                err_msg="RETRO WS and CB WS are not approximately equal",
                                verbose=True,
                            )
                        if  cb_retro_obs_values["Visibility"] is not None and cb_legacy_obs_values["Visibility"] is not None:
                            np.testing.assert_allclose(
                                cb_retro_obs_values["Visibility"],
                                cb_legacy_obs_values["Visibility"],
                                atol=10,
                                rtol=0,
                                err_msg="RETRO Visibility and CB Visibility are not approximately equal",
                                verbose=True,
                            )
                        if  cb_retro_obs_values["Ceiling"] is not None and cb_legacy_obs_values["Ceiling"] is not None:
                            np.testing.assert_allclose(
                                cb_retro_obs_values["Ceiling"],
                                cb_legacy_obs_values["Ceiling"],
                                atol=30,
                                rtol=0,
                                err_msg="RETRO Ceiling and CB Ceiling are not approximately equal",
                                verbose=True,
                            )
                except:
                    print(str(sys.exc_info()))
                    self.fail("TestGsdIngestManager Exception failure for time: " + str(time) + " and station:" + station + " exception: " + str(sys.exc_info()))
        except:
            print(str(sys.exc_info()))
            self.fail("TestGsdIngestManager Exception failure: " + str(sys.exc_info()))

class TestNetcdfObsBuilderV01(TestCase):

    def test_compare_obs_to_mysql(self):
        """This test attempts to find recent observations that match in both the mysql and the CB
        databases and compare them. This test isn't likely to succeed unless both the legacy
        ingest and the VxIngest have recently run.
        """
        try:
            station = "KPDX"
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
            statement = """SELECT DISTINCT floor((o.time+1800)/(3600))*3600 AS time
                            FROM   ceiling2.obs AS o,
                            madis3.metars AS s
                            WHERE  s.name = %s
                            AND s.madis_id = o.madis_id
                            AND o.time <= %s
                            AND o.time >= %s order by o.time; """
            cursor.execute(
                statement, (station, cb_obs_fcst_valid_epochs[0], cb_obs_fcst_valid_epochs[-1])
            )
            intersect_mysql_times_tmp = cursor.fetchall()
            intersect_mysql_times = [t["time"] for t in intersect_mysql_times_tmp]
            valid_times = [
                value
                for value in intersect_mysql_times
                if value in cb_obs_fcst_valid_epochs
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
                result = cluster.query(
                    """SELECT raw data_item
                        FROM mdata
                        UNNEST mdata.data AS data_item
                        WHERE mdata.type='DD'
                            AND mdata.docType="obs"
                            AND mdata.version='V01'
                            AND mdata.subset='METAR'
                            AND data_item.name=$station
                            AND mdata.fcstValidEpoch=$time""",
                    time=time, station=station
                )
                cb_obs_values = list(result)[0]

                statement = """select o.*
                from  madis3.metars as s, madis3.obs as o
                WHERE 1=1
                AND s.madis_id = o.sta_id
                AND s.name = %s
                AND  o.time >= %s - 1800 and o.time < %s + 1800 order by abs(%s - o.time) limit 1;"""
                cursor.execute(statement, (station, time, time, time))
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
                AND s.name = %s
                AND  o.time >= %s - 1800 and o.time < %s + 1800 order by abs(%s - o.time) limit 1;"""
                cursor.execute(statement, (station, time, time, time))
                mysql_obs_ceiling_values_tmp = cursor.fetchall()
                if len(mysql_obs_ceiling_values_tmp) > 0:
                    mysql_obs_ceiling = mysql_obs_ceiling_values_tmp[0]["ceil"] * 10
                else:
                     mysql_obs_ceiling = None

                statement = """select o.*
                from  madis3.metars as s, visibility.obs as o
                WHERE 1=1
                AND s.madis_id = o.madis_id
                AND s.name = %s
                AND  o.time >= %s - 1800 and o.time < %s + 1800 order by abs(%s - o.time) limit 1;"""
                cursor.execute(statement, (station, time, time, time))
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
                print("time: {0}\t\tstation: {1}".format(time, station))
                print("field\t\tmysql\t\tcb\t\t\tdelta\t\t\tunits")

                if intersect_data_dict["mysql"]["press"] is not None and intersect_data_dict["cb"]["Surface Pressure"] is not None:
                    delta = intersect_data_dict["mysql"]["press"] - intersect_data_dict["cb"]["Surface Pressure"]
                else:
                    delta = None
                print(
                    "'press'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                        intersect_data_dict["mysql"]["press"],
                        intersect_data_dict["cb"]["Surface Pressure"],
                        delta, units['Surface Pressure']
                    )
                )

                if intersect_data_dict["mysql"]["temp"] is not None and intersect_data_dict["cb"]["Temperature"] is not None:
                    delta = abs(
                        intersect_data_dict["mysql"]["temp"] - intersect_data_dict["cb"]["Temperature"]
                    )
                else:
                    delta = None
                print(
                    "'temp'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                        intersect_data_dict["mysql"]["temp"],
                        intersect_data_dict["cb"]["Temperature"],
                        delta, units['Temperature']
                    )
                )

                if intersect_data_dict["mysql"]["dp"] is not None and intersect_data_dict["cb"]["DewPoint"] is not None:
                    delta = intersect_data_dict["mysql"]["dp"] - intersect_data_dict["cb"]["DewPoint"]
                else:
                    delta = None
                print(
                    "'dp'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                        intersect_data_dict["mysql"]["dp"],
                        intersect_data_dict["cb"]["DewPoint"],
                        delta, units['DewPoint']
                    )
                )

                if intersect_data_dict["mysql"]["wd"] is not None and intersect_data_dict["cb"]["WD"] is not None:
                    delta = intersect_data_dict["mysql"]["wd"] - intersect_data_dict["cb"]["WD"]
                    if delta > 180:
                        delta = 360 - delta
                    if delta < -180:
                        delta = 360 + delta
                else:
                    delta = None
                print(
                    "'wd'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                        intersect_data_dict["mysql"]["wd"],
                        intersect_data_dict["cb"]["WD"],
                        delta, units['WD']
                    )
                )

                if intersect_data_dict["mysql"]["ws"] is not None and intersect_data_dict["cb"]["WS"]  is not None:
                    delta =intersect_data_dict["mysql"]["ws"] - intersect_data_dict["cb"]["WS"]
                else:
                    delta = None
                print(
                    "'ws'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                        intersect_data_dict["mysql"]["ws"],
                        intersect_data_dict["cb"]["WS"],
                        delta, units['WS']
                    )
                )

                if intersect_data_dict["mysql"]["ceiling"] is not None and intersect_data_dict["cb"]["Ceiling"] is not None:
                    delta = intersect_data_dict["mysql"]["ceiling"] - intersect_data_dict["cb"]["Ceiling"]
                else:
                    delta = None
                print(
                    "'ceiling'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                        intersect_data_dict["mysql"]["ceiling"],
                        intersect_data_dict["cb"]["Ceiling"],
                        delta, units['Ceiling']
                    )
                )

                if intersect_data_dict["mysql"]["visibility"] is not None and intersect_data_dict["cb"]["Visibility"] is not None:
                    delta = intersect_data_dict["mysql"]["visibility"] - intersect_data_dict["cb"]["Visibility"]
                else:
                    delta = None
                print(
                    "'visibility'\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                        intersect_data_dict["mysql"]["visibility"],
                        intersect_data_dict["cb"]["Visibility"],
                        delta, units['Visibility']
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
                    atol=3,
                    rtol=0,
                    err_msg="MYSQL temp and CB Temperature are not approximately equal",
                    verbose=True,
                )
                np.testing.assert_allclose(
                    intersect_data_dict["mysql"]["dp"],
                    intersect_data_dict["cb"]["DewPoint"],
                    atol=2.0,
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
                        atol=10,
                        rtol=0,
                        err_msg="MYSQL wd and CB WD are not approximately equal",
                        verbose=True,
                    )
                if intersect_data_dict["mysql"]["ws"] is not None and intersect_data_dict["cb"]["WS"] is not None:
                    np.testing.assert_allclose(
                        intersect_data_dict["mysql"]["ws"],
                        intersect_data_dict["cb"]["WS"],
                        atol=10,
                        rtol=0,
                        err_msg="MYSQL ws and CB WS are not approximately equal",
                        verbose=True,
                    )
                if intersect_data_dict["mysql"]["visibility"] is not None and intersect_data_dict["cb"]["Visibility"] is not None:
                    np.testing.assert_allclose(
                        intersect_data_dict["mysql"]["visibility"],
                        intersect_data_dict["cb"]["Visibility"],
                        atol=10,
                        rtol=0,
                        err_msg="MYSQL Visibility and CB Visibility are not approximately equal",
                        verbose=True,
                    )
                if intersect_data_dict["mysql"]["ceiling"] is not None and intersect_data_dict["cb"]["Ceiling"] is not None:
                    np.testing.assert_allclose(
                        intersect_data_dict["mysql"]["ceiling"],
                        intersect_data_dict["cb"]["Ceiling"],
                        atol=30,
                        rtol=0,
                        err_msg="MYSQL Ceiling and CB Ceiling are not approximately equal",
                        verbose=True,
                    )
        except:
            print(str(sys.exc_info()))
            self.fail("TestGsdIngestManager Exception failure: " + str(sys.exc_info()))

    def test_one_thread_spedicfy_file_pattern(self):
        try:
            cwd = os.getcwd()
            self.spec_file = (
                cwd + "/netcdf_to_cb/test/test_load_spec_netcdf_metar_obs_V01.yaml"
            )
            # setup - remove output files
            for _f in glob('/opt/data/netcdf_to_cb/output/test1/*.json'):
                os.remove(_f)
            vx_ingest = VXIngest()
            vx_ingest.runit(
                {
                    "spec_file": self.spec_file,
                    "credentials_file": os.environ["HOME"] + "/adb-cb1-credentials",
                    "path": "/opt/data/netcdf_to_cb/input_files",
                    "file_name_mask": "%Y%m%d_%H%M",
                    "output_dir": "/opt/data/netcdf_to_cb/output/test1",
                    "threads": 1,
                    "file_pattern": "20211108_0000"
                }
            )
            self.assertTrue(len(glob("/opt/data/netcdf_to_cb/output/test1/[0123456789]???????_[0123456789]???.json")) > 0,msg="There are no output files")
            self.assertTrue(len(glob("/opt/data/netcdf_to_cb/output/test1/LJ:netcdf_to_cb.run_ingest_threads:VXIngest:*.json")) == 1, msg="there is no load job output file")
            # use file globbing to see if we got one output file for each input file plus one load job file
            self.assertTrue(len(glob("/opt/data/netcdf_to_cb/output/test1/20211108*.json")) ==
                len(glob("/opt/data/netcdf_to_cb/input_files/20211108_0000")), msg="number of output files is incorrect")
            # teardown remove output files
            for _f in glob('/opt/data/netcdf_to_cb/output/test1/*.json'):
                os.remove(_f)
        except Exception as e:
            self.fail("TestGsdIngestManager Exception failure: " + str(e))


    def test_two_threads_spedicfy_file_pattern(self):
        try:
            cwd = os.getcwd()
            self.spec_file = (
                cwd + "/netcdf_to_cb/test/test_load_spec_netcdf_metar_obs_V01.yaml"
            )
            # setup - remove output files
            for _f in glob('/opt/data/netcdf_to_cb/output/test2/*.json'):
                os.remove(_f)
            vx_ingest = VXIngest()
            vx_ingest.runit(
                {
                    "spec_file": self.spec_file,
                    "credentials_file": os.environ["HOME"] + "/adb-cb1-credentials",
                    "path": "/opt/data/netcdf_to_cb/input_files",
                    "file_name_mask": "%Y%m%d_%H%M",
                    "output_dir": "/opt/data/netcdf_to_cb/output/test2",
                    "threads": 2,
                    "file_pattern": "20210919*"
                }
            )
            self.assertTrue(len(glob("/opt/data/netcdf_to_cb/output/test2/[0123456789]???????_[0123456789]???.json")) > 0,msg="There are no output files")
            self.assertTrue(len(glob("/opt/data/netcdf_to_cb/output/test2/LJ:netcdf_to_cb.run_ingest_threads:VXIngest:*.json")) == 1, msg="there is no load job output file")
            # use file globbing to see if we got one output file for each input file plus one load job file
            self.assertTrue(len(glob("/opt/data/netcdf_to_cb/output/test2/20210919*.json")) ==
                len(glob("/opt/data/netcdf_to_cb/input_files/20210919*")), msg="number of output files is incorrect")

            # teardown remove output files
            for _f in glob('/opt/data/netcdf_to_cb/output/test2/*.json'):
                os.remove(_f)
        except Exception as e:
            self.fail("TestGsdIngestManager Exception failure: " + str(e))

    def test_one_thread_default(self):
        """This test will start one thread of the ingestManager and simply make sure it runs with no Exceptions.
        It will attempt to process any files that are in the input directory that atch the file_name_mask.
        TIP: you might want to use local credentials to a local couchbase. If you do
        you will need to run the scripts in the matsmetadata directory to load the local metadata.
        Remove any documents type DD prior to using this test."""
        try:
            cwd = os.getcwd()
            self.spec_file = (
                cwd + "/netcdf_to_cb/test/test_load_spec_netcdf_metar_obs_V01.yaml"
            )
            # setup - remove output files
            for _f in glob('/opt/data/netcdf_to_cb/output/test3/*.json'):
                os.remove(_f)
            vx_ingest = VXIngest()
            vx_ingest.runit(
                {
                    "spec_file": self.spec_file,
                    "credentials_file": os.environ["HOME"] + "/adb-cb1-credentials",
                    "path": "/opt/data/netcdf_to_cb/input_files",
                    "file_name_mask": "%Y%m%d_%H%M",
                    "output_dir": "/opt/data/netcdf_to_cb/output/test3",
                    "threads": 1
                }
            )
            self.assertTrue(len(glob("/opt/data/netcdf_to_cb/output/test3/[0123456789]???????_[0123456789]???.json")) > 0,msg="There are no output files")
            self.assertTrue(len(glob("/opt/data/netcdf_to_cb/output/test3/LJ:netcdf_to_cb.run_ingest_threads:VXIngest:*.json")) == 1, msg="there is no load job output file")
            # use file globbing to see if we got one output file for each input file plus one load job file
            self.assertTrue(len(glob("/opt/data/netcdf_to_cb/output/test3/[0123456789]???????_[0123456789]???.json")) ==
                len(glob("/opt/data/netcdf_to_cb/input_files/[0123456789]???????_[0123456789]???")), msg="number of output files is incorrect")

            # teardown remove output files
            for _f in glob('/opt/data/netcdf_to_cb/output/test3/*.json'):
                os.remove(_f)
        except Exception as e:
            self.fail("TestGsdIngestManager Exception failure: " + str(e))


    def test_two_threads_default(self):
        """This test will start one thread of the ingestManager and simply make sure it runs with no Exceptions.
        It will attempt to process any files that are in the input directory that atch the file_name_mask.
        TIP: you might want to use local credentials to a local couchbase. If you do
        you will need to run the scripts in the matsmetadata directory to load the local metadata.
        Remove any documents type DD prior to using this test."""
        try:
            cwd = os.getcwd()
            self.spec_file = (
                cwd + "/netcdf_to_cb/test/test_load_spec_netcdf_metar_obs_V01.yaml"
            )
            # setup - remove output files
            for _f in glob('/opt/data/netcdf_to_cb/output/test4/*.json'):
                os.remove(_f)
            vx_ingest = VXIngest()
            vx_ingest.runit(
                {
                    "spec_file": self.spec_file,
                    "credentials_file": os.environ["HOME"] + "/adb-cb1-credentials",
                    "path": "/opt/data/netcdf_to_cb/input_files",
                    "file_name_mask": "%Y%m%d_%H%M",
                    "output_dir": "/opt/data/netcdf_to_cb/output/test4",
                    "threads": 2
                }
            )
            self.assertTrue(len(glob("/opt/data/netcdf_to_cb/output/test4/[0123456789]???????_[0123456789]???.json")) > 0,msg="There are no output files")
            self.assertTrue(len(glob("/opt/data/netcdf_to_cb/output/test4/LJ:netcdf_to_cb.run_ingest_threads:VXIngest:*.json")) == 1, msg="there is no load job output file")
            # use file globbing to see if we got one output file for each input file plus one load job file
            self.assertTrue(len(glob("/opt/data/netcdf_to_cb/output/test4/[0123456789]???????_[0123456789]???.json")) ==
                len(glob("/opt/data/netcdf_to_cb/input_files/[0123456789]???????_[0123456789]???")), msg="number of output files is incorrect")

            # teardown remove output files
            for _f in glob('/opt/data/netcdf_to_cb/output/test4/*.json'):
                os.remove(_f)
        except Exception as e:
            self.fail("TestGsdIngestManager Exception failure: " + str(e))

    def check_mismatched_fcstValidEpoch_to_id(self):
        """This is a simple ultility test that can be used to see if there are
        any missmatched fcstValidEpoch values among the observations i.e. the fcstValidEpoch in the id
        does not match the fcstValidEpoch in the top level fcstValidEpoch field"""
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
            result = cluster.query("""
            select mdata.fcstValidEpoch, mdata.id
            FROM mdata
            WHERE
                mdata.docType = "obs"
                AND mdata.subset = "METAR"
                AND mdata.type = "DD"
                AND mdata.version = "V01"
                AND NOT CONTAINS(mdata.id,to_string(mdata.fcstValidEpoch)) """)
            for row in result:
                self.fail("These do not have the same fcstValidEpoch: {0}".format(str(row['fcstValidEpoch']) + row['id']))
        except Exception as e:
            self.fail("TestGsdIngestManager Exception failure: " + str(e))
