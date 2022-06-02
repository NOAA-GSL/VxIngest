"""
    integration tests for netcdf
"""
import os
import sys
from glob import glob
from pathlib import Path
from unittest import TestCase

import numpy as np
import pymysql
import yaml
from couchbase.cluster import Cluster, ClusterOptions
from couchbase_core.cluster import PasswordAuthenticator
from pymysql.constants import CLIENT

from netcdf_to_cb.run_ingest_threads import VXIngest


class TestNetcdfObsBuilderV01(TestCase):
    """
    integration tests for netcdf
    """

    def test_compare_obs_to_mysql(self):
        """This test attempts to find recent observations that match in both the mysql and the CB
        databases and compare them. This test isn't likely to succeed unless both the legacy (sql)
        ingest and the VxIngest have recently run.
        """
        try:
            station = "KPDX"
            credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
            self.assertTrue(
                Path(credentials_file).is_file(), "credentials_file Does not exist"
            )

            _f = open(credentials_file)
            yaml_data = yaml.load(_f, yaml.SafeLoader)
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
                statement,
                (station, cb_obs_fcst_valid_epochs[0], cb_obs_fcst_valid_epochs[-1]),
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
                time=valid_times[0],
                station=station,
            )
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
                    time=time,
                    station=station,
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
                    mysql_obs_visibility = (
                        mysql_obs_visibility_values_tmp[0]["vis100"] / 100
                    )
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

                if (
                    intersect_data_dict["mysql"]["press"] is not None
                    and intersect_data_dict["cb"]["Surface Pressure"] is not None
                ):
                    delta = (
                        intersect_data_dict["mysql"]["press"]
                        - intersect_data_dict["cb"]["Surface Pressure"]
                    )
                else:
                    delta = None
                print(
                    "'press'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                        intersect_data_dict["mysql"]["press"],
                        intersect_data_dict["cb"]["Surface Pressure"],
                        delta,
                        units["Surface Pressure"],
                    )
                )

                if (
                    intersect_data_dict["mysql"]["temp"] is not None
                    and intersect_data_dict["cb"]["Temperature"] is not None
                ):
                    delta = abs(
                        intersect_data_dict["mysql"]["temp"]
                        - intersect_data_dict["cb"]["Temperature"]
                    )
                else:
                    delta = None
                print(
                    "'temp'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                        intersect_data_dict["mysql"]["temp"],
                        intersect_data_dict["cb"]["Temperature"],
                        delta,
                        units["Temperature"],
                    )
                )

                if (
                    intersect_data_dict["mysql"]["dp"] is not None
                    and intersect_data_dict["cb"]["DewPoint"] is not None
                ):
                    delta = (
                        intersect_data_dict["mysql"]["dp"]
                        - intersect_data_dict["cb"]["DewPoint"]
                    )
                else:
                    delta = None
                print(
                    "'dp'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                        intersect_data_dict["mysql"]["dp"],
                        intersect_data_dict["cb"]["DewPoint"],
                        delta,
                        units["DewPoint"],
                    )
                )

                if (
                    intersect_data_dict["mysql"]["wd"] is not None
                    and intersect_data_dict["cb"]["WD"] is not None
                ):
                    delta = (
                        intersect_data_dict["mysql"]["wd"]
                        - intersect_data_dict["cb"]["WD"]
                    )
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
                        delta,
                        units["WD"],
                    )
                )

                if (
                    intersect_data_dict["mysql"]["ws"] is not None
                    and intersect_data_dict["cb"]["WS"] is not None
                ):
                    delta = (
                        intersect_data_dict["mysql"]["ws"]
                        - intersect_data_dict["cb"]["WS"]
                    )
                else:
                    delta = None
                print(
                    "'ws'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                        intersect_data_dict["mysql"]["ws"],
                        intersect_data_dict["cb"]["WS"],
                        delta,
                        units["WS"],
                    )
                )

                if (
                    intersect_data_dict["mysql"]["ceiling"] is not None
                    and intersect_data_dict["cb"]["Ceiling"] is not None
                ):
                    delta = (
                        intersect_data_dict["mysql"]["ceiling"]
                        - intersect_data_dict["cb"]["Ceiling"]
                    )
                else:
                    delta = None
                print(
                    "'ceiling'\t\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                        intersect_data_dict["mysql"]["ceiling"],
                        intersect_data_dict["cb"]["Ceiling"],
                        delta,
                        units["Ceiling"],
                    )
                )

                if (
                    intersect_data_dict["mysql"]["visibility"] is not None
                    and intersect_data_dict["cb"]["Visibility"] is not None
                ):
                    delta = (
                        intersect_data_dict["mysql"]["visibility"]
                        - intersect_data_dict["cb"]["Visibility"]
                    )
                else:
                    delta = None
                print(
                    "'visibility'\t{0}\t\t{1}\t\t\t{2}\t\t\t{3}".format(
                        intersect_data_dict["mysql"]["visibility"],
                        intersect_data_dict["cb"]["Visibility"],
                        delta,
                        units["Visibility"],
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
                if (
                    intersect_data_dict["mysql"]["ws"] is not None
                    and intersect_data_dict["cb"]["WS"] is not None
                ):
                    np.testing.assert_allclose(
                        intersect_data_dict["mysql"]["ws"],
                        intersect_data_dict["cb"]["WS"],
                        atol=10,
                        rtol=0,
                        err_msg="MYSQL ws and CB WS are not approximately equal",
                        verbose=True,
                    )
                if (
                    intersect_data_dict["mysql"]["visibility"] is not None
                    and intersect_data_dict["cb"]["Visibility"] is not None
                ):
                    np.testing.assert_allclose(
                        intersect_data_dict["mysql"]["visibility"],
                        intersect_data_dict["cb"]["Visibility"],
                        atol=10,
                        rtol=0,
                        err_msg="MYSQL Visibility and CB Visibility are not approximately equal",
                        verbose=True,
                    )
                if (
                    intersect_data_dict["mysql"]["ceiling"] is not None
                    and intersect_data_dict["cb"]["Ceiling"] is not None
                ):
                    np.testing.assert_allclose(
                        intersect_data_dict["mysql"]["ceiling"],
                        intersect_data_dict["cb"]["Ceiling"],
                        atol=30,
                        rtol=0,
                        err_msg="MYSQL Ceiling and CB Ceiling are not approximately equal",
                        verbose=True,
                    )
        except:  # pylint: disable=bare-except
            print(str(sys.exc_info()))
            self.fail("TestGsdIngestManager Exception failure: " + str(sys.exc_info()))

    def test_one_thread_spedicfy_file_pattern(
        self,
    ):  # pylint:disable=missing-function-docstring
        try:
            cwd = os.getcwd()
            spec_file = (
                cwd + "/netcdf_to_cb/test/test_load_spec_netcdf_metar_obs_V01.yaml"
            )
            # setup - remove output files
            for _f in glob("/opt/data/netcdf_to_cb/output/test1/*.json"):
                os.remove(_f)
            vx_ingest = VXIngest()
            vx_ingest.runit(
                {
                    "spec_file": spec_file,
                    "credentials_file": os.environ["HOME"] + "/adb-cb1-credentials",
                    "path": "/opt/data/netcdf_to_cb/input_files",
                    "file_name_mask": "%Y%m%d_%H%M",
                    "output_dir": "/opt/data/netcdf_to_cb/output/test1",
                    "threads": 1,
                    "file_pattern": "20211108_0000",
                }
            )
            self.assertTrue(
                len(
                    glob(
                        "/opt/data/netcdf_to_cb/output/test1/[0123456789]???????_[0123456789]???.json"
                    )
                )
                > 0,
                msg="There are no output files",
            )
            self.assertTrue(
                len(
                    glob(
                        "/opt/data/netcdf_to_cb/output/test1/LJ:METAR:netcdf_to_cb.run_ingest_threads:VXIngest:*.json"
                    )
                )
                == 1,
                msg="there is no load job output file",
            )
            # use file globbing to see if we got one output file for each input file plus one load job file
            self.assertTrue(
                len(glob("/opt/data/netcdf_to_cb/output/test1/20211108*.json"))
                == len(glob("/opt/data/netcdf_to_cb/input_files/20211108_0000")),
                msg="number of output files is incorrect",
            )
            # teardown remove output files
            for _f in glob("/opt/data/netcdf_to_cb/output/test1/*.json"):
                os.remove(_f)
        except Exception as _e:  # pylint: disable=broad-except
            self.fail("TestGsdIngestManager Exception failure: " + str(_e))

    def test_two_threads_spedicfy_file_pattern(self):
        """
        integration test for testing multithreaded capability
        """
        try:
            cwd = os.getcwd()
            spec_file = (
                cwd + "/netcdf_to_cb/test/test_load_spec_netcdf_metar_obs_V01.yaml"
            )
            # setup - remove output files
            for _f in glob("/opt/data/netcdf_to_cb/output/test2/*.json"):
                os.remove(_f)
            vx_ingest = VXIngest()
            vx_ingest.runit(
                {
                    "spec_file": spec_file,
                    "credentials_file": os.environ["HOME"] + "/adb-cb1-credentials",
                    "path": "/opt/data/netcdf_to_cb/input_files",
                    "file_name_mask": "%Y%m%d_%H%M",
                    "output_dir": "/opt/data/netcdf_to_cb/output/test2",
                    "threads": 2,
                    "file_pattern": "20210919*",
                }
            )
            self.assertTrue(
                len(
                    glob(
                        "/opt/data/netcdf_to_cb/output/test2/[0123456789]???????_[0123456789]???.json"
                    )
                )
                > 0,
                msg="There are no output files",
            )
            self.assertTrue(
                len(
                    glob(
                        "/opt/data/netcdf_to_cb/output/test2/LJ:METAR:netcdf_to_cb.run_ingest_threads:VXIngest:*.json"
                    )
                )
                == 1,
                msg="there is no load job output file",
            )
            # use file globbing to see if we got one output file for each input file plus one load job file
            self.assertTrue(
                len(glob("/opt/data/netcdf_to_cb/output/test2/20210919*.json"))
                == len(glob("/opt/data/netcdf_to_cb/input_files/20210919*")),
                msg="number of output files is incorrect",
            )

            # teardown remove output files
            for _f in glob("/opt/data/netcdf_to_cb/output/test2/*.json"):
                os.remove(_f)
        except Exception as _e:  # pylint: disable=broad-except
            self.fail("TestGsdIngestManager Exception failure: " + str(_e))

    def test_one_thread_default(self):
        """This test will start one thread of the ingestManager and simply make sure it runs with no Exceptions.
        It will attempt to process any files that are in the input directory that atch the file_name_mask.
        TIP: you might want to use local credentials to a local couchbase. If you do
        you will need to run the scripts in the matsmetadata directory to load the local metadata.
        Remove any documents type DD prior to using this test."""
        try:
            cwd = os.getcwd()
            spec_file = (
                cwd + "/netcdf_to_cb/test/test_load_spec_netcdf_metar_obs_V01.yaml"
            )
            # setup - remove output files
            for _f in glob("/opt/data/netcdf_to_cb/output/test3/*.json"):
                os.remove(_f)
            vx_ingest = VXIngest()
            vx_ingest.runit(
                {
                    "spec_file": spec_file,
                    "credentials_file": os.environ["HOME"] + "/adb-cb1-credentials",
                    "path": "/opt/data/netcdf_to_cb/input_files",
                    "file_name_mask": "%Y%m%d_%H%M",
                    "output_dir": "/opt/data/netcdf_to_cb/output/test3",
                    "file_pattern": "[0123456789]???????_[0123456789]???",
                    "threads": 1,
                }
            )
            self.assertTrue(
                len(
                    glob(
                        "/opt/data/netcdf_to_cb/output/test3/[0123456789]???????_[0123456789]???.json"
                    )
                )
                > 0,
                msg="There are no output files",
            )
            self.assertTrue(
                len(
                    glob(
                        "/opt/data/netcdf_to_cb/output/test3/LJ:METAR:netcdf_to_cb.run_ingest_threads:VXIngest:*.json"
                    )
                )
                == 1,
                msg="there is no load job output file",
            )
            # use file globbing to see if we got one output file for each input file plus one load job file
            self.assertTrue(
                len(
                    glob(
                        "/opt/data/netcdf_to_cb/output/test3/[0123456789]???????_[0123456789]???.json"
                    )
                )
                == len(
                    glob(
                        "/opt/data/netcdf_to_cb/input_files/[0123456789]???????_[0123456789]???"
                    )
                ),
                msg="number of output files is incorrect",
            )

            # teardown remove output files
            for _f in glob("/opt/data/netcdf_to_cb/output/test3/*.json"):
                os.remove(_f)
        except Exception as _e:  # pylint: disable=broad-except
            self.fail("TestGsdIngestManager Exception failure: " + str(_e))

    def test_two_threads_default(self):
        """This test will start one thread of the ingestManager and simply make sure it runs with no Exceptions.
        It will attempt to process any files that are in the input directory that atch the file_name_mask.
        TIP: you might want to use local credentials to a local couchbase. If you do
        you will need to run the scripts in the matsmetadata directory to load the local metadata.
        Remove any documents type DD prior to using this test."""
        try:
            cwd = os.getcwd()
            spec_file = (
                cwd + "/netcdf_to_cb/test/test_load_spec_netcdf_metar_obs_V01.yaml"
            )
            # setup - remove output files
            for _f in glob("/opt/data/netcdf_to_cb/output/test4/*.json"):
                os.remove(_f)
            vx_ingest = VXIngest()
            vx_ingest.runit(
                {
                    "spec_file": spec_file,
                    "credentials_file": os.environ["HOME"] + "/adb-cb1-credentials",
                    "path": "/opt/data/netcdf_to_cb/input_files",
                    "file_name_mask": "%Y%m%d_%H%M",
                    "output_dir": "/opt/data/netcdf_to_cb/output/test4",
                    "threads": 2,
                }
            )
            self.assertTrue(
                len(
                    glob(
                        "/opt/data/netcdf_to_cb/output/test4/[0123456789]???????_[0123456789]???.json"
                    )
                )
                > 0,
                msg="There are no output files",
            )
            self.assertTrue(
                len(
                    glob(
                        "/opt/data/netcdf_to_cb/output/test4/LJ:METAR:netcdf_to_cb.run_ingest_threads:VXIngest:*.json"
                    )
                )
                >= 1,
                msg="there is no load job output file",
            )
            # use file globbing to see if we got one output file for each input file plus one load job file
            self.assertTrue(
                len(
                    glob(
                        "/opt/data/netcdf_to_cb/output/test4/[0123456789]???????_[0123456789]???.json"
                    )
                )
                == len(
                    glob(
                        "/opt/data/netcdf_to_cb/input_files/[0123456789]???????_[0123456789]???"
                    )
                ),
                msg="number of output files is incorrect",
            )

            # teardown remove output files
            for _f in glob("/opt/data/netcdf_to_cb/output/test4/*.json"):
                os.remove(_f)
        except Exception as _e:  # pylint: disable=broad-except
            self.fail("TestGsdIngestManager Exception failure: " + str(_e))

    def check_mismatched_fcst_valid_epoch_to_id(self):
        """This is a simple ultility test that can be used to see if there are
        any missmatched fcstValidEpoch values among the observations i.e. the fcstValidEpoch in the id
        does not match the fcstValidEpoch in the top level fcstValidEpoch field"""
        try:
            credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
            self.assertTrue(
                Path(credentials_file).is_file(), "credentials_file Does not exist"
            )

            _f = open(credentials_file)
            yaml_data = yaml.load(_f, yaml.SafeLoader)
            host = yaml_data["cb_host"]
            user = yaml_data["cb_user"]
            password = yaml_data["cb_password"]
            options = ClusterOptions(PasswordAuthenticator(user, password))
            cluster = Cluster("couchbase://" + host, options)
            result = cluster.query(
                """
            select mdata.fcstValidEpoch, mdata.id
            FROM mdata
            WHERE
                mdata.docType = "obs"
                AND mdata.subset = "METAR"
                AND mdata.type = "DD"
                AND mdata.version = "V01"
                AND NOT CONTAINS(mdata.id,to_string(mdata.fcstValidEpoch)) """
            )
            for row in result:
                self.fail(
                    "These do not have the same fcstValidEpoch: {0}".format(
                        str(row["fcstValidEpoch"]) + row["id"]
                    )
                )
        except Exception as _e:  # pylint: disable=broad-except
            self.fail("TestGsdIngestManager Exception failure: " + str(_e))
