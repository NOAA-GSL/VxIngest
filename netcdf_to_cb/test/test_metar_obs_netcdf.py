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

    def test_compare_model_obs_to_mysql(self):
        """This test attempts to find recent observations that match in both the mysql and the CB
            databases and compare them. This test isn't likely to succedd unless both the legacy 
            ingest and the VxIngest have recently run.
        """
        try:
            credentials_file = os.environ['HOME'] + '/adb-cb1-credentials'
            self.assertTrue(Path(credentials_file).is_file(),
                            "credentials_file Does not exist")

            f = open(credentials_file)
            yaml_data = yaml.load(f, yaml.SafeLoader)
            host = yaml_data['cb_host']
            user = yaml_data['cb_user']
            password = yaml_data['cb_password']
            options = ClusterOptions(PasswordAuthenticator(user, password))
            cluster = Cluster('couchbase://' + host, options)

            result = cluster.query("""SELECT DISTINCT RAW fcstValidEpoch
                FROM mdata
                WHERE type='DD'
                AND docType="model"
                AND model="HRRR_OPS"
                AND version='V01'
                AND subset='METAR'
                ORDER BY fcstValidEpoch DESC
                """)
            cb_hrrr_ops_fcst_valid_epochs = list(result)

            result = cluster.query("""SELECT raw fcstValidEpoch
                FROM mdata
                WHERE type='DD'
                AND docType="obs"
                AND version='V01'
                AND subset='METAR'
                ORDER BY fcstValidEpoch DESC
                """)
            cb_obs_fcst_valid_epochs = list(result)

            # find the intersection of the cb models and the obs
            intersect_cb_times = [
                value for value in cb_hrrr_ops_fcst_valid_epochs if value in cb_obs_fcst_valid_epochs]

            # find the mysql obs

            host = yaml_data['mysql_host']
            user = yaml_data['mysql_user']
            passwd = yaml_data['mysql_password']
            local_infile = True
            connection = pymysql.connect(host=host, user=user, passwd=passwd, local_infile=local_infile,
                                         autocommit=True, charset='utf8mb4',
                                         cursorclass=pymysql.cursors.SSDictCursor,
                                         client_flag=CLIENT.MULTI_STATEMENTS)
            cursor = connection.cursor(pymysql.cursors.SSDictCursor)
            statement = """SELECT floor((m0.time+1800)/(3600))*3600 AS time
                            FROM   madis3.HRRR_OPSqp AS m0,
                            madis3.obs AS o,
                            madis3.metars AS s
                            WHERE  s.name = "KDEN"
                            AND m0.time = o.time 
                            AND s.madis_id = m0.sta_id
                            AND o.sta_id = m0.sta_id
                            AND m0.fcst_len = 0
                            AND m0.time <= %s
                            AND m0.time >= %s; """
            cursor.execute(
                statement, (intersect_cb_times[0], intersect_cb_times[-1]))
            intersect_mysql_times_tmp = cursor.fetchall()
            intersect_mysql_times = [t['time']
                                     for t in intersect_mysql_times_tmp]
            valid_times = [
                value for value in intersect_mysql_times if value in intersect_cb_times]

            for time in valid_times:
                result = cluster.query("""SELECT mdata.fcstValidEpoch,
                        mdata.fcstLen, data_item
                        FROM mdata
                        UNNEST mdata.data AS data_item
                        WHERE mdata.type='DD'
                            AND mdata.docType="model"
                            AND mdata.model="HRRR_OPS"
                            AND mdata.version='V01'
                            AND mdata.subset='METAR'
                            AND data_item.name="KDEN"
                            AND mdata.fcstValidEpoch=$time
                            ORDER BY mdata.fcstLen""", time=time)
                cb_model_values = list(result)

                statement = """select m0.* 
                from  madis3.metars as s, madis3.HRRR_OPSqp as m0 
                WHERE 1=1 
                AND s.madis_id = m0.sta_id 
                AND s.name = "KDEN" 
                AND  m0.time >= %s - 1800 and m0.time < %s + 1800
                ORDER BY m0.fcst_len;"""
                cursor.execute(statement, (time, time))
                mysql_model_values_tmp = cursor.fetchall()
                mysql_model_fcst_len = [v['fcst_len'] for v in mysql_model_values_tmp]
                mysql_model_press = [v['press']/10 for v in mysql_model_values_tmp]
                mysql_model_temp = [v['temp']/10 for v in mysql_model_values_tmp]
                mysql_model_dp = [v['dp']/10 for v in mysql_model_values_tmp]
                mysql_model_wd = [v['wd'] for v in mysql_model_values_tmp]
                mysql_model_ws = [v['ws'] for v in mysql_model_values_tmp]
                mysql_model_rh = [v['rh']/10 for v in mysql_model_values_tmp]

                statement = """select m0.* 
                from  madis3.metars as s, ceiling2.HRRR_OPS as m0 
                WHERE 1=1 
                AND s.madis_id = m0.madis_id 
                AND s.name = "KDEN" 
                AND  m0.time >= %s - 1800 and m0.time < %s + 1800
                ORDER BY m0.fcst_len;"""
                cursor.execute(statement, (time, time))
                mysql_model_ceiling_values_tmp = cursor.fetchall()
                mysql_model_ceiling = [v['ceil']*10 for v in mysql_model_ceiling_values_tmp]

                statement = """select m0.* 
                from  madis3.metars as s, visibility.HRRR_OPS as m0 
                WHERE 1=1 
                AND s.madis_id = m0.madis_id 
                AND s.name = "KDEN" 
                AND  m0.time >= %s - 1800 and m0.time < %s + 1800
                ORDER BY m0.fcst_len;"""
                cursor.execute(statement, (time, time))
                mysql_model_visibility_values_tmp = cursor.fetchall()
                mysql_model_visibility = [v['vis100']/100 for v in mysql_model_visibility_values_tmp]
                # now we have values for this time for each fcst_len, iterate the fcst_len and assert each value
                for i in range(len(mysql_model_fcst_len)):
                    self.assertEquals(
                        mysql_model_fcst_len[i], cb_model_values[i]['fcstLen'], message='MYSQL fcst_len and CB fcstLen are not equal')
                    np.testing.assert_almost_equal(mysql_model_press[i], cb_model_values[i]['data_item']['Surface Pressure'],
                                                   decimal=4, err_msg='MYSQL Pressure and CB Surface Pressure are not approximately equal', verbose=True)
                    np.testing.assert_almost_equal(mysql_model_temp[i], cb_model_values[i]['data_item']['Temperature'],
                                                   decimal=4, err_msg='MYSQL temp and CB Temperature are not approximately equal', verbose=True)
                    np.testing.assert_almost_equal(mysql_model_dp[i], cb_model_values[i]['data_item']['DewPoint'],
                                                   decimal=4, err_msg='MYSQL dp and CB Dew Point are not approximately equal', verbose=True)
                    np.testing.assert_almost_equal(mysql_model_rh[i], cb_model_values[i]['data_item']['RH'],
                                                   decimal=3, err_msg='MYSQL rh and CB RH are not approximately equal', verbose=True)
                    np.testing.assert_almost_equal(mysql_model_wd[i], cb_model_values[i]['data_item']['WD'],
                                                   decimal=3, err_msg='MYSQL wd and CB WD are not approximately equal', verbose=True)
                    np.testing.assert_almost_equal(mysql_model_ws[i], cb_model_values[i]['data_item']['WS'],
                                                   decimal=3, err_msg='MYSQL ws and CB WS are not approximately equal', verbose=True)
                    np.testing.assert_almost_equal(mysql_model_visibility[i], cb_model_values[i]['data_item']['Visibility'],
                                                   decimal=3, err_msg='MYSQL Visibility and CB Visibility are not approximately equal', verbose=True)
                    np.testing.assert_almost_equal(mysql_model_ceiling[i], cb_model_values[i]['data_item']['Ceiling'],
                                                   decimal=3, err_msg='MYSQL Ceiling and CB Ceiling are not approximately equal', verbose=True)
        except:
            self.fail("TestGsdIngestManager Exception failure: " +
                      str(sys.exc_info()[0]))

    def test_one_thread_default_epoch(self):
        # noinspection PyBroadException
        try:
            cwd = os.getcwd()
            self.spec_file = cwd + '/netcdf_to_cb/test/test_load_spec_netcdf_metar_obs_V01.yaml'
            vxIngest = VXIngest()
            vxIngest.runit({'spec_file': self.spec_file,
                            'credentials_file': os.environ['HOME'] + '/adb-cb1-credentials',
                            'path': '/opt/data/netcdf_to_cb/input_files',
                            'file_name_mask': "%Y%m%d_%H%M",
                            'output_dir': '/opt/data/netcdf_to_cb/output',
                            'threads': 1
                            })
        except:
            self.fail("TestGsdIngestManager Exception failure: " +
                      str(sys.exc_info()[0]))

    def test_one_thread_first_epoch(self):
        # noinspection PyBroadException
        try:
            cwd = os.getcwd()
            self.spec_file = cwd + '/netcdf_to_cb/test/test_load_spec_netcdf_metar_obs_V01.yaml'
            vxIngest = VXIngest()
            vxIngest.runit({'spec_file': self.spec_file,
                            'credentials_file': os.environ['HOME'] + '/adb-cb1-credentials',
                            'path': '/opt/data/netcdf_to_cb/input_files',
                            'file_name_mask': "%Y%m%d_%H%M",
                            'output_dir': '/opt/data/netcdf_to_cb/output',
                            'threads': 1,
                            'first_epoch': 100
                            })
        except:
            self.fail("TestGsdIngestManager Exception failure: " +
                      str(sys.exc_info()[0]))

    def test_two_threads_default_epoch(self):
        # noinspection PyBroadException
        try:
            cwd = os.getcwd()
            self.spec_file = cwd + '/netcdf_to_cb/test/test_load_spec_netcdf_metar_obs_V01.yaml'
            vxIngest = VXIngest()
            vxIngest.runit({'spec_file': self.spec_file,
                            'credentials_file': os.environ['HOME'] + '/adb-cb1-credentials',
                            'path': '/opt/data/netcdf_to_cb/input_files',
                            'file_name_mask': "%Y%m%d_%H%M",
                            'output_dir': '/opt/data/netcdf_to_cb/output',
                            'threads': 2
                            })
        except:
            self.fail("TestGsdIngestManager Exception failure: " +
                      str(sys.exc_info()[0]))

    def test_two_threads_first_epoch(self):
        # noinspection PyBroadException
        try:
            cwd = os.getcwd()
            self.spec_file = cwd + '/netcdf_to_cb/test/test_load_spec_netcdf_metar_obs_V01.yaml'
            vxIngest = VXIngest()
            vxIngest.runit({'spec_file': self.spec_file,
                            'credentials_file': os.environ['HOME'] + '/adb-cb1-credentials',
                            'path': '/opt/data/netcdf_to_cb/input_files',
                            'file_name_mask': "%Y%m%d_%H%M",
                            'output_dir': '/opt/data/netcdf_to_cb/output',
                            'threads': 2,
                            'first_epoch': 1625875200
                            })
        except:
            self.fail("TestGsdIngestManager Exception failure: " +
                      str(sys.exc_info()[0]))
