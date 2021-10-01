"""
test for VxIngest CTC builders
"""
import glob
import json
import os
import sys
import unittest
import yaml
import pymysql
from pymysql.constants import CLIENT
from pathlib import Path
from couchbase.cluster import Cluster, ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.search import GeoBoundingBoxQuery, SearchOptions
from ctc_to_cb.run_ingest_threads import VXIngest

class TestCTCBuilderV01(unittest.TestCase):
    """
    This test expects to find obs data and model data for hrrr_ops.
    This test expects to write to the local output directory /opt/data/ctc_to_cb/output
    so that directory should exist.
    """
# /public/data/grib/hrrr_wrfsfc/7/0/83/0_1905141_30/2125112000000
# "DD:V01:METAR:HRRR_OPS:1631102400:0
# DD:V01:METAR:obs:1631102400
# wd 87.92309758021554

    def test_get_stations_geo_search(self):
        try:
            credentials_file = os.environ['HOME'] + '/adb-cb1-credentials'
            self.assertTrue(Path(credentials_file).is_file(),
                            "credentials_file Does not exist")
            f = open(self.credentials_file)
            yaml_data = yaml.load(f, yaml.SafeLoader)
            host = yaml_data['cb_host']
            user = yaml_data['cb_user']
            password = yaml_data['cb_password']
            options = ClusterOptions(PasswordAuthenticator(user, password))
            cluster = Cluster('couchbase://' + host, options)
            collection = cluster.bucket("mdata").default_collection()
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
                """)
            for row in result:
                result1 = cluster.search_query("station_geo", GeoBoundingBoxQuery(
                    top_left=(row['tl_lon'], row['tl_lat']), bottom_right=(row['br_lon'], row['br_lat']), field="geo"), SearchOptions(fields=["name"], limit=10000))
                classic_station_id = "MD-TEST:V01:CLASSIC_STATIONS:" + row['name']
                doc = collection.get(classic_station_id.strip())
                classic_stations = doc.content['stations']
                classic_stations.sort()
                stations = []
                for elem in list(result1):
                    stations.append(elem.fields['name'])
                stations.sort()
                intersection = [value for value in stations if value in classic_stations]
                self.assertTrue (len(classic_stations) - len(intersection) < 100, "difference between expected and actual greater than 100")
        except Exception as e:
            self.fail("TestGsdIngestManager Exception failure: " + str(e))

    def test_ctc_builder_hrrr_ops_all_hrrr(self):
        # noinspection PyBroadException
        """
        Couchbase:
        SELECT mdata.fcstValidEpoch, mdata.fcstLen, mdata.data
        FROM mdata
        WHERE mdata.type='DD'
            AND mdata.docType = "CTC"
            AND mdata.subDocType = "CEILING"
            AND mdata.model='HRRR_OPS'
            AND mdata.region='ALL_HRRR'
            AND mdata.version='V01'
            AND mdata.subset='METAR'
            AND mdata.fcstValidEpoch = 1632898800
            AND mdata.fcstLen = 9
            order by mdata.fcstValidEpoch, mdata.fcstLen

            mysql:
            select * from ceiling_sums2.HRRR_OPS_ALL_HRRR
            where time = 1632898800
                AND fcst_len = 9;
            """
        try:
            cwd = os.getcwd()
            credentials_file = os.environ['HOME'] + '/adb-cb1-credentials'
            self.assertTrue(Path(credentials_file).is_file(),"credentials_file Does not exist")
            spec_file = cwd + '/ctc_to_cb/test/test_load_spec_metar_hrrr_ops_all_hrrr_ctc_V01.yaml'
            vx_ingest = VXIngest()
            vx_ingest.runit({'spec_file': spec_file,
                            'credentials_file': credentials_file,
                            'output_dir': '/opt/data/ctc_to_cb/output',
                            'threads': 1,
                            'first_epoch': 100
                            })
            list_of_output_files = glob.glob('/opt/data/ctc_to_cb/output/*')
            latest_output_file = max(list_of_output_files, key=os.path.getctime)
            try:
                # Opening JSON file
                output_file = open(latest_output_file)
                # returns JSON object as a dictionary
                vx_ingest_output_data = json.load(output_file)
                # get fcstValidEpoch
                fcst_valid_epoch = vx_ingest_output_data[0]['fcstValidEpoch']
                output_file.close()
            except:
                self.fail("TestCTCBuilderV01 Exception failure opening output: " + str(sys.exc_info()[0]))

            cf = open(credentials_file)
            yaml_data = yaml.load(cf, yaml.SafeLoader)
            host = yaml_data['cb_host']
            user = yaml_data['cb_user']
            password = yaml_data['cb_password']
            cf.close()
            options = ClusterOptions(PasswordAuthenticator(user, password))
            cluster = Cluster('couchbase://' + host, options)
            collection = cluster.bucket("mdata").default_collection()
            # upsert test document
            try:
                docs = {}
                for doc in vx_ingest_output_data:
                    docs[doc['id']] = doc
                collection.upsert_multi(docs)
            except:
                self.fail("upserting test - trying upsert: Got TimeOutException -  Document may not be persisted." + str(sys.exc_info()[0]))

            result = cluster.query(
                """
                SELECT raw mdata.fcstLen
                FROM mdata
                WHERE mdata.type='DD-TEST'
                    AND mdata.docType = "CTC"
                    AND mdata.subDocType = "CEILING"
                    AND mdata.model='HRRR_OPS'
                    AND mdata.region='ALL_HRRR'
                    AND mdata.version='V01'
                    AND mdata.subset='METAR'
                    AND mdata.fcstValidEpoch = $time
                    order by mdata.fcstLen
                """, time=fcst_valid_epoch)

            # get all the cb fcstLen values
            cb_fcst_valid_lens = list(result)

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
            statement = "select DISTINCT fcst_len from ceiling_sums2.HRRR_OPS_ALL_HRRR where time = %s;"
            cursor.execute(statement, (fcst_valid_epoch))
            mysql_fcst_valid_lens_result = cursor.fetchall()
            mysql_fcst_valid_lens=[o['fcst_len'] for o in mysql_fcst_valid_lens_result]
            intersect_fcst_lens = [value for value in mysql_fcst_valid_lens if value in cb_fcst_valid_lens]

            result = cluster.query(
                """
                SELECT *
                FROM mdata
                WHERE mdata.type='DD-TEST'
                    AND mdata.docType = "CTC"
                    AND mdata.subDocType = "CEILING"
                    AND mdata.model='HRRR_OPS'
                    AND mdata.region='ALL_HRRR'
                    AND mdata.version='V01'
                    AND mdata.subset='METAR'
                    AND mdata.fcstValidEpoch = $time
                    AND mdata.fcstLen IN $intersect_fcst_lens
                    order by mdata.fcstLen;
                """, time=fcst_valid_epoch, intersect_fcst_lens=intersect_fcst_lens)
            cb_results = list(result)

            result = cluster.query("""
                SELECT RAW mdata.thresholdDescriptions
                FROM mdata
                WHERE type="MD"
                    AND docType="matsAux"
                """, read_only=True)
            thresholds = list(map(int, list((list(result)[0])['ceiling'].keys())))

            format_strings = ','.join(['%s'] * len(intersect_fcst_lens))
            params = [fcst_valid_epoch]
            params.extend(intersect_fcst_lens)
            statement = "select fcst_len,trsh, yy as hits, yn as false_alarms, ny as misses, nn as correct_negatives from ceiling_sums2.HRRR_OPS_ALL_HRRR where time = %s AND fcst_len IN (""" + format_strings + ") ORDER BY fcst_len;"
            cursor.execute(statement, tuple(params))
            mysql_results = cursor.fetchall()

            mysql_fcst_len_thrsh = {}
            for i in intersect_fcst_lens:
                mysql_fcst_len = [value for value in mysql_results if value['fcst_len'] == i]
                for t in thresholds:
                    for mysql_fcst_len_thrsh in mysql_fcst_len:
                        if mysql_fcst_len_thrsh['trsh'] * 10 == t:
                            break
                    self.assertEqual(cb_results[i]['mdata']['data'][str(t)]['hits'], mysql_fcst_len_thrsh['hits'],
                        "mysql hits {mhits} do not match couchbase hits {chits} for fcst_len {f} and threshold {t}".format(
                            mhits=mysql_fcst_len_thrsh['hits'], chits=cb_results[i]['mdata']['data'][str(t)]['hits'],f=i, t=t))
                    self.assertEqual(cb_results[i]['mdata']['data'][str(t)]['misses'], mysql_fcst_len_thrsh['misses'],
                        "mysql misses {mmisses} do not match couchbase misses {cmisses} for fcst_len {f} and threshold {t}".format(
                            mmisses=mysql_fcst_len_thrsh['misses'], cmisses=cb_results[i]['mdata']['data'][str(t)]['misses'],f=i, t=t))
                    self.assertEqual(cb_results[i]['mdata']['data'][str(t)]['false_alarms'], mysql_fcst_len_thrsh['false_alarms'],
                        "mysql false_alarms {mfalse_alarms} do not match couchbase false_alarms {cfalse_alarms} for fcst_len {f} and threshold {t}".format(
                            mfalse_alarms=mysql_fcst_len_thrsh['false_alarms'], cfalse_alarms=cb_results[i]['mdata']['data'][str(t)]['false_alarms'],f=i, t=t))
                    self.assertEqual(cb_results[i]['mdata']['data'][str(t)]['correct_negatives'], mysql_fcst_len_thrsh['correct_negatives'],
                        "mysql correct_negatives {mcorrect_negatives} do not match couchbase correct_negatives {ccorrect_negatives} for fcst_len {f} and threshold {t}".format(
                            mcorrect_negatives=mysql_fcst_len_thrsh['correct_negatives'], ccorrect_negatives=cb_results[i]['mdata']['data'][str(t)]['correct_negatives'],f=i, t=t))
        except:
            self.fail("TestCTCBuilderV01 Exception failure: " + str(sys.exc_info()[0]))
        return
