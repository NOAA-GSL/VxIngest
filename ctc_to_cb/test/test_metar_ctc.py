"""
test for VxIngest CTC builders
"""
import glob
import json
import os
import sys
import unittest
import yaml
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

    def test_get_stations_geo_search(self):
        try:
            cwd = os.getcwd()
            self.credentials_file = os.environ['HOME'] + '/adb-cb1-credentials'
            self.assertTrue(Path(self.credentials_file).is_file(),
                            "credentials_file Does not exist")
            f = open(self.credentials_file)
            yaml_data = yaml.load(f, yaml.SafeLoader)
            host = yaml_data['cb_host']
            user = yaml_data['cb_user']
            password = yaml_data['cb_password']
            options = ClusterOptions(PasswordAuthenticator(user, password))
            self.cluster = Cluster('couchbase://' + host, options)
            self.collection = self.cluster.bucket("mdata").default_collection()
            result = self.cluster.query(
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
                result1 = self.cluster.search_query("station_geo", GeoBoundingBoxQuery(
                    top_left=(row['tl_lon'], row['tl_lat']), bottom_right=(row['br_lon'], row['br_lat']), field="geo"), SearchOptions(fields=["name"], limit=10000))
                classic_station_id = "MD-TEST:V01:CLASSIC_STATIONS:" + row['name']
                doc = self.collection.get(classic_station_id.strip())
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
        try:
            cwd = os.getcwd()
            credentials_file = os.environ['HOME'] + '/adb-cb1-credentials'
            spec_file = cwd + '/ctc_to_cb/test/test_load_spec_metar_hrrr_ops_all_hrrr_ctc_V01.yaml'
            vx_ingest = VXIngest()
            vx_ingest.runit({'spec_file': spec_file,
                            'credentials_file': credentials_file,
                            'output_dir': '/opt/data/ctc_to_cb/output',
                            'threads': 1,
                            'first_epoch': 1629828000,
                            'last_epoch': 162983000
                            })
            list_of_output_files = glob.glob('/opt/data/ctc_to_cb/output/*')
            latest_output_file = max(list_of_output_files, key=os.path.getctime)
            # Opening JSON file
            f = open(latest_output_file)
            # returns JSON object as
            # a dictionary
            vx_ingest_output_data = json.load(f)
            self.assertEqual(len(vx_ingest_output_data), 9, "There aren't 9 elements in the output data")
            self.assertEqual(vx_ingest_output_data[0]['data']['500']['hits'], 3, "hits should be 3 for threshold 500 not %s" + str(vx_ingest_output_data[0]['data']['500']['hits']))
            self.assertEqual(vx_ingest_output_data[0]['data']['500']['false_alarms'], 5, "hits should be 5 for threshold 500 not %s" + str(vx_ingest_output_data[0]['data']['500']['false_alarms']))
            self.assertEqual(vx_ingest_output_data[0]['data']['500']['misses'], 4, "misses should be 8 for threshold 500 not %s" + str(vx_ingest_output_data[0]['data']['500']['misses']))
            self.assertEqual(vx_ingest_output_data[0]['data']['500']['correct_negatives'], 1782, "correct_negatives should be 1782 for threshold 500 not %s" + str(vx_ingest_output_data[0]['data']['500']['correct_negatives']))
            # Closing file
            f.close()

        except:
            self.fail("TestCTCBuilderV01 Exception failure: " +
                      str(sys.exc_info()[0]))
        return

    def test_ctc_builder_hrrr_ops_all_hrrr_first_last(self):
        # noinspection PyBroadException
        try:
            cwd = os.getcwd()
            credentials_file = os.environ['HOME'] + '/adb-cb1-credentials'
            spec_file = cwd + '/ctc_to_cb/test/test_load_spec_metar_hrrr_ops_all_hrrr_ctc_V01.yaml'
            vx_ingest = VXIngest()
            vx_ingest.runit({'spec_file': spec_file,
                            'credentials_file': credentials_file,
                            'output_dir': '/opt/data/ctc_to_cb/output',
                            'threads': 1,
                            'first_epoch': 1629122400,
                            'last_epoch': 1629122400
                            })
            list_of_output_files = glob.glob('/opt/data/ctc_to_cb/output/*')
            latest_output_file = max(list_of_output_files, key=os.path.getctime)
            # Opening JSON file
            f = open(latest_output_file)
            # returns JSON object as
            # a dictionary
            vx_ingest_output_data = json.load(f)
            self.assertEqual(len(vx_ingest_output_data), 18, "There aren't 18 elements in the output data")
            self.assertEqual(vx_ingest_output_data[0]['data']['500']['hits'], 40, "hits should be 40 for threshold 500 not %s" + str(vx_ingest_output_data[0]['data']['500']['hits']))
            self.assertEqual(vx_ingest_output_data[0]['data']['500']['false_alarms'], 28, "hits should be 28 for threshold 500 not %s" + str(vx_ingest_output_data[0]['data']['500']['false_alarms']))
            self.assertEqual(vx_ingest_output_data[0]['data']['500']['misses'], 30, "misses should be 30 for threshold 500 not %s" + str(vx_ingest_output_data[0]['data']['500']['misses']))
            self.assertEqual(vx_ingest_output_data[0]['data']['500']['correct_negatives'], 1684, "correct_negatives should be 1684 for threshold 500 not %s" + str(vx_ingest_output_data[0]['data']['500']['correct_negatives']))
            # Closing file
            f.close()

        except:
            self.fail("TestCTCBuilderV01 Exception failure: " +
                      str(sys.exc_info()[0]))
        return


    def test_ctc_builder_hrrr_ops_e_hrrr(self):
        # noinspection PyBroadException
        try:
            cwd = os.getcwd()
            credentials_file = os.environ['HOME'] + '/adb-cb1-credentials'
            spec_file = cwd + '/ctc_to_cb/test/test_load_spec_metar_hrrr_ops_e_hrrr_ctc_V01.yaml'
            vx_ingest = VXIngest()
            vx_ingest.runit({'spec_file': spec_file,
                            'credentials_file': credentials_file,
                            'output_dir': '/opt/data/ctc_to_cb/output',
                            'threads': 1,
                            'first_epoch': 1627149600,
                            'last_epoch': 1627149600
                            })
            list_of_output_files = glob.glob('/opt/data/ctc_to_cb/output/*')
            latest_output_file = max(list_of_output_files, key=os.path.getctime)
            # Opening JSON file
            f = open(latest_output_file)
            # returns JSON object as
            # a dictionary
            vx_ingest_output_data = json.load(f)
            self.assertEqual(len(vx_ingest_output_data), 9, "There aren't 9 elements in the output data")
            self.assertEqual(vx_ingest_output_data[0]['data']['500']['hits'], 3, "hits should be 3 for threshold 500 not %s" + str(vx_ingest_output_data[0]['data']['500']['hits']))
            self.assertEqual(vx_ingest_output_data[0]['data']['500']['false_alarms'], 5, "hits should be 5 for threshold 500 not %s" + str(vx_ingest_output_data[0]['data']['500']['false_alarms']))
            self.assertEqual(vx_ingest_output_data[0]['data']['500']['misses'], 4, "misses should be 8 for threshold 500 not %s" + str(vx_ingest_output_data[0]['data']['500']['misses']))
            self.assertEqual(vx_ingest_output_data[0]['data']['500']['correct_negatives'], 1782, "correct_negatives should be 1782 for threshold 500 not %s" + str(vx_ingest_output_data[0]['data']['500']['correct_negatives']))
            # Closing file
            f.close()

        except:
            self.fail("TestCTCBuilderV01 Exception failure: " +
                      str(sys.exc_info()[0]))
        return
