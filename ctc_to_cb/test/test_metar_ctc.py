"""
test for VxIngest CTC builders
"""
import glob
import json
import os
import sys
import unittest
from ctc_to_cb.run_ingest_threads import VXIngest


class TestGribBuilderV01(unittest.TestCase):
    """
    This test expects to find obs data and model data for hrrr_ops.
    This test expects to write to the local output directory /opt/data/ctc_to_cb/output
    so that directory should exist.
    """

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
            # Closing file
            f.close()

        except:
            self.fail("TestCTCBuilderV01 Exception failure: " +
                      str(sys.exc_info()[0]))
        return
