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

    def test_ctcBuilder(self):
        # noinspection PyBroadException
        try:
            cwd = os.getcwd()
            credentials_file = os.environ['HOME'] + '/adb-cb1-credentials'
            spec_file = cwd + '/ctc_to_cb/test/test_load_spec_metar_ctc_V01.yaml'
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
