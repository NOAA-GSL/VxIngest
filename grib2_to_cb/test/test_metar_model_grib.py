import sys
import os
import unittest
import yaml
import time
import pyproj
import pygrib
import grib2_to_cb.get_grid as gg
from pathlib import Path
from couchbase.cluster import Cluster, ClusterOptions
from couchbase.exceptions import TimeoutException
from couchbase_core.cluster import PasswordAuthenticator
from grib2_to_cb.run_ingest_threads import VXIngest
class TestGribBuilderV01(unittest.TestCase):

    def test_main(self):
        # noinspection PyBroadException
        try:
            cwd = os.getcwd()
            self.spec_file = cwd + 'test_load_spec_grib_metar_model_V01.yaml'
            vxIngest = VXIngest()
            vxIngest.runit({'spec_file': self.spec_file,
                            'credentials_file': os.environ['HOME'] + '/adb-cb1-credentials'
                            })
        except:
            self.fail("TestGsdIngestManager Exception failure: " +
                      str(sys.exc_info()[0]))
