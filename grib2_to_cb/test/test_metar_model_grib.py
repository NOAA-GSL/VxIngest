import sys
import os
import unittest
import grib2_to_cb.get_grid as gg
from pathlib import Path
from couchbase.cluster import Cluster, ClusterOptions
from couchbase.exceptions import TimeoutException
from couchbase_core.cluster import PasswordAuthenticator
from grib2_to_cb.run_ingest_threads import VXIngest
class TestGribBuilderV01(unittest.TestCase):
#21 196 14 000018 %y %j %H %f  treating the last 6 decimals as microseconds even though they are not.
# these files are two digit year, day of year, hour, and forecast lead time (6 digit ??)
    def test_main(self):
        # noinspection PyBroadException
        try:
            cwd = os.getcwd()
            self.spec_file = cwd + '/grib2_to_cb/test/test_load_spec_grib_metar_hrrr_ops_V01.yaml'
            vxIngest = VXIngest()
            vxIngest.runit({'spec_file': self.spec_file,
                            'credentials_file': os.environ['HOME'] + '/adb-cb1-credentials',
                            'path': '/opt/public/data/grids/hrrr/conus/wrfprs/grib2',
                            'file_name_mask': '%y%j%H%f',
                            'output_dir': '/opt/data/grib_to_cb/output',
                            'threads': 1
                            })
        except:
            self.fail("TestGsdIngestManager Exception failure: " +
                      str(sys.exc_info()[0]))
