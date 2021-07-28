import sys
import os
from unittest import TestCase
from netcdf_to_cb.run_ingest_threads import VXIngest

class TestNetcdfObsBuilderV01(TestCase):

    def test_main(self):
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
