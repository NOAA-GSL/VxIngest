import sys
import os
from unittest import TestCase
from netcdf_builder import NetcdfObsBuilderV01

class TestNetcdfObsBuilderV01(TestCase):
    
    def test_main(self):
        # noinspection PyBroadException
        try:
            cwd = os.getcwd()
            self.spec_file = cwd + '/test_load_spec_netcdf_metar_obs_V01.yaml'
            self.thread_count = 1
            self.cert_path = None
            netcdfObsBuilder = NetcdfObsBuilderV01()
            netcdfObsBuilder.runit({'spec_file': self.spec_file,
                             'credentials_file':
                                 os.environ['HOME'] + '/adb-cb1-credentials'
                             )
        except:
            self.fail("TestGsdIngestManager Exception failure: " +
                      str(sys.exc_info()[0]))
