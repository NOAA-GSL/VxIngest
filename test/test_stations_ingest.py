import sys
import os
from unittest import TestCase
from stations.stations_ingest import VXStationsIngest


class TestVXStationsIngest(TestCase):
    def test_main(self):
        # noinspection PyBroadException
        try:
            cwd = os.getcwd()
            self.spec_file = cwd + '/load_spec_stations.yaml'
            self.cert_path = None
            vx_ingest = VXStationsIngest()
            vx_ingest.runit({'spec_file': self.spec_file,
                             'credentials_file':
                                 '/Users/randy.pierce/adb-credentials-local',
                             'cert_path': self.cert_path})
        except:
            self.fail("TestGsdIngestManager Exception failure: " +
                      str(sys.exc_info()[0]))
