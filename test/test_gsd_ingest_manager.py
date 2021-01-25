import sys
import os
from unittest import TestCase
from gsd_sql_to_cb.run_gsd_ingest_threads import VXIngestGSD


class TestGsdIngestManager(TestCase):
    
    def test_main(self):
        # noinspection PyBroadException
        try:
            cwd = os.getcwd()
            self.spec_file = cwd + '/load_spec_gsd-v02.yaml'
            self.gsd_spec = True
            self.thread_count = 1
            self.cert_path = None
            vx_ingest = VXIngestGSD()
            vx_ingest.runit({'spec_file': self.spec_file,
                             'threads': self.thread_count,
                             'cert_path': self.cert_path,
                             'gsd_spec': self.gsd_spec})
        except:
            self.fail("TestGsdIngestManager Exception failure: " + str(
                sys.exc_info()[0]))
