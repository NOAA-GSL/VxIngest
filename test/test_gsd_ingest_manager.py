import sys
import os
import time
from unittest import TestCase

from run_gsd_ingest_threads import VXIngestGSD


class TestGsdIngestManager(TestCase):
    def test_main(self):
        try:
            load_time_start = time.perf_counter()
            cwd = os.getcwd()
            spec_file = cwd + '/load_spec_gsd.yaml'
            gsd_spec = True
            thread_count = 1
            cert_path = None
            my_args = {'spec_file': spec_file, 'gsd_spec': gsd_spec,
                       'thread_count': thread_count, 'cert_path': cert_path}
            VXIngestGSD(my_args).main()
        except:
            self.fail("TestGsdIngestManager Exception failure: " + str(
                sys.exc_info()[0]))
