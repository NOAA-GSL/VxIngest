import sys
import os
from unittest import TestCase
from classic_sql_to_cb.run_gsd_ingest_threads import VXIngestGSD


class TestGsdIngestManager(TestCase):
    def test_main(self):
        # noinspection PyBroadException
        try:
            self.thread_count = 1
            self.cert_path = None
            vx_ingest = VXIngestGSD()
            vx_ingest.runit({'job_id': "MD:V01:METAR:stations:ingest",
                             'credentials_file':
                                 os.environ['HOME'] + '/adb-cb1-credentials',
                             'threads': self.thread_count,
                             'cert_path': self.cert_path})
        except:
            self.fail("TestGsdIngestManager Exception failure: " +
                      str(sys.exc_info()[0]))
