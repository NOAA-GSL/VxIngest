from unittest import TestCase

from run_gsd_ingest_threads import VXIngestGSD


class TestVXIngestGSD(TestCase):
    def test_main(self):
        vx_ingest = VXIngestGSD()
        vx_ingest.main()
        self.fail()

