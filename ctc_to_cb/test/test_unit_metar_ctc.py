# pylint: disable=missing-module-docstring
import os
from multiprocessing import JoinableQueue
from unittest import TestCase
from ctc_to_cb.run_ingest_threads import VXIngest
from ctc_to_cb.vx_ingest_manager import VxIngestManager
from builder_common.load_spec_yaml import LoadYamlSpecFile


class TestCTCBuilderV01Unit(TestCase):  # pylint: disable=missing-class-docstring

    vx_ingest_manager = None

    def setup_ingest(self):
        """test setup"""
        try:
            cwd = os.getcwd()
            _vx_ingest = VXIngest()
            _vx_ingest.spec_file = (
                cwd + "/ctc_to_cb/test/test_load_spec_metar_ctc_V01.yaml"
            )
            _vx_ingest.credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
            _load_spec_file = LoadYamlSpecFile({"spec_file": _vx_ingest.spec_file})
            # read in the load_spec file
            _vx_ingest.load_spec = dict(_load_spec_file.read())
            _vx_ingest.cb_credentials = _vx_ingest.get_credentials(_vx_ingest.load_spec)
            _vx_ingest.connect_cb()
            TestCTCBuilderV01Unit.vx_ingest_manager = VxIngestManager(
                "test", _vx_ingest.load_spec, JoinableQueue(), "/tmp"
            )
            return _vx_ingest
        except Exception as _e:  # pylint:disable=broad-except
            self.fail("test_credentials_and_load_spec Exception failure: " + str(_e))
            return None

    def test_cb_connect_disconnect(self):
        """test the cb connect and close"""
        try:
            self.setup_ingest()
            TestCTCBuilderV01Unit.vx_ingest_manager.set_connection()
            result = TestCTCBuilderV01Unit.vx_ingest_manager.cluster.query(
                "SELECT raw CLOCK_LOCAL() as time"
            )
            local_time = [list(result)[0]]
            self.assertTrue(local_time is not None)
            TestCTCBuilderV01Unit.vx_ingest_manager.close_cb()
        except Exception as _e:  # pylint:disable=broad-except
            self.fail("test_cb_connect_disconnect Exception failure: " + str(_e))
        finally:
            TestCTCBuilderV01Unit.vx_ingest_manager.close_cb()

    def test_credentials_and_load_spec(self):
        """test the get_credentials and load_spec"""
        try:
            vx_ingest = self.setup_ingest()
            self.assertTrue(vx_ingest.load_spec["cb_connection"]["user"], "cb_user")
        except Exception as _e:  # pylint:disable=broad-except
            self.fail("test_credentials_and_load_spec Exception failure: " + str(_e))

    def test_write_load_job_to_files(self):
        """test write the load job"""
        try:
            vx_ingest = self.setup_ingest()
            vx_ingest.load_job_id = "test_id"
            vx_ingest.output_dir = "/tmp"
            vx_ingest.load_spec["load_job_doc"] = {"test": "a line of text"}
            vx_ingest.write_load_job_to_files()
            os.remove("/tmp/test_id.json")
        except Exception as _e:  # pylint:disable=broad-except
            self.fail("test_write_load_job_to_files Exception failure: " + str(_e))

    def test_build_load_job_doc(self):
        """test the build load job"""
        try:
            vx_ingest = self.setup_ingest()
            vx_ingest.load_job_id = "test_id"
            vx_ingest.path = "/tmp"
            vx_ingest.load_spec["load_job_doc"] = {"test": "a line of text"}
            vx_ingest.spec_file = "/tmp/test_file"
            ljd = vx_ingest.build_load_job_doc("ctc")
            self.assertTrue(
                ljd["id"].startswith("LJ:METAR:ctc_to_cb.run_ingest_threads:VXIngest")
            )
        except Exception as _e:  # pylint:disable=broad-except
            self.fail("test_build_load_job_doc Exception failure: " + str(_e))
