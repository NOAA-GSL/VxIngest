import sys
import os
import yaml
from multiprocessing import JoinableQueue
from unittest import TestCase
from netcdf_to_cb.vx_ingest_manager import VxIngestManager
from netcdf_to_cb.load_spec_yaml import LoadYamlSpecFile

class TestNetcdfObsBuilderV01(TestCase):
    
    def test_main(self):
        # noinspection PyBroadException
        try:
            cwd = os.getcwd()
            self.spec_file = cwd + '/netcdf_to_cb/test/test_load_spec_netcdf_metar_obs_V01.yaml'
            self.thread_count = 1
            load_spec_file = LoadYamlSpecFile(
                {'spec_file': self.spec_file})
            # read in the load_spec file
            load_spec = dict(load_spec_file.read())
            # put the real credentials into the load_spec
            _f = open(os.environ['HOME'] + "/adb-cb1-credentials")
            _yaml_data = yaml.load(_f, yaml.SafeLoader)
            load_spec['cb_connection']['host'] = _yaml_data['cb_host']
            load_spec['cb_connection']['user'] = _yaml_data['cb_user']
            load_spec['cb_connection']['password'] = _yaml_data['cb_password']
            _f.close()
            q = JoinableQueue()
            _output_dir = "/tmp"
            q.put(cwd + '/netcdf_to_cb/test/20210624_1200')
            vxIngestManager = VxIngestManager("test-ingest-manager", load_spec, q, _output_dir)
            vxIngestManager.start()
        except:
            self.fail("TestGsdIngestManager Exception failure: " +
                      str(sys.exc_info()[0]))
