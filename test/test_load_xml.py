import os
from unittest import TestCase

from load_spec_xml import LoadXmlSpecFile


class TestLoadXmlSpecFile(TestCase):
    def test_load_xml(self):
        cwd = os.getcwd()
        spec_file = cwd + '/load_spec.xml'
        load_spec_file = LoadXmlSpecFile({'spec_file': spec_file})
        load_spec = load_spec_file.read()
        self.assertEqual(load_spec['email'], "randy.pierce@noaa.gov")
        self.assertEqual(load_spec['initialize_db'], 'true')
        self.assertEqual(load_spec['organization'], "vxt"),
        self.assertEqual(load_spec['group'], "large_tables"),
        self.assertEqual(load_spec['connection']['management_system'], "cb")
        self.assertEqual(load_spec['connection']['host'], "127.0.0.1")
        self.assertEqual(load_spec['connection']['user'], "met_admin")
        self.assertEqual(load_spec['connection']['password'], "met_adm_pwd")
        self.assertEqual(load_spec['connection']['database'],
                         "mv_gfs_grid2obs_vsdb1")
        self.assertEqual(load_spec['flags']['verbose'], False)
        self.assertEqual(load_spec['insert_size'], 1)
        self.assertEqual(load_spec['flags']['stat_header_db_check'], True)
        self.assertEqual(load_spec['flags']['mode_header_db_check'], False)
        self.assertEqual(load_spec['flags']['drop_indexes'], False)
        self.assertEqual(load_spec['flags']['apply_indexes'], True)
        self.assertEqual(load_spec['flags']['load_stat'], True)
        self.assertEqual(load_spec['flags']['load_mode'], False)
        self.assertEqual(load_spec['flags']['load_mpr'], False)
        self.assertEqual(load_spec['flags']['load_orank'], False)
        self.assertEqual(load_spec['flags']['force_dup_file'], True)
        self.assertEqual(load_spec['load_files'][0],
                         "./data_files/vsdb_data/anom/00Z/gfs/gfs_20190101"
                         ".vsdb")
        self.assertEqual(load_spec['load_files'][29],
                         "./data_files/vsdb_data/grid2obs/12Z/ecm"
                         "/ecm_air_20190101.vsdb")
