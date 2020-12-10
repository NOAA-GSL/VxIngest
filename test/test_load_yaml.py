import unittest
import yaml
import os


class LoadYaml(unittest.TestCase):
    def test_load_yaml(self):
        cwd = os.getcwd()
        with open(cwd + '/load_spec.yaml') as f:
            yaml_data = yaml.safe_load(f)
        self.assertEqual(yaml_data['load_spec']['email'], "randy.pierce@noaa.gov")
        self.assertEqual(yaml_data['load_spec']['initialize_db'], True)
        self.assertEqual(yaml_data['load_spec']['email'], "randy.pierce@noaa.gov")
        self.assertEqual(yaml_data['load_spec']['initialize_db'], True)
        self.assertEqual(yaml_data['load_spec']['organization'], "vxt"),
        self.assertEqual(yaml_data['load_spec']['group'], "large_tables"),
        self.assertEqual(yaml_data['load_spec']['connection']['management_system'], "cb")
        self.assertEqual(yaml_data['load_spec']['connection']['host'], "127.0.0.1")
        self.assertEqual(yaml_data['load_spec']['connection']['database'], "mv_gfs_grid2obs_vsdb1")
        self.assertEqual(yaml_data['load_spec']['connection']['user'], "met_admin")
        self.assertEqual(yaml_data['load_spec']['connection']['password'], "met_adm_pwd")
        self.assertEqual(yaml_data['load_spec']['verbose'], False)
        self.assertEqual(yaml_data['load_spec']['insert_size'], 1)
        self.assertEqual(yaml_data['load_spec']['stat_header_db_check'], True)
        self.assertEqual(yaml_data['load_spec']['mode_header_db_check'], False)
        self.assertEqual(yaml_data['load_spec']['drop_indexes'], False)
        self.assertEqual(yaml_data['load_spec']['apply_indexes'], True)
        self.assertEqual(yaml_data['load_spec']['load_stat'], True)
        self.assertEqual(yaml_data['load_spec']['load_mode'], False)
        self.assertEqual(yaml_data['load_spec']['load_mpr'], False)
        self.assertEqual(yaml_data['load_spec']['load_orank'], False)
        self.assertEqual(yaml_data['load_spec']['force_dup_file'], True)
        self.assertEqual(yaml_data['load_spec']['folder_tmpl'],
                         "/Users/randy.pierce/WebstormProjects/CouchbasePerformanceTest/" +
                         "test/load/vsdb_data/{stattype}/{cycle}/{model}")
        self.assertEqual(yaml_data['load_spec']['load_val']['stattype'], ["anom", "pres", "sfc", "grid2obs"])
        self.assertEqual(yaml_data['load_spec']['load_val']['cycle'], ["00Z", "06Z", "12Z", "18Z"])
        self.assertEqual(yaml_data['load_spec']['load_val']['model'], ["gfs", "ecm"])
        self.assertEqual(yaml_data['load_spec']['load_xml'], True)
        self.assertEqual(yaml_data['load_spec']['load_note'], "mv_gfs_grid2obs_vsdb")


if __name__ == '__main__':
    unittest.main()
