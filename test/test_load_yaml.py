import unittest
import yaml
import os

from load_spec import LoadSpecFile


class LoadYaml(unittest.TestCase):
    # tests loading gsd yaml file
    def test_load_GSD_yaml(self):
        cwd = os.getcwd()
        spec_file = cwd + '/load_spec_gsd.yaml'
        load_spec_file = LoadSpecFile({'spec_file': spec_file, 'gsd_spec': True})
        load_spec = load_spec_file.read()
        self.assertEqual(load_spec['email'], "randy.pierce@noaa.gov")
        self.assertEqual(load_spec['ingest_document_ids'][0],
                         'MD::V01::METAR::HRRR_OPS::ceiling::obs')
        self.assertEqual(load_spec['cb_connection']['management_system'], "cb")
        self.assertEqual(load_spec['cb_connection']['host'], "127.0.0.1")
        self.assertEqual(load_spec['cb_connection']['user'], "gsd")
        self.assertEqual(load_spec['cb_connection']['password'], "gsd_pwd")

        self.assertEqual(load_spec['mysql_connection']['management_system'], "mysql")
        self.assertEqual(load_spec['mysql_connection']['host'], "127.0.0.1")
        self.assertEqual(load_spec['mysql_connection']['user'], "gsd")
        self.assertEqual(load_spec['mysql_connection']['password'], "gsd_pwd")

    # tests loading met yaml file
    def test_load_yaml(self):
        cwd = os.getcwd()
        spec_file = cwd + '/load_spec.yaml'
        load_spec_file = LoadSpecFile({'spec_file': spec_file})
        load_spec = load_spec_file.read()
        self.assertEqual(load_spec["email"], "randy.pierce@noaa.gov")
        self.assertEqual(load_spec["initialize_db"], True)
        self.assertEqual(load_spec["organization"], "vxt"),
        self.assertEqual(load_spec["group"], "large_tables"),
        self.assertEqual(load_spec["connection"]['management_system'], "cb")
        self.assertEqual(load_spec["connection"]['host'], "127.0.0.1")
        self.assertEqual(load_spec["connection"]['database'], "mv_gfs_grid2obs_vsdb1")
        self.assertEqual(load_spec["connection"]['user'], "met_admin")
        self.assertEqual(load_spec["connection"]['password'], "met_adm_pwd")
        self.assertEqual(load_spec["flags"]['verbose'], False)
        self.assertEqual(load_spec["insert_size"], 1)
        self.assertEqual(load_spec["flags"]['stat_header_db_check'], True)
        self.assertEqual(load_spec["flags"]['mode_header_db_check'], False)
        self.assertEqual(load_spec["flags"]['drop_indexes'], False)
        self.assertEqual(load_spec["flags"]['apply_indexes'], True)
        self.assertEqual(load_spec["flags"]['load_stat'], True)
        self.assertEqual(load_spec["flags"]['load_mode'], False)
        self.assertEqual(load_spec["flags"]['load_mpr'], False)
        self.assertEqual(load_spec["flags"]['load_orank'], False)
        self.assertEqual(load_spec["flags"]['force_dup_file'], True)
        self.assertEqual(load_spec["load_files"][0],
                         "./data_files/vsdb_data/anom/00Z/gfs/gfs_20190101.vsdb")
        self.assertEqual(load_spec["load_files"][29],
                         "./data_files/vsdb_data/grid2obs/12Z/ecm/ecm_air_20190101.vsdb")

    # tests to demonstrate parsing a met yaml load_spec file. Doesn't return any derived filenames
    def test_yaml_parse(self):
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
                         "./data_files/vsdb_data/{stattype}/{cycle}/{model}")
        self.assertEqual(yaml_data['load_spec']['load_val']['stattype'], ["anom", "pres", "sfc", "grid2obs"])
        self.assertEqual(yaml_data['load_spec']['load_val']['cycle'], ["00Z", "06Z", "12Z", "18Z"])
        self.assertEqual(yaml_data['load_spec']['load_val']['model'], ["gfs", "ecm"])
        self.assertEqual(yaml_data['load_spec']['load_xml'], True)
        self.assertEqual(yaml_data['load_spec']['load_note'], "mv_gfs_grid2obs_vsdb")

    # tests to demonstrate parsing a gsd yaml load_spec file. Doesn't return any derived filenames
    def test_yaml_gsd_parse(self):
        cwd = os.getcwd()
        with open(cwd + '/load_spec_gsd.yaml') as f:
            yaml_data = yaml.safe_load(f)
        self.assertEqual(yaml_data['load_spec']['email'], "randy.pierce@noaa.gov")
        self.assertEqual(yaml_data['load_spec']['ingest_document_ids'][0],
                         "MD::V01::METAR::HRRR_OPS::ceiling::obs")
        self.assertEqual(yaml_data['load_spec']['cb_connection']['management_system'], "cb")
        self.assertEqual(yaml_data['load_spec']['cb_connection']['host'], "127.0.0.1")
        self.assertEqual(yaml_data['load_spec']['cb_connection']['user'], "gsd")
        self.assertEqual(yaml_data['load_spec']['cb_connection']['password'], "gsd_pwd")

        self.assertEqual(yaml_data['load_spec']['mysql_connection']['management_system'], "mysql")
        self.assertEqual(yaml_data['load_spec']['mysql_connection']['host'], "127.0.0.1")
        self.assertEqual(yaml_data['load_spec']['mysql_connection']['user'], "gsd")
        self.assertEqual(yaml_data['load_spec']['mysql_connection']['password'], "gsd_pwd")


if __name__ == '__main__':
    unittest.main()
