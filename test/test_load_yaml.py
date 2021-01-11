import unittest
import yaml
import os

from gsd_sql_to_cb.load_spec_yaml import LoadYamlSpecFile


class TestLoadYamlSpecFile(unittest.TestCase):
    # tests loading gsd yaml file
    def test_load_GSD_yaml(self):
        cwd = os.getcwd()
        spec_file = cwd + '/load_spec_gsd.yaml'
        load_spec_file = LoadYamlSpecFile(
            {'spec_file': spec_file, 'gsd_spec': True})
        load_spec = load_spec_file.read()
        self.assertEqual(load_spec['email'], "randy.pierce@noaa.gov")
        self.assertEqual(load_spec['ingest_document_ids'][0],
                         'MD::V01::METAR::obs')
        self.assertEqual(load_spec['cb_connection']['host'], "localhost")
        self.assertEqual(load_spec['cb_connection']['user'], "gsd")
        self.assertEqual(load_spec['cb_connection']['password'], "gsd_pwd")
        
        self.assertEqual(load_spec['mysql_connection']['host'],
                         "wolphin.fsl.noaa.gov")
        self.assertEqual(load_spec['mysql_connection']['user'], "readonly")
        self.assertEqual(load_spec['mysql_connection']['password'],
                         "ReadOnly@2016!")
    
    # tests to demonstrate parsing a gsd yaml load_spec file. Doesn't return
    # any derived filenames
    def test_yaml_gsd_parse(self):
        cwd = os.getcwd()
        with open(cwd + '/load_spec_gsd.yaml') as f:
            yaml_data = yaml.safe_load(f)
        self.assertEqual(yaml_data['load_spec']['email'],
                         "randy.pierce@noaa.gov")
        self.assertEqual(yaml_data['load_spec']['ingest_document_ids'][0],
                         "MD::V01::METAR::obs")
        self.assertEqual(yaml_data['load_spec']['cb_connection']['host'],
                         "localhost")
        self.assertEqual(yaml_data['load_spec']['cb_connection']['user'],
                         "gsd")
        self.assertEqual(yaml_data['load_spec']['cb_connection']['password'],
                         "gsd_pwd")
        
        self.assertEqual(
            yaml_data['load_spec']['mysql_connection']['management_system'],
            "mysql")
        self.assertEqual(yaml_data['load_spec']['mysql_connection']['host'],
                         "wolphin.fsl.noaa.gov")
        self.assertEqual(yaml_data['load_spec']['mysql_connection']['user'],
                         "readonly")
        self.assertEqual(
            yaml_data['load_spec']['mysql_connection']['password'],
            "ReadOnly@2016!")


if __name__ == '__main__':
    unittest.main()
