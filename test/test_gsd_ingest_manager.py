import sys
import os
import yaml
from unittest import TestCase
from classic_sql_to_cb.run_gsd_ingest_threads import VXIngestGSD
from couchbase.cluster import Cluster, ClusterOptions
from couchbase_core.cluster import PasswordAuthenticator


class TestGsdIngestManager(TestCase):
    def connect_cb(self):
        # noinspection PyBroadException
        try:
            _f = open(os.environ['HOME'] + '/adb-credentials-local')
            _yaml_data = yaml.load(_f, yaml.SafeLoader)
            _cb_host = _yaml_data['cb_host']
            _cb_user = _yaml_data['cb_user']
            _cb_password = _yaml_data['cb_password']
            options = ClusterOptions(
                PasswordAuthenticator(_cb_user, _cb_password))
            self.cluster = Cluster('couchbase://' + _cb_host, options)
            self.collection = self.cluster.bucket("mdata").default_collection()
        except:
            self.fail("*** %s in connect_cb ***" + str(sys.exc_info()[0]))
            sys.exit("*** Error when connecting to mysql database: ")

    def test_main(self):
        # noinspection PyBroadException
        try:
            self.connect_cb()
            self.thread_count = 1
            self.cert_path = None
            vx_ingest = VXIngestGSD()
            vx_ingest.runit({'job_id': "JOB:V01:METAR:NETCDF:OBS",
                             'credentials_file':
                                 os.environ['HOME'] + '/adb-credentials-local',
                             'threads': self.thread_count,
                             'cert_path': self.cert_path})
            test_document = self.collection.get("MD:V01:METAR:test:SqlIngestManager").content
            self.assertEqual(test_document['description'], "a description", "test document name is wrong: " +
                             test_document['description'] + " is not 'a description'")
            self.assertEqual(test_document['name'], "SqlIngestManager", "test document name is wrong: " +
                             test_document['name'] + " is not SqlIngestManager")
            self.assertEqual(test_document['firstTime'], 1, "test document firstTime is not 1 and it should be")
            self.assertEqual(test_document['lastTime'], 10, "test document lastTime is not 10 and it should be")
            self.assertIsNotNone(test_document['updateTime'], "test document has None for updateTime")
            # tests pass = delete the document
        except:
            self.collection.remove("MD:V01:METAR:test:SqlIngestManager")
            if self.cluster:
                self.cluster.disconnect()
            self.fail("TestGsdIngestManager Exception failure: " + str(sys.exc_info()))

    def tearDown(self):
        self.collection.remove("MD:V01:METAR:test:SqlIngestManager")
        if self.cluster:
            self.cluster.disconnect()
