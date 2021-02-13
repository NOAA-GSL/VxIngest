import sys
from pathlib import Path
from unittest import TestCase
import yaml
from couchbase.cluster import Cluster, ClusterOptions
from couchbase_core.cluster import PasswordAuthenticator


class TestConnection(TestCase):
    def get_credentials(self, _credentials_file):
        _f = open(_credentials_file)
        _yaml_data = yaml.load(_f, yaml.SafeLoader)
        self.cb_host = _yaml_data['cb_host']
        self.cb_user = _yaml_data['cb_user']
        self.cb_password = _yaml_data['cb_password']
        _f.close()
        return

    def test_adb_cb1_connection_no_cert(self):
        # noinspection PyBroadException
        try:
            _credentials_file = '/Users/randy.pierce/adb-cb1-credentials'
            # specify the cluster and specify an authenticator containing a
            # username and password to be passed to the cluster.
            if not Path(_credentials_file).is_file():
                sys.exit("*** credentials_file file " + _credentials_file + " can not be found!")
            self.get_credentials(_credentials_file)
            cluster = Cluster('couchbase://' + self.cb_host, ClusterOptions(PasswordAuthenticator(self.cb_user,
                                                                                                  self.cb_password)))
            # following a successful authentication, a bucket can be opened.
            # access a bucket in that cluster
            bucket = cluster.bucket('mdata')
            collection = bucket.default_collection()
            ingest_document_result = collection.get("MD::V01::METAR::stations_ingest")
            print("test_adb_cb1_connection_no_cert: successfully read ", ingest_document_result.content)
        except:
            self.fail("test_adb_cb1_connection_no_cert: TestConnection.test_connection Exception failure: " +
                      str(sys.exc_info()))
    
    def test_adb_cb4_connection_no_cert(self):
        # noinspection PyBroadException
        try:
            _credentials_file = '/Users/randy.pierce/adb-cb4-credentials'
            # specify the cluster and specify an authenticator containing a
            # username and password to be passed to the cluster.
            if not Path(_credentials_file).is_file():
                sys.exit("*** credentials_file file " + _credentials_file + " can not be found!")
            self.get_credentials(_credentials_file)
            cluster = Cluster('couchbase://' + self.cb_host, ClusterOptions(PasswordAuthenticator(self.cb_user,
                                                                                                  self.cb_password)))
            # following a successful authentication, a bucket can be opened.
            # access a bucket in that cluster
            bucket = cluster.bucket('mdata')
            collection = bucket.default_collection()
            ingest_document_result = collection.get("MD::V01::METAR::stations_ingest")
            print("test_adb_cb4_connection_no_cert: successfully read ", ingest_document_result.content)
        except:
            self.fail("test_adb_cb4_connection_no_cert: TestConnection.test_connection Exception failure: " +
                      str(sys.exc_info()))

