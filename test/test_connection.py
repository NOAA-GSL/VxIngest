"""
    test the connection to couhbase, data and query service
"""
import sys
import os
from pathlib import Path
from unittest import TestCase
import yaml
from couchbase.cluster import Cluster
from couchbase.options import  ClusterOptions
from couchbase.auth import PasswordAuthenticator


class TestConnection(TestCase):
    """
        test connection, get, query to adb-cb1, adb-cb4
    """
    def get_credentials(self, _credentials_file):
        """
        get the arguments from the credentials file
        Args:
            _credentials_file: a credentials file
        """
        _f = open(_credentials_file)
        _yaml_data = yaml.load(_f, yaml.SafeLoader)
        cb_host = _yaml_data['cb_host']
        cb_user = _yaml_data['cb_user']
        cb_password = _yaml_data['cb_password']
        _f.close()
        return cb_host, cb_user, cb_password

    def test_adb_cb1_connection_no_cert(self):
        """
            test connection, get, query to adb-cb1
        """
        # noinspection PyBroadException
        try:
            _credentials_file = os.environ['HOME'] + "/adb-cb1-credentials"
            # specify the cluster and specify an authenticator containing a
            # username and password to be passed to the cluster.
            if not Path(_credentials_file).is_file():
                sys.exit("*** credentials_file file " + _credentials_file + " can not be found!")
            cb_host, cb_user, cb_password = self.get_credentials(_credentials_file)
            cluster = Cluster('couchbase://' + cb_host, ClusterOptions(PasswordAuthenticator(cb_user,cb_password)))
            # following a successful authentication, a bucket can be opened.
            # access a bucket in that cluster
            bucket = cluster.bucket('mdata')
            collection = bucket.default_collection()
            ingest_document_result = collection.get("MD:V01:METAR:stations:ingest")
            print("test_adb_cb1_connection_no_cert: successfully read ", ingest_document_result.content)
        except:   # pylint:disable=(bare-except)
            self.fail("test_adb_cb1_connection_no_cert: TestConnection.test_connection Exception failure: " +
                      str(sys.exc_info()))

    def test_adb_cb4_connection_no_cert(self):
        """
            test connection, get, query to adb-cb4
        """
        # noinspection PyBroadException
        try:
            _credentials_file = os.environ['HOME'] + "/adb-cb4-credentials"
            # specify the cluster and specify an authenticator containing a
            # username and password to be passed to the cluster.
            if not Path(_credentials_file).is_file():
                sys.exit("*** credentials_file file " + _credentials_file + " can not be found!")
            self.get_credentials(_credentials_file)
            cb_host, cb_user, cb_password = self.get_credentials(_credentials_file)
            cluster = Cluster('couchbase://' + cb_host, ClusterOptions(PasswordAuthenticator(cb_user,cb_password)))
            # following a successful authentication, a bucket can be opened.
            # access a bucket in that cluster
            bucket = cluster.bucket('mdata')
            collection = bucket.default_collection()
            ingest_document_result = collection.get("MD:V01:METAR:stations:ingest")
            print("test_adb_cb4_connection_no_cert: successfully read ", ingest_document_result.content)
        except:   # pylint:disable=(bare-except)
            self.fail("test_adb_cb4_connection_no_cert: TestConnection.test_connection Exception failure: " +
                      str(sys.exc_info()))
