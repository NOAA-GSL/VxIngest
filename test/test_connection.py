import sys
from unittest import TestCase
from pathlib import Path
import yaml

# these modules are used to access and authenticate with your
# database cluster:
from couchbase.cluster import Cluster, ClusterOptions, Bucket
from couchbase_core.cluster import PasswordAuthenticator


class TestConnection(TestCase):
    def test_adb_cb1_connection_cert(self):
        # noinspection PyBroadException
        try:
            _credentials_file = '/Users/randy.pierce/adb-cb1-credentials'
            _cert_path = '/Users/randy.pierce/servercertfiles' \
                         '/clientcertfiles/adb-cb1.cert'
            # specify the cluster and specify an authenticator containing a
            # username, password, and client cert to be passed to the cluster.
            if not Path(_credentials_file).is_file():
                sys.exit(
                    "*** credentials_file file " + _credentials_file +
                    " can not be found!")
            _f = open(_credentials_file)
            _yaml_data = yaml.load(_f, yaml.SafeLoader)
            _cb_host = _yaml_data['cb_host']
            _cb_user = _yaml_data['cb_user']
            _cb_password = _yaml_data['cb_password']
            _mysql_host = _yaml_data['mysql_host']
            _mysql_user = _yaml_data['mysql_user']
            _mysql_password = _yaml_data['mysql_password']
            
            _f.close()
            
            cluster = Cluster('couchbases://' + _cb_host,  ClusterOptions(
                PasswordAuthenticator(_cb_user, _cb_password, "cert_path=" +
                                      _cert_path)))

            # following a successful authentication, a bucket can be opened.
            # access a bucket in that cluster

            bucket = cluster.bucket('mdata')
            collection = bucket.default_collection()
            print("success")
        except:
            self.fail(
                "TestConnection.test_connection Exception failure: " + str(
                    sys.exc_info()[0]))
    
    def test_adb_cb1_connection_no_cert(self):
        # noinspection PyBroadException
        try:
            _credentials_file = '/Users/randy.pierce/adb-cb1-credentials'
            # specify the cluster and specify an authenticator containing a
            # username and password to be passed to the cluster.
            if not Path(_credentials_file).is_file():
                sys.exit(
                    "*** credentials_file file " + _credentials_file +
                    " can not be found!")
            _f = open(_credentials_file)
            _yaml_data = yaml.load(_f, yaml.SafeLoader)
            _cb_host = _yaml_data['cb_host']
            _cb_user = _yaml_data['cb_user']
            _cb_password = _yaml_data['cb_password']
            _mysql_host = _yaml_data['mysql_host']
            _mysql_user = _yaml_data['mysql_user']
            _mysql_password = _yaml_data['mysql_password']
            
            _f.close()
            
            cluster = Cluster('couchbase://' + _cb_host, ClusterOptions(
                PasswordAuthenticator(_cb_user, _cb_password)))
            
            # following a successful authentication, a bucket can be opened.
            # access a bucket in that cluster
            
            bucket = cluster.bucket('mdata')
            collection = bucket.default_collection()
            print("success")
        except:
            self.fail(
                "TestConnection.test_connection Exception failure: " + str(
                    sys.exc_info()[0]))
    
    def test_adb_cb4_connection_no_cert(self):
        # noinspection PyBroadException
        try:
            _credentials_file = '/Users/randy.pierce/adb-cb4-credentials'
            # specify the cluster and specify an authenticator containing a
            # username and password to be passed to the cluster.
            if not Path(_credentials_file).is_file():
                sys.exit(
                    "*** credentials_file file " + _credentials_file + " can "
                                                                       "not "
                                                                       "be "
                                                                       "found!")
            _f = open(_credentials_file)
            _yaml_data = yaml.load(_f, yaml.SafeLoader)
            _cb_host = _yaml_data['cb_host']
            _cb_user = _yaml_data['cb_user']
            _cb_password = _yaml_data['cb_password']
            _mysql_host = _yaml_data['mysql_host']
            _mysql_user = _yaml_data['mysql_user']
            _mysql_password = _yaml_data['mysql_password']
            
            _f.close()
            
            cluster = Cluster('couchbase://' + _cb_host, ClusterOptions(
                PasswordAuthenticator(_cb_user, _cb_password)))
            
            # following a successful authentication, a bucket can be opened.
            # access a bucket in that cluster
            
            bucket = cluster.bucket('mdata')
            collection = bucket.default_collection()
            print("success")
        except:
            self.fail(
                "TestConnection.test_connection Exception failure: " + str(
                    sys.exc_info()[0]))
