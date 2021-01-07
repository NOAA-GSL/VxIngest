import sys
from unittest import TestCase
from couchbase.bucket import Bucket


class TestConnection(TestCase):
    
    def test_connection(self):
        try:
            # conn_str = 'couchbases://127.0.0.1/{
            # }?certpath=/Users/randy.pierce/servercertfiles/ca.pem'
            # credentials = dict(username='met_admin', password='met_adm_pwd')
            # cb = Bucket(conn_str.format('mdata'), **credentials)
            # print("success")
            
            conn_str = 'couchbases://adb-cb4.gsd.esrl.noaa.gov/{' \
                      '}?certpath=/certs/adb-cb4.gsd.esrl.noaa.gov/cert.pem '
            credentials = dict(username='met_admin', password='met_adm_pwd')
            cb = Bucket(conn_str.format('mdata'), **credentials)
            print("success")
        
        except:
            print("*** %s Error in data_type_manager run ***",
                  sys.exc_info()[0])
            print("*** %s Error in data_type_manager run ***",
                  sys.exc_info()[1])
