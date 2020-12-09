import sys
from couchbase.bucket import Bucket
try:
    # connstr = 'couchbases://127.0.0.1/{}?certpath=/Users/randy.pierce/servercertfiles/ca.pem'
    # credentials = dict(username='met_admin', password='met_adm_pwd')
    # cb = Bucket(connstr.format('mdata'), **credentials)
    # print("success")

    connstr = 'couchbases://adb-cb4.gsd.esrl.noaa.gov/{}?certpath=/certs/adb-cb4.gsd.esrl.noaa.gov/cert.pem '
    credentials = dict(username='met_admin', password='met_adm_pwd')
    cb = Bucket(connstr.format('mdata'), **credentials)
    print("success")

except:
    print("*** %s Error in data_type_manager run ***", sys.exc_info()[0])
    print("*** %s Error in data_type_manager run ***", sys.exc_info()[1])
