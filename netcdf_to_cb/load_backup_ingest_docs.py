"""
Program Name: Class LoadBackupIngestDocs.py
Contact(s): Randy Pierce
Abstract:

History Log:  Initial version

Usage: The LoadBackupIngestDocs -f backup_file -s server
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSD
"""
import json
import sys
import argparse
import yaml
from pathlib import Path
from couchbase.cluster import Cluster, ClusterOptions
from couchbase_core.cluster import PasswordAuthenticator


def parse_args(args):
    # a_time execution
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--credentials_file", type=str, help="Please provide required credentials_file")
    parser.add_argument("-f", "--file_name", type=str,  help="The backup file to upload")
    # get the command line arguments
    args = parser.parse_args(args)
    return args


class LoadBackupIngestDocs:
    """
    LoadBackupIngestDocs reads a backup file and multi-upserts it into couchbase.
    This class receives connection credentials for couchbase.
    """
    
    def __init__(self):
        # The Constructor for the RunCB class.
        self.cb_credentials = {}
        self.cb_host = None
        self.cb_user = None
        self.cb_password = None
        self.collection = None
        self.cluster = None

    def run(self, args):
        # noinspection PyBroadException
        try:
            _credentials_file = args['credentials_file']
            # check for existence of file
            if not Path(_credentials_file).is_file():
                sys.exit(
                    "*** credentials_file file " + _credentials_file +
                    " can not be found!")
            _f = open(_credentials_file)
            _yaml_data = yaml.load(_f, yaml.SafeLoader)
            self.cb_credentials['host'] = _yaml_data['cb_host']
            self.cb_credentials['user'] = _yaml_data['cb_user']
            self.cb_credentials['password'] = _yaml_data['cb_password']
            _f.close()

            _f_name = args['file_name']
            # Opening JSON file
            f = open(_f_name)
            # returns JSON object as
            # a dictionary
            _list_data = json.load(f)
            _data = {}
            for elem in _list_data:
                _id = elem['id']
                del elem['id']
                _data[_id] = elem
            f.close()
            self.connect_cb()
            self.collection.upsert_multi(_data)
        except:
            print(": *** %s Error in multi-upsert *** " + str(sys.exc_info()))
        finally:
            # close any mysql connections
            self.close_cb()
        
    def close_cb(self):
        if self.cluster:
            self.cluster.disconnect()
    
    def connect_cb(self):
        # get a reference to our cluster
        # noinspection PyBroadException
        try:
            options = ClusterOptions(
                PasswordAuthenticator(self.cb_credentials['user'], self.cb_credentials['password']))
            self.cluster = Cluster('couchbase://' + self.cb_credentials['host'], options)
            self.collection = self.cluster.bucket("mdata").default_collection()
        except:
            print("*** %s in connect_cb ***" + str(sys.exc_info()))
            sys.exit("*** Error when connecting to mysql database: ")

    def main(self):
        args = parse_args(sys.argv[1:])
        self.run(vars(args))


if __name__ == '__main__':
    LoadBackupIngestDocs().main()
