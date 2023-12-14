"""
Program Name: Class LoadBackupIngestDocs.py
Contact(s): Randy Pierce
Abstract:

History Log:  Initial version

Usage: The LoadBackupIngestDocs -f backup_file -s server
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSD
"""
import argparse
import json
import sys
from pathlib import Path

import yaml
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions


def parse_args(args):
    """parse command line arguments"""
    # a_time execution
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--credentials_file",
        type=str,
        help="Please provide required credentials_file",
    )
    parser.add_argument("-f", "--file_name", type=str, help="The backup file to upload")
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
        "thread start"
        # noinspection PyBroadException
        try:
            credentials_file = args["credentials_file"]
            # check for existence of file
            if not Path(credentials_file).is_file():
                sys.exit(
                    "*** credentials_file file "
                    + credentials_file
                    + " can not be found!"
                )
            _f = open(credentials_file, encoding="utf-8")
            yaml_data = yaml.load(_f, yaml.SafeLoader)
            self.cb_credentials["host"] = yaml_data["cb_host"]
            self.cb_credentials["user"] = yaml_data["cb_user"]
            self.cb_credentials["password"] = yaml_data["cb_password"]
            _f.close()

            f_name = args["file_name"]
            # Opening JSON file
            _f = open(f_name, encoding="utf-8")
            # returns JSON object as
            # a dictionary
            list_data = json.load(_f)
            data = {}
            for elem in list_data:
                _id = elem["id"]
                del elem["id"]
                data[_id] = elem
            _f.close()
            self.connect_cb()
            self.collection.upsert_multi(data)
        except:  # pylint: disable=bare-except, disable=broad-except
            print(": *** %s Error in multi-upsert *** " + str(sys.exc_info()))
        finally:
            # close any mysql connections
            self.close_cb()

    def close_cb(self):
        """close the cluster"""
        if self.cluster:
            self.cluster.close()

    def connect_cb(self):
        """Connect to database"""
        # get a reference to our cluster
        # noinspection PyBroadException
        try:
            options = ClusterOptions(
                PasswordAuthenticator(
                    self.cb_credentials["user"], self.cb_credentials["password"]
                )
            )
            self.cluster = Cluster(
                "couchbase://" + self.cb_credentials["host"], options
            )
            self.collection = self.cluster.bucket("mdata").default_collection()
        except:  # pylint: disable=bare-except, disable=broad-except
            print("*** %s in connect_cb ***" + str(sys.exc_info()))
            sys.exit("*** Error when connecting to mysql database: ")

    def main(self):
        """main"""
        args = parse_args(sys.argv[1:])
        self.run(vars(args))


if __name__ == "__main__":
    LoadBackupIngestDocs().main()
