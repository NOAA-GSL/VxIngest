"""
Program Name: main script for VXingest
Contact(s): Randy Pierce
Abstract:

History Log:  Initial version

Usage:
run_gsd_ingest_threads spec_file -c credentials_file [-t thread_count -p
crt_path]
This script processes arguments which define a a yaml load_spec file,
a defaults file (for credentials),
a thread count, and certificate for TSL.
The script maintains a thread pool of GsdIngestManagers and a queue of
load_metadata_document ids that is loaded from the load_spec_stations.yaml
ingest_document_ids field.
The number of threads in the thread pool is set to the -t n (or --threads n)
argument, where n is the number of threads to start. The script is expected
to define a list of ingest_document ids that identify ingest documents
can be retrieved in the IngestManager, which reads ids from the queue, via the
cb_connection that is specified in the load_spec.

This is an example load_spec...

load_spec:
  email: "randy.pierce@noaa.gov"
  ingest_document_ids: ['MD::V01::METAR::obs']
  cb_connection:
    management_system: cb
    host: "cb_host"   - should come from defaults file
    user: "cb_user"   - should come from defaults file
    password: "cb_pwd" - should come from defaults file
  mysql_connection:
    management_system: mysql   - should come from defaults file
    host: "mysql_host"   - should come from defaults file
    user: "mysql_host"   - should come from defaults file
    password: "mysql_pwd"   - should come from defaults file

This is an example defaults file. The keys should match
the keys in the connection clauses of the load_spec.
defaults:
  cb_host: my_cb_host.some_subdomain.some_domain
  cb_user: some_cb_user_name
  cb_password: password_for_some_cb_user_name
  mysql_host: my_mysql_host.some_subdomain.some_domain
  mysql_user: some_mysql_user_name
  mysql_password: password_for_some_mysql_user_name

Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSD
"""
import argparse
import logging
import sys
import time
import yaml
from pathlib import Path
from datetime import datetime
from datetime import timedelta
from multiprocessing import JoinableQueue
from gsd_sql_to_cb.gsd_ingest_manager import GsdIngestManager
from gsd_sql_to_cb.load_spec_yaml import LoadYamlSpecFile


def parse_args(args):
    begin_time = str(datetime.now())
    logging.basicConfig(level=logging.INFO)
    logging.info("--- *** --- Start METdbLoad --- *** ---")
    logging.info("Begin a_time: %s" + begin_time)
    # a_time execution
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--spec_file", type=str,
                        help="Please provide required load_spec filename "
                             "-s something.xml or -s something.yaml")
    parser.add_argument("-c", "--credentials_file", type=str,
                        help="Please provide required credentials_file")
    parser.add_argument("-t", "--threads", type=int, default=1,
                        help="Number of threads to use")
    parser.add_argument("-f", "--first_epoch", type=int, default=0,
                        help="The first epoch to use, inclusive")
    parser.add_argument("-l", "--last_epoch", type=int, default=0,
                        help="The last epoch to use, exclusive")

    parser.add_argument("-p", "--cert_path", type=str, default='',
                        help="path to server public cert")
    # get the command line arguments
    args = parser.parse_args(args)
    return args


class VXIngestGSD(object):
    def __init__(self):
        self.load_time_start = time.perf_counter()
        self.spec_file = ""
        self.credentials_file = ""
        self.thread_count = ""
        self.cert_path = None
        self.statement_replacement_params = None

    def runit(self, args):
        """
        This is the entry point for run_cb_threads.py
        """
        self.spec_file = args['spec_file']
        self.credentials_file = args['credentials_file']
        self.thread_count = args['threads']
        self.cert_path = None if 'cert_path' not in args.keys() else args[
            'cert_path']
        # capture any statement replacement params
        self.statement_replacement_params = {key: val for key, val in args.items() if key.startswith('{')}

        #
        #  Read the credentials
        #
        logging.debug("credentials filename is %s" + self.credentials_file)
        try:
            # check for existence of file
            if not Path(self.credentials_file).is_file():
                sys.exit(
                    "*** credentials_file file " + self.credentials_file +
                    " can not be found!")
            _f = open(self.credentials_file)
            _yaml_data = yaml.load(_f, yaml.SafeLoader)
            _cb_host = _yaml_data['cb_host']
            _cb_user = _yaml_data['cb_user']
            _cb_password = _yaml_data['cb_password']
            _mysql_host = _yaml_data['mysql_host']
            _mysql_user = _yaml_data['mysql_user']
            _mysql_password = _yaml_data['mysql_password']

            _f.close()
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read ***", sys.exc_info()[0])
            sys.exit("*** Parsing error(s) in load_spec file!")

        #
        #  Read the load_spec file
        #
        try:
            logging.debug("load_spec filename is %s" + self.spec_file)
            
            # instantiate a load_spec file
            # read in the load_spec file and get the information out of its
            # tags
            load_spec_file = LoadYamlSpecFile(
                {'spec_file': self.spec_file})
            # read in the load_spec file and get the information out of its
            # tags
            load_spec = load_spec_file.read()
            # assign the credentials from the credentials file
            load_spec['cb_connection']['host'] = _cb_host
            load_spec['cb_connection']['user'] = _cb_user
            load_spec['cb_connection']['password'] = _cb_password
            load_spec['mysql_connection']['host'] = _mysql_host
            load_spec['mysql_connection']['user'] = _mysql_user
            load_spec['mysql_connection']['password'] = _mysql_password

        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error(
                "*** %s occurred in Main reading load_spec " +
                self.spec_file + " ***",
                sys.exc_info()[0])
            sys.exit("*** Error reading load_spec: " + self.spec_file)
        
        # process the spec file??
        # get all the ingest_document_ids and put them into a my_queue
        # load the my_queue with
        # Constructor for an infinite size  FIFO my_queue
        q = JoinableQueue()
        for f in load_spec['ingest_document_ids']:
            q.put(f)
        # instantiate data_type_manager pool - each data_type_manager is a
        # thread that uses builders to process a file
        # Make the Pool of data_type_managers
        _dtm_list = []
        for _threadCount in range(int(self.thread_count)):
            try:
                dtm_thread = GsdIngestManager(
                    "GsdIngestManager-" + str(self.thread_count),
                    load_spec['cb_connection'], load_spec['mysql_connection'],
                    q, self.statement_replacement_params)
                _dtm_list.append(dtm_thread)
                dtm_thread.start()
            except:
                logging.error(
                    "*** Error in  VXIngestGSD ***" + str(sys.exc_info()[0]))
        # be sure to join all the threads to wait on them
        [proc.join() for proc in _dtm_list]
        logging.info("finished starting threads")
        load_time_end = time.perf_counter()
        load_time = timedelta(seconds=load_time_end - self.load_time_start)
        logging.info("    >>> Total load a_time: %s" + str(load_time))
        logging.info("End a_time: %s" + str(datetime.now()))
        logging.info("--- *** --- End METdbLoad --- *** ---")
    
    def main(self):
        args = parse_args(sys.argv[1:])
        self.runit(vars(args))


if __name__ == '__main__':
    VXIngestGSD().main()
