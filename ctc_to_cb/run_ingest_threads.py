"""
Program Name: main script for VXingest
Contact(s): Randy Pierce
Abstract:

History Log:  Initial version

Usage:
run_ingest_threads -s spec_file -c credentials_file [-o output_dir -t thread_count -f first_epoch -l last_epoch -n number_stations]
This script processes arguments which define a a yaml load_spec file,
a defaults file (for credentials),
and a thread count.
The script maintains a thread pool of VxIngestManagers and a queue of
ingest documents that are retrieved from the load_spec file.
The number of threads in the thread pool is set to the -t n (or --threads n)
argument, where n is the number of threads to start. The default is one thread.
The optional -n number_stations will restrict the processing to n number of stations to limit run time.
This is analgous to specifying a small custom domain. The default is all the stations
in the region specified in the ingest document.
Each thread will run a VxIngestManager which will pull ingest documents, one at a time,
from the queue and fully process that document.
When the queue is empty each NetcdfIngestManager will gracefully die.

This is an example load_spec...

load_spec:
  email: "randy.pierce@noaa.gov"
  ingest_document_ids: ['MD:V01:METAR:HRRR_OPS:ALL_HRRR:CTC:CEILING:ingest'....]
  cb_connection:
    management_system: cb
    host: "cb_host"   - should come from defaults file
    user: "cb_user"   - should come from defaults file
    password: "cb_pwd" - should come from defaults file

The optional output_dir specifies the directory where output files will be written instead
of writing them directly to couchbase. If the output_dir is not specified data will be written
to couchbase cluster specified in the cb_connection.
For each ingest document the template will be rendered for each fcstValidEpoch between the
specified first_epoch and the last_epoch. If the first_epoch is unspecified then the latest
fcstValidEpoch currently in the db will be chosen as the first_epoch.

This is an example credentials file. The keys should match
the keys in the connection clauses of the load_spec.
defaults:
  cb_host: my_cb_host.some_subdomain.some_domain
  cb_user: some_cb_user_name
  cb_password: password_for_some_cb_user_name

This is an example invocation in bash. t=The python must be python3.
export PYTHONPATH=${HOME}/VXingest
python grib2_to_cb/run_ingest_threads.py -s /data/grib2_to_cb/load_specs/load_spec_metar_ctc_V01.yaml -c ~/adb-cb1-credentials -o /data/grib2_to_cb/output

Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSL
"""
import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from multiprocessing import JoinableQueue
from pathlib import Path

import yaml

from ctc_to_cb.load_spec_yaml import LoadYamlSpecFile
from ctc_to_cb.vx_ingest_manager import VxIngestManager


def parse_args(args):
    """
    Parse command line arguments
    """
    begin_time = str(datetime.now())
    logging.getLogger().setLevel(logging.INFO)
    logging.info("--- *** --- Start --- *** ---")
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
    parser.add_argument("-o", "--output_dir", type=str, default="/tmp",
                        help="Specify the output directory to put the json output files")
    parser.add_argument("-f", "--first_epoch", type=int, default=0,
                        help="The first epoch to use, inclusive")
    parser.add_argument("-l", "--last_epoch", type=int, default=sys.maxsize,
                        help="The last epoch to use, exclusive")
    # get the command line arguments
    args = parser.parse_args(args)
    return args


class VXIngest(object):
    def __init__(self):
        self.load_time_start = time.perf_counter()
        self.spec_file = ""
        self.credentials_file = ""
        self.thread_count = ""
        # -f first_epoch and -l last_epoch are optional time params.
        # If these are present only the files in the path with filename masks
        # that fall between these epochs will be processed.
        self.first_last_params = None
        self.output_dir = None

    def runit(self, args):
        """
        This is the entry point for run_ingest_threads.py
        """
        self.spec_file = args['spec_file'].strip()
        self.credentials_file = args['credentials_file'].strip()
        self.thread_count = args['threads']
        self.output_dir = args['output_dir'].strip()
        _args_keys = args.keys()
        if 'first_epoch' in _args_keys and 'last_epoch' in _args_keys:
            self.first_last_params = {'first_epoch': args['first_epoch'],
                                      'last_epoch': args['last_epoch']}
        else:
            self.first_last_params = {}
            self.first_last_params['first_epoch'] = 0
            self.first_last_params['last_epoch'] = sys.maxsize
        #
        #  Read the load_spec file
        #
        try:
            logging.debug("load_spec filename is %s", self.spec_file)
            load_spec_file = LoadYamlSpecFile(
                {'spec_file': self.spec_file})
            # read in the load_spec file
            load_spec = dict(load_spec_file.read())
            # put the real credentials into the load_spec
            load_spec['cb_connection'] = self.get_credentials(load_spec)['cb_connection']
            # stash the first_last_params because the builder will need to detrmine
            # if it needs to check for the latest validEpoch from the database (first_epoch == 0)
            load_spec['first_last_params'] = self.first_last_params
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error(
                "*** %s occurred in Main reading load_spec " +
                self.spec_file + " ***",
                sys.exc_info())
            sys.exit("*** Error reading load_spec: " + self.spec_file)


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
            # noinspection PyBroadException
            try:
                dtm_thread = VxIngestManager(
                    "VXIngestManager-" + str(_threadCount), load_spec, q, self.output_dir)
                _dtm_list.append(dtm_thread)
                dtm_thread.start()
            except:
                logging.error(
                    "*** Error in  VXIngestGSL ***" + str(sys.exc_info()))
        # be sure to join all the threads to wait on them
        [proc.join() for proc in _dtm_list]
        logging.info("finished starting threads")
        load_time_end = time.perf_counter()
        load_time = timedelta(seconds=load_time_end - self.load_time_start)
        logging.info("    >>> Total load a_time: %s" + str(load_time))
        logging.info("End a_time: %s" + str(datetime.now()))
        logging.info("--- *** --- End  --- *** ---")

    def get_credentials(self, load_spec):
        #
        #  Read the credentials
        #
        logging.debug("credentials filename is %s" + self.credentials_file)
        try:
            # check for existence of file
            if not Path(self.credentials_file).is_file():
                sys.exit("*** credentials_file file " +
                         self.credentials_file + " can not be found!")
            f = open(self.credentials_file)
            yaml_data = yaml.load(f, yaml.SafeLoader)
            load_spec['cb_connection']['host'] = yaml_data['cb_host']
            load_spec['cb_connection']['user'] = yaml_data['cb_user']
            load_spec['cb_connection']['password'] = yaml_data['cb_password']
            f.close()
            return load_spec
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read ***", sys.exc_info()[0])
            sys.exit("*** Parsing error(s) in load_spec file!")

    def main(self):
        logging.info("PYTHONPATH: " + os.environ['PYTHONPATH'])
        args = parse_args(sys.argv[1:])
        self.runit(vars(args))
        sys.exit("*** FINISHED ***")


if __name__ == '__main__':
    VXIngest().main()
