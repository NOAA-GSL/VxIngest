"""
Program Name: main script for VXingest
Contact(s): Randy Pierce
Abstract:

History Log:  Initial version

Usage: This script processes arguments which define a a yaml load_spec file,
a thread count, and certificate for TSL.
The script maintains a thread pool of Data_Managers, and a my_queue of tasks
that is derived from the load_spec.yaml input.
The number of threads in the thread pool is set to the -t n (or --threads n)
argument, where n is the number of threads
to start. The script is expected to define a list of metadata document ids
that can be retrieved via the
cb_connection that is specified in the load_spec.
Each metadata document defines an ingest process. The metadata documents are
assigned to a my_queue
and each my_queue entry will be handled by a VXDataTypeManager thread.

This is an example load_spec...

load_spec:
  email: "randy.pierce@noaa.gov"
  cb_connection:
    management_system: cb
    host: "127.0.0.1"
    user: gsd
    password: gsd_pwd
  mysql_connection:
    management_system: mysql
    host: "127.0.0.1"
    user: gsd
    password: gsd_pwd
  ingest_document_ids: [MD::V01::METAR::HRRR_OPS::ceiling::obs]

Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSD
"""
import argparse
import logging
import sys
import time

from datetime import datetime
from datetime import timedelta
from multiprocessing import JoinableQueue
from gsd_ingest_manager import GsdIngestManager
from load_spec import LoadSpecFile


class VXIngestGSD(object):
    def __init__(self, args):
        begin_time = str(datetime.now())
        logging.basicConfig(level=logging.INFO)
        logging.info("--- *** --- Start METdbLoad --- *** ---")
        logging.info("Begin time: %s", begin_time)
        # time execution
        parser = argparse.ArgumentParser()
        parser.add_argument("spec_file",
                            help="Please provide required load_spec filename "
                                 "- something.xml or something.yaml")
        parser.add_argument("-t", "--threads", type=int, default=1,
                            help="Number of threads to use")
        parser.add_argument("-c", "--cert_path", type=str, default='',
                            help="path to server public cert")
        # get the command line arguments
        args = parser.parse_args()
        self.load_time_start = time.perf_counter()
        self.spec_file = args['spec_file']
        self.gsd_spec = True
        self.thread_count = args['threads']
        self.cert_path = None if 'cert_path' not in args.keys() else args[
            'cert_path']
    
    def main(self):
        """
        This is the entry point for run_cb_threads.py
        """
        #
        #  Read the load_spec file
        #
        try:
            logging.debug("load_spec filename is %s", self.spec_file)
            
            # instantiate a load_spec file
            # read in the load_spec file and get the information out of its
            # tags
            load_spec_file = LoadSpecFile(
                {'spec_file': self.spec_file, 'spec_type': 'gsd'})
            # read in the load_spec file and get the information out of its
            # tags
            load_spec = load_spec_file.read()
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error(
                "*** %s occurred in Main reading load_spec " +
                self.spec_file + " ***",
                sys.exc_info()[0])
            sys.exit("*** Error reading XML")
        
        # process the spec file??
        # get all the ingest_document_ids and put them into a my_queue
        # load the my_queue with
        # Constructor for an infinite size  FIFO my_queue
        q = JoinableQueue()
        for f in load_spec.ingest_document_ids:
            q.put(f)
        # instantiate data_type_manager pool - each data_type_manager is a
        # thread that uses builders to process a file
        # Make the Pool of data_type_managers
        _dtm_list = []
        for _threadCount in range(self.thread_count):
            try:
                dtm_thread = GsdIngestManager(
                    "GsdIngestManager-" + str(self.thread_count),
                    load_spec.connection, q)
                _dtm_list.append(dtm_thread)
                dtm_thread.start()
            except:
                e = sys.exc_info()
                logging.error("*** %s occurred in purge_files ***", e[0])
        # be sure to join all the threads to wait on them
        [proc.join() for proc in _dtm_list]
        self.clean_up(self.load_time_start)
        
        def clean_up(start):
            logging.info("finished starting threads")
            load_time_end = time.perf_counter()
            load_time = timedelta(seconds=load_time_end - start)
            logging.info("    >>> Total load time: %s", str(load_time))
            logging.info("End time: %s", str(datetime.now()))
            logging.info("--- *** --- End METdbLoad --- *** ---")
    
    def clean_up(self, load_time_start):
        pass


if __name__ == '__main__':
    VXIngestGSD(sys.argv[1:]).main()
