"""
Program Name: main script for VXingest
Contact(s): Randy Pierce
Abstract:

History Log:  Initial version

Usage: This script processes arguments which define a a yaml load_spec file,
a thread count, and certificate for TSL.
The script maintains a thread pool of GsdIngestManagers and a queue of
load_metadata_document ids that is loaded from the load_spec.yaml
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
    xhost: "adb-cb1.gsd.esrl.noaa.gov"
    host: "localhost"
    user: gsd
    password: gsd_pwd
  mysql_connection:
    management_system: mysql
    host: "wolphin.fsl.noaa.gov"
    user: readonly
    password: ReadOnly@2016!

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
from load_spec_yaml import LoadYamlSpecFile


class VXIngestGSD(object):
    def __init__(self):
        self.load_time_start = time.perf_counter()
        self.spec_file = ""
        self.thread_count = ""
        self.cert_path = None
    
    def parse_args(self, args):
        begin_time = str(datetime.now())
        logging.basicConfig(level=logging.INFO)
        logging.info("--- *** --- Start METdbLoad --- *** ---")
        logging.info("Begin time: %s" + begin_time)
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
        args = parser.parse_args(args)
        return args
    
    def runit(self, args):
        """
        This is the entry point for run_cb_threads.py
        """
        self.spec_file = args['spec_file']
        self.thread_count = args['threads']
        self.cert_path = None if 'cert_path' not in args.keys() else args[
            'cert_path']
        
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
                    q)
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
        logging.info("    >>> Total load time: %s" + str(load_time))
        logging.info("End time: %s" + str(datetime.now()))
        logging.info("--- *** --- End METdbLoad --- *** ---")
    
    def main(self):
        args = self.parse_args(sys.argv)
        self.runit(args)


if __name__ == '__main__':
    VXIngestGSD().main()
