"""
Program Name: main script for VXingest
Contact(s): Randy Pierce
Abstract:

History Log:  Initial version

Usage:
run_netcdf_ingest_threads -s spec_file -c credentials_file [ -t thread_count -f first_epoch -l last_epoch]
This script processes arguments which define a a yaml load_spec file,
a defaults file (for credentials),
and a thread count.
The script maintains a thread pool of VxIngestManagers and a queue of
filenames that are derived from the path, mask, first_epoch, and last_epoch.
that are defined in the load_spec file.
The number of threads in the thread pool is set to the -t n (or --threads n)
argument, where n is the number of threads to start. The default is one thread. 
Each thread will run a VxIngestManager which will pull filenames, one at a time, 
from the filename queue and fully process that netcdf file. 
When the queue is empty each NetcdfIngestManager will gracefully die.

This is an example load_spec...

load_spec:
  email: "randy.pierce@noaa.gov"
  ingest_document_id: 'MD:V01:METAR:obs'
  cb_connection:
    management_system: cb
    host: "cb_host"   - should come from defaults file
    user: "cb_user"   - should come from defaults file
    password: "cb_pwd" - should come from defaults file
  path: /public/data/madis/point/metar/netcdf
  file_name_mask: "%Y%m%d_%H%M"  
  
(For the mask - python time.strftime format e.g. 20210619_1300)

Files in the path will be enqueued if the file name mask falls between the first_epoch
and the last_epoch. These values may be omitted in which case all the files in the path
will be processed.

This is an example defaults file. The keys should match
the keys in the connection clauses of the load_spec.
defaults:
  cb_host: my_cb_host.some_subdomain.some_domain
  cb_user: some_cb_user_name
  cb_password: password_for_some_cb_user_name

Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSL
"""
import argparse
import logging
import os
import sys
import time
import yaml
import json
from pathlib import Path
from datetime import datetime
from datetime import timedelta
from multiprocessing import JoinableQueue
from netcdf_to_cb.vx_ingest_manager import VxIngestManager
from netcdf_to_cb.load_spec_yaml import LoadYamlSpecFile


def parse_args(args):
    """
    Parse command line arguments
    """
    begin_time = str(datetime.now())
    logging.basicConfig(level=logging.INFO)
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
    parser.add_argument("-f", "--{first_epoch}", type=int, default=0,
                        help="The first epoch to use, inclusive")
    parser.add_argument("-l", "--{last_epoch}", type=int, default=0,
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
        # -f {first_epoch} and -l {last_epoch} are optional time params.
        # If these are present only the files in the path with filename masks
        # that fall between these epochs will be processed.
        self.first_last_params = None

    def runit(self, args):
        """
        This is the entry point for run_ingest_threads.py
        """
        self.spec_file = args['spec_file'].strip()
        self.credentials_file = args['credentials_file'].strip()
        self.thread_count = args['threads']
        self.first_last_params = {key: val for key,
                                  val in args.items() if key.startswith('{')}

        #
        #  Read the load_spec file
        #
        try:
            logging.debug("load_spec filename is %s" + self.spec_file)
            load_spec_file = LoadYamlSpecFile(
                {'spec_file': self.spec_file})
            # read in the load_spec file
            load_spec = dict(load_spec_file.read())
            # put the real credentials into the load_spec
            load_spec = self.get_credentials(load_spec)
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error(
                "*** %s occurred in Main reading load_spec " +
                self.spec_file + " ***",
                sys.exc_info())
            sys.exit("*** Error reading load_spec: " + self.spec_file)

        # get all the ingest_document_ids and put them into a my_queue
        # load the my_queue with filenames that match the mask and are between first and last epoch (if they are in the args)
        # Constructor for an infinite size  FIFO my_queue
        q = JoinableQueue()
        path = load_spec.path
        fmask = load_spec.mask
        file_names = []
        if path.exists(path) and path.isdir(path):
            with os.scandir(path) as entries:
                for entry in entries:
                    # check to see if it is within first and last epoch
                    if self.first_last_params is not None:
                        # convert the file name to an epoch using the mask
                        try:
                            utc_time = datetime.strptime(entry, fmask)
                            file_time = (
                                utc_time - datetime(1970, 1, 1)).total_seconds()
                            if self.first_last_params['{first_epoch}'] <= file_time and file_time <= self.first_last_params['{last_epoch}']:
                                file_names.append(entry.name)
                        except:
                            # don't care, it just means it wasn't the properly formatted file per the mask
                            continue

        if len(file_names) == 0:
            raise Exception("No files to Process!")
        for f in file_names:
            q.put(f)

        # instantiate ingest_manager pool - each ingest_manager is a process
        # thread that uses builders to process one file at a time from the queue
        # Make the Pool of ingest_managers
        _ingest_manager_list = []
        for _threadCount in range(int(self.thread_count)):
            # noinspection PyBroadException
            try:
                ingest_manager_thread = VxIngestManager(
                    "VxIngestManager-" + str(self.thread_count), load_spec, q)
                _ingest_manager_list.append(ingest_manager_thread)
                ingest_manager_thread.start()
            except:
                logging.error(
                    "*** Error in  VxIngestManager ***" + str(sys.exc_info()))
        # be sure to join all the threads to wait on them
        [proc.join() for proc in _ingest_manager_list]
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
            _f = open(self.credentials_file)
            _yaml_data = yaml.load(_f, yaml.SafeLoader)
            load_spec['cb_connection']['host'] = _yaml_data['cb_host']
            load_spec['cb_connection']['user'] = _yaml_data['cb_user']
            load_spec['cb_connection']['password'] = _yaml_data['cb_password']
            _f.close()
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
