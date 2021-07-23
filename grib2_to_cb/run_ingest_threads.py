"""
Program Name: main script for VXingest
Contact(s): Randy Pierce
Abstract:

History Log:  Initial version

Usage:
run_ingest_threads -s spec_file -c credentials_file -p path -m _file_mask[-o output_dir -t thread_count -f first_epoch -l last_epoch -n number_stations]
This script processes arguments which define a a yaml load_spec file,
a defaults file (for credentials),
and a thread count.
The script maintains a thread pool of VxIngestManagers and a queue of
filenames that are derived from the path, mask, first_epoch, and last_epoch.
that are defined in the load_spec file.
The number of threads in the thread pool is set to the -t n (or --threads n)
argument, where n is the number of threads to start. The default is one thread. 
The optional -n number_stations will restrict the processing to n number of stations to limit run time.
Each thread will run a VxIngestManager which will pull filenames, one at a time, 
from the filename queue and fully process that input file. 
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
  
The mask  is a python time.strftime format e.g. '%y%j%H%f',
The optional output_dir specifies the directory where output files will be written instead
of writing them directly to couchbase. If the output_dir is not specified data will be written
to couchbase cluster specified in the cb_connection.
Files in the path will be enqueued if the file name mask renders a valid datetime that 
falls between the first_epoch and the last_epoch. 
The first_epoch and the last_epoch may be omitted in which case all the files in the path
will be processed.

This is an example defaults file. The keys should match
the keys in the connection clauses of the load_spec.
defaults:
  cb_host: my_cb_host.some_subdomain.some_domain
  cb_user: some_cb_user_name
  cb_password: password_for_some_cb_user_name

This is an example invocation in bash. t=The python must be python3.
export PYTHONPATH=${HOME}/VXingest
python grib2_to_cb/run_ingest_threads.py -s /data/grib2_to_cb/load_specs/load_spec_grib_metar_hrrr_ops_V01.yaml -c ~/adb-cb1-credentials -p /data/grib2_to_cb/input_files -m %y%j%H%f -o /data/grib2_to_cb/output 


Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSL
"""
import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from multiprocessing import JoinableQueue
from pathlib import Path

import yaml

from grib2_to_cb.load_spec_yaml import LoadYamlSpecFile
from grib2_to_cb.vx_ingest_manager import VxIngestManager


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
    parser.add_argument("-p", "--path", type=str, default="./",
                        help="Specify the input directory that contains the input files")
    parser.add_argument("-m", "--file_name_mask", type=str, default="%Y%m%d_%H%M",
                        help="Specify the file name mask for the input files ()")
    parser.add_argument("-o", "--output_dir", type=str, default="/tmp",
                        help="Specify the output directory to put the json output files")
    parser.add_argument("-f", "--first_epoc", type=int, default=0,
                        help="The first epoch to use, inclusive")
    parser.add_argument("-l", "--last_epoch", type=int, default=sys.maxsize,
                        help="The last epoch to use, exclusive")
    parser.add_argument("-n", "--number_stations", type=int, default=sys.maxsize,
                        help="The maximum number of stations to process")
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
        self.path = None
        self.fmask = None
        self.output_dir = None
        # optional: used to limit the number of stations processed
        self.number_stations = sys.maxsize

    def runit(self, args):
        """
        This is the entry point for run_ingest_threads.py
        """
        self.spec_file = args['spec_file'].strip()
        self.credentials_file = args['credentials_file'].strip()
        self.path = args['path'].strip()
        self.fmask = args['file_name_mask'].strip()
        self.thread_count = args['threads']
        self.output_dir = args['output_dir'].strip()
        _args_keys = args.keys()
        if 'first_epoch' in _args_keys and 'second_epoch' in _args_keys:
            self.first_last_params = {'first_epoch': args['first_epoch'],
                                      'last_epoch': args['last_epoch']}
        else:
            self.first_last_params = {}
            self.first_last_params['first_epoch'] = 0
            self.first_last_params['last_epoch'] = sys.maxsize
        if 'number_stations' in _args_keys:
            self.number_stations = args['number_stations']
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
        file_names = []
        if os.path.exists(self.path) and os.path.isdir(self.path):
            with os.scandir(self.path) as entries:
                for entry in entries:
                    # convert the file name to an epoch using the mask
                    # first remove any characters from the file_name that correpond
                    # to "|" in the mask. Also remove the "|" characters from the mask itself
                    try:
                        entry_name = entry.name
                        file_utc_time = datetime.strptime(
                            entry_name, self.fmask)
                        file_time = (file_utc_time -
                                     datetime(1970, 1, 1)).total_seconds()
                        # check to see if it is within first and last epoch (default is 0 and maxsize)
                        if self.first_last_params['first_epoch'] <= file_time and file_time <= self.first_last_params['last_epoch']:
                            file_names.append(
                                os.path.join(self.path, entry.name))
                    except:
                        # don't care, it just means it wasn't a properly formatted file per the mask
                        continue

        if len(file_names) == 0:
            raise Exception("No files to Process!")
        for f in file_names:
            q.put(f)

        # instantiate ingest_manager pool - each ingest_manager is a process
        # thread that uses builders to process one file at a time from the queue
        # Make the Pool of ingest_managers
        ingest_manager_list = []
        for _threadCount in range(int(self.thread_count)):
            # noinspection PyBroadException
            try:
                ingest_manager_thread = VxIngestManager(
                    "VxIngestManager-" + str(self.thread_count), load_spec, q, self.output_dir, self.number_stations)
                ingest_manager_list.append(ingest_manager_thread)
                ingest_manager_thread.start()
            except:
                logging.error(
                    "*** Error in  VxIngestManager ***" + str(sys.exc_info()))
        # be sure to join all the threads to wait on them
        [proc.join() for proc in ingest_manager_list]
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
