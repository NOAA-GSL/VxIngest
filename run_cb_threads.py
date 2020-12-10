"""
Program Name: main script for DataTypeManager
Contact(s): Randy Pierce
Abstract:

History Log:  Initial version

Usage: This script process arguments which define a Metviewer xml_load_spec, a thread count, and a number
of other mvload related parameters.
The script maintains a thread pool of Data_Managers, and a queue of filenames that is derived from the load_spec.xml input.
The number of threads in the thread pool is set to the -t n (or --threads n) argument, where n is the number of threads to start. The file_name list
is unlimited.
For the moment this script can only process a couchbase (cb) type management_system. This means that the tag
"<connection>
    <management_system>cb</management_system>
 "
 must be set to "cb".
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of Colorado, NOAA/OAR/ESRL/GSD
"""
import argparse
import logging
import sys
import time

from datetime import datetime
from datetime import timedelta
from multiprocessing import JoinableQueue
from data_type_manager import DataTypeManager
from load_spec import LoadSpecFile

def main():
    """
    This is the entry point for run_cb_threads.py
    """
    begin_time = str(datetime.now())
    logging.basicConfig(level=logging.DEBUG)
    logging.info("--- *** --- Start METdbLoad --- *** ---")
    logging.info("Begin time: %s", begin_time)
    # time execution
    load_time_start = time.perf_counter()
    parser = argparse.ArgumentParser()
    parser.add_argument("specfile", help="Please provide required load_spec filename - something.xml or something.yaml")
    parser.add_argument("-index", action="store_true", help="Only process index, do not load data")
    parser.add_argument("-t", "--threads", type=int, default=1, help="Number of threads to use")
    parser.add_argument("-c", "--cert_path", type=str, default='', help="path to server public cert")
    # get the command line arguments
    args = parser.parse_args()

    #
    #  Read the load_spec file
    #
    try:
        logging.debug("load_spec filename is %s", args.specfile)

        # instantiate a load_spec file
        # load_specfile = LoadSpecFile(args.specfile)
        #
        # # read in the XML file and get the information out of its tags
        # load_specfile.read_xml()
        load_specfile = LoadSpecFile(args.specfile)

        # read in the XML file and get the information out of its tags
        load_specfile.read()

    except (RuntimeError, TypeError, NameError, KeyError):
        logging.error("*** %s occurred in Main reading XML ***", sys.exc_info()[0])
        sys.exit("*** Error reading XML")

    # if -index is used, only process the index
    if args.index:
        logging.debug("-index is true - only process index")
    #
    #  Purge files if flags set to not load certain types of files
    #
    try:
        # If user set flags to not read files, remove those files from load_files list
        load_specfile.load_files = purge_files(load_specfile.load_files, load_specfile.flags)

        if not load_specfile.load_files:
            logging.warning("!!! No files to load")
            sys.exit("*** No files to load")

        if not load_specfile.connection['db_management_system'].upper() == 'CB':
            logging.warning("wrong db_management_system. Can only support 'CB'")
            sys.exit("*** wrong db_management_system " + load_specfile.connection['db_management_system'])

    except (RuntimeError, TypeError, NameError, KeyError):
        logging.error("*** %s occurred in Main purging files not selected ***", sys.exc_info()[0])
        sys.exit("*** Error when removing files from load list per XML")
    # load the queue with filenames
    # Constructor for an infinite size  FIFO queue
    q = JoinableQueue()
    for f in load_specfile.load_files:
        q.put(f)
    thread_limit = args.threads
    if args.cert_path:
        cert_path = args.cert_path
        load_specfile.connection['cert_path'] = cert_path
    # instantiate data_type_manager pool - each data_type_manager is a thread that uses builders to process a file
    # Make the Pool of data_type_managers
    _dtm_list = []
    for _threadCount in range(thread_limit):
        try:
            dtm_thread = DataTypeManager("DataTypeManager-" + str(_threadCount), load_specfile.connection, q)
            _dtm_list.append(dtm_thread)
            dtm_thread.start()
        except:
            e = sys.exc_info()
            logging.error("*** %s occurred in purge_files ***", e[0])
    # be sure to join all the threads to wait on them
    [proc.join() for proc in _dtm_list]
    logging.info("finished starting threads")
    load_time_end = time.perf_counter()
    load_time = timedelta(seconds=load_time_end - load_time_start)

    logging.info("    >>> Total load time: %s", str(load_time))
    logging.info("End time: %s", str(datetime.now()))
    logging.info("--- *** --- End METdbLoad --- *** ---")


def purge_files(load_files, xml_flags):
    """ purge_files  - removes any files from load list that user has disallowed in XML tags
        Returns:
           List with files user wants to load
    """
    updated_list = load_files
    try:
        # Remove names of MET and VSDB files if user set load_stat tag to false
        if not xml_flags["load_stat"]:
            updated_list = [item for item in updated_list
                            if not (item.lower().endswith(".stat") or
                                    item.lower().endswith(".vsdb"))]
        # Remove names of MODE files if user set load_mode tag to false
        if not xml_flags["load_mode"] and updated_list:
            updated_list = [item for item in updated_list
                            if not (item.lower().endswith("cts.txt") or
                                    item.lower().endswith("obj.txt"))]

        # Remove names of MTD files if user set load_mtd tag to false
        if not xml_flags["load_mtd"] and updated_list:
            updated_list = [item for item in updated_list
                            if not (item.lower().endswith("2d.txt") or
                                    "3d_s" in item.lower() or
                                    "3d_p" in item.lower())]

    except (RuntimeError, TypeError, NameError, KeyError):
        logging.error("*** %s occurred in purge_files ***", sys.exc_info()[0])
        logging.error("*** %s occurred in Main purging files not selected ***", sys.exc_info()[0])
        sys.exit("*** Error in purge files")

    return updated_list


if __name__ == '__main__':
    main()
