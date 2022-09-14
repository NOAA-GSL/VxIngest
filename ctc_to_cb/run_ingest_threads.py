"""
Program Name: main script for VXingest
Contact(s): Randy Pierce
Abstract:

History Log:  Initial version

Usage:
run_ingest_threads -j job_document_id -c credentials_file [-o output_dir -f first_epoch -l last_epoch -t thread_count]
This script processes arguments which specify a job document id,
a defaults file (for credentials), an input file path, an optional output directory, thread count, and file matching pattern.
The job document id is the id of a job document in the couchbase database.
The job document might look like this...
{
  "id": "JOB:V01:METAR:SUM:CTC:MODEL:HRRR_RAP_130",
  "status": "active",
  "type": "JOB",
  "version": "V01",
  "subset": "METAR",
  "subType": "SUM",
  "subDoc": "CTC",
  "subDocType": "HRRR",
  "run_priority": 5,
  "schedule": "0 * * * *",
  "offset_minutes": 15,
  "ingest_document_ids": [
    "MD:V01:METAR:HRRR_OPS:E_US:CTC:CEILING:ingest",
    "MD:V01:METAR:HRRR_OPS:ALL_HRRR:CTC:CEILING:ingest",
    "MD:V01:METAR:HRRR_OPS:E_HRRR:CTC:CEILING:ingest",
    "MD:V01:METAR:HRRR_OPS:W_HRRR:CTC:CEILING:ingest",
    "MD:V01:METAR:HRRR_OPS:GtLk:CTC:CEILING:ingest",
    "MD:V01:METAR:RAP_OPS_130:E_US:CTC:CEILING:ingest",
    "MD:V01:METAR:RAP_OPS_130:ALL_HRRR:CTC:CEILING:ingest",
    "MD:V01:METAR:RAP_OPS_130:E_HRRR:CTC:CEILING:ingest",
    "MD:V01:METAR:RAP_OPS_130:W_HRRR:CTC:CEILING:ingest",
    "MD:V01:METAR:RAP_OPS_130:GtLk:CTC:CEILING:ingest"
  ]
}
The important run time field is "ingest_document_ids".
The ingest_document_ids specify a list of ingest_document ids that a job
must process.The script maintains a thread pool of VxIngestManagers and a queue of
ingest_documents.
The number of threads in the thread pool is set to the -t n (or --threads n)
argument, where n is the number of threads to start. The default is one thread.
Each thread will run a VxIngestManager which will pull ingest documents, one at a time,
from the queue and fully process that document.
When the queue is empty each NetcdfIngestManager will gracefully die.


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
from builder_common.vx_ingest import CommonVxIngest
from ctc_to_cb.vx_ingest_manager import VxIngestManager


def parse_args(args):
    """
    Parse command line arguments
    """
    begin_time = str(datetime.now())
    logging.getLogger().setLevel(logging.INFO)
    logging.info("--- *** --- Start --- *** ---")
    logging.info("Begin a_time: %s", begin_time)
    # a_time execution
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-j",
        "--job_id",
        type=str,
        help="Please provide required Job document id",
    )
    parser.add_argument(
        "-c",
        "--credentials_file",
        type=str,
        help="Please provide required credentials_file",
    )
    parser.add_argument(
        "-t", "--threads", type=int, default=1, help="Number of threads to use"
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        type=str,
        default="/tmp",
        help="Specify the output directory to put the json output files",
    )
    parser.add_argument(
        "-f",
        "--first_epoch",
        type=int,
        default=0,
        help="The first epoch to use, inclusive",
    )
    parser.add_argument(
        "-l",
        "--last_epoch",
        type=int,
        default=sys.maxsize,
        help="The last epoch to use, exclusive",
    )
    # get the command line arguments
    args = parser.parse_args(args)
    return args


class VXIngest(CommonVxIngest):
    """
    This class is the commandline mechanism for using the builder.
    This class will maintain the couchbase collection and cluster objects for all
    the ingest managers that this thread will use. There will be VxIngestManagers started
    to match the threadcount that is passed in. The default number of threads is one.
    Args:
        object ([dict]): [parsed cmdline arguments]
    Raises:
        _e: [general exception]
    """

    def __init__(self):
        self.load_time_start = time.perf_counter()
        self.credentials_file = ""
        self.thread_count = ""
        # -f first_epoch and -l last_epoch are optional time params.
        # If these are present only the files in the path with filename masks
        # that fall between these epochs will be processed.
        self.first_last_params = None
        self.output_dir = None
        self.job_document_id = None
        self.load_job_id = None
        self.load_spec = {}
        self.cb_credentials = None
        self.collection = None
        self.cluster = None
        self.ingest_document_id = None
        self.ingest_document = None
        super().__init__()
        logging.getLogger().setLevel(logging.INFO)

    def runit(self, args):  # pylint:disable=too-many-locals
        """
        This is the entry point for run_ingest_threads.py
        """
        self.credentials_file = args["credentials_file"].strip()
        self.thread_count = args["threads"]
        self.output_dir = args["output_dir"].strip()
        self.job_document_id = args["job_id"].strip()
        _args_keys = args.keys()
        if "first_epoch" in _args_keys and "last_epoch" in _args_keys:
            self.first_last_params = {
                "first_epoch": args["first_epoch"],
                "last_epoch": args["last_epoch"],
            }
        else:
            self.first_last_params = {}
            self.first_last_params["first_epoch"] = 0
            self.first_last_params["last_epoch"] = sys.maxsize
        # stash the first_last_params into the load spec
        self.load_spec["first_last_params"] = self.first_last_params
        logging.info(
                "*** Using first_last_params: %s ***",
                str(self.load_spec["first_last_params"])
            )
        try:
            # put the real credentials into the load_spec
            self.cb_credentials = self.get_credentials(self.load_spec)
            #establish connections to cb, collection
            self.connect_cb()
            # load the ingest document ids into the load_spec (this might be redundant)
            stmnt="Select ingest_document_ids from mdata where meta().id = \"{id}\"".format(id=self.job_document_id)
            result = self.cluster.query(stmnt)
            self.load_spec['ingest_document_ids'] = list(result)[0]["ingest_document_ids"]
            # put all the ingest documents into the load_spec too
            self.load_spec["ingest_documents"] = {}
            for _id in self.load_spec["ingest_document_ids"]:
                self.load_spec["ingest_documents"][_id]= self.collection.get(_id).content
            #stash the load_job in the load_spec
            self.load_spec["load_job_doc"] = self.build_load_job_doc("madis")
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error(
                "*** Error occurred in Main reading load_spec: %s ***",
                str(sys.exc_info()),
            )
            sys.exit("*** Error reading load_spec:")

        # get all the ingest_document_ids and put them into a my_queue
        # load the my_queue with
        # Constructor for an infinite size  FIFO my_queue
        _q = JoinableQueue()
        for _f in self.load_spec["ingest_document_ids"]:
            _q.put(_f)
        # instantiate data_type_manager pool - each data_type_manager is a
        # thread that uses builders to process a file
        # Make the Pool of data_type_managers
        ingest_manager_list = []
        for thread_count in range(int(self.thread_count)):
            # noinspection PyBroadException
            try:
                ingest_manager_thread = VxIngestManager(
                    "VxIngestManager-" + str(thread_count),
                    self.load_spec,
                    _q,
                    self.output_dir,
                )
                ingest_manager_list.append(ingest_manager_thread)
                ingest_manager_thread.start()
            except Exception as _e:  # pylint:disable=broad-except
                logging.error("*** Error in VXIngest %s***", str(_e))
        # be sure to join all the threads to wait on them
        finished = [proc.join() for proc in ingest_manager_list]
        self.write_load_job_to_files()
        logging.info("finished starting threads")
        load_time_end = time.perf_counter()
        load_time = timedelta(seconds=load_time_end - self.load_time_start)
        logging.info(" finished %s", str(finished))
        logging.info("    >>> Total load a_time: %s", str(load_time))
        logging.info("End a_time: %s", str(datetime.now()))
        logging.info("--- *** --- End  --- *** ---")

    def main(self):
        """
        This is the entry for run_ingest_threads
        """
        logging.info("PYTHONPATH: %s", os.environ["PYTHONPATH"])
        args = parse_args(sys.argv[1:])
        self.runit(vars(args))
        logging.info("*** FINISHED ***")
        sys.exit(0)

if __name__ == "__main__":
    VXIngest().main()
