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
  "id": "JOB:V01:METAR:SUMS:SURFACE:MODEL:HRRR_OPS",
  "status": "active",
  "type": "JOB",
  "version": "V01",
  "subset": "METAR",
  "subType": "PARTIALSUMS",
  "subDoc": "SURFACE",
  "subDocType": "HRRR",
  "run_priority": 5,
  "schedule": "0 * * * *",
  "offset_minutes": 15,
  "ingest_document_ids": [
    "MD:V01:METAR:HRRR_OPS:E_US:SUMS:SURFACE:ingest",
    "MD:V01:METAR:HRRR_OPS:ALL_HRRR:SUMS:SURFACE:ingest",
    "MD:V01:METAR:HRRR_OPS:E_HRRR:SUMS:SURFACE:ingest",
    "MD:V01:METAR:HRRR_OPS:W_HRRR:SUMS:SURFACE:ingest",
    "MD:V01:METAR:HRRR_OPS:GtLk:SUMS:SURFACE:ingest",
    "MD:V01:METAR:RAP_OPS_130:E_US:SUMS:SURFACE:ingest",
    "MD:V01:METAR:RAP_OPS_130:ALL_HRRR:SUMS:SURFACE:ingest",
    "MD:V01:METAR:RAP_OPS_130:E_HRRR:SUMS:SURFACE:ingest",
    "MD:V01:METAR:RAP_OPS_130:W_HRRR:SUMS:SURFACE:ingest",
    "MD:V01:METAR:RAP_OPS_130:GtLk:SUMS:SURFACE:ingest"
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
from multiprocessing import JoinableQueue, Queue, set_start_method
from pathlib import Path
from typing import Callable

from vxingest.builder_common.vx_ingest import CommonVxIngest
from vxingest.log_config import configure_logging, worker_log_configurer
from vxingest.partial_sums_to_cb.vx_ingest_manager import VxIngestManager

# Get a logger with this module's name to help with debugging
logger = logging.getLogger(__name__)


def parse_args(args):
    """
    Parse command line arguments
    """
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

    def runit(self, args, log_queue: Queue, log_configurer: Callable[[Queue], None]):
        """
        This is the entry point for run_ingest_threads.py
        """
        begin_time = str(datetime.now())
        logger.info("--- *** --- Start --- *** ---")
        logger.info("Begin a_time: %s", begin_time)

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
        logger.info(
            "*** Using first_last_params: %s ***",
            str(self.load_spec["first_last_params"]),
        )
        try:
            # put the real credentials into the load_spec
            self.cb_credentials = self.get_credentials(self.load_spec)
            # establish connections to cb, collection
            self.connect_cb()
            # load the ingest document ids into the load_spec (this might be redundant)
            ingest_document_result = self.collection.get(self.job_document_id)
            ingest_document = ingest_document_result.content_as[dict]
            self.load_spec["ingest_document_ids"] = ingest_document[
                "ingest_document_ids"
            ]
            # put all the ingest documents into the load_spec too
            self.load_spec["ingest_documents"] = {}
            for _id in self.load_spec["ingest_document_ids"]:
                self.load_spec["ingest_documents"][_id] = self.collection.get(
                    _id
                ).content_as[dict]
            # stash the load_job in the load_spec
            self.load_spec["load_job_doc"] = self.build_load_job_doc(
                "partial_sums_surface"
            )
        except (RuntimeError, TypeError, NameError, KeyError):
            logger.error(
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
        logger.info(
            f"The ingest documents in the queue are: {self.load_spec['ingest_document_ids']}"
        )
        logger.info(f"Starting {self.thread_count} processes")
        for thread_count in range(int(self.thread_count)):
            try:
                ingest_manager_thread = VxIngestManager(
                    f"VxIngestManager-{thread_count + 1}",  # Processes are 1 indexed in the logger
                    self.load_spec,
                    _q,
                    self.output_dir,
                    log_queue,  # Queue to pass logging messages back to the main process on
                    log_configurer,  # Config function to set up the logger in the multiprocess Process
                )
                ingest_manager_list.append(ingest_manager_thread)
                ingest_manager_thread.start()
                logger.info(f"Started thread: VxIngestManager-{thread_count + 1}")
            except Exception as _e:
                logger.error("*** Error in VXIngest %s***", str(_e))
        # be sure to join all the threads to wait on them
        finished = [proc.join() for proc in ingest_manager_list]
        logger.info("Finished processes")
        self.write_load_job_to_files()
        logger.info("Finished writing files")
        load_time_end = time.perf_counter()
        load_time = timedelta(seconds=load_time_end - self.load_time_start)
        logger.info(" finished %s", str(finished))
        logger.info("    >>> Total load a_time: %s", str(load_time))
        logger.info("End a_time: %s", str(datetime.now()))
        logger.info("--- *** --- End  --- *** ---")

    def main(self):
        """
        This is the entry for run_ingest_threads
        """
        # Force new processes to start with a clean environment
        # "fork" is the default on Linux and can be unsafe
        set_start_method("spawn")

        # Setup logging for the main process so we can use the "logger"
        log_queue = Queue()
        runtime = datetime.now()
        log_queue_listener = configure_logging(
            log_queue, Path(f"all_logs-{runtime.strftime('%Y-%m-%dT%H:%M:%S%z')}.log")
        )
        try:
            logger.info("PYTHONPATH: %s", os.environ["PYTHONPATH"])
            args = parse_args(sys.argv[1:])
            self.runit(vars(args), log_queue, worker_log_configurer)
            logger.info("*** FINISHED ***")
            # Tell the logging thread to finish up, too
            log_queue_listener.stop()
            sys.exit(0)
        except Exception as _e:
            logger.info("*** FINISHED with exception %s***", str(_e))
            # Tell the logging thread to finish up, too
            log_queue_listener.stop()


if __name__ == "__main__":
    VXIngest().main()
