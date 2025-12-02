"""
Program Name: main script for VXingest
Contact(s): Randy Pierce
Abstract:

History Log:  Initial version

Usage:
run_ingest_threads -j job_document_id -c credentials_file [-o output_dir -t thread_count -f file_pattern -n number_stations]
This script processes arguments which specify a job document id,
a defaults file (for credentials), an optional output directory, thread count, and file matching pattern.
The job document id is the id of a job document in the couchbase database.
The job document might look like this...
{
  "id": "JOB:V01:METAR:NETCDF:OBS",
  "status": "active",
  "type": "JOB",
  "version": "V01",
  "subset": "METAR",
  "subDocType": "NETCDF",
  "subDoc": "OBS",
  "run_priority": 1,
  "file_mask": "%Y%m%d_%H%M",
  "schedule": "0 * * * *",
  "offset_minutes": 0,
  "ingest_document_ids": [
    "MD:V01:METAR:obs:ingest:netcdf"
  ]
}
The important run time fields are "file_mask" and "ingest_document_ids".
The file mask is a python time.strftime that specifies how the code will
decipher a file name for time. These file names are derived from the file
modification time, according to a specific mask.
The ingest_document_ids specify a list of ingest_document ids that a job
must process.
The script maintains a thread pool of VxIngestManagers and a queue of
filenames that are derived from the path and the optional file_pattern parameter.
If a file_pattern is provided - as a parameter - then globbing will be used to
determine which which filenames in the input_path are included for ingesting.
The default file_pattern is "*", which will include all files.
The number of threads in the thread pool is set to the -t n (or --threads n)
argument, where n is the number of threads to start. The default is one thread.
Each thread will run a VxIngestManager which will pull filenames, one at a time,
from the filename queue and fully process that input file.
When the queue is empty each NetcdfIngestManager will gracefully die.
Only files that do not have a DataFile entry in the database will be added to the file queue.
When a file is processed a datafile entry will be made for that file and added to the result documents to ne imported.

The file_mask is a python time.strftime format e.g. '%y%j%H%f'.
The file_pattern is a file glob string. e.g. '202409*'.
The optional output_dir specifies the directory where output files will be written instead
of writing them directly to couchbase. If the output_dir is not specified data will be written
to couchbase cluster specified in the cb_connection.
Files in the path will be enqueued if there is no corresponding dataFile entry in the database.

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
from collections.abc import Callable
from datetime import datetime, timedelta
from multiprocessing import JoinableQueue, Queue, set_start_method
from pathlib import Path

from vxingest.builder_common.vx_ingest import CommonVxIngest
from vxingest.log_config import configure_logging, worker_log_configurer
from vxingest.netcdf_to_cb.vx_ingest_manager import VxIngestManager

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
        "-f",
        "--file_pattern",
        type=str,
        default="*",
        help="Specify the file name pattern for the input files ()",
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        type=str,
        default="/tmp",
        help="Specify the output directory to put the json output files",
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
        self.fmask = None
        self.file_pattern = "*"
        self.output_dir = None
        self.job_document_id = None
        self.load_job_id = None
        self.load_spec = {}
        self.cb_credentials = None
        self.collection = None
        self.common_collection = None
        self.cluster = None
        self.ingest_document_id = None
        self.ingest_document = None
        super().__init__()

    def runit(self, config, log_queue: Queue, log_configurer: Callable[[Queue], None]):
        """
        This is the entry point for run_ingest_threads.py
        """
        begin_time = str(datetime.now())
        logger.info("--- *** --- Start --- *** ---")
        logger.info("Begin a_time: %s", begin_time)

        self.credentials_file = config.get("credentials_file", None)
        self.thread_count = config.get("threads", 1)
        self.output_dir = config.get("output_dir", "/tmp").strip()
        self.job_document_id = config.get("job_id", None)
        self.file_pattern = config.get("file_pattern", "*").strip()
        self.ingest_document_ids = config.get("ingest_document_ids", None)
        self.fmask = config.get("file_mask", None)
        self.input_data_path = config.get("input_data_path", None)

        try:
            # put the real credentials into the load_spec
            logger.info("getting cb_credentials")
            self.cb_credentials = self.get_credentials(self.load_spec)
            # get the intended subset (collection from the job_id)
            self.cb_credentials["collection"] = config["collection"]
            # establish connections to cb, collection
            self.connect_cb()
            logger.info("connected to cb - collection is %s", self.collection.name)
            collection = self.load_spec["cb_connection"]["collection"]
            bucket = self.load_spec["cb_connection"]["bucket"]
            scope = self.load_spec["cb_connection"]["scope"]
            # load the ingest document ids into the load_spec (this might be redundant) - from COMMON
            self.load_spec["ingest_document_ids"] = self.ingest_document_ids
            # put all the ingest documents into the load_spec too
            self.load_spec["ingest_documents"] = {}
            for _id in self.load_spec["ingest_document_ids"]:
                if _id.startswith("MD"):
                    self.load_spec["ingest_documents"][_id] = (
                        self.common_collection.get(_id).content_as[dict]
                    )
                else:
                    self.load_spec["ingest_documents"][_id] = (
                        self.runtime_collection.get(_id).content_as[dict]
                    )
            self.load_spec["fmask"] = self.fmask
            self.load_spec["input_data_path"] = self.input_data_path
            # stash the load_job in the load_spec
            self.load_spec["load_job_doc"] = self.build_load_job_doc(
                self.load_spec["cb_connection"]["collection"]
            )
        except (RuntimeError, TypeError, NameError, KeyError):
            logger.error(
                "*** Error occurred in Main reading load_spec: %s ***",
                str(sys.exc_info()),
            )
            sys.exit("*** Error reading load_spec:")

        # load the my_queue with filenames that match the mask and have not already been ingested
        # (do not have associated datafile documents)
        # Constructor for an infinite size  FIFO my_queue
        _q = JoinableQueue()
        file_names = []
        # get the urls (full_file_names) from all the datafiles for this type of ingest
        # for netcdf type ingests there is only one ingest document so we can just use the first
        # subset
        subset = self.load_spec["ingest_documents"][
            self.load_spec["ingest_document_ids"][0]
        ]["subset"]
        file_query = f"""
            SELECT url, mtime
            FROM `{bucket}`.{scope}.{collection}
            WHERE
            subset='{subset}'
            AND type='DF'
            AND fileType='netcdf'
            AND originType='madis' order by url;
            """
        # file_pattern is a glob string not a python file match string
        file_names = self.get_file_list(
            file_query, self.input_data_path, self.file_pattern, self.fmask
        )
        for _f in file_names:
            _q.put(_f)

        # instantiate ingest_manager pool - each ingest_manager is a process
        # thread that uses builders to process one file at a time from the queue
        # Make the Pool of ingest_managers
        ingest_manager_list = []
        for thread_count in range(int(self.thread_count)):
            try:
                ingest_manager_thread = VxIngestManager(
                    "VxIngestManager-" + str(thread_count),
                    self.load_spec,
                    _q,
                    self.output_dir,
                    log_queue,  # Queue to pass logging messages back to the main process on
                    log_configurer,  # Config function to set up the logger in the multiprocess Process
                )
                ingest_manager_list.append(ingest_manager_thread)
                ingest_manager_thread.start()
            except Exception as _e:
                logger.error("*** Error in VXIngest %s***", str(_e))
        # be sure to join all the threads to wait on them
        finished = [proc.join() for proc in ingest_manager_list]
        self.write_load_job_to_files()
        logger.info("finished starting threads")
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
        logger.info("PYTHONPATH: %s", os.environ["PYTHONPATH"])
        args = parse_args(sys.argv[1:])
        self.runit(vars(args), log_queue, worker_log_configurer)
        logger.info("*** FINISHED ***")
        log_queue_listener.stop()
        sys.exit(0)


if __name__ == "__main__":
    VXIngest().main()
