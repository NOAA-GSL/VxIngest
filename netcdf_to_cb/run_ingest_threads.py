"""
Program Name: main script for VXingest
Contact(s): Randy Pierce
Abstract:

History Log:  Initial version

Usage:
run_ingest_threads -j job_document_id -c credentials_file -p path [-o output_dir -t thread_count -f file_pattern -n number_stations]
This script processes arguments which specify a job document id,
a defaults file (for credentials), an input file path, an optional output directory, thread count, and file matching pattern.
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
  "file_pattern": "%Y%m%d_%H%M",
  "schedule": "0 * * * *",
  "offset_minutes": 0,
  "ingest_document_ids": [
    "MD:V01:METAR:obs:ingest:netcdf"
  ]
}
The important run time fields are "file_mask" and "ingest_document_ids".
The file mask is a python time.strftime that specifies what files will
be chosen based on pattern matching.
The ingest_document_ids specify a list of ingest_document ids that a job
must process.
The script maintains a thread pool of VxIngestManagers and a queue of
filenames that are derived from the path and file_mask.
If a file_pattern is provided further restrictions, beyond the file_mask match pattern,
on the selected filenames happen as globbing is used to qualify
which filenames in the input_path are included for ingesting.
The number of threads in the thread pool is set to the -t n (or --threads n)
argument, where n is the number of threads to start. The default is one thread.
There is a file_pattern argument that allows to specify a filename pattern to which
all the file_mask matching files in the input directory will be further qualified
with standard globing. Only matching files will be ingested if this option is used.
Each thread will run a VxIngestManager which will pull filenames, one at a time,
from the filename queue and fully process that input file.
When the queue is empty each NetcdfIngestManager will gracefully die.
Only files that do not have a DataFile entry in the database will be added to the file queue.
When a file is processed a datafile entry will be made for that file and added to the result documents to ne imported.

The file_mask  is a python time.strftime format e.g. '%y%j%H%f'.
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
from datetime import datetime, timedelta
from multiprocessing import JoinableQueue
from builder_common.vx_ingest import CommonVxIngest
from netcdf_to_cb.vx_ingest_manager import VxIngestManager


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
        "-p",
        "--path",
        type=str,
        default="./",
        help="Specify the input directory that contains the input files",
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
        self.path = None
        self.fmask = None
        self.file_pattern = "*"
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
        self.path = args["path"].strip()
        self.fmask = args["file_name_mask"].strip()
        self.thread_count = args["threads"]
        self.output_dir = args["output_dir"].strip()
        self.job_document_id = args["job_id"].strip()
        if "file_pattern" in args.keys():
            self.file_pattern = args["file_pattern"].strip()
        _args_keys = args.keys()
        try:
            # put the real credentials into the load_spec
            self.cb_credentials = self.get_credentials(self.load_spec)
            # establish connections to cb, collection
            self.connect_cb()
            # load the ingest document ids into the load_spec (this might be redundant)
            stmnt="Select ingest_document_ids from mdata where meta().id = \"{id}\"".format(id=self.job_document_id)
            result = self.cluster.query(stmnt)
            self.load_spec['ingest_document_ids'] = list(result)[0]["ingest_document_ids"]
            # put all the ingest documents into the load_spec too
            self.load_spec["ingest_documents"] = {}
            for _id in self.load_spec["ingest_document_ids"]:
                self.load_spec["ingest_documents"][_id]= self.collection.get(_id).content
            # load the fmask into the load_spec
            stmnt="Select file_mask from mdata where meta().id = \"{id}\"".format(id=self.job_document_id)
            result = self.cluster.query(stmnt)
            self.fmask = list(result)[0]["file_mask"]
            #stash the load_job in the load_spec
            self.load_spec["load_job_doc"] = self.build_load_job_doc("madis")
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error(
                "*** Error occurred in Main reading load_spec: %s ***",
                str(sys.exc_info()),
            )
            sys.exit("*** Error reading load_spec: ")

        # load the my_queue with filenames that match the mask and have not already been ingested
        # (do not have associated datafile documents)
        # Constructor for an infinite size  FIFO my_queue
        _q = JoinableQueue()
        file_names = []
        # get the urls (full_file_names) from all the datafiles for this type of ingest
        # for netcdf type ingests there is only one ingest document so we can just use the first
        # subset
        subset = self.load_spec["ingest_documents"][self.load_spec["ingest_document_ids"][0]]["subset"]
        file_query = """
            SELECT url, mtime
            FROM mdata
            WHERE
            subset='{subset}'
            AND type='DF'
            AND fileType='netcdf'
            AND originType='madis' order by url;
            """.format(
            subset=subset
        )
        file_names = self.get_file_list(file_query, self.path, self.file_pattern)
        for _f in file_names:
            _q.put(_f)

        # instantiate ingest_manager pool - each ingest_manager is a process
        # thread that uses builders to process one file at a time from the queue
        # Make the Pool of ingest_managers
        ingest_manager_list = []
        for thread_count in range(int(self.thread_count)):
            # noinspection PyBroadException
            try:
                self.load_spec["fmask"] = self.fmask
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
