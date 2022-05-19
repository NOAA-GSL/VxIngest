"""
Program Name: main script for VXingest
Contact(s): Randy Pierce
Abstract:

History Log:  Initial version

Usage:
run_ingest_threads -s spec_file -c credentials_file -p path -m _file_mask[-o output_dir -t thread_count -f file_pattern -n number_stations]
This script processes arguments which define a a yaml load_spec file,
a defaults file (for credentials),
and a thread count.
The script maintains a thread pool of VxIngestManagers and a queue of
filenames that are derived from the path and file_mask.
If a file_pattern is provided globbing is used to qualify which filenames in the input_path
are included for ingesting.
The number of threads in the thread pool is set to the -t n (or --threads n)
argument, where n is the number of threads to start. The default is one thread.
The optional -n number_stations will restrict the processing to n number of stations to limit run time.
There is a file_pattern argument that allows to specify a filename pattern to which
all the files in the input directory will be matched with standard globing. Only
matching files will be ingested if this option is used.
Each thread will run a VxIngestManager which will pull filenames, one at a time,
from the filename queue and fully process that input file.
When the queue is empty each NetcdfIngestManager will gracefully die.
Only files that do not have a DataFile entry in the database will be added to the file queue.
When a file is processed a datafile entry will be made for that file and added to the result documents to ne imported.

This is an example load_spec...

load_spec:
  email: "randy.pierce@noaa.gov"
  ingest_document_id: 'MD:V01:METAR:obs'

The mask  is a python time.strftime format e.g. '%y%j%H%f'.
The optional output_dir specifies the directory where output files will be written instead
of writing them directly to couchbase. If the output_dir is not specified data will be written
to couchbase cluster specified in the cb_connection.
Files in the path will be enqueued if there is no corresponding dataFile entry in the database.

This is an example defaults file. The keys should match
the keys in the connection clauses of the load_spec.
defaults:
  cb_host: my_cb_host.some_subdomain.some_domain
  cb_user: some_cb_user_name
  cb_password: password_for_some_cb_user_name
This is an example invocation in bash.
outdir="/data/grib2_to_cb/rap_ops_130/output/${pid}"
mkdir $outdir
python ${clonedir}/grib2_to_cb/run_ingest_threads.py -s /data/grib2_to_cb/load_specs/load_spec_grib_metar_rap_ops_130_V01.yaml -c ~/adb-cb1-credentials -p /public/data/grids/rap/iso_130/grib2 -m %y%j%H%f -o $outdir -t8
${clonedir}/scripts/VXingest_utilities/import_docs.sh -c ~/adb-cb1-credentials -p $outdir -n 8 -l ${clonedir}/logs

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
from builder_common.load_spec_yaml import LoadYamlSpecFile
from grib2_to_cb.vx_ingest_manager import VxIngestManager


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
        "-s",
        "--spec_file",
        type=str,
        help="Please provide required load_spec filename "
        "-s something.xml or -s something.yaml",
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
        "-m",
        "--file_name_mask",
        type=str,
        default="%Y%m%d_%H%M",
        help="Specify the file name mask for the input files ()",
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
    parser.add_argument(
        "-n",
        "--number_stations",
        type=int,
        default=sys.maxsize,
        help="The maximum number of stations to process",
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
        self.spec_file = ""
        self.credentials_file = ""
        self.thread_count = ""
        self.path = None
        self.fmask = None
        self.file_pattern = "*"
        self.output_dir = None
        # optional: used to limit the number of stations processed
        self.number_stations = sys.maxsize
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
        self.spec_file = args["spec_file"].strip()
        self.credentials_file = args["credentials_file"].strip()
        self.path = args["path"].strip()
        self.fmask = args["file_name_mask"].strip()
        self.thread_count = args["threads"]
        self.output_dir = args["output_dir"].strip()
        if "file_pattern" in args.keys():
            self.file_pattern = args["file_pattern"].strip()
        _args_keys = args.keys()
        if "number_stations" in _args_keys:
            self.number_stations = args["number_stations"]
        else:
            self.number_stations = sys.maxsize
        #
        #  Read the load_spec file
        #
        try:
            logging.debug("load_spec filename is %s", self.spec_file)
            load_spec_file = LoadYamlSpecFile({"spec_file": self.spec_file})
            # read in the load_spec file
            self.load_spec = dict(load_spec_file.read())
            # put the real credentials into the load_spec
            self.cb_credentials = self.get_credentials(self.load_spec)
            # stash the load_job
            self.load_spec["load_job_doc"] = self.build_load_job_doc("ctc")
            # get the ingest document id.
            # NOTE: in future we may make this (ingest_document_id) a list
            # and start each VxIngestManager with its own ingest_document_id
            self.ingest_document_id = self.load_spec["ingest_document_id"]
            # establish connections to cb, collection
            self.connect_cb()
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error(
                "*** Error occurred in Main reading load_spec %s: %s ***",
                self.spec_file,
                str(sys.exc_info()),
            )
            sys.exit("*** Error reading load_spec: " + self.spec_file)

        self.ingest_document = self.collection.get(self.ingest_document_id).content
        # load the my_queue with filenames that match the mask and have not already been ingested
        # (do not have associated datafile documents)
        # Constructor for an infinite size  FIFO my_queue
        _q = JoinableQueue()
        model = self.ingest_document["model"]
        # get the urls (full_file_names) from all the datafiles for this type of ingest
        file_query = """
            SELECT url, mtime
            FROM mdata
            WHERE
            subset={subset}
            AND type='DF'
            AND fileType='grib2'
            AND originType='model'
            AND model='{model}' order by url;
            """.format(
            subset=self.ingest_document["subset"], model=model
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
                    self.ingest_document,
                    _q,
                    self.output_dir,
                    number_stations=self.number_stations,
                )
                ingest_manager_list.append(ingest_manager_thread)
                ingest_manager_thread.start()
            except Exception as _e:  # pylint:disable=broad-except
                logging.error("*** Error in  VXIngest %s***", str(_e))
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
        sys.exit("*** FINISHED ***")


if __name__ == "__main__":
    VXIngest().main()
