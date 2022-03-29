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
  cb_connection:
    management_system: cb
    host: "cb_host"   - should come from defaults file
    user: "cb_user"   - should come from defaults file
    password: "cb_pwd" - should come from defaults file

(For the mask - python time.strftime format e.g. 20210619_1300)
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
# clean the output directory
rm -rf /data/netcdf_to_cb/output/*
python netcdf_to_cb/run_ingest_threads.py -s /data/netcdf_to_cb/load_specs/load_spec_netcdf_metar_obs_V01.yaml -c ~/adb-cb1-credentials -p /public/data/madis/point/metar/netcdf -m %Y%m%d_%H%M -o /data/netcdf_to_cb/output -t8

Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSL
"""
import argparse
import logging
import os
import sys
import time
from glob import glob
from datetime import datetime, timedelta
from multiprocessing import JoinableQueue
from pathlib import Path
import json
import yaml
from couchbase.cluster import Cluster, ClusterOptions
from couchbase_core.cluster import PasswordAuthenticator
from netcdf_to_cb.vx_ingest_manager import VxIngestManager
from netcdf_to_cb.load_spec_yaml import LoadYamlSpecFile


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
    # get the command line arguments
    args = parser.parse_args(args)
    return args


class VXIngest:
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
        self.load_job_id = None
        self.load_spec = {}
        self.cb_credentials = None
        self.collection = None
        self.cluster = None
        self.ingest_document_id = None
        self.ingest_document = None
        logging.getLogger().setLevel(logging.INFO)

    def write_load_job_to_files(self):
        """
        write all the documents in the document_map into files in the output_dir
        """
        # noinspection PyBroadException
        try:
            Path(self.output_dir).mkdir(parents=True, exist_ok=True)
            try:
                file_name = self.load_job_id + ".json"
                complete_file_name = os.path.join(self.output_dir, file_name)
                _f = open(complete_file_name, "w")
                _f.write(json.dumps([self.load_spec["load_job_doc"]]))
                _f.close()
            except Exception as _e:  # pylint: disable=broad-except
                logging.info(
                    "process_file - trying write load_job: Got Exception - %s", str(_e)
                )
        except Exception as _e:
            logging.error(": *** Error writing load_job to files: %s***", str(_e))
            raise _e

    def build_load_job_doc(self):
        """
        This method will build a load_job document for GribBuilder
        """
        self.load_job_id = "LJ:{m}:{c}:{t}".format(
            m=self.__module__, c=self.__class__.__name__, t=str(int(time.time()))
        )
        stream = os.popen("git rev-parse HEAD")
        git_hash = stream.read().strip()
        ingest_document_id = self.load_spec['ingest_document_id']
        subset = ingest_document_id.split(":")[2]
        lj_doc = {
            "id": self.load_job_id,
            "subset": subset,
            "type": "LJ",
            "lineageId": "madis",
            "script": __file__,
            "scriptVersion": git_hash,
            "loadSpec": self.spec_file,
            "note": "",
        }
        return lj_doc

    def close_cb(self):
        """
        close couchbase connection
        """
        if self.cluster:
            self.cluster.disconnect()

    def connect_cb(self):
        """
        create a couchbase connection and maintain the collection and cluster objects.
        """
        logging.info("%s: data_type_manager - Connecting to couchbase")
        # get a reference to our cluster
        # noinspection PyBroadException
        try:
            options = ClusterOptions(
                PasswordAuthenticator(
                    self.cb_credentials["user"], self.cb_credentials["password"]
                )
            )
            self.cluster = Cluster(
                "couchbase://" + self.cb_credentials["host"], options
            )
            self.collection = self.cluster.bucket("mdata").default_collection()
            logging.info("%s: Couchbase connection success")
        except Exception as _e:  # pylint:disable=broad-except
            logging.error("*** %s in connect_cb ***", str(_e))
            sys.exit("*** Error when connecting to mysql database: ")

    def get_file_list(self, df_query, directory, file_pattern):
        """This method accepts a file path (directory), a query statement (df_query),
        and a file pattern (file_pattern). It uses the df_query statement to retrieve a
        list of file {url:file_url, mtime:mtime} records from DataFile
        objects and compares the file names in the directory that match the file_pattern (using glob)
        to the file url list that is returned from the df_query.
        Any file names that are not in the returned url list are added and any files
        that are in the list but have newer mtime entries are also added.
        Args:
            df_query (string): this is a query statement that should return a list of {url:file_url, mtime:mtime}
            directory (string): The full path to a directory that contains files to be ingested
            file_pattern (string): A file glob pattern that matches the files desired.
        Raises:
            Exception: general exception
        """
        file_names = []
        try:
            result = self.cluster.query(df_query)
            df_elements = list(result)
            df_full_names = [ element['url'] for element in df_elements ]
            if os.path.exists(directory) and os.path.isdir(directory):
                file_list = sorted(glob(directory + os.path.sep + file_pattern), key=os.path.getmtime)
                for filename in file_list:
                    try:
                        # check to see if this file has already been ingested
                        # (if it is not in the df_full_names - add it)
                        if filename not in df_full_names:
                            file_names.append(filename)
                        else:
                            # it was already processed so check to see if the mtime of the
                            # file is greater than the mtime in the database entry, if so then add it
                            df_entry = next(element for element in df_elements if element["url"] == filename)
                            if os.path.getmtime(filename) > int(df_entry['mtime']):
                                file_names.append(filename)
                    except Exception as _e:  # pylint:disable=broad-except
                        # don't care, it just means it wasn't a properly formatted file per the mask
                        continue
            if len(file_names) == 0:
                raise Exception("No files to Process!")
            return file_names
        except Exception as e:
            logging.error(
                "%s get_file_list Error: %s",
                self.__class__.__name__,
                str(e),
            )
            return file_names

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
            self.load_spec["load_job_doc"] = self.build_load_job_doc()
            # get the ingest document id.
            # NOTE: in future we may make this (ingest_document_id) a list
            # and start each VxIngestManager with its own ingest_document_id
            self.ingest_document_id = self.load_spec["ingest_document_id"]
            # stash the fmask for future use
            self.load_spec = self.fmask
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
        file_names = []
        # get the urls (full_file_names) from all the datafiles for this type of ingest
        file_query = """
            SELECT url, mtime
            FROM mdata
            WHERE
            subset={subset}
            AND type='DF'
            AND fileType='netcdf'
            AND originType='madis' order by url;
            """.format(subset=self.ingest_document['subset'])
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
                )
                ingest_manager_list.append(ingest_manager_thread)
                ingest_manager_thread.start()
            except Exception as _e:  # pylint:disable=broad-except
                logging.error("*** Error in  VxIngestManager %s***", str(_e))
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

    def get_credentials(self, load_spec):
        """get credentials from a credentials file and puts them into the load_spec
        Args:
            load_spec (dict): [this is generated from the load spec file]
        Returns:
            [dict]: [the new load_spec with the credentials]
        """
        logging.debug("credentials filename is %s", self.credentials_file)
        try:
            # check for existence of file
            if not Path(self.credentials_file).is_file():
                sys.exit(
                    "*** credentials_file file "
                    + self.credentials_file
                    + " can not be found!"
                )
            _f = open(self.credentials_file)
            _yaml_data = yaml.load(_f, yaml.SafeLoader)
            load_spec["cb_connection"] = {}
            load_spec["cb_connection"]["host"] = _yaml_data["cb_host"]
            load_spec["cb_connection"]["user"] = _yaml_data["cb_user"]
            load_spec["cb_connection"]["password"] = _yaml_data["cb_password"]
            _f.close()
            return load_spec["cb_connection"]
        except (RuntimeError, TypeError, NameError, KeyError):
            logging.error("*** %s in read ***", sys.exc_info()[0])
            sys.exit("*** Parsing error(s) in load_spec file!")

    def main(self):
        """run_ingest_threads main entry. Manages a set of VxIngestManagers for processing input files"""
        logging.info("PYTHONPATH: %s", os.environ["PYTHONPATH"])
        args = parse_args(sys.argv[1:])
        self.runit(vars(args))
        sys.exit("*** FINISHED ***")


if __name__ == "__main__":
    VXIngest().main()
