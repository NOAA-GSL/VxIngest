"""
CommonVxIngest - parent class for all VxIngest classes
NOTE about threading, database connections, and pickling! Each VxIngest
runs in its own thread. Each VxIngest also needs to query the database to find out
if a load job has already been processed - so it needs a database connection.
Each Ingest manager (usually more than one) runs in its own thread which is
maintained by its Vxingest. It is impossible to pass the VxIngest database connection
to the VxIngestManager - i.e. accross python process objects (multithreading process objects)
because a database connection cannot be pickled (pythons name for object serialization).
Therefore the database credentials are stored in the load_spec, but not the database connection.
The database connection must be recreated in each process thread using the credentials that are
stored in the load_spec. It feels redundant and it is definitelty confusing but blame pythons
threading model.
"""

import logging
import sys
import os
import time
import json
from glob import glob
from pathlib import Path
import yaml
from couchbase.cluster import Cluster, ClusterOptions
from couchbase_core.cluster import PasswordAuthenticator


class CommonVxIngest:  # pylint: disable=too-many-arguments disable=too-many-instance-attributes
    """
    Parent class for all VxIngest.
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
        self.first_last_params = None
        self.output_dir = None
        self.load_job_id = None
        self.load_spec = {}
        self.ingest_document_id = None
        self.cb_credentials = {}
        self.collection = None
        self.cluster = None
        self.ingest_document_id = None
        self.ingest_document = None
        logging.getLogger().setLevel(logging.INFO)

    def parse_args(self, args):  # pylint: disable=missing-function-docstring
        """This method is intended to be overriden"""
        return args

    def runit(self, args):  # pylint: disable=missing-function-docstring
        pass

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

    def build_load_job_doc(self, lineage):
        """
        This method will build a load_job document
        """
        stream = os.popen("git rev-parse HEAD")
        git_hash = stream.read().strip()
        _document_id = (
            self.load_spec["ingest_document_id"]
            if "ingest_document_id" in self.load_spec.keys()
            else self.load_spec["ingest_document_ids"][0]
        )
        subset = _document_id.split(":")[2]
        self.load_job_id = "LJ:{s}:{m}:{c}:{t}".format(
            s=subset,
            m=self.__module__,
            c=self.__class__.__name__,
            t=str(int(time.time())),
        )
        lj_doc = {
            "id": self.load_job_id,
            "subset": subset,
            "type": "LJ",
            "lineageId": lineage,
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
            # stash the credentials for the VxIngestManager - see NOTE at the top of this file.
            self.load_spec["cb_credentials"] = self.cb_credentials
            logging.info("%s: Couchbase connection success")
        except Exception as _e:  # pylint:disable=broad-except
            logging.error("*** %s in connect_cb ***", str(_e))
            sys.exit("*** Error when connecting to cb database: ")

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
            # it is possible to set a query option for scan consistency but it makes this operation really slow.
            # In actual operation the ingest will do a bulk insert and wait for a period of time before getting
            # around to processing another file. I think the risk of reprocessing a file because the DF record is
            # not fully persisted is small enough to not burden the ingest with a scan consistency check.
            # Things like tests or special ingest operations may need to wait for consistency. In that case do another query with
            # scan consistency set outside of this operation.
            result = self.cluster.query(df_query)
            df_elements = list(result)
            df_full_names = [element["url"] for element in df_elements]
            if os.path.exists(directory) and os.path.isdir(directory):
                file_list = sorted(
                    glob(directory + os.path.sep + file_pattern), key=os.path.getmtime
                )
                for filename in file_list:
                    try:
                        # check to see if this file has already been ingested
                        # (if it is not in the df_full_names - add it)
                        if filename not in df_full_names:
                            logging.info(
                                    "%s - File %s is added because it isn't in df_full_names - %s",
                                    self.__class__.__name__,
                                    filename,
                                    str(df_full_names),
                                )
                            file_names.append(filename)
                        else:
                            # it was already processed so check to see if the mtime of the
                            # file is greater than the mtime in the database entry, if so then add it
                            df_entry = next(
                                element
                                for element in df_elements
                                if element["url"] == filename
                            )
                            if os.path.getmtime(filename) > int(df_entry["mtime"]):
                                logging.info(
                                    "%s - File %s is added because file mtime %s is greater than df mtime %s",
                                    self.__class__.__name__,
                                    filename,
                                    os.path.getmtime(filename),
                                    int(df_entry["mtime"]),
                                )
                                file_names.append(filename)
                            else:
                                logging.info(
                                    "%s - File %s has already been processed - not adding",
                                    self.__class__.__name__,
                                    filename,
                                )
                    except Exception as _e:  # pylint:disable=broad-except
                        # don't care, it just means it wasn't a properly formatted file per the mask
                        continue
            if len(file_names) == 0:
                raise Exception("No files to Process!")
            return file_names
        except Exception as _e:  # pylint: disable=bare-except, disable=broad-except
            logging.error(
                "%s get_file_list Error: %s",
                self.__class__.__name__,
                str(_e),
            )
            return file_names

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
        args = self.parse_args(sys.argv[1:])
        self.runit(vars(args))
        sys.exit("*** FINISHED ***")
