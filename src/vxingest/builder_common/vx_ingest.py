"""
CommonVxIngest - parent class for all VxIngest classes
NOTE about threading, database connections, and pickling! Each VxIngest
runs in its own thread. Each VxIngest also needs to query the database to find out
if a load job has already been processed - so it needs a database connection.
Each Ingest manager (usually more than one) runs in its own thread which is
maintained by its Vxingest. It is impossible to pass the VxIngest database connection
to the VxIngestManager - i.e. across python process objects (multithreading process objects)
because a database connection cannot be pickled (pythons name for object serialization).
Therefore the database credentials are stored in the load_spec, but not the database connection.
The database connection must be recreated in each process thread using the credentials that are
stored in the load_spec. It feels redundant and it is definitely confusing but blame pythons
threading model.
"""

import datetime as dt
import json
import logging
import os
import pathlib
import sys
import time

# This pyproj import has to remain here in order to enforce the
# order of loading of the pyproj and couchbase libraries.  If pyproj is loaded after
# the couchbase library, it will cause a segmentation fault.
# pyproj is used by the grib2_to_cb IngestManger and supporting
# test code. The root cause of this is Couchbase. This incompatibility is supposed to be fixed
# in the next release of Couchbase.
import pyproj  # noqa: F401
import yaml
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import CouchbaseException
from couchbase.options import ClusterOptions, ClusterTimeoutOptions

# Get a logger with this module's name to help with debugging
logger = logging.getLogger(__name__)


class CommonVxIngest:
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

    def parse_args(self, args):
        """This method is intended to be overridden"""
        return args

    def runit(self, args):
        pass

    def write_load_job_to_files(self):
        """
        write all the documents in the document_map into files in the output_dir
        """
        try:
            pathlib.Path(self.output_dir).mkdir(parents=True, exist_ok=True)
            try:
                file_name = self.load_job_id + ".json"
                complete_file_name = pathlib.Path(self.output_dir) / file_name
                with pathlib.Path(complete_file_name).open("w", encoding="utf-8") as _f:
                    _f.write(json.dumps([self.load_spec["load_job_doc"]]))
            except Exception as _e:
                logger.info(
                    "process_file - trying write load_job: Got Exception - %s", str(_e)
                )
        except Exception as _e:
            logger.error(": *** Error writing load_job to files: %s***", str(_e))
            raise _e

    def build_load_job_doc(self, lineage):
        """
        This method will build a load_job document
        """
        # stream = os.popen("git rev-parse HEAD")
        # git_hash = stream.read().strip()
        git_hash = os.environ.get("COMMIT", "unknown")
        _document_id = (
            self.load_spec["ingest_document_ids"][0]
            if "ingest_document_ids" in self.load_spec
            else None
        )
        subset = _document_id.split(":")[2]
        self.load_job_id = f"LJ:{subset}:{self.__module__}:{self.__class__.__name__}:{str(int(time.time()))}"
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
            self.cluster.close()

    def connect_cb(self):
        """
        create a couchbase connection and maintain the collection and cluster objects.
        """
        logger.debug("%s: data_type_manager - Connecting to couchbase")
        # get a reference to our cluster

        try:
            timeout_options = ClusterTimeoutOptions(
                kv_timeout=dt.timedelta(seconds=25),
                query_timeout=dt.timedelta(seconds=120),
            )
            options = ClusterOptions(
                PasswordAuthenticator(
                    self.cb_credentials["user"], self.cb_credentials["password"]
                ),
                timeout_options=timeout_options,
            )
            _attempts = 0
            while _attempts < 3:
                try:
                    self.cluster = Cluster(self.cb_credentials["host"], options)
                    break
                except CouchbaseException as _e:
                    time.sleep(5)
                    _attempts = _attempts + 1
            if _attempts == 3:
                raise CouchbaseException(
                    "Could not connect to couchbase after 3 attempts"
                )
            self.collection = self.cluster.bucket(
                self.cb_credentials["bucket"]
            ).collection(self.cb_credentials["collection"])
            # stash the credentials for the VxIngestManager - see NOTE at the top of this file.
            self.load_spec["cb_credentials"] = self.cb_credentials
            logger.info("%s: Couchbase connection success")
        except Exception as _e:
            logger.exception(
                "*** builder_common.CommonVxIngest Error in connect_cb *** %s", str(_e)
            )
            sys.exit(
                "*** builder_common.CommonVxIngest Error when connecting to cb database: "
            )

    def get_file_list(self, df_query, directory, file_pattern, file_mask):
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
            if pathlib.Path(directory).exists() and pathlib.Path(directory).is_dir():
                file_list = sorted(
                    pathlib.Path(directory).glob(file_pattern), key=os.path.getmtime
                )
                for filename in file_list:
                    try:
                        # test to see if the first part of this filename can be parsed into a valid date using the fmask, i.e. it is a valid file name
                        try:
                            _d = dt.datetime.strptime(
                                (pathlib.PurePath(filename).name).split(".")[0],
                                file_mask,
                            )
                        except ValueError:
                            continue
                        # check to see if this file has already been ingested
                        # (if it is not in the df_full_names - add it)
                        if str(filename) not in df_full_names:
                            logger.debug(
                                "%s - File %s is added because it isn't in any datafile document",
                                self.__class__.__name__,
                                filename,
                            )
                            file_names.append(str(filename))
                        else:
                            # it was already processed so check to see if the mtime of the
                            # file is greater than the mtime in the database entry, if so then add it
                            df_entry = next(
                                element
                                for element in df_elements
                                if element["url"] == str(filename)
                            )
                            if int(filename.stat().st_mtime) > int(df_entry["mtime"]):
                                logger.debug(
                                    "%s - File %s is added because file mtime %s is greater than df mtime %s",
                                    self.__class__.__name__,
                                    filename,
                                    int(filename.stat().st_mtime),
                                    int(df_entry["mtime"]),
                                )
                                file_names.append(str(filename))
                            else:
                                logger.debug(
                                    "%s - File %s has already been processed - not adding",
                                    self.__class__.__name__,
                                    filename,
                                )
                    except Exception as _e:
                        # don't care, it just means it wasn't a properly formatted file per the mask
                        continue
            if len(file_names) == 0:
                logger.info("get_file_list: No files to Process!")
            return file_names
        except Exception as _e:
            logger.error(
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
        logger.debug("credentials filename is %s", self.credentials_file)
        try:
            # check for existence of file
            if not pathlib.Path(self.credentials_file).is_file():
                sys.exit(
                    "*** credentials_file file "
                    + self.credentials_file
                    + " can not be found!"
                )
            with pathlib.Path(self.credentials_file).open(encoding="utf-8") as _f:
                _yaml_data = yaml.load(_f, yaml.SafeLoader)
            load_spec["cb_connection"] = {}
            load_spec["cb_connection"]["host"] = _yaml_data["cb_host"]
            load_spec["cb_connection"]["user"] = _yaml_data["cb_user"]
            load_spec["cb_connection"]["password"] = _yaml_data["cb_password"]
            load_spec["cb_connection"]["bucket"] = _yaml_data["cb_bucket"]
            load_spec["cb_connection"]["collection"] = _yaml_data["cb_collection"]
            load_spec["cb_connection"]["scope"] = _yaml_data["cb_scope"]
            return load_spec["cb_connection"]
        except (RuntimeError, TypeError, NameError, KeyError) as e:
            logger.error(f"*** Error reading credential file: {e} ***")
            sys.exit("*** Parsing error(s) in load_spec file!")

    def main(self):
        """run_ingest_threads main entry. Manages a set of VxIngestManagers for processing input files"""
        logger.info("PYTHONPATH: %s", os.environ["PYTHONPATH"])
        args = self.parse_args(sys.argv[1:])
        self.runit(vars(args))
        sys.exit(0)
