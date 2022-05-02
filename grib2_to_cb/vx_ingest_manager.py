"""
Program Name: Class IngestManager
Contact(s): Randy Pierce
Abstract:

History Log:  Initial version

Usage: The IngestManager extends Process - python multiprocess thread -
and runs as a Process and pulls from a queue of file names. It uses the collection and the cluster
objects that are passed from the run_ingest_threads (VXIngest class).
It finishes when the file_name_queue is empty.

It gets file names serially from a queue that is shared by a
thread pool of data_type_manager's and processes them one at a a_time. It gets
the concrete builder type from the metadata document and uses a
concrete builder to process the file.

The builders are instantiated once and kept in a map of objects for the
duration of the programs life. For IngestManager it is likely that
each file will require only one builder type to be instantiated.
When IngestManager finishes a document specification  it  writes the document to the output directory,
if an output directory was specified.

        Attributes:
            name a threadName for logging and debugging purposes.
            load_spec a load_spec object contains ingest_document_id, credentials, and first and last epoch
            ingest_document an ingest document from the database
            file_name_queue a shared queue of filenames.
            output_dir where the output documents will be written
            collection couchbase collection object for data service access
            cluster couchbase cluster object for query service access
            number_stations=sys.maxsize (you can limit how many stations will be processed - for debugging)

Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSD
"""
import json
import logging
import os
import queue
import sys
import time
from multiprocessing import Process
from pathlib import Path

from couchbase.cluster import Cluster, ClusterOptions
from couchbase.exceptions import \
    TimeoutException  # pylint:disable=ungrouped-imports
from couchbase_core.cluster import PasswordAuthenticator

from grib2_to_cb import grib_builder


class VxIngestManager(Process):  # pylint:disable=too-many-instance-attributes
    """
    IngestManager is a Process Thread that manages an object pool of
    builders to ingest data from GSD grib2 files or netcdf files into documents that can be
    inserted into couchbase.

    This class will process data by reading an ingest_document_id
    and instantiating a builder class of the type specified in the
    ingest_document.
    The ingest document specifies the builder class, and a template that defines
    how to place the variable values into a couchbase document and how to
    construct the couchbase data document id.

    It will then read file_names, one by one,
    from the file_name_queue.  The builders use the template to create documents for
    each filename and put them into the document map.

    When all of the result set entries for a file are processed, the IngestManager upserts
    the document(s) to couchbase, or writes to an output directory and retrieves a new filename from
    the queue and starts over.

    Each builder is kept in an object pool so that they do not need to be re instantiated.
    When the queue has been emptied the IngestManager closes its connections
    and dies.
    """

    # pylint:disable=too-many-arguments
    def __init__(
        self,
        name,
        load_spec,
        ingest_document,
        file_name_queue,
        output_dir,
        number_stations=sys.maxsize,
    ):
        """constructor for VxIngestManager
        Args:
            name (str): the thread name for this IngestManager
            load_spec (Object): contains Couchbase credentials
            ingest_document (Object): the ingest document
            file_name_queue (Queue): reference to the ingest Queue
            output_dir (string): output directory path
            number_stations (int, optional): limit the number of stations to process (debugging). Defaults to sys.maxsize.
        """
        # The Constructor for the RunCB class.
        Process.__init__(self)
        self.thread_name = name
        self.load_spec = load_spec
        self.ingest_document = ingest_document
        self.cb_credentials = self.load_spec["cb_connection"]
        self.ingest_document_id = self.load_spec["ingest_document_id"]
        self.ingest_type_builder_name = None
        self.queue = file_name_queue
        self.builder_map = {}
        self.cluster = None
        self.collection = None
        self.output_dir = output_dir
        self.number_stations = number_stations
        self.cb_credentials = {}
        self.cb_credentials["host"] = load_spec["cb_connection"]["host"]
        self.cb_credentials["user"] = load_spec["cb_connection"]["user"]
        self.cb_credentials["password"] = load_spec["cb_connection"]["password"]

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
            logging.exception("*** %s in connect_cb ***")
            sys.exit("*** Error when connecting to mysql database: ")

    # entry point of the thread. Is invoked automatically when the thread is
    # started.

    def run(self):
        """
        This is the entry point for the IngestManager thread. It runs an
        infinite loop that only terminates when the file_name_queue is
        empty. For each enqueued file name it calls
        process_file with the file_name and the couchbase
        connection to process the file.
        """
        # noinspection PyBroadException
        try:
            logging.getLogger().setLevel(logging.INFO)
            # Read the ingest document
            # get the document from couchbase
            # noinspection PyBroadException
            try:
                self.ingest_type_builder_name = self.ingest_document["builder_type"]
            except Exception as _e:  # pylint:disable=broad-except
                logging.exception(
                    "%s: process_file: Exception getting ingest document",
                    self.thread_name
                )
                sys.exit("*** Error getting ingest document ***")
            # get a connection
            self.connect_cb()
            # infinite loop terminates when the file_name_queue is empty
            empty_count = 0
            while True:
                try:
                    file_name = self.queue.get_nowait()
                    logging.info(
                        self.thread_name
                        + ": IngestManager - processing file "
                        + file_name
                    )
                    self.process_file(file_name)
                    logging.info(
                        self.thread_name
                        + ": IngestManager - finished processing file "
                        + file_name
                    )
                    self.queue.task_done()
                except queue.Empty:
                    # three strikes and your out! finished! kaput!
                    if empty_count < 3:
                        empty_count += 1
                        time.sleep(1)
                        continue
                    else:
                        logging.info(
                            "%s: IngestManager - Queue empty - disconnecting couchbase",
                            self.thread_name
                        )
                        break
        except Exception as _e:  # pylint:disable=broad-except
            logging.exception(
                "%s: *** Error in IngestManager run ***", self.thread_name)
            raise _e
        finally:
            self.close_cb()
            logging.info("%s: IngestManager finished", self.thread_name)

    def process_file(self, file_name):
        """Process this grib file
        Args:
            file_name (string): name of the grib file
        Raises:
            _e: [description]
        """
        # get or instantiate the builder
        # noinspection PyBroadException
        start_process_time = int(time.time())
        document_map = {}
        # noinspection PyBroadException
        try:
            logging.info("process_file - : start time: %s", str(start_process_time))
            if self.ingest_type_builder_name in self.builder_map.keys():
                builder = self.builder_map[self.ingest_type_builder_name]
            else:
                builder_class = getattr(grib_builder, self.ingest_type_builder_name)
                builder = builder_class(
                    self.load_spec,
                    self.ingest_document,
                    self.cluster,
                    self.collection,
                    self.number_stations,
                )
                self.builder_map[self.ingest_type_builder_name] = builder
            document_map = builder.build_document(file_name)
            if self.output_dir:
                self.write_document_to_files(file_name, document_map)
            else:
                self.write_document_to_cb(file_name, document_map)
        except Exception as _e:  # pylint:disable=broad-except
            logging.exception(
                "%s: Exception in builder: %s",
                self.thread_name,
                str(self.ingest_type_builder_name)
            )
            raise _e
        finally:
            # reset the document map and record stop time
            stop_process_time = int(time.time())
            document_map = {}
            logging.info(
                "IngestManager.process_file: elapsed time: %s",
                str(stop_process_time - start_process_time),
            )

    def write_document_to_cb(self, file_name, document_map):
        """This method writes the current document directly to couchbase
        Args:
            file_name: The basename of the input file
            document_map (object): this object contains the output documents that will be upserted into couchbase
        Raises:
            _e: generic exception
        """
        # The document_map is all built now so write all the
        # documents in the document_map into couchbase
        # noinspection PyBroadException
        try:
            logging.info(
                "%s: process_file writing documents for ingest_document :%s  with threadName: %s",
                self.thread_name,
                str(file_name),
                self.thread_name,
            )
            # this call is volatile i.e. it might change syntax in
            # the future.
            # if it does, please just fix it.
            upsert_start_time = int(time.time())
            logging.info(
                "process_file - executing upsert: stop time: %s", str(upsert_start_time)
            )
            if not document_map:
                logging.info(
                    "%s: process_file: would upsert documents but DOCUMENT_MAP IS EMPTY",
                    self.thread_name,
                )
            else:
                try:
                    self.collection.upsert_multi(document_map)
                except TimeoutException:
                    logging.info(
                        "process_file - trying upsert: Got TimeOutException -  Document may not be persisted."
                    )
            upsert_stop_time = int(time.time())
            logging.info(
                "process_file - executing upsert: stop time: %s", str(upsert_stop_time)
            )
            logging.info(
                "process_file - executing upsert: elapsed time: %s",
                str(upsert_stop_time - upsert_start_time),
            )
        except Exception as _e:  # pylint:disable=broad-except
            logging.exception(
                "%s: *** Error writing to Couchbase: in process_file writing document ***",
                self.thread_name
            )
            raise _e

    def write_document_to_files(self, file_name, document_map):
        """This method writes the current document directly to couchbase
        Args:
            file_name: The basename of the input file
            document_map (object): this object contains the output documents that will be upserted into couchbase
        Raises:
            _e: generic exception
        """
        try:
            logging.info(
                "%s: process_file writing documents into %s ingest_document :  ",
                self.thread_name,
                self.output_dir,
            )
            write_start_time = int(time.time())
            logging.info(
                "process_file - executing write: start time: %s", str(write_start_time)
            )
            if not document_map:
                logging.info(
                    "%s: process_file: would write documents but DOCUMENT_MAP IS EMPTY",
                    self.thread_name,
                )
            else:
                Path(self.output_dir).mkdir(parents=True, exist_ok=True)
                try:
                    file_name = os.path.basename(file_name) + ".json"
                    complete_file_name = os.path.join(self.output_dir, file_name)
                    logging.info(
                        "%s: process_file writing documents into %s",
                        self.thread_name,
                        complete_file_name,
                    )
                    _f = open(complete_file_name, "w")
                    # we need to write out a list of the values of the _document_map for cbimport
                    _f.write(json.dumps(list(document_map.values())))
                    _f.close()
                except Exception as _e1:  # pylint:disable=broad-except
                    logging.exception("process_file - trying write: Got Exception")
            write_stop_time = int(time.time())
            logging.info(
                "process_file - executing file write: stop time: %s",
                str(write_stop_time),
            )
            logging.info(
                "process_file - executing file write: elapsed time: %s",
                str(write_stop_time - write_start_time),
            )
        except Exception as _e:  # pylint:disable=broad-except
            logging.exception(
                ": *** %s Error writing to files: in process_file writing document***",
                self.thread_name)
            raise _e
