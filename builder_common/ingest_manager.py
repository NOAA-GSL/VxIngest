"""
CommonVxIngestManager - Parent class for all VxIngestManager classes
"""

import logging
import sys
import os
import queue
from multiprocessing import Process
import time
import json
from pathlib import Path
from couchbase.exceptions import TimeoutException
from couchbase.cluster import Cluster, ClusterOptions
from couchbase_core.cluster import PasswordAuthenticator


class CommonVxIngestManager(Process):  # pylint:disable=too-many-instance-attributes
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
        element_queue,
        output_dir,
        number_stations=sys.maxsize,
    ):
        """constructor for VxIngestManager
        Args:
            name (string): the thread name for this IngestManager
            load_spec (Object): contains Couchbase credentials
            ingest_document (Object): the ingest document
            element_queue (Queue): reference to the element Queue
            output_dir (string): output directory path
            number_stations (int, optional): limit the number of stations to process (debugging). Defaults to sys.maxsize.
        """
        # The Constructor for the RunCB class.
        Process.__init__(self)
        self.thread_name = name
        self.load_spec = load_spec
        self.ingest_document = ingest_document
        # some managers operate on a list of keys and some on a single key, capture both cases
        self.ingest_document_ids = (
            self.load_spec["ingest_document_ids"]
            if "ingest_document_ids" in self.load_spec.keys()
            else None
        )
        self.ingest_document_id = (
            self.load_spec["ingest_document_id"]
            if "ingest_document_id" in self.load_spec.keys()
            else None
        )
        self.ingest_type_builder_name = None
        self.queue = element_queue
        self.builder_map = {}
        self.cluster = None
        self.collection = None
        self.output_dir = output_dir
        self.number_stations = number_stations

    def process_queue_element(
        self, queue_element
    ):  # pylint: disable=missing-function-docstring
        pass

    def close_cb(self):
        """
        close couchbase connection
        """
        if self.cluster:
            self.cluster.disconnect()

    def connect_cb(self):
        """
        create a couchbase connection and maintain the collection and cluster objects.
        See the note at the top of vx_ingest.py for an explanation of why this seems redundant.
        """
        logging.info("data_type_manager - Connecting to couchbase")
        # get a reference to our cluster
        # noinspection PyBroadException
        try:
            options = ClusterOptions(
                PasswordAuthenticator(
                    self.load_spec["cb_credentials"]["user"],
                    self.load_spec["cb_credentials"]["password"],
                )
            )
            self.cluster = Cluster(
                "couchbase://" + self.load_spec["cb_credentials"]["host"], options
            )
            self.collection = self.cluster.bucket("mdata").default_collection()
            # stash the database connection for the builders to reuse
            self.load_spec["cluster"] = self.cluster
            self.load_spec["collection"] = self.collection
            logging.info("Couchbase connection success")
        except Exception as _e:  # pylint:disable=broad-except
            logging.error("*** %s in connect_cb ***", str(_e))
            sys.exit("*** Error when connecting to cb database: ")

    # entry point of the thread. Is invoked automatically when the thread is
    # started.

    def run(self):
        """
        This is the entry point for the IngestManager thread. It runs an
        infinite loop that only terminates when the element_queue is
        empty. For each enqueued element it calls
        process_queue_element with the queue_element and the couchbase
        connection to process the file.
        """
        # noinspection PyBroadException
        try:
            logging.getLogger().setLevel(logging.INFO)
            # get a connection
            self.connect_cb()
            # infinite loop terminates when the file_name_queue is empty
            empty_count = 0
            while True:
                try:
                    queue_element = self.queue.get_nowait()
                    logging.info(
                        self.thread_name
                        + ": IngestManager - processing "
                        + queue_element
                    )
                    self.process_queue_element(queue_element)
                    logging.info(
                        self.thread_name
                        + ": IngestManager - finished processing "
                        + queue_element
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
                            self.thread_name,
                        )
                        break
        except Exception as _e:  # pylint:disable=broad-except
            logging.exception(
                "%s: *** Error in IngestManager run ***", self.thread_name
            )
            raise _e
        finally:
            logging.info("%s: IngestManager finished", self.thread_name)

    def write_document_to_cb(self, queue_element, document_map):
        """This method writes the current document directly to couchbase
        Args:
            queue_element
            document_map (object): this object contains the output documents that will be upserted into couchbase
        Raises:
            _e: generic exception
        """
        # The document_map is all built now so write all the
        # documents in the document_map into couchbase
        # noinspection PyBroadException
        try:
            logging.info(
                "process_element writing documents for queue_element :%s  with threadName: %s",
                str(queue_element),
                self.thread_name,
            )
            # this call is volatile i.e. it might change syntax in
            # the future.
            # if it does, please just fix it.
            upsert_start_time = int(time.time())
            logging.info(
                "process_element - executing upsert: stop time: %s",
                str(upsert_start_time),
            )
            if not document_map:
                logging.info(
                    "%s: process_element: would upsert documents but DOCUMENT_MAP IS EMPTY",
                    self.thread_name,
                )
            else:
                try:
                    self.collection.upsert_multi(document_map)
                except TimeoutException:
                    logging.info(
                        "process_element - trying upsert: Got TimeOutException -  Document may not be persisted."
                    )
            upsert_stop_time = int(time.time())
            logging.info(
                "process_element - executing upsert: stop time: %s",
                str(upsert_stop_time),
            )
            logging.info(
                "process_element - executing upsert: elapsed time: %s",
                str(upsert_stop_time - upsert_start_time),
            )
        except Exception as _e:  # pylint:disable=broad-except
            logging.exception(
                "%s: *** Error writing to Couchbase: in process_element writing document ***",
                self.thread_name,
            )
            raise _e

    def write_document_to_files(self, file_name, document_map):
        """This method writes the current document directly to couchbase
        Args:
            file_name: the name to use for the files
            document_map (object): this object contains the output documents that will be upserted into couchbase
        Raises:
            _e: generic exception
        """
        try:
            logging.info(
                "%s: write_document_to_files output %s ingest_document :  ",
                self.thread_name,
                self.output_dir,
            )
            write_start_time = int(time.time())
            logging.info(
                "write_document_to_files - executing write: start time: %s",
                str(write_start_time),
            )
            if not document_map:
                logging.info(
                    "%s: write_document_to_files: would write documents but DOCUMENT_MAP IS EMPTY",
                    self.thread_name,
                )
            else:
                Path(self.output_dir).mkdir(parents=True, exist_ok=True)
                try:
                    file_name = os.path.basename(file_name) + ".json"
                    complete_file_name = os.path.join(self.output_dir, file_name)
                    # how many documents are we writing? Log it for alert
                    num_documents = len(list(document_map.values()))
                    logging.info(
                        "%s: write_document_to_files writing %s documents into %s",
                        self.thread_name,
                        num_documents,
                        complete_file_name,
                    )
                    _f = open(complete_file_name, "w")
                    # we need to write out a list of the values of the _document_map for cbimport
                    _f.write(json.dumps(list(document_map.values())))
                    _f.close()
                except Exception as _e1:  # pylint:disable=broad-except
                    logging.exception("write_document_to_files - trying write: Got Exception")
            write_stop_time = int(time.time())
            logging.info(
                "write_document_to_files - executing file write: stop time: %s",
                str(write_stop_time),
            )
            logging.info(
                "write_document_to_files - executing file write: elapsed time: %s",
                str(write_stop_time - write_start_time),
            )
        except Exception as _e:  # pylint:disable=broad-except
            logging.exception(
                ": *** %s Error writing to files: in process_element writing document***",
                self.thread_name,
            )
            raise _e