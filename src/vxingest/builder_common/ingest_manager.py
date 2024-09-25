"""
CommonVxIngestManager - Parent class for all VxIngestManager classes
"""

import json
import logging
import os
import queue
import sys
import time
from datetime import timedelta
from multiprocessing import Process
from pathlib import Path

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import CouchbaseException, TimeoutException
from couchbase.options import ClusterOptions, ClusterTimeoutOptions

logger = logging.getLogger(__name__)


class CommonVxIngestManager(Process):
    """
    IngestManager is a Process Thread that manages an object pool of
    builders to ingest data from GSD grib2 files or netcdf files into documents that can be
    inserted into couchbase.

    It will read queue_elements, one by one,
    from the element_queue.  Elements might be file names or they might be ingest_document_ids.
    The builders use the template to create documents for
    each filename and put them into the document map.

    When all of the result set entries for a file are processed, the IngestManager upserts
    the document(s) to couchbase, or writes to an output directory and retrieves a new filename from
    the queue and starts over.

    Each builder is kept in an object pool so that they do not need to be re instantiated.
    When the queue has been emptied the IngestManager closes its connections
    and dies.
    """

    def __init__(
        self,
        name,
        load_spec,
        element_queue,
        output_dir,
        logging_queue,
        logging_configurer,
    ):
        """constructor for VxIngestManager
        Args:
            name (string): the thread name for this IngestManager
            load_spec (Object): contains Couchbase credentials
            element_queue (Queue): reference to the element Queue
            output_dir (string): output directory path
        """
        # The Constructor for the RunCB class.
        Process.__init__(self)
        self.thread_name = name
        self.load_spec = load_spec
        self.ingest_type_builder_name = None
        self.queue = element_queue
        self.builder_map = {}
        self.cb_credentials = self.cb_credentials
        self.cluster = None
        self.collection = None
        self.output_dir = output_dir
        self.logging_queue = logging_queue
        self.logging_configurer = logging_configurer

        if not Path(self.output_dir).exists():
            Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        if not os.access(self.output_dir, os.W_OK):
            _re = RuntimeError("Output directory: %s is not writable!", self.output_dir)
            logger.exception(_re)
            raise _re

    def process_queue_element(self, queue_element):
        pass

    def close_cb(self):
        """
        close couchbase connection
        """
        if self.cluster:
            self.cluster.close()
        self.cluster = None
        self.collection = None

    def connect_cb(self):
        """
        create a couchbase connection and maintain the collection and cluster objects.
        See the note at the top of vx_ingest.py for an explanation of why this seems redundant.
        """
        logger.info("data_type_manager - Connecting to couchbase")
        # get a reference to our cluster

        try:
            timeout_options = ClusterTimeoutOptions(
                kv_timeout=timedelta(seconds=25),
                query_timeout=timedelta(seconds=120),
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
            # stash the database connection for the builders to reuse
            self.load_spec["cluster"] = self.cluster
            self.load_spec["collection"] = self.collection
            logger.info("Couchbase connection success")
        except Exception as _e:
            logger.exception(
                "*** builder_common.CommonVxIngestManager in connect_cb ***"
            )
            sys.exit(
                "*** builder_common.CommonVxIngestManager Error when connecting to cb database"
            )

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
        # Configure this Process's logger
        self.logging_configurer(self.logging_queue)
        logger.info(f"Registered new process: {self.thread_name}")

        try:
            self.cb_credentials = self.load_spec["cb_connection"]
            # get a connection
            self.connect_cb()
            # infinite loop terminates when the file_name_queue is empty
            empty_count = 0
            while True:
                try:
                    queue_element = self.queue.get_nowait()
                    logger.info(
                        self.thread_name
                        + ": IngestManager - processing "
                        + queue_element
                    )
                    if queue_element is not None:
                        # it seems it is possible to have an empty queue_element
                        # but we cannot process one so skip it
                        self.process_queue_element(queue_element)
                        logger.info(
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
                        logger.info(
                            "%s: IngestManager - Queue empty - disconnecting couchbase",
                            self.thread_name,
                        )
                        break
        except Exception as _e:
            logger.exception("%s: *** Error in IngestManager run ***", self.thread_name)
            raise _e
        finally:
            logger.info("%s: IngestManager finished", self.thread_name)

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

        try:
            logger.info(
                "process_element writing documents for queue_element :%s  with threadName: %s",
                str(queue_element),
                self.thread_name,
            )
            # this call is volatile i.e. it might change syntax in
            # the future.
            # if it does, please just fix it.
            upsert_start_time = int(time.time())
            logger.info(
                "process_element - executing upsert: stop time: %s",
                str(upsert_start_time),
            )
            if not document_map:
                logger.info(
                    "%s: process_element: would upsert documents but DOCUMENT_MAP IS EMPTY",
                    self.thread_name,
                )
            else:
                try:
                    self.collection.upsert_multi(document_map)
                except TimeoutException:
                    logger.info(
                        "process_element - trying upsert: Got TimeOutException -  Document may not be persisted."
                    )
            upsert_stop_time = int(time.time())
            logger.info(
                "process_element - executing upsert: stop time: %s",
                str(upsert_stop_time),
            )
            logger.info(
                "process_element - executing upsert: elapsed time: %s",
                str(upsert_stop_time - upsert_start_time),
            )
        except Exception as _e:
            logger.exception(
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
            logger.info(
                "%s: write_document_to_files output %s:  ",
                self.thread_name,
                self.output_dir,
            )
            if not document_map:
                logger.info(
                    "%s: write_document_to_files: would write documents but DOCUMENT_MAP IS EMPTY",
                    self.thread_name,
                )
            else:
                Path(self.output_dir).mkdir(parents=True, exist_ok=True)
                try:
                    file_name = Path(file_name).name + ".json"
                    complete_file_name = Path(self.output_dir) / file_name
                    # how many documents are we writing? Log it for alert
                    num_documents = len(list(document_map.values()))
                    logger.info(
                        "%s: write_document_to_files writing %s documents into %s",
                        self.thread_name,
                        num_documents,
                        complete_file_name,
                    )
                    with Path(complete_file_name).open("w", encoding="utf-8") as _f:
                        # we need to write out a list of the values of the _document_map for cbimport
                        json_data = json.dumps(list(document_map.values()))
                        _f.write(json_data)
                except Exception as _e1:
                    logger.exception(
                        "write_document_to_files - trying write: Got Exception %s",
                        str(_e1),
                    )
        except Exception as _e:
            logger.exception(
                ": *** {self.thread_name} Error writing to files: in process_element writing document*** %s",
                str(_e),
            )
            raise _e
