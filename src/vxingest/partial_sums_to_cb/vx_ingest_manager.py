"""
Program Name: Class IngestManager
Contact(s): Randy Pierce
Abstract:

History Log:  Initial version

Usage: The IngestManager extends Process - python multiprocess thread -
and runs as a Process and pulls from a queue of ingest document ids. It
maintains its own connection to couchbase which it keeps open until it finishes.

It finishes and closes its database connection when the queue is
empty.

It gets ingest document ids serially from a queue that is shared by a
thread pool of data_type_manager's and processes them one at a a_time. It gets
the concrete builder type from the metadata document and uses a
concrete builder to process the file.

The builders are instantiated once and kept in a map of objects for the
duration of the programs life.  When IngestManager finishes a document
specification it  either "upserts" a document_map to the couchbase database
or it writes the document to an output directory.

        Attributes:
            queue - a shared queue of ingest document ids.
            threadName - a threadName for logging and debugging purposes.
            cb_credentials - a set of cb_credentials that
            the DataTypeManager will use to connect to the database. This
            connection will be maintained until the thread terminates.
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSD
"""
import logging
import sys
import time

from vxingest.builder_common.ingest_manager import CommonVxIngestManager
from vxingest.partial_sums_to_cb import partial_sums_builder as my_builder

# Get a logger with this module's name to help with debugging
logger = logging.getLogger(__name__)


class VxIngestManager(CommonVxIngestManager):
    """
    IngestManager is a Process Thread that manages an object pool of
    builders to ingest data from GSD couchbase documents, producing new documents
    that can be inserted into couchbase or written to json files in the specified output directory.

    This class will process data by reading an ingest_document
    and instantiating a builder class of the type specified in the
    ingest_document.

    The ingest document specifies the builder class, and a template that defines how to
    place the variable values into a couchbase document and how to construct the couchbase data document id.

    It will then read model and obs documents from the data base and use the template to create
    contingency count documents for each fcstValidEpoch that falls between the first_epoch and the
    last_epoch.

    When all of the result set entries for a file are processed, the IngestManager upserts
    the document(s) to couchbase or writes the json into the output directory, retrieves
    a new ingest document from the queue and starts over.

    Each builder is kept in an object pool so that they do not need to
    be re instantiated.
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
        self.thread_name = name
        self.load_spec = load_spec
        self.cb_credentials = self.load_spec["cb_connection"]
        self.ingest_document_ids = self.load_spec["ingest_document_ids"]
        self.ingest_document = None
        self.ingest_type_builder_name = None
        self.queue = element_queue
        self.builder_map = {}
        self.cluster = None
        self.collection = None
        self.output_dir = output_dir

        super().__init__(
            self.thread_name,
            self.load_spec,
            self.queue,
            self.output_dir,
            logging_queue,
            logging_configurer,
        )

    def set_builder_name(self, queue_element):
        """
        get the builder name from the ingest document
        """
        if queue_element is None:
            raise Exception("ingest_document is undefined")
        try:
            self.ingest_type_builder_name = self.load_spec["ingest_documents"][
                queue_element
            ]["builder_type"]
        except Exception as _e:
            logger.exception(
                "%s: process_element: Exception getting ingest document for %s",
                self.thread_name,
                queue_element,
            )
            raise _e

    def process_queue_element(self, queue_element):
        """Process this queue_element
        Args:
            queue_element (string): queue_element
        Raises:
            _e: exception
        """
        # get or instantiate the builder

        start_process_time = int(time.time())
        document_map = {}

        try:
            logger.info("process_element - : start time: %s", str(start_process_time))
            try:
                self.set_builder_name(queue_element)
            except Exception as _e:
                logger.exception(
                    "%s: *** Error in IngestManager run getting builder name ***",
                    self.thread_name,
                )
                sys.exit("*** Error getting builder name: ")

            if self.ingest_type_builder_name in self.builder_map:
                builder = self.builder_map[self.ingest_type_builder_name]
            else:
                builder_class = getattr(my_builder, self.ingest_type_builder_name)
                self.ingest_document = self.load_spec["ingest_documents"][queue_element]
                builder = builder_class(self.load_spec, self.ingest_document)
                self.builder_map[self.ingest_type_builder_name] = builder
            logger.info("building document map for %s", queue_element)
            document_map = builder.build_document(queue_element)
            if self.output_dir:
                logger.info(
                    "writing document map for %s to %s", queue_element, self.output_dir
                )
                self.write_document_to_files(queue_element, document_map)
            else:
                logger.info("writing document map for %s to database", queue_element)
                self.write_document_to_cb(queue_element, document_map)
        except Exception as _e:
            logger.exception(
                "%s: Exception in builder: %s",
                self.thread_name,
                str(self.ingest_type_builder_name),
            )
            raise _e
        finally:
            # reset the document map and record stop time
            stop_process_time = int(time.time())
            document_map = {}
            logger.info(
                "IngestManager.process_element: elapsed time: %s",
                str(stop_process_time - start_process_time),
            )
