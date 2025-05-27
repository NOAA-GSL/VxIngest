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
            name -a threadName for logging and debugging purposes.
            credentials, first and last epoch,
            file_name_queue a shared queue of filenames.
            output_dir where the output documents will be written
            collection couchbase collection object for data service access
            cluster couchbase cluster object for query service access
            number_stations=sys.maxsize (you can limit how many stations will be processed - for debugging)

Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSD
"""

import logging
import sys
import time

from vxingest.builder_common.ingest_manager import CommonVxIngestManager
from vxingest.grib2_to_cb import grib_builder as my_builder
from vxingest.grib2_to_cb.raob_model_native_builder import (
    RaobModelNativeLevelBuilderV01 as RaobModelNativeLevelBuilderV01,
)
from vxingest.grib2_to_cb.raob_model_pressure_level_builder import (
    RaobModelPressureLevelBuilderV01 as RaobModelPressureLevelBuilderV01,
)

# Get a logger with this module's name to help with debugging
logger = logging.getLogger(__name__)


class VxIngestManager(CommonVxIngestManager):
    """
    IngestManager is a Process Thread that manages an object pool of
    builders to ingest data from GSD grib2 files or netcdf files into documents that can be
    inserted into couchbase or written to json files in the specified output directory.

    This class will process data by retrieving an ingest_document specified
    by an ingest_document_id and instantiating a builder class of the type specified in the
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

    def __init__(
        self,
        name,
        load_spec,
        element_queue,
        output_dir,
        logging_queue,
        logging_configurer,
        number_stations=sys.maxsize,
    ):
        """constructor for VxIngestManager
        Args:
            name (string): the thread name for this IngestManager
            load_spec (Object): contains Couchbase credentials
            element_queue (Queue): reference to the element Queue
            output_dir (string): output directory path
            number_stations (int, optional): limit the number of stations to process (debugging). Defaults to sys.maxsize.
        """
        # The Constructor for the RunCB class.
        self.thread_name = name
        self.load_spec = load_spec
        self.cb_credentials = self.load_spec["cb_connection"]
        self.ingest_document_ids = self.load_spec["ingest_document_ids"]
        # use the first one, there aren't multiples anyway
        self.ingest_document = self.load_spec["ingest_documents"][
            self.ingest_document_ids[0]
        ]
        self.ingest_type_builder_name = None
        self.queue = element_queue
        self.builder_map = {}
        self.cluster = None
        self.collection = None
        self.output_dir = output_dir
        self.number_stations = number_stations

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
        if self.ingest_type_builder_name is None:
            try:
                self.ingest_type_builder_name = self.ingest_document["builder_type"]
            except Exception as _e:
                logger.exception(
                    "%s: process_element: Exception getting ingest document for %s ",
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
                builder = builder_class(
                    self.load_spec, self.ingest_document, self.number_stations
                )
                self.builder_map[self.ingest_type_builder_name] = builder
            document_map = builder.build_document(queue_element)
            if self.output_dir:
                self.write_document_to_files(queue_element, document_map)
            else:
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
