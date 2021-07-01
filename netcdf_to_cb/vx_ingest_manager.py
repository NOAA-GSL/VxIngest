"""
Program Name: Class NetcdfIngestManager
Contact(s): Randy Pierce
Abstract:

History Log:  Initial version

IMPORTANT NOTE ABOUT PYTHON THREADS!!!
Please read https://docs.couchbase.com/python-sdk/2.0/threads.html
Due to the Global Interpreter Lock (GIL), only one python thread can execute
Python code at a a_time.
See  https://docs.python.org/2/library/threading.html.
However Python also has multiprocessing which can be used if the memory
footprint of the application is small enough.
This program uses multiprocessing which essentially uses an entire
interpreter for each thread, thus avoiding the GIL.
The python SDK 3.0 is thread safe.
See https://docs.couchbase.com/python-sdk/current/howtos/managing
-connections.html
"Most of the high-level classes in the Python SDK are designed to be safe
for concurrent use by multiple threads. For asynchronous modes, you will get
the best performance if you share and reuse instances of Cluster, Bucket,
Scope, and Collection, all of which are thread-safe."
Observations have shown that two or four threads help reduce the execution
a_time of the program significantly.
More than that does not. I would have left out threading all-together but
someday we may decide to port this to a truly thread capable language.

Usage: The NetcdfIngestManager extends Process - python multiprocess thread -
and runs as a Process and pulls from a queue of file names. It
maintains its own connection to couchbase which it keeps open until it finishes.

It finishes and closes its database connection when the file_name_queue is
empty.

It gets file names serially from a queue that is shared by a
thread pool of data_type_manager's and processes them one at a a_time. It gets
the concrete builder type from the metadata document and uses a
concrete builder to process the file.

The builders are instantiated once and kept in a map of objects for the
duration of the programs life. For NetcdfIngestManager it is likely that 
each file will require only one builder type to be instantiated.
When NetcdfIngestManager finishes a document specification  it  "upserts"
a document_map to the couchbase database.

        Attributes:
            file_queue - a shared queue of filenames.
            threadName - a threadName for logging and debugging purposes.
            cb_credentials - a set of cb_credentials that
            the DataTypeManager will use to connect to the database. This
            connection will be maintained until the thread terminates.
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSD
"""
import logging
import queue
import sys
import time
from multiprocessing import Process
from couchbase.cluster import Cluster, ClusterOptions
from couchbase.exceptions import TimeoutException
from couchbase_core.cluster import PasswordAuthenticator
from itertools import islice
from netcdf_to_cb import netcdf_builder as netcdf_builder


def document_map_chunks(data, chunk_size=1000):
    """
    Simple utility for chunking document maps into reasonable upsert sizes
    """
    it = iter(data)
    for i in range(0, len(data), chunk_size):
        yield {k: data[k] for k in islice(it, chunk_size)}


class VxIngestManager(Process):
    """
    NetcdfIngestManager is a Process Thread that manages an object pool of
    NetcdfBuilders to ingest data from GSD netcdf files or grib files into documents that can be
    inserted into couchbase.

    This class will process data by reading an ingest_document_id
    and instantiating a NetcdfBuilder class of the type specified in the
    ingest_document. 
    The ingest document specifies the builder, class, a set of variables that are expected to be
    in each file, and a template that defines how to place the variable values
    into a couchbase document and how to construct the couchbase data document id.

    It will then read file_names, one by one,
    from the file_name_queue.  The builders use the template to create documents for
    each filename and put them into the document map.

    When all of the result set entries for a file are processed, the IngestManager upserts
    the document(s) to couchbase, retrieves a new filename from
    the queue and starts over.

    Each NetcdfBuilder is kept in a n object pool so that they do not need to
    be re instantiated.
    When the queue has been emptied the IngestManager closes its connections
    and dies.
    """

    def __init__(self, name, load_spec, file_name_queue):
        """
        :param name: (str) the thread name for this IngestManager
        :param load_spec: (Object) contains Couchbase credentials
        :param file_name_queue: (Object) reference to a queue
        """
        # The Constructor for the RunCB class.
        Process.__init__(self)
        self.threadName = name
        self.load_spec = load_spec
        self.cb_credentials = self.load_spec['cb_connection']
        self.ingest_document_id = self.load_spec['ingest_document_id']
        self.ingest_type_builder_name = None
        self.ingest_document = None
        self.queue = file_name_queue
        self.builder_map = {}
        self.cluster = None
        self.collection = None

    # entry point of the thread. Is invoked automatically when the thread is
    # started.
    def run(self):
        """
        This is the entry point for the NetcdfIngestManager thread. It runs an
        infinite loop that only terminates when the  file_name_queue is 
        empty. For each enqueued file name it calls 
        process_file with the file_name and the couchbase 
        connection to process the file.
        """
        # noinspection PyBroadException
        try:
            logging.basicConfig(level=logging.INFO)
            # establish connections to cb, collection
            self.connect_cb()
            # Read the ingest document
            _start_process_time = int(time.time())
            builder = None
            # get the document from couchbase
            # noinspection PyBroadException
            try:
                if self.ingest_document_id is None:
                    raise Exception('ingest_document is undefined')
                ingest_document_result = self.collection.get(
                    self.ingest_document_id)
                self.ingest_document = ingest_document_result.content
                self.ingest_type_builder_name = self.ingest_document['builder_type']
            except Exception as e:
                logging.error(
                    self.threadName + ".process_file: Exception getting ingest document: " + str(e))
                sys.exit("*** Error getting ingest document ***")

            # infinite loop terminates when the file_name_queue is empty
            empty_count = 0
            while True:
                try:
                    file_name = self.queue.get_nowait()
                    self.process_file(file_name)
                    self.queue.task_done()
                    empty_count = 0
                except (Exception):
                    continue
                except queue.Empty:
                    if empty_count < 3:
                        empty_count += 1
                        time.sleep(1)
                        continue
                    else:
                        logging.info(
                            self.threadName + ': NetcdfIngestManager - Queue ' + 'empty - disconnecting ' + 'couchbase')
                        break
        except Exception as e:
            logging.error(self.threadName + ": *** %s Error in NetcdfIngestManager run "
                                            "***" + str(e))
            raise e
        finally:
            self.close_cb()

    def close_cb(self):
        if self.cluster:
            self.cluster.disconnect()

    def connect_cb(self):
        logging.info(self.threadName +
                     ': data_type_manager - Connecting to couchbase')
        # get a reference to our cluster
        # noinspection PyBroadException
        try:
            options = ClusterOptions(
                PasswordAuthenticator(self.cb_credentials['user'], self.cb_credentials['password']))
            self.cluster = Cluster(
                'couchbase://' + self.cb_credentials['host'], options)
            self.collection = self.cluster.bucket("mdata").default_collection()
            logging.info(self.threadName + ': Couchbase connection success')
        except Exception as e:
            logging.error("*** %s in connect_cb ***" + str(e))
            sys.exit("*** Error when connecting to mysql database: ")

    def process_file(self, file_name):
        # get or instantiate the builder
        # noinspection PyBroadException
        _start_process_time = int(time.time())
        _document_map = {}
        # noinspection PyBroadException
        try:
            if self.ingest_type_builder_name in self.builder_map.keys():
                builder = self.builder_map[self.ingest_type_builder_name]
            else:
                builder_class = getattr(
                    netcdf_builder, self.ingest_type_builder_name)
                builder = builder_class(self.load_spec, self.ingest_document,
                                        self.cluster, self.collection)
                self.builder_map[self.ingest_type_builder_name] = builder
            _document_map = builder.build_document(file_name)
        except Exception as e:
            logging.error(self.threadName + ": Exception instantiating builder: " +
                          str(self.ingest_type_builder_name) + " error: " + str(e))
            raise e

        # The document_map is all built now so write all the
        # documents in the document_map into couchbase
        # noinspection PyBroadException
        try:
            logging.info(self.threadName + ': process_file writing documents for '
                                           'ingest_document :  ' + str(file_name) + "threadName: " + self.threadName)
            # this call is volatile i.e. it might change syntax in
            # the future.
            # if it does, please just fix it.
            _upsert_start_time = int(time.time())
            logging.info(
                "process_file - executing upsert: stop time: " + str(_upsert_start_time))
            if not _document_map:
                logging.info(
                    self.threadName + ": process_file: would upsert documents but DOCUMENT_MAP IS "
                                      "EMPTY")
            else:
                for _item in document_map_chunks(_document_map):
                    try:
                        _ret = self.collection.upsert_multi(_item)
                        time.sleep(1)
                    except TimeoutException as t:
                        logging.info(
                            "process_file - trying upsert: Got TimeOutException - " +
                            " Document may not be persisted. Retrying: " + str(t.result.errstr))
                        _retry_items = {}
                        for _key, _result in t.all_results.items():
                            if not _result.success:
                                _retry_items[_key] = _result
                        # wait 2 seconds, be polite to the server
                        time.sleep(2)
                        try:
                            _ret = self.collection.upsert_multi(_retry_items)
                        except TimeoutException as t1:
                            logging.info(
                                "process_file - retrying upsert: Got TimeOutException - " +
                                " Document may not be persisted.Giving up: " + str(t1.result.errstr))
            _upsert_stop_time = int(time.time())
            logging.info(
                "process_file - executing upsert: stop time: " + str(_upsert_stop_time))
            logging.info("process_file - executing upsert: elapsed time: " + str(
                _upsert_stop_time - _upsert_start_time))
        except Exception as e:
            logging.error(self.threadName + ": *** %s Error writing to Couchbase: in "
                                            "process_file writing document ***" + str(e))
            raise e
        finally:
            # reset the document map
            _stop_process_time = int(time.time())
            logging.info("NetcdfIngestManager.process_file: "
                         "elapsed time: " + str(_stop_process_time - _start_process_time))
