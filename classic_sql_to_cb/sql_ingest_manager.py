"""
Program Name: Class SqlIngestManager.py
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

Usage: The SqlIngestManager extends Process - python multiprocess thread -
and runs as a Process and pulls from a queue of metadata document ids. It
maintains its own connections one to
the mysql database and one to couchbase which it keeps open until it finishes.

It finishes and closes its database connection when the document_id_queue is
empty.

It gets document ids serially from a document_id_queue that is shared by a
thread pool of data_type_manager's and processes them one at a a_time. It gets
the concrete builder type from the metadata document and uses a
concrete builder to process the line.

The builders are instantiated once and kept in a map of objects for the
duration of the programs life.
When SqlIngestManager finishes a document specification  it  "upserts"
document_map to the couchbase database.

        Attributes:
            document_id_queue - a document_id_queue of filenames that are
            MET files.
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
from couchbase.exceptions import DocumentNotFoundException, TimeoutException
from couchbase.cluster import Cluster
from couchbase.options import  ClusterOptions, ClusterTimeoutOptions
from couchbase.auth import PasswordAuthenticator
from itertools import islice


def document_map_chunks(data, chunk_size=1000):
    """
    Simple utility for chunking document maps into reasonable upsert sizes
    """
    it = iter(data)
    for i in range(0, len(data), chunk_size):
        yield {k: data[k] for k in islice(it, chunk_size)}


class SqlIngestManager(Process):
    """
    SqlIngestManager is a Thread that manages an object pool of
    GsdBuilders to ingest data from GSD databases into documents that can be
    inserted into couchbase.

    This class will process data by collecting ingest_document_ids - one at a
    a_time - from the document_id_queue. For each ingest_document_id it
    retrieves the identified load metadata document[s] from the couchbase
    collection. From that document it retrieves an sql statement,
    a concrete SqlBuilder class name, a document template, and some other
    fields. It uses the statement in the load metadata document to
    retrieve a result set from the mysql databases and then instantiates
    an appropriate builder using the SqlBuilder class name, and the template
    as construction parameters, and passes the result set entries one at a
    a_time into the builders handle_entry method, along with a reference to
    the document map. The builders use the template to create documents for
    each entry and put them into the document map.
    When all of the result set entries are processed the IngestManager upserts
    the documents to couchbase, retrieves a new ingest_document from
    the queue and starts over.
    Each SqlBuilder is kept in a n object pool so that they do not need to
    be re instantiated.
    When the queue has been emptied the IngestManager closes its connections
    and dies.
    """
    def __init__(self, name, load_spec, document_id_queue, statement_replacement_params):
        """
        :param name: (str) the thread name for this IngestManager
        :param load_spec: (Object) contains Couchbase credentials
        :param document_id_queue: (Object) reference to a queue
        """
        # The Constructor for the RunCB class.
        Process.__init__(self)
        self.threadName = name
        self.load_spec = load_spec
        self.cb_credentials = self.load_spec['cb_connection']
        self.statement_replacement_params = statement_replacement_params
        # made this an instance variable because I don't know how to pass it
        # into the run method
        self.queue = document_id_queue
        self.builder_map = {}
        self.cluster = None
        self.collection = None
    # entry point of the thread. Is invoked automatically when the thread is
    # started.
    def run(self):
        """
        This is the entry point for the SqlIngestManager thread. It runs an
        infinite loop that only terminates when the  document_id_queue is
        empty. For each enqueued document id it calls
        process_meta_ingest_document with the document id and the couchbase
        collection to process the ingest_document.
        """
        # noinspection PyBroadException
        try:
            logging.basicConfig(level=logging.INFO)
            # establish connections to cb, collection
            self.connect_cb()
            # infinite loop terminates when the document_id_queue is empty
            empty_count = 0
            while True:
                try:
                    document_id = self.queue.get_nowait()
                    self.process_meta_ingest_document(document_id)
                    self.queue.task_done()
                    empty_count = 0
                except (DocumentNotFoundException, TimeoutException):
                    continue
                except queue.Empty:
                    if empty_count < 3:
                        empty_count += 1
                        time.sleep(1)
                        continue
                    else:
                        logging.info(
                            self.threadName + ': SqlIngestManager - Queue ' + 'empty - disconnecting ' + 'couchbase')
                        break
        except Exception as e:
            logging.error(self.threadName + ": *** %s Error in SqlIngestManager run "
                                            "***" + str(e))
            raise e
        finally:
            self.close_cb()

    def close_cb(self):
        if self.cluster:
            self.cluster.disconnect()

    def connect_cb(self):
        logging.info(self.threadName + ': data_type_manager - Connecting to couchbase')
        # get a reference to our cluster
        # noinspection PyBroadException
        try:
            options = ClusterOptions(
                PasswordAuthenticator(self.cb_credentials['user'], self.cb_credentials['password']))
            self.cluster = Cluster('couchbase://' + self.cb_credentials['host'], options)
            self.collection = self.cluster.bucket("mdata").default_collection()
            logging.info(self.threadName + ': Couchbase connection success')
        except Exception as e:
            logging.error("*** %s in connect_cb ***" + str(e))
            sys.exit("*** Error when connecting to mysql database: ")

    def process_meta_ingest_document(self, document_id):
        _start_process_time = int(time.time())
        _document_id = document_id
        builder = None
        # get the document from couchbase
        # noinspection PyBroadException
        try:
            ingest_document_result = self.collection.get(_document_id)
            _ingest_document = ingest_document_result.content
            _ingest_type_builder_name = _ingest_document['builder_type']
        except Exception as e:
            logging.error(
                self.threadName + ".process_meta_ingest_document: Exception getting ingest document: " + str(e))
            sys.exit("*** Error getting ingest document ***")
        # get or instantiate the builder
        # noinspection PyBroadException
        _document_map = {}
        # noinspection PyBroadException
        try:
            if _ingest_type_builder_name in self.builder_map.keys():
                builder = self.builder_map[_ingest_type_builder_name]
            else:
                builder_class = getattr(sql_builder, _ingest_type_builder_name)
                builder = builder_class(self.load_spec, self.statement_replacement_params, _ingest_document,
                                        self.cluster, self.collection)
                self.builder_map[_ingest_type_builder_name] = builder
            _document_map = builder.build_document(_ingest_document)
        except Exception as e:
            logging.error(self.threadName + ": Exception instantiating builder: " +
                          str(_ingest_type_builder_name) + " error: " + str(e))
            raise e

        # The document_map is all built now so write all the
        # documents in the document_map into couchbase
        # noinspection PyBroadException
        try:
            logging.info(self.threadName + ': process_meta_ingest_document writing documents for '
                                           'ingest_document :  ' + str(_document_id) + "threadName: " + self.threadName)
            # this call is volatile i.e. it might change syntax in
            # the future.
            # if it does, please just fix it.
            _upsert_start_time = int(time.time())
            logging.info("process_meta_ingest_document - executing upsert: stop time: " + str(_upsert_start_time))
            if not _document_map:
                logging.info(
                    self.threadName + ": process_meta_ingest_document: would upsert documents but DOCUMENT_MAP IS "
                                      "EMPTY")
            else:
                for _item in document_map_chunks(_document_map):
                    try:
                        _ret = self.collection.upsert_multi(_item)
                        time.sleep(1)
                    except TimeoutException as t:
                        logging.info(
                            "process_meta_ingest_document - trying upsert: Got TimeOutException - " +
                            " Document may not be persisted. Retrying: " + str(t.result.errstr))
                        _retry_items = {}
                        for _key, _result in t.all_results.items():
                            if not _result.success:
                                _retry_items[_key] = _result
                        time.sleep(2)   # wait 2 seconds, be polite to the server
                        try:
                            _ret = self.collection.upsert_multi(_retry_items)
                        except TimeoutException as t1:
                            logging.info(
                                "process_meta_ingest_document - retrying upsert: Got TimeOutException - " +
                                " Document may not be persisted.Giving up: " + str(t1.result.errstr))
            _upsert_stop_time = int(time.time())
            logging.info("process_meta_ingest_document - executing upsert: stop time: " + str(_upsert_stop_time))
            logging.info("process_meta_ingest_document - executing upsert: elapsed time: " + str(
                _upsert_stop_time - _upsert_start_time))
        except Exception as e:
            logging.error(self.threadName + ": *** %s Error writing to Couchbase: in "
                                            "process_meta_ingest_document writing document ***" + str(e))
            raise e
        finally:
            # reset the document map
            _stop_process_time = int(time.time())
            logging.info("SqlIngestManager.process_meta_ingest_document: "
                         "elapsed time: " + str(_stop_process_time - _start_process_time))
