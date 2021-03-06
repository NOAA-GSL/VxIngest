"""
Program Name: Class GsdIngestManager.py
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

Usage: The GsdIngestManager extends Process - python multiprocess thread -
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
When GsdIngestManager finishes a document specification  it  "upserts"
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
import json
import logging
import queue
import re
import sys
import time
from multiprocessing import Process

import pymysql
from couchbase.cluster import Cluster, ClusterOptions
from couchbase.exceptions import DocumentNotFoundException, TimeoutException
from couchbase_core.cluster import PasswordAuthenticator
from pymysql.constants import CLIENT

from gsd_sql_to_cb import gsd_builder as gsd_builder

SQL_PORT = 3306


class GsdIngestManager(Process):
    """
    GsdIngestManager is a Thread that manages an object pool of
    GsdBuilders to ingest data from GSD databases into documents that can be
    inserted into couchbase.
    
    This class receives connection credentials for couchbase and for mysql.
    It uses the credentials to open connections to both database systems. These
    connections are maintained by this thread.
    
    This class will process data by collecting ingest_document_ids - one at a
    a_time - from the document_id_queue. For each ingest_document_id it
    retrieves the identified load metadata document[s] from the couchbase
    collection. From that document it retrieves an sql statement,
    a concrete GsdBuilder class name, a document template, and some other
    fields. It uses the statement in the load metadata document to
    retrieve a result set from the mysql databases and then instantiates
    an appropriate builder using the GsdBuilder class name, and the template
    as construction parameters, and passes the result set entries one at a
    a_time into the builders handle_entry method, along with a reference to
    the document map. The builders use the template to create documents for
    each entry and put them into the document map.
    When all of the result set entries are processed the IngestManager upserts
    the documents to couchbase, retrieves a new ingest_document from
    the queue and starts over.
    Each GsdBuilder is kept in a n object pool so that they do not need to
    be re instantiated.
    When the queue has been emptied the IngestManager closes its connections
    and dies.
    """
    
    def __init__(self, name, cb_credentials, mysql_credentials, document_id_queue, statement_replacement_params):
        """
        :param name: (str) the thread name for this IngestManager
        :param cb_credentials: (Object) Couchbase credentials
        :param mysql_credentials: (Object) mysql credentials
        :param document_id_queue: (Object) reference to a queue
        """
        # The Constructor for the RunCB class.
        Process.__init__(self)
        self.threadName = name
        self.cb_credentials = cb_credentials
        self.mysql_credentials = mysql_credentials
        self.statement_replacement_params = statement_replacement_params
        # made this an instance variable because I don't know how to pass it
        # into the run method
        self.queue = document_id_queue
        self.document_map = {}
        self.builder_map = {}
        self.database_name = ""
        self.cluster = None
        self.collection = None
        self.connection = None
        self.cursor = None
    
    # entry point of the thread. Is invoked automatically when the thread is
    # started.
    def run(self):
        """
        This is the entry point for the GsdIngestManager thread. It runs an
        infinite loop that only terminates when the  document_id_queue is 
        empty. For each enqueued document id it calls 
        process_meta_ingest_document with the document id and the couchbase 
        collection to process the ingest_document.
        """
        # noinspection PyBroadException
        try:
            logging.basicConfig(level=logging.INFO)
            # establish connections to mysql and cb, collection, connection,
            # and cursor are contained in self
            self.connect_cb()
            self.connect_mysql()
            
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
                            self.threadName + ': GsdIngestManager - Queue ' + 'empty - disconnecting ' + 'couchbase')
                        break
        except:
            logging.error(self.threadName + ": *** %s Error in GsdIngestManager run "
                                            "***" + str(sys.exc_info()[1]))
        finally:
            # close any mysql connections
            self.close_mysql()
            self.close_cb()
    
    def close_mysql(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
    
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
        except:
            logging.error("*** %s in connect_cb ***" + str(sys.exc_info()[0]))
            sys.exit("*** Error when connecting to mysql database: ")
    
    def connect_mysql(self):
        # Connect to the database using connection info from XML file
        try:
            host = self.mysql_credentials['host']
            if 'port' in self.mysql_credentials.keys():
                port = int(self.mysql_credentials['port'])
            else:
                port = SQL_PORT
            user = self.mysql_credentials['user']
            passwd = self.mysql_credentials['password']
            local_infile = True
            self.connection = pymysql.connect(host=host, port=port, user=user,
                                              passwd=passwd,
                                              local_infile=local_infile,
                                              autocommit=True,
                                              charset='utf8mb4',
                                              cursorclass=pymysql.cursors.SSDictCursor,
                                              client_flag=CLIENT.MULTI_STATEMENTS)
        except pymysql.OperationalError as pop_err:
            logging.error("*** %s in connect_mysql ***" + str(pop_err))
            sys.exit("*** Error when connecting to mysql database: ")
        try:
            self.cursor = self.connection.cursor(pymysql.cursors.DictCursor)
        except (RuntimeError, TypeError, NameError, KeyError, AttributeError):
            logging.error("*** %s in run_sql ***" + str(sys.exc_info()[0]))
            sys.exit("*** Error when creating cursor: ")
        
        logging.info(self.threadName + ': Mysql connection success')
    
    def process_meta_ingest_document(self, document_id):
        _start_process_time = int(time.time())
        _document_id = document_id
        # get the document from couchbase
        # noinspection PyBroadException
        try:
            ingest_document_result = self.collection.get(_document_id)
            _ingest_document = ingest_document_result.content
            _ingest_type_builder_name = _ingest_document['builder_type']
        except:
            e = sys.exc_info()[0]
            logging.error(self.threadName + ".process_meta_ingest_document: Exception getting ingest document: "
                          + str(e))
            sys.exit("*** Error getting ingest document ***")
        # get or instantiate the builder
        # noinspection PyBroadException
        try:
            if _ingest_type_builder_name in self.builder_map.keys():
                builder = self.builder_map[_ingest_type_builder_name]
            else:
                builder_class = getattr(gsd_builder, _ingest_type_builder_name)
                builder = builder_class(_ingest_document, self.cluster, self.collection)
                self.builder_map[_ingest_type_builder_name] = builder
        except:
            logging.error(self.threadName + ": Exception instantiating builder: " +
                          str(_ingest_type_builder_name) + " error: " + str(sys.exc_info()))

        # noinspection PyBroadException
        try:
            # process the document
            _statement = _ingest_document['statement']
            # replace any statement params - replacement params are like {param}=replacement
            for _k in self.statement_replacement_params.keys():
                _statement = _statement.replace(_k, str(self.statement_replacement_params[_k]))
            _statements = _statement.split(';')
            for s in _statements:
                if s.strip().upper().startswith('SET'):
                    _value = re.split("=", s)[1].strip()
                    _m = re.findall(r'[@]\w+', s)[0]
                    _statement = _statement.replace(s + ';', '')
                    _statement = _statement.replace(_m, _value)
            _query_start_time = int(time.time())
            logging.info("executing query: start time: " + str(_query_start_time))
            self.cursor.execute(_statement)
            _query_stop_time = int(time.time())
            logging.info("executing query: stop time: " + str(_query_stop_time))
            logging.info("executing query: elapsed seconds: " + str(_query_stop_time - _query_start_time))
        except:
            logging.error(self.threadName + ": Exception processing the statement: " + str(
                _ingest_type_builder_name) + " error: " + str(sys.exc_info()))
        # noinspection PyBroadException
        try:
            while True:
                row = self.cursor.fetchone()
                if not row:
                    break
                builder.handle_row(row)
            # The document_map could potentially have a lot of documents in it
            # depending on how the builder collated the rows into documents
            # i.e. by time like for obs, or by time and fcst_len like for models,
            # or all in one like for stations
            self.document_map = builder.getDocumentMap()
        except:
            e = sys.exc_info()[0]
            logging.error(self.threadName + ": Exception with builder handle_row: " +
                          str(_ingest_type_builder_name) + " error: " + str(e))
        # all the lines are now processed for this result set so write all the
        # documents in the document_map
        # noinspection PyBroadException
        try:
            logging.info(self.threadName + ': data_type_manager writing documents for '
                                           'ingest_document :  ' + str(_document_id) + "threadName: " + self.threadName)
            # this call is volatile i.e. it might change syntax in
            # the future.
            # if it does, please just fix it.
            _upsert_start_time = int(time.time())
            logging.info("executing upsert: stop time: " + str(_upsert_start_time))
            _ret = self.collection.upsert_multi(self.document_map)
            _upsert_stop_time = int(time.time())
            logging.info("executing upsert: stop time: " + str(_upsert_stop_time))
            logging.info("executing upsert: elapsed time: " + str(_upsert_stop_time - _upsert_start_time))
            logging.info(self.threadName + ': data_type_manager wrote ' + str(
                _ret.all_ok) + ' document[s] for ingest_document :  ' + str(
                _document_id) + "threadName: " + self.threadName)
        except:
            e = sys.exc_info()
            logging.error(self.threadName + ": *** %s Error writing to Couchbase: in "
                                            "data_type_manager writing document ***" + str(e))
        finally:
            # reset the document map
            _stop_process_time = int(time.time())
            logging.info("GsdIngestManager.process_meta_ingest_document: "
                         "elapsed time: " + str(_stop_process_time - _start_process_time))
