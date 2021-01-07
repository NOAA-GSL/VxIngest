"""
Program Name: Class GsdIngestManager.py
Contact(s): Randy Pierce
Abstract:

History Log:  Initial version

IMPORTANT NOTE ABOUT PYTHON THREADS!!!
Please read https://docs.couchbase.com/python-sdk/2.0/threads.html
Due to the Global Interpreter Lock (GIL), only one python thread can execute
Python code at a time.
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
time of the program significantly.
More than that does not. I would have left out threading all-together but
someday we may decide to port this to a truly thread capable language.

Usage: The GsdIngestManager extends Process - python multiprocess thread -
and runs as a Process and pulls from a queue of metadata document ids. It
maintains its own connections one to
the mysql database and one to couchbase which it keeps open until it finishes.

It finishes and closes its database connection when the document_id_queue is
empty.

It gets document ids serially from a document_id_queue that is shared by a
thread pool of data_type_manager's and processes them one at a time. It gets
the concrete builder type from the metadata document and uses a
concrete builder to process the line.

The builders are instantiated once and kept in a map of objects for the
duration of the programs life.
When GsdIngestManager finishes a document specification  it converts the
data in the document_map into a document and "upserts" it to the couchbase
database.

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
import sys
import time
from multiprocessing import Process
import queue
import pymysql
from couchbase.cluster import Cluster, ClusterOptions
from couchbase.exceptions import DocumentNotFoundException
from couchbase_core.cluster import PasswordAuthenticator
import gsd_builder as gsd
import constants as cn


class GsdIngestManager(Process):
    """
    GsdIngestManager is a Thread that manages an object pool of
    GsdBuilders to ingest data from GSD databases into documents that can be
    inserted into couchbase.
    This class will process data by collecting ingest_document_ids - one at a
    time - from document_id_queue. For each ingest_document_id it will
    use the template contained in the ingest_document that is identified by
    the ingest_document_id and process each line.
    From each ingest_document it retrieves the builder_type.
    It uses the builder_type to either retrieve the reference of a
    corresponding ingest_type_builder from the object
    pool or instantiate an appropriate builder_type_builder and put it in the
    pool and retrieve its reference.
    It uses the builder_type_builder to process an ingest_document and either
    start a new document_map entry, or add data_records to an existing
    data_map entry.
    """
    
    def __init__(self, name, cb_credentials, mysql_credentials,
                 document_id_queue):
        # The Constructor for the RunCB class.
        Process.__init__(self)
        self.threadName = name
        self.cb_credentials = cb_credentials
        self.mysql_credentials = mysql_credentials
        # made this an instance variable because I don't know how to pass it
        # into the run method
        self.queue = document_id_queue
        
        self.builder_map = {}
        self.document_map = {}
        self.database_name = ""
        self.cluster = None
        self.collection = None
        self.connection = None
        self.cursor = None
    
    # entry point of the thread. Is invoked automatically when the thread is
    # started.
    def run(self):
        """
        This is the entry point for the DataTypeManager thread. It runs an
        infinite loop that only
        terminates when the  document_id_queue is empty. For each enqueued
        document
        it calls process_document with the document id to process the file.
        """
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
                    self.process_meta_ingest_document(document_id,
                                                      self.collection)
                    self.queue.task_done()
                    empty_count = 0
                except DocumentNotFoundException:
                    continue
                except queue.Empty:
                    if empty_count < 3:
                        empty_count += 1
                        logging.info(
                            self.threadName + ': GsdIngestManager - got ' +
                            'Queue.Empty - retrying: ' +
                            str(empty_count) + " of 3 times")
                        time.sleep(1)
                        continue
                    else:
                        logging.info(
                            self.threadName + ': GsdIngestManager - Queue ' +
                                              'empty - disconnecting ' +
                                              'couchbase')
                        break
        except:
            logging.error(
                self.threadName + ": *** %s Error in GsdIngestManager run "
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
        logging.info(self.threadName + ': data_type_manager - Connecting to '
                                       'couchbase')
        # get a reference to our cluster
        # derive the path to the public certificate for the host
        # see ...
        # https://docs.couchbase.com/server/current/manage/manage
        # -security/configure-server-certificates.html#root-and-node
        # -certificates
        # and
        # https://docs.couchbase.com/server/current/manage/manage
        # -security/configure-client-certificates.html#client
        # -certificate-authorized-by-a-root-certificate
        if 'cert_path' in self.cb_credentials:
            logging.info(
                self.threadName + ': attempting cb connection with cert')
            # this does not work yet - to get here use the -c option
            # with a cert_path
            self.cluster = Cluster(
                'couchbase://' + self.cb_credentials['host'], ClusterOptions(
                    PasswordAuthenticator(self.cb_credentials['user'],
                                          self.cb_credentials['password'],
                                          cert_path=self.cb_credentials[
                                              'cert_path'])))
            self.collection = self.cluster.bucket(
                "mdata").default_collection()  # this works with a cert  #
            # to local server - but not to adb-cb4  # connstr =   #  #  #  #
            # 'couchbases://127.0.0.1/{  #  #  #  #   #   #  #   #  #  #  #
            # }?certpath=/Users/randy.pierce/servercertfiles/ca.pem'  #  #
            # credentials = dict(username='met_admin',
            # password='met_adm_pwd')  # cb = Bucket(connstr.format(  #  #
            # 'mdata'), **credentials)  # collection =   #  #  #  #   #  #
            # cb.default_collection()
        
        else:
            # this works but is not secure - don't provide the -c option
            # to get here
            # get a reference to our cluster
            logging.info(
                self.threadName + ': attempting cb connection with NO '
                                  'cert')
            self.cluster = Cluster(
                'couchbase://' + self.cb_credentials['host'], ClusterOptions(
                    PasswordAuthenticator(self.cb_credentials['user'],
                                          self.cb_credentials['password'])))
            self.collection = self.cluster.bucket("mdata").default_collection()
        logging.info(self.threadName + ': connection success')
    
    def connect_mysql(self):
        # Connect to the database using connection info from XML file
        try:
            host = self.mysql_credentials['host']
            if 'port' in self.mysql_credentials.keys():
                port = int(self.mysql_credentials['port'])
            else:
                port = cn.SQL_PORT
            user = self.mysql_credentials['user']
            passwd = self.mysql_credentials['password']
            local_infile = True
            self.connection = pymysql.connect(host=host, port=port, user=user,
                                              passwd=passwd,
                                              local_infile=local_infile)
        except pymysql.OperationalError as pop_err:
            logging.error("*** %s in connect_mysql ***" + str(pop_err))
            sys.exit("*** Error when connecting to mysql database: ")
        try:
            self.cursor = self.connection.cursor()
        except (RuntimeError, TypeError, NameError, KeyError, AttributeError):
            logging.error("*** %s in run_sql ***" + str(sys.exc_info()[0]))
            sys.exit("*** Error when creating cursor: ")
    
    def process_meta_ingest_document(self, document_id, collection):
        self.document_map = {}
        _document_id = document_id
        # get the document from couchbase
        try:
            ingest_document_result = collection.get(_document_id)
        except DocumentNotFoundException:
            logging.warning(
                self.threadName + ": DocumentNotFoundException instantiating "
                                  "builder: " + "document_id: " +
                _document_id)
            raise DocumentNotFoundException('document_id: ' + _document_id)
        ingest_document = ingest_document_result.content
        ingest_type_builder_name = ingest_document['builder_type']
        # get or instantiate the builder
        try:
            if ingest_type_builder_name in self.builder_map.keys():
                builder = self.builder_map[ingest_type_builder_name]
            else:
                builder_class = getattr(gsd, ingest_type_builder_name)
                builder = builder_class()
                self.builder_map[ingest_type_builder_name] = builder
            # process the document
            builder.handle_document(ingest_document)
        except:
            e = sys.exc_info()[0]
            logging.error(
                self.threadName + ": Exception instantiating builder: " +
                str(ingest_type_builder_name) + " error: " +
                str(e))
        # all the lines are now processed for this file so write all the
        # documents in the document_map
        try:
            logging.info(
                self.threadName + ': data_type_manager writing documents for '
                                  'ingest_document :  ' + str(_document_id) +
                "threadName: " + self.threadName)
            for key in self.document_map.keys():
                try:
                    # this call is volatile i.e. it might change syntax in
                    # the future.
                    # if it does, please just fix it.
                    collection.upsert_multi(self.document_map[key])
                    logging.info(self.threadName + ': data_type_manager wrote '
                                                   'documents for '
                                                   'ingest_document :  ' +
                                 str(_document_id) + "threadName: " +
                                 self.threadName)
                except:
                    e = sys.exc_info()[0]
                    e1 = sys.exc_info()[1]
                    logging.error(
                        self.threadName + ": *** %s Error multi-upsert to "
                                          "Couchbase: in data_type_manager "
                                          "*** " + str(e))
                    logging.error(
                        self.threadName + ": *** %s Error multi-upsert to "
                                          "Couchbase: in data_type_manager "
                                          "*** " + str(e1))
        except:
            e = sys.exc_info()[0]
            e1 = sys.exc_info()[1]
            logging.error(
                self.threadName + ": *** %s Error writing to Couchbase: in "
                                  "data_type_manager writing document ***" +
                str(e))
            logging.error(
                self.threadName + ": *** %s Error writing to Couchbase: in "
                                  "data_type_manager writing document ***" +
                str(e1))
        finally:
            # reset the document map
            self.document_map = {}
