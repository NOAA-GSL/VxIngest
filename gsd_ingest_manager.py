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
Because of this the couchbase SDK recommends that a different bucket is used
for each multiprocess thread and they are
very sure to point out that if you do not honor this your program WILL HAVE
UNEXPECTED ERRORS! So if you modify this thread relationship between the
main program and the DataTypeManager be very very careful.
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
            connection_credentials - a set of connection_credentials that
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

from couchbase.cluster import Cluster, ClusterOptions
from couchbase_core.cluster import PasswordAuthenticator
import data_type_builder as dtb


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
    
    def __init__(self, name, connection_credentials, document_id_queue):
        # The Constructor for the RunCB class.
        Process.__init__(self)
        self.threadName = name
        self.connection_credentials = connection_credentials
        # made this an instance variable because I don't know how to pass it
        # into the run method
        self.queue = document_id_queue
        
        self.builder_map = {}
        self.document_map = {}
        self.database_name = ""
    
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
            logging.info(
                self.threadName + ': data_type_manager - Connecting to '
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
            
            if 'cert_path' in self.connection_credentials:
                logging.info(
                    self.threadName + ': attempting cb connection with cert')
                # this does not work yet - to get here use the -c option
                # with a cert_path
                cluster = Cluster(
                    'couchbase://' + self.connection_credentials['db_host'],
                    ClusterOptions(PasswordAuthenticator(
                        self.connection_credentials['db_user'],
                        self.connection_credentials['db_password'],
                        cert_path=self.connection_credentials['cert_path'])))
                self.database_name = self.connection_credentials['db_name']
                collection = cluster.bucket(
                    "mdata").default_collection()  # this works with a cert
                # to local server - but not to adb-cb4  # connstr =   #  #
                # 'couchbases://127.0.0.1/{  #  #  #  #   #   #  #   #
                # }?certpath=/Users/randy.pierce/servercertfiles/ca.pem'  #
                # credentials = dict(username='met_admin',
                # password='met_adm_pwd')  # cb = Bucket(connstr.format(  #
                # 'mdata'), **credentials)  # collection =   #  #  #  #   #
                # cb.default_collection()
            
            else:
                # this works but is not secure - don't provide the -c option
                # to get here
                # get a reference to our cluster
                logging.info(
                    self.threadName + ': attempting cb connection with NO '
                                      'cert')
                cluster = Cluster(
                    'couchbase://' + self.connection_credentials['db_host'],
                    ClusterOptions(PasswordAuthenticator(
                        self.connection_credentials['db_user'],
                        self.connection_credentials['db_password'])))
                self.database_name = self.connection_credentials['db_name']
                collection = cluster.bucket("mdata").default_collection()
            
            logging.info(self.threadName + ': connection success')
            self.database_name = self.connection_credentials['db_name']
            # infinite loop terminates when the document_id_queue is empty
            empty_count = 0
            while True:
                try:
                    document_id = self.queue.get_nowait()
                    self.process_document(document_id, collection)
                    self.queue.task_done()
                    empty_count = 0
                except queue.Empty:
                    if empty_count < 3:
                        empty_count += 1
                        logging.info(
                            self.threadName + ': GsdIngestManager - got '
                                              'Queue.Empty - retrying: ' + str(
                                empty_count) + " of 3 times")
                        time.sleep(1)
                        continue
                    else:
                        logging.info(
                            self.threadName + ': GsdIngestManager - Queue '
                                              'empty - disconnecting '
                                              'couchbase')
                        break
        except:
            logging.error(
                self.threadName + ": *** %s Error in GsdIngestManager run "
                                  "***", sys.exc_info()[0])
            logging.error(
                self.threadName + ": *** %s Error in GsdIngestManager run "
                                  "***", sys.exc_info()[1])
            logging.info(
                self.threadName + ': GsdIngestManager - disconnecting '
                                  'couchbase')
    
    # process a file line by line
    def process_document(self, document_id, collection):
        self.document_map = {}
        # get the document from couchbase
        ingest_document = collection.get(self.document_id)
        ingest_type_builder_name = ingest_document['builder_type']
        # get or instantiate the builder
        try:
            if ingest_type_builder_name in self.builder_map.keys():
                builder = self.builder_map[ingest_type_builder_name]
            else:
                builder_class = getattr(dtb, ingest_type_builder_name)
                builder = builder_class()
                self.builder_map[ingest_type_builder_name] = builder
            # process the line
            builder.handle_document(ingest_document)
        except:
            e = sys.exc_info()[0]
            logging.error(
                self.threadName + ": Exception instantiating builder: " +
                ingest_type_builder_name + " error: " + str(
                    e))
        # all the lines are now processed for this file so write all the
        # documents in the document_map
        try:
            logging.info(
                self.threadName + ': data_type_manager writing documents for '
                                  'ingest_document :  ' + self.document_id +
                "threadName: " + self.threadName)
            for key in self.document_map.keys():
                try:
                    # this call is volatile i.e. it might change syntax in
                    # the future.
                    # if it does, please just fix it.
                    collection.upsert_multi(self.document_map[key])
                    logging.info(
                        self.threadName + ': data_type_manager wrote '
                                          'documents for '
                                          'ingest_document :  ' +
                        self.document_id + "threadName: " + self.threadName)
                except:
                    e = sys.exc_info()[0]
                    e1 = sys.exc_info()[1]
                    logging.error(
                        self.threadName + ": *** %s Error multi-upsert to "
                                          "Couchbase: in data_type_manager "
                                          "***", str(e))
                    logging.error(
                        self.threadName + ": *** %s Error multi-upsert to "
                                          "Couchbase: in data_type_manager "
                                          "***", str(e1))
        except:
            e = sys.exc_info()[0]
            e1 = sys.exc_info()[1]
            logging.error(
                self.threadName + ": *** %s Error writing to Couchbase: in "
                                  "data_type_manager writing document ***",
                str(e))
            logging.error(
                self.threadName + ": *** %s Error writing to Couchbase: in "
                                  "data_type_manager writing document ***",
                str(e1))
        finally:
            # reset the document map
            self.document_map = {}
