"""
Program Name: Class gsd_builder.py
Contact(s): Randy Pierce
Abstract:

History Log:  Initial version

Usage: Abstract GsdBuilder has common code for concrete
GsdBuilders. A pool of instantiated GsdBuilders is
        used by a GsdIngestManager to parse a MET output file and produce a
        set of documents suitable for insertion into couchbase.
        Concrete instantiations convert a line of a MET output file to a
        document map that is keyed by a document ID.
        Each concrete builder must over ride the function ...
        def handle_line(self, data_type, line, document_map, database_name):
        ....
        which will be called from the Data_Type_MAnager like this ...
        builder.handle_line(data_type, line, document_map, database_name)
        ....
        where builder is the concrete builder instance, line is the line to
        be parsed, data_type is the data_type of the line,
        data_base name is just a name that will be used in the subset field
        of an output document, and document_map
        is a map that is maintained ny the builder manager.
        Each concrete builder must create these two lists on construction...
            self.header_field_names = a standardized ordered list of HEADER
            fields for this data_type.
            self.data_field_names = a standardized ordered list of DATA
            fields for this data_type.
            The order of these fields is specific to the lines of the MET
            input file for a given data_type.

        Each concrete subclass method handle_line must derive from the line
            a record which is a dictionary of fields that is keyed by header
            fields and data fields and contains values
            that are parsed from the line.
            The handle_line method then uses the record HEADER fields to
            create a unique id specific to the data_type.
            The record and id are then used to either start a new entry into
            the document_map[data_type] dictionary that is keyed by the
            id, or to derive and add a data_record dictionary from the
            record to the document_map[data_type][id][data] dictionary.

        Attributes:
        self.header_field_names - the ordered list of HEADER field names
        self.data_field_names - the  ordered list of DATA field names

Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSD
"""

import logging
import sys
from abc import ABC
import json


def get_id(record):
    # Private method to derive a document id from the current line.
    my_id = "DD::"
    return my_id


class GsdBuilder(ABC):
    # Abstract Class for data_type builders
    def __init__(self):
        # The Constructor for the RunCB class.
        self.header_field_names = None
        self.data_field_names = None
    
    # common helper methods

    def get_data_record(self, record):
        try:
            data_record = {}
            return data_record
        except:
            e = sys.exc_info()[0]
            logging.error("Exception instantiating builder - "
                          "get_data_record_VSDB_V01_L1L2: " +
                          self.__class__.__name__ +
                          " get_data_record_VSDB_V01_L1L2 error: " +
                          str(e))
            return {}
    
    def start_new_document(self):
        # Private method to start a new document
        try:
            my_id = get_id({})
        
        except:
            e = sys.exc_info()[0]
            logging.error("Exception instantiating builder - "
                          "start_new_document_VSDB_V01_L1L2: " +
                          self.__class__.__name__ +
                          " start_new_document_VSDB_V01_L1L2 error: " + str(e))
    

# Concrete data_type builders:
# Each data_type builder has to be able to do two things.
# one: construct the self._document_field_names list that is an ordered list
# of field names,
# first header then data fields,
# that correlates positionally to each line of a specific builder type i.e.
# VSDB_V001_SL1L2.
# using standardized names from the cn constants
# two: implement _handle_line(self, data_type, record):
# where data_type is the datatype of a given line i.e. VSDB_V001_SL1L2 and
# record is a
# map derived from the parsed line and the self._document_field_names
# NOTE that these concrete builder classes are not CamelCase on purpose. The
# concrete class names match the data
# in the vsdb files on purpose. These classes are instantiated dynamically
# and naming them after
# the data fields makes that process easier and cleaner. Sorry pylint...
class GsdObsBuilder(GsdBuilder):
    # This data_type builder can leverage the parent
    # self.start_new_document_VSDB_V01_L1L2, and
    # self._handle_line_VSDB_V01_L1L2 because they are same for several data
    # types.
    def __init__(self):
        super(GsdBuilder, self).__init__()
    
    def handle_document(self, ingest_document):
    
        try:
            logging.info("GsdObsBuilder: building this ingest document: " +
                         str(ingest_document['id']))
            # print(json.dumps(ingest_document))
            first_fcst_valid_epoch = ingest_document['firstFcstValidEpoch']
            last_fcst_valid_epoch = ingest_document['lastFcstValidEpoch']
            _document_template = ingest_document['template']
            logging.info("GsdObsBuilder: building with "
                         "first_valid_epoch: " + str(first_fcst_valid_epoch) +
                         "last_valid_epoch: " + str(last_fcst_valid_epoch))
            logging.info(_document_template)
        except:
            e = sys.exc_info()[0]
            logging.error(
                "Exception instantiating builder: " +
                self.__class__.__name__ + " error: " + str(e))
