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
        which will be called from the Data_Type_Manager like this ...
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
import copy
from abc import ABC
import datetime as dt

TS_OUT_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class GsdBuilder(ABC):
    # Abstract Class for data_type builders
    def __init__(self):
        # used for date conversions
    
        return
    
    def get_id(self, a_time, an_id):
        # Private method to derive a document id from the current line.
        my_id = an_id
        my_id = my_id.replace('time', a_time)
        return my_id
    
    def convert_to_iso(self, a_time):
        _valid_time_str = dt.datetime.utcfromtimestamp(
            a_time).strftime(TS_OUT_FORMAT)
        return _valid_time_str


# Concrete data_type builders:
class GsdMetarObsBuilder(GsdBuilder):
    def __init__(self, template):
        super(GsdBuilder, self).__init__()
        self.template = template
    
    def handle_document(self, row, document_map):
        try:
            _document = copy.deepcopy(self.template)
            for k in _document.keys():
                if k == "id":
                    _document['id'] = GsdBuilder.get_id(self, str(row['time']),
                                                        self.template['id'])
                    continue
                if k == "data":
                    for kd in _document['data'].keys():
                        if _document['data'][kd].startswith("*"):
                            row_key = _document['data'][kd][1:]
                            _document['data'][kd] = row[row_key]
                        else:
                            if _document['data'][kd].startswith("ISO*"):
                                row_key = _document['data'][kd].replace('ISO*',
                                                                        '')
                                _document['data'][kd] = \
                                    GsdBuilder.convert_to_iso(self,
                                                              row[row_key])
                else:
                    if _document[k].startswith("*"):
                        row_key = _document[k][1:]
                        _document[k] = row[row_key]
                    else:
                        if _document[k].startswith("ISO*"):
                            row_key = _document[k].replace('ISO*', '')
                            _document[k] = \
                                GsdBuilder.convert_to_iso(self, row[row_key])
            # put document into document map
            document_map[_document['id']] = _document
        except:
            e = sys.exc_info()[0]
            logging.error(
                "Exception instantiating builder: " +
                self.__class__.__name__ + " error: " + str(
                    e))
