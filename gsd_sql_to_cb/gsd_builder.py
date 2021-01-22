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
from abc import ABC, abstractmethod
import datetime as dt

TS_OUT_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def get_id(an_id, row, interpolated_time):
    # Private method to derive a document id from the current row,
    # substituting *values from the corresponding row field as necessary.
    _parts = an_id.split('::')
    new_parts = []
    for _part in _parts:
        if _part.startswith("*"):
            if _part == "*time":
                value = str(interpolated_time)
            else:
                value = str(row[_part[1:]])
            new_parts.append(value)
        else:
            new_parts.append(str(_part))
    _new_id = "::".join(new_parts)
    return _new_id


def convert_to_iso(an_epoch):
    _valid_time_str = dt.datetime.utcfromtimestamp(an_epoch).strftime(
        TS_OUT_FORMAT)
    return _valid_time_str


class GsdBuilder(ABC):
    # Abstract Class for data_type builders
    def __init__(self):
        self.doc = {}
        self.row = {}
        self.template = self.get_template()
        
    def handle_document(self, interpolated_time, rows, document_map):
        """
        This is the entry point for any GsdBuilder, it must be called
        from a GsdIngestManager.
        :param interpolated_time: The closest time to the cadence within the
        delta.
        :param rows: This is a row array that contains rows from the result set
        that all have the same a_time. There may be many stations in this row
        array, AND importantly the document id derived from this a_time may
        already exist in the document. If the id does not exist it will be
        created, if it does exist, the data will be appended.
        :param document_map: This is the top level dictionary to which this
        builder's documents will be added, the GsdIngestManager will do the
        upseert
        :return: The modified document_map
        """
        # noinspection PyBroadException
        try:
            self.doc = copy.deepcopy(self.template)
            if len(rows) == 0:
                return document_map
            for r in rows:
                self.row = r
                self.doc['data'] = {}
                for k in self.doc.keys():
                    if k == "data":
                        self.handle_data()
                        continue
                    self.handle_key(k, interpolated_time)
            # put document into document map
            if self.doc['id'] in document_map.keys():
                # put data into existing data map
                for madis_id in self.doc['data'].keys():
                    document_map[self.doc['id']]['data'][madis_id] = \
                        self.doc['data'][madis_id]
            else:
                document_map[self.doc['id']] = self.doc
            return document_map
        except:
            e = sys.exc_info()[0]
            logging.error(
                "Exception instantiating builder: " +
                self.__class__.__name__ + " error: " + str(e))
    
    def handle_key(self, key, interpolated_time):
        """
        This routine handles keys by substituting row fields into the values
        in the template that begin with *
        :param interpolated_time: The closest time to the cadence within the
        delta.
        :param key: A key to be processed, This can be a key to a primitive
        or to another dictionary
        """
        # noinspection PyBroadException
        try:
            if key == 'id':
                self.doc[key] = get_id(self.template['id'], self.row,
                                       interpolated_time)
            
            if isinstance(self.doc[key], dict):
                # process an embedded dictionary
                for sub_key in self.template[key].keys():
                    self.handle_key(sub_key)  # recursion here
            if self.template[key].startswith("*"):
                if self.template[key] == "*time":
                    value = str(interpolated_time)
                else:
                    value = str(self.row[self.template[key][1:]])
                self.doc[key] = value
            else:
                if self.template[key].startswith("ISO*"):
                    row_key = self.template[key].replace('ISO*', '')
                    self.doc[key] = convert_to_iso(self.row[row_key])
        except:
            e = sys.exc_info()[0]
            logging.error(
                "Exception instantiating builder: " +
                self.__class__.__name__ + " error: " + str(
                    e))
    
    @abstractmethod
    def get_template(self):
        """
        template is overridden in subclass so we don't have to pass it all
        the a_time
        :return: template
        """
        raise NotImplementedError("Must override get_template")

    @abstractmethod
    def handle_data(self):
        """
        This is the method that processes the data key. It must be
        overridden by the concrete builder
        """
        raise NotImplementedError("Must override handle_data")


# Concrete GsdBuilders:
"""
GsdMetarObsBuilder
This class is the builder for METAR obs. METAR obs are
kept in the GSL tables madis3, ceiling2, and visibility.
This class will transform those tables, based on a template from a metdata
object, into Couchbase documents.
"""


class GsdMetarObsBuilder(GsdBuilder):
    
    def __init__(self, template):
        super(GsdBuilder, self).__init__()
        self.template = template
        
    def get_template(self):
        return self.template

    def handle_data(self):
        """
        This is the only responsibility of the builder.
        It receives a database row and processes it into
        a data element based on the template modifying the self.doc that is
        maintained in the parent class GsdBuilder.
        """
        # noinspection PyBroadException
        try:
            _data_elem = {}
            for k in self.template['data'].keys():
                if self.template['data'][k].startswith("*"):
                    row_key = self.template['data'][k][1:]
                    _data_elem[row_key] = self.row[row_key]
                else:
                    if self.template['data'][k].startswith("ISO*"):
                        row_key = self.template['data'][k].replace('ISO*', '')
                        _data_elem[k] = convert_to_iso(self.row[row_key])
            self.doc['data'][self.row['madis_id']] = _data_elem
        except:
            e = sys.exc_info()[0]
            logging.error(
                "Exception instantiating builder: " +
                self.__class__.__name__ + " error: " + str(e))
