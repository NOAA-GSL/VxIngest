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

import copy
import datetime as dt
import logging
import sys
from abc import ABC, abstractmethod

TS_OUT_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def convert_to_iso(an_epoch):
    if not isinstance(an_epoch, int):
        an_epoch = int(an_epoch)
    _valid_time_str = dt.datetime.utcfromtimestamp(an_epoch).strftime(TS_OUT_FORMAT)
    return _valid_time_str


def derive_id(an_id, row, interpolated_time):
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


class GsdBuilder(ABC):
    # Abstract Class for data_type builders
    def __init__(self):
        self.id = ""
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
            """
            We keep a local copy of the document map because I don't know
            how to pass by reference in python and I don't want the
            interpreter to constantly be making copies of the document map.
            The only rational way I see to do this is to use an abstract
            setter so that the child class actually implements the map.
            """
            self.set_document_map(document_map)
            self.doc = copy.deepcopy(self.template)
            if len(rows) == 0:
                return document_map
            for r in rows:
                self.row = r
                self.initialize_data()
                for k in self.template.keys():
                    if k == "data":
                        self.handle_data()
                        continue
                    self.handle_key(k, interpolated_time)
            # put document into document map
            self.load_data()
            return self.get_document_map()
        except:
            e = sys.exc_info()[0]
            logging.error("GsdBuilder.handle_document: Exception instantiating "
                          "builder: " + self.__class__.__name__ + " error: " + str(e))
    
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
                _an_id = derive_id(self.template['id'], self.row, interpolated_time)
                self.set_id(_an_id)
                return
            if isinstance(self.doc[key], dict):
                # process an embedded dictionary
                for sub_key in self.template[key].keys():
                    self.handle_key(sub_key, interpolated_time)  # recursion
            if self.template[key].startswith("*"):
                row_key = self.template[key][1:]
                if self.template[key] == "fcstValidEpoch":
                    value = interpolated_time
                else:
                    value = self.row[row_key]
                self.doc[key] = value
            else:
                if self.template[key].startswith("ISO*"):
                    row_key = self.template[key].replace('ISO*', '')
                    if self.template[key] == "fcstValidBeg":
                        value = convert_to_iso(interpolated_time)
                    else:
                        value = convert_to_iso(self.row[row_key])
                    self.doc[key] = value
        except:
            e = sys.exc_info()[0]
            logging.error(
                "GsdBuilder.handle_key: Exception instantiating builder: " + self.__class__.__name__ + " error: " + str(
                    e))
    
    """
    We keep a local copy of the document map because I don't know how to
    pass by reference in python and I don't want the interpreter
    to constantly be making copies of the document map. The only rational
    way I see to do this is to use an abstract setter so that the child class
    actually implements the map.
    """
    
    @abstractmethod
    def set_document_map(self, document_map):
        raise NotImplementedError("Must override set_document_map")
    
    @abstractmethod
    def get_document_map(self):
        raise NotImplementedError("Must override get_document_map")
    
    @abstractmethod
    def set_id(self, an_id):
        raise NotImplementedError("Must override set_id")
    
    @abstractmethod
    def get_id(self):
        raise NotImplementedError("Must override get_id")
    
    @abstractmethod
    def initialize_data(self):
        raise NotImplementedError("Must override initialize_data")
    
    @abstractmethod
    def load_data(self):
        raise NotImplementedError("Must override load_data")
    
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
This class is the builder for METAR obs. METAR obs are derived from
the GSL tables madis3.obs, ceiling2.obs, and visibility.obs.
This class will transform those tables, based on a "MD::V01::METAR::obs"
metdata object, into Couchbase documents.
"""


class GsdMetarObsBuilder(GsdBuilder):
    
    def __init__(self, template):
        super(GsdBuilder, self).__init__()
        self.document_map = {}
        self.template = template
        self.id = ""
    
    def get_template(self):
        return self.template
    
    def initialize_data(self):
        self.doc['data'] = []
    
    def set_document_map(self, document_map):
        self.document_map = document_map
    
    def get_document_map(self):
        return self.document_map
    
    def set_id(self, an_id):
        self.id = an_id
    
    def get_id(self):
        return self.id
    
    def load_data(self):
        # we do not really want the id to be IN the document
        # so we delete the id element. We needed it in the
        # template to tell us to set an id and what the id format
        # would be
        del self.doc['id']
        if self.id in self.document_map.keys():
            # append data to existing document data map
            self.document_map[self.id]['data'].extend(self.doc['data'])
        else:
            # it is a new document
            self.document_map[self.id] = self.doc
    
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
            self.doc['data'].append(_data_elem)
        except:
            e = sys.exc_info()[0]
            logging.error("GsdMetarObsBuilder.handle_data: Exception instantiating "
                          "builder: " + self.__class__.__name__ + " error: " + str(e))


"""
GsdSingleDocumentMapBuilder
This class is the builder for things like GSD stations. Stations are derived
from the madis3.metars_mats_global table. This class will transform that
table, based on a template from an "MD::V01::METAR::STATIONS" metadata
document, into a single Couchbase document. The stations are keyed
by the stationName and an ancestor number
"""


class GsdSingleDocumentMapBuilder(GsdBuilder):
    def __init__(self, template):
        super(GsdBuilder, self).__init__()
        self.document_map = {}
        self.template = template
        self.id = ""
    
    def get_template(self):
        return self.template
    
    def initialize_data(self):
        """ for a singleDocumentMapBuilder we initialize
        the data by just making sure the data element has been removed.
        All the data elements are going to be top level elements"""
        if 'data' in self.doc.keys():
            del self.doc['data']
    
    def set_document_map(self, document_map):
        self.document_map = document_map
    
    def get_document_map(self):
        return self.document_map
    
    def set_id(self, an_id):
        self.id = an_id
    
    def get_id(self):
        return self.id
    
    def load_data(self):
        # we do not really want the id to be IN the document
        # so we delete the id element. We needed it in the
        # template to tell us to set an id and what the id format
        # would be
        del self.doc['id']
        self.document_map[self.id] = self.doc
    
    def translate_template_item(self, value):
        _replacements = value.split('*')[1:]
        # skip the first replacement, its never
        # really a replacement. It is either '' or not a
        # replacement
        if len(_replacements) > 0:
            for _ri in _replacements:
                if _ri.startswith("{ISO}"):
                    row_key = _ri.replace('{ISO}', '')
                    value = value.replace("*" + _ri, convert_to_iso(self.row[row_key]))
                else:
                    value = value.replace("*" + _ri, str(self.row[_ri]))
        return value
    
    def handle_data(self):
        """
        This is the only responsibility of the builder.
        It receives a database row and processes it into
        a data element based on the template modifying the self.doc that is
        maintained in the parent class GsdBuilder.
        For a singleDocumentArrayBuilder the output will be a single document
        that has a data section that is an array of objects.
        """
        # noinspection PyBroadException
        try:
            _data_elem = {}
            _data_key = next(iter(self.template['data']))
            _data_template = self.template['data'][_data_key]
            for key in _data_template.keys():
                value = _data_template[key]
                value = self.translate_template_item(value)
                _data_elem[key] = value
            _data_key = self.translate_template_item(_data_key)
            self.doc[_data_key] = _data_elem
        except:
            e = sys.exc_info()[0]
            logging.error("GsdSingleDocumentArrayBuilder.handle_data: "
                          "Exception instantiating "
                          "builder: " + self.__class__.__name__ + " error: " + str(e))
