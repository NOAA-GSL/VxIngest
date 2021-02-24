"""
Program Name: Class gsd_builder.py
Contact(s): Randy Pierce
History Log:  Initial version
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSD
"""

import copy
import datetime as dt
import logging
import sys

from couchbase.exceptions import DocumentNotFoundException, TimeoutException

TS_OUT_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def convert_to_iso(an_epoch):
    if not isinstance(an_epoch, int):
        an_epoch = int(an_epoch)
    _valid_time_str = dt.datetime.utcfromtimestamp(an_epoch).strftime(TS_OUT_FORMAT)
    return _valid_time_str


def derive_id(an_id, row, interpolated_time):
    # Private method to derive a document id from the current row,
    # substituting *values from the corresponding row field as necessary.
    _parts = an_id.split(':')
    new_parts = []
    for _part in _parts:
        if _part.startswith("*"):
            if _part == "*time":
                value = str(interpolated_time)
            else:
                value = GsdBuilder.translate_template_item(_part, row, interpolated_time)
            new_parts.append(value)
        else:
            new_parts.append(str(_part))
    _new_id = ":".join(new_parts)
    return _new_id


def initialize_data(doc):
    """ initialize the data by just making sure the template data element has been removed.
    All the data elements are going to be top level elements"""
    if 'data' in doc.keys():
        del doc['data']
    return doc


class GsdBuilder:
    def __init__(self, template, collection):
        self.metadata = None
        self.template = template
        self.collection = collection
        self.id = None

    def load_data(self, doc, key, element):
        pass

    @staticmethod
    def conv_latlon(meta_data, params_dict):
        return "{0:.4f}".format(next(iter(params_dict.values())) / 182)
    
    @staticmethod
    # not yet implemented
    def conv_elev(meta_data, params_dict):
        return next(iter(params_dict.values()))

    @staticmethod
    def get_name(metadata, params_dict):
        _lat = params_dict['lat']
        _lon = params_dict['lon']
        # _elev = params_dict['elev']
        # noinspection PyBroadException
        try:
            if metadata['version'] == "V02":
                for station in metadata['data']:
                    if isinstance(station, dict):
                        if station['lat'] == _lat and station['lon'] == _lon:
                            return station['name']
            elif metadata['version'] == "V01":
                for elem in metadata:
                    if isinstance(metadata[elem], dict):
                        if metadata[elem]['lat'] == _lat and metadata[elem]['lon'] == _lon:
                            return elem
        except:
            e = sys.exc_info()
            logging.error("GsdBuilder.get_name: Exception finding station to match lat and lon  error: " + str(
                e) + " params: " + str(params_dict))
        return None

    @staticmethod
    def translate_template_item(value, row, interpolated_time):
        """
        This method translates template replacements (*item).
        It can translate keys or values. In addition it can eval a named function
        with locals(). Example get_name(param_dict)
        :param interpolated_time: either the time in the row['time'] or the interpolated_time if required.
        :param row: a row from the result set
        :param value: a value from the template
        :return:
        """
        _replacements = []
        if isinstance(value, str):
            _replacements = value.split('*')[1:]
        # skip the first replacement, its never
        # really a replacement. It is either '' or not a
        # replacement
        _make_str = False
        if len(_replacements) > 0:
            for _ri in _replacements:
                if _ri.startswith('{ISO}') or type(row[_ri]) is str:
                    _make_str = True
                    break
            for _ri in _replacements:
                if _ri.startswith("{ISO}"):
                    if _ri == '{ISO}time':
                        value = value.replace("*" + _ri, convert_to_iso(interpolated_time))
                    else:
                        value = value.replace("*" + _ri, convert_to_iso(row[_ri]))
                else:
                    if _ri == 'time':
                        value = interpolated_time
                    else:
                        if _make_str:
                            value = value.replace('*' + _ri, str(row[_ri]))
                        else:
                            value = row[_ri]
        return value
    
    def handle_document(self, interpolated_time, rows, document_map):
        """
        This is the entry point for any GsdBuilder, it must be called
        from a GsdIngestManager.
        :param interpolated_time: The time field in a row, if there is one,
        may have been interpolated in the ingest_manager to the closest time
        to the cadence within the delta. If interpolation was not required then the
        interpolated_time will be the same as the row['time']
        :param rows: This is a row array that contains rows from the result set
        that all have the same a_time. There may be many stations in this row
        array, AND importantly the document id derived from this a_time may
        already exist in the document. If the id does not exist it will be
        created, if it does exist, the data will be appended.
        :param document_map: This is the top level dictionary to which this
        builder's documents will be added, the GsdIngestManager will do the
        upsert
        :return: The modified document_map
        """
        # noinspection PyBroadException
        try:
            doc = copy.deepcopy(self.template)
            document_map = document_map
            if len(rows) == 0:
                return document_map
            doc = initialize_data(doc)
            for r in rows:
                for k in self.template.keys():
                    if k == "data":
                        doc = self.handle_data(doc, r, interpolated_time)
                        continue
                    doc = self.handle_key(doc, r, k, interpolated_time)
            # remove id (it isn't needed inside the doc, we needed it in the template
            # to tell us how to format the id)
            del doc['id']
            # put document into document map
            document_map[self.id] = doc
            return document_map
        except:
            e = sys.exc_info()
            logging.error("GsdBuilder.handle_document: Exception instantiating "
                          "builder: " + self.__class__.__name__ + " error: " + str(e))
    
    def handle_key(self, doc, row, key, interpolated_time):
        """
        This routine handles keys by substituting row fields into the values
        in the template that begin with *
        :param doc: the current document
        :param row: The data row from the mysql result set
        :param interpolated_time: The closest time to the cadence within the
        delta.
        :param key: A key to be processed, This can be a key to a primitive
        or to another dictionary
        """
        # noinspection PyBroadException
        try:
            if key == 'id':
                self.id = derive_id(self.template['id'], row, interpolated_time)
                return doc
            if isinstance(doc[key], dict):
                # process an embedded dictionary
                _tmp_doc = copy.deepcopy(self.template[key])
                for sub_key in _tmp_doc.keys():
                    _tmp_doc = self.handle_key(_tmp_doc, row, sub_key, interpolated_time)  # recursion
                doc[key] = _tmp_doc
            if not isinstance(doc[key], dict) and \
                    isinstance(doc[key], str) and doc[key].startswith('&'):
                doc[key] = self.handle_named_function(self.metadata, doc[key], interpolated_time, row)
            else:
                doc[key] = self.translate_template_item(doc[key], row, interpolated_time)
            return doc
        except:
            e = sys.exc_info()
            logging.error("GsdBuilder.handle_key: Exception instantiating builder:  error: " + str(e))
    
    def handle_named_function(self, metadata, _data_key, interpolated_time, row):
        # noinspection PyBroadException
        try:
            _func = _data_key.split(':')[0].replace('&', '')
            _params = _data_key.split(':')[1].split(',')
            _dict_params = {}
            for _p in _params:
                # be sure to slice the * off of the front of the param
                _dict_params[_p[1:]] = self.translate_template_item(_p, row, interpolated_time)
            _data_key = getattr(self, _func)(metadata, _dict_params)
            if _data_key is None:
                logging.warning(
                    "GsdBuilder: Using " + _func + " - could not find station for " + row['name'] + str(_dict_params))
                _data_key = row['name'] + "0"
                return _data_key
        except:
            e = sys.exc_info()
            logging.error("handle_named_function: Exception instantiating builder:  error: " + str(e))
        return _data_key

    def handle_data(self, doc, row, interpolated_time):
        # noinspection PyBroadException
        try:
            _data_elem = {}
            _data_key = next(iter(self.template['data']))
            _data_template = self.template['data'][_data_key]
            for key in _data_template.keys():
                value = _data_template[key]
                if value.startswith('&'):
                    value = self.handle_named_function(self.metadata, value, interpolated_time, row)
                else:
                    value = self.translate_template_item(value, row, interpolated_time)
                _data_elem[key] = value
            if _data_key.startswith('&'):
                _data_key = self.handle_named_function(self.metadata, _data_key, interpolated_time, row)
            else:
                _data_key = self.translate_template_item(_data_key, row, interpolated_time)
            if _data_key is None:
                logging.warning("GsdBuilder: Using template - could not find station for " + row['name'])
            doc = self.load_data(doc, _data_key, _data_elem)
            return doc
        
        except:
            e = sys.exc_info()
            logging.error("handle_data: Exception instantiating builder:  error: " + str(e))
            return doc


""" Concrete builders """


class GsdObsBuilderV01(GsdBuilder):
    def __init__(self, template, collection):
        GsdBuilder.__init__(self, template, collection)
        # noinspection PyBroadException
        try:
            self.metadata = collection.get("MD:V01:METAR:stations").content
        except:
            logging.error("GsdStationsBuilderV01: error getting metadata, " + str(sys.exc_info()))
    
    def load_data(self, doc, key, element):
        doc[key] = element
        return doc


class GsdObsBuilderV02(GsdBuilder):
    def __init__(self, template, collection):
        GsdBuilder.__init__(self, template, collection)
        # noinspection PyBroadException
        try:
            self.metadata = collection.get("MD:V02:METAR:stations").content
        except:
            logging.error("GsdStationsBuilderV02: error getting metadata, " + str(sys.exc_info()))

    def load_data(self, doc, key, element):
        if 'data' not in doc.keys() or doc['data'] is None:
            doc['data'] = []
        doc['data'].append(element)
        return doc


class GsdStationsBuilderV01(GsdBuilder):
    def __init__(self, template, collection):
        """
        This builder creates a single document of stations with the stations being top level
        objects keyed by the station name which is an ICAO with a trailing ancestor count.
        like CWDP0.
        :type template: object - This is the template from "MD:V01:METAR:stations"
        :type collection: object - this is a connection to the couchbase mdata default collection
        """
        GsdBuilder.__init__(self, template, collection)
    
    def load_data(self, doc, key, element):
        doc[key] = element
        return doc


class GsdStationsBuilderV02(GsdBuilder):
    def __init__(self, template, collection):
        """
        This builder creates a single document of stations with the stations being top level
        objects keyed by the station name which is an ICAO with a trailing ancestor count.
        like CWDP0.
        :type template: object - This is the template from "MD:V02:METAR:stations"
        :type collection: object - this is a connection to the couchbase mdata default collection
        """
        GsdBuilder.__init__(self, template, collection)

    def load_data(self, doc, key, element):
        if 'data' not in doc.keys() or doc['data'] is None:
            doc['data'] = []
        doc['data'].append(element)
        return doc


class GsdStationsBuilderV03(GsdBuilder):
    def __init__(self, template, collection):
        """
        This builder creates multiple documents one per station with the id being
        MD:V03:METAR:stations:*name*ancestor_count where the variable part of the id
        is the the station name which, is an ICAO with a trailing ancestor count.
        like CWDP0. So an ID might be "MD:V03:METAR:stations:CWDP0". The lat, lon, and elvation are
        encoded into a "geo" element such that we can create a geojson searc index over the data.
        :type template: object - This is the template from "MD:V03:METAR:stations"
        :type collection: object - this is a connection to the couchbase mdata default collection
        """
        GsdBuilder.__init__(self, template, collection)
