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
import re

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


def initialize_data(doc):
    """ initialize the data by just making sure the template data element has been removed.
    All the data elements are going to be top level elements"""
    if 'data' in doc.keys():
        del doc['data']
        return doc


class GsdBuilder:
    def __init__(self, template, metadata):
        self.template = template
        self.metadata = metadata
        self.id = None
    
    def get_name(self, params_dict):
        _lat = int(params_dict['lat'])
        _lon = int(params_dict['lon'])
        # _elev = int(params_dict['elev'])
        for station_key in self.metadata.keys():
            # noinspection PyBroadException
            try:
                if not isinstance(self.metadata[station_key], dict):
                    continue
                if 'lat' not in self.metadata[station_key].keys():
                    continue
                if int(self.metadata[station_key]['lat']) == _lat and int(self.metadata[station_key]['lon']) == _lon:
                    return station_key
            except:
                e = sys.exc_info()[0]
                logging.error("GsdBuilder.get_name: Exception finding station to match lat and lon  error: " + str(e) +
                              " params: " + str(params_dict))

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
        _replacements = value.split('*')[1:]
        # skip the first replacement, its never
        # really a replacement. It is either '' or not a
        # replacement
        if len(_replacements) > 0:
            for _ri in _replacements:
                if _ri.startswith("{ISO}"):
                    if _ri == '{ISO}time':
                        value = value.replace("*" + _ri, convert_to_iso(interpolated_time))
                    else:
                        value = value.replace("*" + _ri, convert_to_iso(row[_ri]))
                else:
                    if _ri == 'time':
                        value = value.replace("*" + _ri, str(interpolated_time))
                    else:
                        value = value.replace("*" + _ri, str(row[_ri]))
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
            e = sys.exc_info()[0]
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
                for sub_key in self.template[key].keys():
                    self.handle_key(doc, row, sub_key, interpolated_time)  # recursion
            doc[key] = self.translate_template_item(doc[key], row, interpolated_time)
            return doc
        except:
            e = sys.exc_info()[0]
            logging.error("GsdBuilder.handle_key: Exception instantiating builder:  error: " + str(e))
    
    def handle_data(self, doc, row, interpolated_time):
        # noinspection PyBroadException
        try:
            _data_elem = {}
            _data_key = next(iter(self.template['data']))
            _data_template = self.template['data'][_data_key]
            for key in _data_template.keys():
                value = _data_template[key]
                value = self.translate_template_item(value, row, interpolated_time)
                _data_elem[key] = value
            if _data_key.startswith('&'):
                # invoke a named function
                # first create a **kwargs list of params.
                # &get_name:*lat,*lon,*elev => locals()['get_name']({lat:nnn,lon:nnn,elev:nnn})
                _func = _data_key.split(':')[0].replace('&', '')
                _params = _data_key.split(':')[1].split(',')
                _dict_params = {}
                for _p in _params:
                    # be sure to slice the * off of the front of the param
                    _dict_params[_p[1:]] = self.translate_template_item(_p, row, interpolated_time)
                _data_key = getattr(self, 'get_name')(_dict_params)
                if _data_key is None:
                    logging.warning(
                        "GsdBuilder: Using get_name - could not find station for " + row['name'] + str(_dict_params))
                    _data_key = row['name'] + "0"
            else:
                _data_key = self.translate_template_item(_data_key, row, interpolated_time)
                if _data_key is None:
                    logging.warning("GsdBuilder: Using template - could not find station for " + row['name'])
            doc[_data_key] = _data_elem
            return doc
        except:
            e = sys.exc_info()[0]
            logging.error("handle_data: Exception instantiating builder:  error: " + str(e))
