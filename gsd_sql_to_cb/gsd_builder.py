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
from decimal import Decimal
# needed to support SQL++ (N1QL) query
from couchbase.cluster import QueryOptions

import math

TS_OUT_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def convert_to_iso(an_epoch):
    if not isinstance(an_epoch, int):
        an_epoch = int(an_epoch)
    _valid_time_str = dt.datetime.utcfromtimestamp(an_epoch).strftime(TS_OUT_FORMAT)
    return _valid_time_str


def derive_id(an_id, row, interpolated_time):
    # Private method to derive a document id from the current row,
    # substituting *values from the corresponding row field as necessary.
    # noinspection PyBroadException
    try:
        _parts = an_id.split(':')
        new_parts = []
        for _part in _parts:
            if _part.startswith("*"):
                if _part == "*time":
                    value = str(interpolated_time)
                else:
                    value = GsdBuilder.translate_template_item(_part, row, interpolated_time)
                new_parts.append(str(value))
            else:
                new_parts.append(str(_part))
        _new_id = ":".join(new_parts)
        return _new_id
    except:
        e = sys.exc_info()
        logging.error("GsdBuilder.derive_id: Exception  error: " + str(e))


def initialize_data(doc):
    """ initialize the data by just making sure the template data element has been removed.
    All the data elements are going to be top level elements"""
    if 'data' in doc.keys():
        del doc['data']
    return doc


class GsdBuilder:
    def __init__(self, template, cluster, collection):
        self.template = template
        self.cluster = cluster
        self.collection = collection
        self.id = None
    
    def load_data(self, doc, key, element):
        pass
    
    def get_name(self, params_dict):
        pass
    
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
        # noinspection PyBroadException
        try:
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
                                if isinstance(row[_ri], Decimal):
                                    value = float(row[_ri])
                                else:
                                    value = row[_ri]
            return value
        except:
            e = sys.exc_info()
            logging.error("GsdBuilder.translate_template_item: Exception  error: " + str(e))
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
            if not isinstance(doc[key], dict) and isinstance(doc[key], str) and doc[key].startswith('&'):
                doc[key] = self.handle_named_function(doc[key], interpolated_time, row)
            else:
                doc[key] = self.translate_template_item(doc[key], row, interpolated_time)
            return doc
        except:
            e = sys.exc_info()
            logging.error("GsdBuilder.handle_key: Exception instantiating builder:  error: " + str(e))
        return doc

    def handle_named_function(self, _data_key, interpolated_time, row):
        # noinspection PyBroadException
        try:
            _func = _data_key.split(':')[0].replace('&', '')
            _params = _data_key.split(':')[1].split(',')
            _dict_params = {}
            for _p in _params:
                # be sure to slice the * off of the front of the param
                _dict_params[_p[1:]] = self.translate_template_item(_p, row, interpolated_time)
            _data_key = getattr(self, _func)(_dict_params)
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
                    value = self.handle_named_function(value, interpolated_time, row)
                else:
                    value = self.translate_template_item(value, row, interpolated_time)
                _data_elem[key] = value
            if _data_key.startswith('&'):
                _data_key = self.handle_named_function(_data_key, interpolated_time, row)
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
    
    def __init__(self, template, cluster, collection):
        """
        This builder creates a set of V01 obs documents using the V01 station metadata.
        The observation data is a set of top level elements that are keyed by the station name.
        :param template:
        :param collection: - essentially a couchbase connection object. It is used to retrieve metadata
        """
        GsdBuilder.__init__(self, template, cluster, collection)
        # noinspection PyBroadException
        try:
            # Retrieve the required station data
            self.stations = collection.get("MD:V01:METAR:stations").content
        except:
            logging.error("GsdStationsBuilderV01: error getting stations, " + str(sys.exc_info()))
    
    def load_data(self, doc, key, element):
        """
        This method adds an observation to the  document as a top level object keyed by the station name.
        :param doc: The document being created
        :param key: The station name
        :param element: the observation data
        :return: the document being created
        """
        doc[key] = element
        return doc
    
    def get_name(self, params_dict):
        """
        This method uses the lat and lon that are in the params_dict
        to find a station at that geopoint using math.isclose().
        :param params_dict:
        :return:
        """
        _lat = params_dict['lat']
        _lon = params_dict['lon']
        # noinspection PyBroadException
        try:
            for elem in self.stations:
                if not isinstance(self.stations[elem], dict):
                    continue
                if math.isclose(self.stations[elem]['lat'], _lat, abs_tol=0.05) and \
                        math.isclose(self.stations[elem]['lon'], _lon, abs_tol=0.05):
                    return elem
        except:
            e = sys.exc_info()
            logging.error("GsdBuilder.get_name: Exception finding station to match lat and lon  error: " + str(e) +
                          " params: " + str(params_dict))
        logging.info("station not found for lat: " + str(_lat) + " and lon " + str(_lon))
        return None


class GsdObsBuilderV02(GsdBuilder):
    def __init__(self, template, cluster, collection):
        """
        This builder creates a set of V02 obs documents using the V02 station metadata.
        In each document the observation data is an array of objects each of which is the obs data
        for a specific station.
        :param template:
        :param cluster: - a Couchbase cluster object, used for N1QL queries (QueryService)
        :param collection: - essentially a couchbase connection object, used to get documents by id (DataService)
        """
        GsdBuilder.__init__(self, template, cluster, collection)
        # noinspection PyBroadException
        try:
            # Retrieve the required stations
            self.stations = collection.get("MD:V02:METAR:stations").content
        except:
            logging.error("GsdStationsBuilderV02: error getting stations, " + str(sys.exc_info()))
    
    def load_data(self, doc, key, element):
        """
        This method appends an observation to the data array
        :param doc: The document being created
        :param key: Not used
        :param element: the observation data
        :return: the document being created
        """
        if 'data' not in doc.keys() or doc['data'] is None:
            doc['data'] = []
        doc['data'].append(element)
        return doc
    
    def get_name(self, params_dict):
        """
        This method uses the lat and lon that are in the params_dict
        to find a station at that geopoint using math.isclose().
        :param params_dict:
        :return:
        """
        _lat = params_dict['lat']
        _lon = params_dict['lon']
        # noinspection PyBroadException
        try:
            for elem in self.stations['data']:
                if not isinstance(elem, dict):
                    continue
                if math.isclose(elem['lat'], _lat, abs_tol=0.05) and math.isclose(elem['lon'], _lon, abs_tol=0.05):
                    return elem['name']
        except:
            e = sys.exc_info()
            logging.error("GsdObsBuilderV02.get_name: Exception finding station to match lat and lon  error: " + str(e) +
                          " params: " + str(params_dict))
        logging.info("station not found for lat: " + str(_lat) + " and lon " + str(_lon))
        return None


class GsdObsBuilderV03(GsdBuilder):
    def __init__(self, template, cluster, collection):
        """
        This builder creates a set of V03 obs documents using the V03 station documents.
        The primary difference is that this builder does not load station data into memory,
        instead it uses a search to find the associated station.
        In each document the observation data is an array of objects each of which is the obs data
        for a specific station.
        :param template: the document template from the ingest document
        :param cluster: - a Couchbase cluster object, used for N1QL queries (QueryService)
        :param collection: - essentially a couchbase connection object, used to get documents by id (DataService)
        """
        GsdBuilder.__init__(self, template, cluster, collection)
        self.cluster = cluster

    def get_name(self, params_dict):
        """
        This method uses the lat and lon that are in the params_dict
        to find a station at that geopoint using a geospatial search.
        :param params_dict:
        :return:
        """
        _lat = params_dict['lat']
        _lon = params_dict['lon']
        # noinspection PyBroadException
        try:
            cluster = self.cluster
            n1ql_query = 'select raw meta().id from mdata where type="DD" and docType="station" ' \
                        'and subset = "METAR"  and version ="V03" and geo.lat = $1 and geo.lon = $2'
            row_iter = cluster.query(n1ql_query, QueryOptions(positional_parameters=[_lat, _lon]))
            _id = next(iter(row_iter))
            # since the id is resident in the index, and the name is part of the id we can just
            # parse the name out of the id and avoid doing a fetch. Saves a few milliseconds, possibly.
            if _id is not None:
                return str.split(_id, ':')[4]
        except:
            e = sys.exc_info()
            logging.error("GsdObsBuilderV03.get_name: Exception finding station to match lat and lon  error: " + str(e) +
                          " params: " + str(params_dict))
        logging.info("station not found for lat: " + str(_lat) + " and lon " + str(_lon))
        return None

    def load_data(self, doc, key, element):
        """
        This method appends an observation to the data array
        :param doc: The document being created
        :param key: Not used
        :param element: the observation data
        :return: the document being created
        """
        if 'data' not in doc.keys() or doc['data'] is None:
            doc['data'] = []
        doc['data'].append(element)
        return doc


class GsdObsBuilderV04(GsdBuilder):
    def __init__(self, template, cluster, collection):
        """
        This builder creates a set of V03 obs documents using the V03 station documents.
        The primary difference is that this builder loads V03 station data into memory,
        like the GsdObsBuilderV01 and GsdObsBuilderV02, but from the V03 station objects.
        This is important because it turns out that we have to keep appending to the station data
        and that is most quickly done with an upsert, even if it is an upsert of thousands of
        small documents, like stations.
        In each document the observation data is an array of objects each of which is the obs data
        for a specific station.
        :param template: the document template from the ingest document
        :param cluster: - a Couchbase cluster object, used for N1QL queries (QueryService)
        :param collection: - essentially a couchbase connection object, used to get documents by id (DataService)
        """
        GsdBuilder.__init__(self, template, cluster, collection)
        self.cluster = cluster
        self.stations = []
        # noinspection PyBroadException
        try:
            # Retrieve the required station data
            n1ql_query = 'SELECT raw {mdata.name, mdata.geo.lat, mdata.geo.lon} FROM mdata ' \
                        'WHERE type="DD" AND docType="station" AND subset="METAR" AND version ="V03"'
            row_iter = cluster.query(n1ql_query, QueryOptions(read_only=True))
            for _station in row_iter:
                self.stations.append(_station)
            
        except:
            logging.error("GsdStationsBuilderV01: error getting stations, " + str(sys.exc_info()))
    
    def get_name(self, params_dict):
        """
         This method uses the lat and lon that are in the params_dict
         to find a station at that geopoint using math.isclose().
         :param params_dict:
         :return:
         """
        _lat = params_dict['lat']
        _lon = params_dict['lon']
        # noinspection PyBroadException
        try:
            for elem in self.stations:
                if not isinstance(elem, dict):
                    continue
                if math.isclose(elem['lat'], _lat, abs_tol=0.05) and math.isclose(elem['lon'], _lon, abs_tol=0.05):
                    return elem['name']
        except:
            e = sys.exc_info()
            logging.error("GsdObsBuilderV02.get_name: Exception finding station to match lat and lon  error: " + str(
                e) + " params: " + str(params_dict))
        logging.info("station not found for lat: " + str(_lat) + " and lon " + str(_lon))
        return None
    
    def load_data(self, doc, key, element):

        """
        This method appends an observation to the data array
        :param doc: The document being created
        :param key: Not used
        :param element: the observation data
        :return: the document being created
        """
        if 'data' not in doc.keys() or doc['data'] is None:
            doc['data'] = []
        doc['data'].append(element)
        return doc


class GsdModelBuilderV04(GsdBuilder):
    def __init__(self, template, cluster, collection):
        """
        This builder creates a set of V03 model documents using the V03 station documents.
        This builder is very much like the GsdObsBuilderV04 except that it builds model documents.
        In each document the specific model data is an array of objects each of which is the model data
        for a specific station. The particular model name is supplied in the template.
        :param template: the document template from the ingest document
        :param cluster: - a Couchbase cluster object, used for N1QL queries (QueryService)
        :param collection: - essentially a couchbase connection object, used to get documents by id (DataService)
        """
        GsdBuilder.__init__(self, template, cluster, collection)
        self.cluster = cluster
        self.stations = []
        # noinspection PyBroadException
        try:
            # Retrieve the required station data
            n1ql_query = 'SELECT raw {mdata.name, mdata.geo.lat, mdata.geo.lon} FROM mdata ' \
                        'WHERE type="DD" AND docType="station" AND subset="METAR" AND version ="V03"'
            row_iter = cluster.query(n1ql_query, QueryOptions(read_only=True))
            for _station in row_iter:
                self.stations.append(_station)
        
        except:
            logging.error("GsdStationsBuilderV01: error getting stations, " + str(sys.exc_info()))
    
    def load_data(self, doc, key, element):
        
        """
        This method appends an observation to the data array
        :param doc: The document being created
        :param key: Not used
        :param element: the observation data
        :return: the document being created
        """
        if 'data' not in doc.keys() or doc['data'] is None:
            doc['data'] = []
        doc['data'].append(element)
        return doc


class GsdStationsBuilderV01(GsdBuilder):
    def __init__(self, template, cluster, collection):
        """
        This builder creates a single document of stations with the stations being top level
        objects keyed by the station name which is an ICAO like 'CWDP'.
        :type template: object - This is the template from "MD:V01:METAR:stations"
        :param cluster: - a Couchbase cluster object, used for N1QL queries (QueryService)
        :param collection: - essentially a couchbase connection object, used to get documents by id (DataService)
        """
        GsdBuilder.__init__(self, template, cluster, collection)
    
    def load_data(self, doc, key, element):
        """
        This method sets the element for a specific data row as a top level object in the
        document.
        :param doc: object - this is the main document that is being created
        :param key: The key that this element will be indexed by
        :param element: an object that has been translated from a data row
        """
        doc[key] = element
        return doc


class GsdStationsBuilderV02(GsdBuilder):
    def __init__(self, template, cluster, collection):
        """
        This builder creates a single document of stations with the stations being
        object elements of the data array.
        :type template: object - This is the template from "MD:V02:METAR:stations"
        :param cluster: - a Couchbase cluster object, used for N1QL queries (QueryService)
        :param collection: - essentially a couchbase connection object, used to get documents by id (DataService)
        """
        GsdBuilder.__init__(self, template, cluster, collection)
    
    def load_data(self, doc, key, element):
        """
        This method sets the element for a specific data row as an object element in the
        document['data'] array.
        :param element: an object that has been translated from a data row
        :param key: The key that this element will be indexed by
        :type doc: object - this is the main document that is being created
        """
        if 'data' not in doc.keys() or doc['data'] is None:
            doc['data'] = []
        doc['data'].append(element)
        return doc


class GsdStationsBuilderV03(GsdBuilder):
    def __init__(self, template, cluster, collection):
        """
        This builder creates multiple documents one per station with the id being
        MD:V03:METAR:stations:*name where the variable part of the id
        is the the station name which, is an ICAO, like CWDP. So an ID
        might be "MD:V03:METAR:stations:CWDP". The lat, lon, and elevation are
        encoded into a "geo" element such that we can create a geojson search index over the data.
        :type template: object - This is the template from "MD:V03:METAR:stations"
        :param cluster: - a Couchbase cluster object, used for N1QL queries (QueryService)
        :param collection: - essentially a couchbase connection object, used to get documents by id (DataService)
        """
        GsdBuilder.__init__(self, template, cluster, collection)
