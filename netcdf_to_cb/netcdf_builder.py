"""
Program Name: Class sql_builder.py
Contact(s): Randy Pierce
History Log:  Initial version
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSL
"""

import copy
import datetime as dt
import logging
import sys
import re
import pymysql
import math
import time
import netCDF4 as nc
import re
from decimal import Decimal
from couchbase.cluster import QueryOptions
from couchbase.search import QueryStringQuery
from pymysql.constants import CLIENT
from datetime import datetime, timedelta

TS_OUT_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

def convert_to_iso(an_epoch):
    if not isinstance(an_epoch, int):
        an_epoch = int(an_epoch)
    _valid_time_str = dt.datetime.utcfromtimestamp(an_epoch).strftime(TS_OUT_FORMAT)
    return _valid_time_str


def derive_id(template_id, recNum):
    # Private method to derive a document id from the current recNum,
    # substituting *values from the corresponding netcdf fields as necessary.
    # noinspection PyBroadException
    try:
        _parts = template_id.split(':')
        new_parts = []
        for _part in _parts:
            if _part.startswith("*"):
                value = NetcdfBuilder.translate_template_item(_part, recNum)
                new_parts.append(str(value))
            else:
                new_parts.append(str(_part))
        _new_id = ":".join(new_parts)
        return _new_id
    except:
        e = sys.exc_info()
        logging.error("NetcdfBuilder.derive_id: Exception  error: " + str(e))


def initialize_data(doc):
    """ initialize the data by just making sure the template data element has been removed.
    All the data elements are going to be top level elements"""
    if 'data' in doc.keys():
        del doc['data']
    return doc


class NetcdfBuilder:
    def __init__(self, load_spec, ingest_document, cluster, collection):
        self.template = ingest_document['template']
        self.load_spec = load_spec
        self.cluster = cluster
        self.collection = collection
        self.id = None
        self.document_map = {}
        self.ncdf_data_set = None
        
    def load_data(self, doc, key, element):
        pass
        
    def get_document_map(self):
        pass
    
    def handle_recNum(self, row):
        pass

    def translate_template_item(self, variable, recNum):
        """
        This method translates template replacements (*item).
        It can translate keys or values. 
        :param variable: a value from the template - should be a netcdf variable
        :param recNum: the current recNum
        :return:
        """
        _replacements = []
        # noinspection PyBroadException
        try:
            if isinstance(variable, str):
                _replacements = variable.split('*')[1:]
            # skip the first replacement, its never
            # really a replacement. It is either '' or not a
            # replacement
            _make_str = False
            value = variable
            if len(_replacements) > 0:
                for _ri in _replacements:
                    vtype = self.ncdf_data_set.variables[variable].dtype
                    if _ri.startswith('{ISO}') or vtype is '|S1':
                        _make_str = True
                        break
                for _ri in _replacements:
                    if _ri.startswith("{ISO}"):
                        value = value.replace("*" + _ri, convert_to_iso(self.ncdf_data_set[_ri][recNum]))
                    else:
                        if _make_str:
                            value = value.replace('*' + _ri, str(self.ncdf_data_set[_ri][recNum]))
                        else:
                            value = self.ncdf_data_set[_ri][recNum]
            return value
        except Exception as e:
            logging.error("NetcdfBuilder.translate_template_item: Exception  error: " + str(e))
        return value
    
    
    def handle_document(self):
        """
        :return: The modified document_map
        """
        # noinspection PyBroadException
        try:
            doc = copy.deepcopy(self.template)
            _recNum_data_size = self.ncdf_data_set.dimensions['recNum'].size
            if _recNum_data_size == 0:
                return
            doc = initialize_data(doc)
            for _recNum in range(_recNum_data_size):
                for _key in self.template.keys():
                    if _key == "data":
                        doc = self.handle_data(doc, _recNum)
                        continue
                    doc = self.handle_key(doc, _recNum, _key)
            # remove id (it isn't needed inside the doc, we needed it in the template
            # to tell us how to format the id)
            del doc['id']
            # put document into document map
            self.document_map[self.id] = doc
        except Exception as e:
            logging.error(self.__class__.__name__ + "NetcdfBuilder.handle_document: Exception instantiating "
                                                    "builder: " + self.__class__.__name__ + " error: " + str(e))
            raise e
        
    def handle_key(self, doc, _recNum, _key):
        """
        This routine handles keys by substituting 
        the netcdf variables that correspond to the key into the values
        in the template that begin with *
        :param doc: the current document
        :param _recNum: The current recNum
        :param _key: A key to be processed, This can be a key to a primitive,
        or to another dictionary, or to a named function
        """
        # noinspection PyBroadException
        try:
            if _key == 'id':
                self.id = derive_id(self.template['id'], _recNum)
                return doc
            if isinstance(doc[_key], dict):
                # process an embedded dictionary
                _tmp_doc = copy.deepcopy(self.template[_key])
                for _sub_key in _tmp_doc.keys():
                    _tmp_doc = self.handle_key(_tmp_doc, _recNum, _sub_key)  # recursion
                doc[_key] = _tmp_doc
            if not isinstance(doc[_key], dict) and isinstance(doc[_key], str) and doc[_key].startswith('&'):
                doc[_key] = self.handle_named_function(doc[_key], _recNum)
            else:
                doc[_key] = self.translate_template_item(doc[_key], _recNum)
            return doc
        except Exception as e:
            logging.error(
                self.__class__.__name__ + "NetcdfBuilder.handle_key: Exception in builder:  error: " + str(e))
        return doc
    
    def handle_named_function(self, _named_function_def, _recNum):
        """
        This routine processes a named function entry from a template.
        :param _named_function_def - this can be either a template key or a template value.
        The _named_function_def looks like "&named_function:*field1:*field2:*field3..."
        where named_function is the literal function name of a defined function.
        It is expected that field1, field2, and field3 etc are all valid variable names.
        Each field will be translated from the netcdf file into value1, value2 etc. 
        The method "named_function" will be called like...
        named_function({field1:value1, field2:value2, ... fieldn:valuen}) and the return value from named_function
        will be substituted into the document.
        :_recNum the recNum being processed.
        """
        # noinspection PyBroadException
        try:
            _func = _named_function_def.split(':')[0].replace('&', '')
            _params = _named_function_def.split(':')[1].split(',')
            _dict_params = {"recNum":_recNum}
            for _p in _params:
                # be sure to slice the * off of the front of the param
                _dict_params[_p[1:]] = self.translate_template_item(_p, _recNum)
            # call the named function using getattr
            _replace_with = getattr(self, _func)(_dict_params)
            if _replace_with is None:
                logging.warning("self.__class__.__name__ + NetcdfBuilder: Using " + _func + " - None returned for " + str(
                    _dict_params))
                _replace_with = _func + "_not_found"
                return _replace_with
        except Exception as e:
            logging.error(
                self.__class__.__name__ + "handle_named_function: Exception instantiating builder:  error: " + str(e))
        return _replace_with
    
    def handle_data(self, doc, recNum):
        # noinspection PyBroadException
        try:
            _data_elem = {}
            _data_key = next(iter(self.template['data']))
            _data_template = self.template['data'][_data_key]
            for key in _data_template.keys():
                value = _data_template[key]
                # values can be null...
                if value and value.startswith('&'):
                    value = self.handle_named_function(value, recNum)
                else:
                    value = self.translate_template_item(value, recNum)
                _data_elem[key] = value
            if _data_key.startswith('&'):
                _data_key = self.handle_named_function(_data_key, recNum)
            else:
                _data_key = self.translate_template_item(_data_key, recNum)
            if _data_key is None:
                logging.warning(self.__class__.__name__ + "NetcdfBuilder.handle_data - _data_key is None")
            doc = self.load_data(doc, _data_key, _data_elem)
            return doc        
        except Exception as e:
            logging.error(self.__class__.__name__ + "handle_data: Exception instantiating builder:  error: " + str(e))
        return doc

    def build_document(self, file_name):
        """
        This is the entry point for the NetcfBuilders from the ingestManager.
        These documents are id'd by fcstValidEpoch. The data section is an array 
        each element of which contains variable data and a station name. To process this
        file we need to itterate the document by recNum and process the station name along
        with all the other variables in the variableList.
        """
        # noinspection PyBroadException
        try:
            self.ncdf_data_set = nc.Dataset(file_name)
            self.handle_document()
            _document_map = self.get_document_map()
            return _document_map
            self.close()
        except Exception as e:
            logging.error(self.__class__.__name__ + ": Exception with builder handle_row: error: " + str(e))
            self.close()
            return {}


# Concrete builders
class NetcdfObsBuilderV01(NetcdfBuilder):
    def __init__(self, load_spec, statement_replacement_params, ingest_document, cluster, collection):
        """
        This builder creates a set of V01 obs documents using the V01 station documents.
        This builder loads V01 station data into memory, and uses them to associate a station with an observation
        lat, lon point.
        In each document the observation data is an array of objects each of which is the obs data
        for a specific station.
        :param ingest_document: the document from the ingest document
        :param cluster: - a Couchbase cluster object, used for N1QL queries (QueryService)
        :param collection: - essentially a couchbase connection object, used to get documents by id (DataService)
        """
        NetcdfBuilder.__init__(self, load_spec, ingest_document, cluster, collection)
        self.cluster = cluster
        self.collection = collection
        self.same_time_rows = []
        self.time = 0
        self.interpolated_time = 0
        self.delta = ingest_document['validTimeDelta']
        self.cadence = ingest_document['validTimeInterval']
        self.variableList = ingest_document['variableList']
        self.template = ingest_document['template']

    
    def get_document_map(self):
        """
        In case there are leftovers we have to process them first.
        :return: the document_map
        """
        if len(self.same_time_rows) != 0:
            self.handle_document(self.interpolated_time, self.same_time_rows)
        return self.document_map

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
    
    # named functions
    def meterspersecond_to_milesperhour(self, params_dict):
        #Meters/second to miles/hour
        _ws_mps = params_dict['windSpeed']
        _ws_mph = _ws_mps * 2.237
        return _ws_mph

    def ceiling_transform(self, params_dict):
        ceiling = None
        _skyCover = params_dict['skyCover']
        _skyLayerBase = params_dict['skyLayerBase']
        # code clear as 60,000 ft 
        ceil_dft = 6000
        mBKN = re.compile('BKN/(\d+)')
        mOVC = re.compile('OVC/(\d+)')
        mVV = re.compile('VV/(\d+)')
        if mBKN.match(_skyCover) or mOVC.match(_skyCover) or mVV.match(_skyCover):
            ceiling = _skyLayerBase * 10  # put in tens of ft
        return ceiling

    def kelvin_to_farenheight(self, params_dict):
        temperature = params_dict['temperature']
        _temp_f = (temperature-273.15)*1.8 + 32
        return _temp_f

    def dewpoint_transform(self, params_dict):
        # Probably need more here....
        dewpoint = params_dict['dewpoint']
        _dp_f = (dewpoint-273.15)*1.8 + 32
        return _dp_f

    def interpolate_time(self, params_dict):
        """
        Rounds to nearest hour by adding a timedelta hour if minute >= 30
        """
        time = None
        recNum = params_dict['recNum']
        timeObs = params_dict['timeObs']
        time = datetime.fromtimestamp(timeObs)
        time.replace(second=0, microsecond=0, minute=0, hour=time.hour) + timedelta(hours=time.minute//30)
        return time

    def handle_station(self, params_dict):
        """
         This method uses the station name in the params_dict
         to find a station with that name.
         If the station does not exist it will be created with data from the 
         netcdf file. If it does exist data from the netcdf file will be compared to what is in the database.
         If the data does not match the database will be updated.
         :param params_dict: {station_name:a_station_name}
         :return: 
         """
        _recNum = params_dict['recNum']
        _station_name = params_dict['station_name']
        id = None
        # noinspection PyBroadException
        try:
            result = self.cluster.search_query("station_geo", QueryStringQuery(_station_name))
            if result.rows == 0:
                #got to add a station
                logging.info("netcdfObsBuilderV01.handle_station - adding station " + _station_name)
                # FIX THIS - ADD A STATION
                latitude = self.ncdf_data_set[_recNum]['latitude']
                longitude = self.ncdf_data_set[_recNum]['longitude']
                elevation = self.ncdf_data_set[_recNum]['elevation']
                locationName = self.ncdf_data_set[_recNum]['locationName']
                stationName = self.ncdf_data_set[_recNum]['stationName']
                if stationName is not _station_name:
                    raise Exception ("netcdfObsBuilderV01.handle_station: The given station name: " + _station_name + 
                    " does not match the station name: " + stationName + " from the record: " + _recNum)
                _new_station = {
                    "id": "MD:V01:METAR:station:" + stationName,
                    "description": locationName,
                    "docType": "station",
                    "firstTime": 0,
                    "geo": {
                        "elev": elevation,
                        "lat": latitude,
                        "lon": longitude
                    },
                    "lastTime": 0,
                    "name": _station_name,
                    "subset": "METAR",
                    "type": "MD",
                    "updateTime": int(time.time()),
                    "version": "V01"
                }
                #FIX THIS!!! QUESTION - should we upsert or just add it to the document??????
                # for the first time around we'll have a zillion upserts!
                self.collection.upsert_multi(_new_station)
                #self.document_map[id] = _new_station
                return id
            if result.rows() > 1:
                raise Exception("netcdfObsBuilderV01.handle_station: There are more than one station with the name " + _station_name + "! FIX THAT!")
            if result.rows == 1:
                return result.rows()[0]['id']
        except Exception as e:
            logging.error(
                self.__class__.__name__ + "netcdfObsBuilderV01.handle_station: Exception finding or creating station to match station_name  "
                                          "error: " + str(e) + " params: " + str(params_dict))
        return None
    


class NetcdfModelBuilderV01(NetcdfBuilder):
    def __init__(self, load_spec, statement_replacement_params, ingest_document, cluster, collection):
        """
        This builder creates a set of V03 model documents using the V01 station documents
        This builder is very much like the SqlObsBuilderV04 except that it builds model documents.
        In each document the specific model data is an array of objects each of which is the model data
        for a specific station. The particular model name is supplied in the template.
        :param ingest_document: the document from the ingest document like MD:V04:METAR:HRRR:ingest
        :param cluster: - a Couchbase cluster object, used for N1QL queries (QueryService)
        :param collection: - essentially a couchbase connection object, used to get documents by id (DataService)
        """
        NetcdfBuilder.__init__(self, load_spec, statement_replacement_params, ingest_document, cluster, collection)
        self.cluster = cluster
        self.same_time_rows = []
        self.time = 0
        self.interpolated_time = 0
        self.delta = ingest_document['validTimeDelta']
        self.cadence = ingest_document['validTimeInterval']
        
    def interpolate_time(self, a_time):
        _remainder_time = a_time % self.cadence
        _cadence_time = a_time / self.cadence * self.cadence
        if _remainder_time < self.delta:
            _t = a_time - _remainder_time
        else:
            _t = a_time - _remainder_time + self.cadence
        return _t
    
    def handle_row(self, row):
        """
        This is the entry point from the IngestManager.
        This method is responsible to collate rows into a set that is to be given to the handle_document.
        Rows are collated according to interpolated time and fcst_len. Each set of rows that have the same interpolated
        time and fcst_len are passed to the handle_document to be processed into a single document.
        :param row: A result set row
        :return:
        """
        self.interpolated_time = self.interpolate_time(int(row['time']))
        if self.time == 0:
            self.time = self.interpolated_time
            self.fcst_len = int(row['fcst_len'])
        if self.interpolated_time != self.time or int(row['fcst_len']) != self.fcst_len:
            self.handle_document(self.interpolated_time, self.same_time_rows)
            self.time = 0
            self.same_time_rows = []
        self.same_time_rows.append(row)

    def get_document_map(self):
        """
        In case there are leftovers we have to process them first.
        :return: the document_map
        """
        if len(self.same_time_rows) != 0:
            self.handle_document(self.interpolated_time, self.same_time_rows)
        return self.document_map

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

class NetcdfCtcBuilderV01(NetcdfModelBuilderV01):
    def __init__(self, load_spec, statement_replacement_params, ingest_document, cluster, collection):
        """
        This builder creates a set of V01 ctc documents using the V01 ingest ctc documents.
        This builder is very much like the SqlModelBuilderV01 (the parent) except that it builds ctc documents.
        The difference is the load_data method is overridden in order to put data into a map instead of a list.
        In each document the specific ctc data is a map of objects each of which is the ctc data
        for a specific threshold indexed by the threshold.
        :param ingest_document: the document from the ingest document like MD:V01:METAR:HRRR:ALL_HRRR:CTC:ingest
        :param cluster: - a Couchbase cluster object, used for N1QL queries (QueryService)
        :param collection: - essentially a couchbase connection object, used to get documents by id (DataService)
        """
        NetcdfBuilder.__init__(self, load_spec, statement_replacement_params, ingest_document, cluster, collection)
        self.cluster = cluster
        self.same_time_rows = []
        self.time = 0
        self.interpolated_time = 0
        self.delta = ingest_document['validTimeDelta']
        self.cadence = ingest_document['validTimeInterval']

    def load_data(self, doc, key, element):
        """
        This method appends an observation to the data array
        :param doc: The document being created
        :param key: Not used
        :param element: the observation data
        :return: the document being created
        """
        if 'data' not in doc.keys() or doc['data'] is None:
            doc['data'] = {}
        doc['data'][key] = element
        return doc
        

class NetcdfStationsBuilderV01(NetcdfBuilder):
    def __init__(self, load_spec, statement_replacement_params, ingest_document, cluster, collection):
        """
        This builder creates multiple documents one per station with the id being
        MD:V03:METAR:stations:*name where the variable part of the id
        is the the station name which, is an ICAO, like CWDP. So an ID
        might be "MD:V03:METAR:stations:CWDP". The lat, lon, and elevation are
        encoded into a "geo" element such that we can create a geojson search index over the data.
        :type ingest_document: object - This is the ingest document from "MD:V01:METAR:stations:ingest"
        :param cluster: - a Couchbase cluster object, used for N1QL queries (QueryService)
        :param collection: - essentially a couchbase connection object, used to get documents by id (DataService)
        """
        NetcdfBuilder.__init__(self, load_spec, statement_replacement_params, ingest_document, cluster, collection)
        self.same_time_rows = []
    
    def get_document_map(self):
        """
        singleDocument builders (each row becomes a document) never have leftovers
        :return:  the document_map
        """
        return self.document_map
    
    def handle_row(self, row):
        """
        This is the entry point from the IngestManager.
        This method is responsible to collate rows into a set that is to be given to the handle_document.
        With the stationBuilder each row becomes a document so each row  passed to the handle_document to be processed
        into a single document.
        :param row: A result set row
        :return:
        """
        NetcdfBuilder.handle_document(self, 0, [row])
