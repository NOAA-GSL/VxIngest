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
import time
import netCDF4 as nc
import re
import math
import calendar
from numpy.lib.nanfunctions import nanvar
import numpy.ma as ma
from decimal import Decimal
from couchbase.cluster import Cluster, ClusterOptions, PasswordAuthenticator
from couchbase.exceptions import CouchbaseException
from couchbase.search import QueryStringQuery, SearchQuery, SearchOptions, PrefixQuery, HighlightStyle, SortField, SortScore, TermFacet
from couchbase.mutation_state import MutationState
from pymysql.constants import CLIENT
from datetime import datetime, timedelta

TS_OUT_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def convert_to_iso(an_epoch):
    if not isinstance(an_epoch, int):
        an_epoch = int(an_epoch)
    _valid_time_str = dt.datetime.utcfromtimestamp(
        an_epoch).strftime(TS_OUT_FORMAT)
    return _valid_time_str



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

    def derive_id(self, template_id, recNum):
        # Private method to derive a document id from the current recNum,
        # substituting *values from the corresponding netcdf fields as necessary.
        # noinspection PyBroadException
        try:
            _parts = template_id.split(':')
            new_parts = []
            for _part in _parts:
                if _part.startswith('&'):
                    value = str(self.handle_named_function(_part, recNum))
                else:
                    if _part.startswith("*"):
                        value = str(self.translate_template_item(_part, recNum))
                    else:
                        value = str(_part)
                new_parts.append(value)
            _new_id = ":".join(new_parts)
            return _new_id
        except:
            e = sys.exc_info()
            logging.error("NetcdfBuilder.derive_id: Exception  error: " + str(e))


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
            Smatch = re.compile(".*S.*")
            Umatch = re.compile(".*U.*")
            if len(_replacements) > 0:
                for _ri in _replacements:
                    vtype = str(self.ncdf_data_set.variables[_ri].dtype)
                    if Smatch.match(vtype) or Umatch.match(vtype):
                        _make_str = True
                        _chartostring = True
                        break
                for _ri in _replacements:
                    if _ri.startswith("{ISO}"):
                        variable = value.replace("*{ISO}", "")
                        if _chartostring:
                            # for these we have to convert the character array AND convert to ISO (it is probably a string date)
                            value = convert_to_iso(
                                "*{ISO}" + nc.chartostring(self.ncdf_data_set[variable][recNum]))
                        else:
                            # for these we have to convert convert to ISO (it is probably an epoch)
                            value = convert_to_iso(
                                "*{ISO}" + self.ncdf_data_set[variable][recNum])
                    else:
                        variable = value.replace("*", "")
                        if _make_str:
                            if _chartostring:
                                # it is a char array of something
                                value = value.replace(
                                    '*' + _ri, str(nc.chartostring(self.ncdf_data_set[variable][recNum])))
                                return value
                            else:
                                # it is probably a number
                                value = str(self.ncdf_data_set[variable][recNum])
                                return value
                        else:
                            # it desn't need to be a string
                            return self.ncdf_data_set[variable][recNum]
        except Exception as e:
            logging.error(
                "NetcdfBuilder.translate_template_item: Exception  error: " + str(e))
        return value

    def handle_document(self):
        """
        :return: The modified document_map
        """
        # noinspection PyBroadException
        try:
            new_document = copy.deepcopy(self.template)
            _recNum_data_size = self.ncdf_data_set.dimensions['recNum'].size
            if _recNum_data_size == 0:
                return
            # make a copy of the template, which will become the new document
            # once all the translations have occured
            new_document = initialize_data(new_document)
            for _recNum in range(_recNum_data_size):
                for _key in self.template.keys():
                    if _key == "data":
                        new_document = self.handle_data(new_document, _recNum)
                        continue
                    new_document = self.handle_key(new_document, _recNum, _key)
            # put document into document map
            self.document_map[self.id] = new_document
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
                _id = self.derive_id(self.template['id'], _recNum)
                if not _id in doc:
                    doc['id'] = _id
                return doc
            if isinstance(doc[_key], dict):
                # process an embedded dictionary
                _tmp_doc = copy.deepcopy(self.template[_key])
                for _sub_key in _tmp_doc.keys():
                    _tmp_doc = self.handle_key(
                        _tmp_doc, _recNum, _sub_key)  # recursion
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
        The _named_function_def looks like "&named_function:*field1,*field2,*field3..."
        where named_function is the literal function name of a defined function.
        The name of the function and the function parameters are seperated by a ":" and
        the parameters are seperated vy a ','.
        It is expected that field1, field2, and field3 etc are all valid variable names.
        Each field will be translated from the netcdf file into value1, value2 etc. 
        The method "named_function" will be called like...
        named_function({field1:value1, field2:value2, ... fieldn:valuen}) and the return value from named_function
        will be substituted into the document.
        :_recNum the recNum being processed.
        """
        # noinspection PyBroadException
        try:
            _func = _named_function_def.split('|')[0].replace('&', '')
            _params = _named_function_def.split('|')[1].split(',')
            _dict_params = {"recNum": _recNum}
            for _p in _params:
                # be sure to slice the * off of the front of the param
                _dict_params[_p[1:]] = self.translate_template_item(_p, _recNum)
            # call the named function using getattr
            _replace_with = getattr(self, _func)(_dict_params)
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
                try:
                    value = _data_template[key]
                    # values can be null...
                    if value and value.startswith('&'):
                        value = self.handle_named_function(value, recNum)
                    else:
                        value = self.translate_template_item(value, recNum)
                except Exception as e:
                    value = None
                    logging.warning(self.__class__.__name__ +
                                "NetcdfBuilder.handle_data - value is None")
                _data_elem[key] = value
            if _data_key.startswith('&'):
                _data_key = self.handle_named_function(_data_key, recNum)
            else:
                _data_key = self.translate_template_item(_data_key, recNum)
            if _data_key is None:
                logging.warning(self.__class__.__name__ +
                                "NetcdfBuilder.handle_data - _data_key is None")
            doc = self.load_data(doc, _data_key, _data_elem)
            return doc
        except Exception as e:
            logging.error(self.__class__.__name__ +
                          "handle_data: Exception instantiating builder:  error: " + str(e))
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
        except Exception as e:
            logging.error(self.__class__.__name__ +
                          ": Exception with builder build_document: error: " + str(e))
            self.close()
            return {}


# Concrete builders
class NetcdfObsBuilderV01(NetcdfBuilder):
    def __init__(self, load_spec, ingest_document, cluster, collection):
        """
        This builder creates a set of V01 obs documents using the V01 station documents.
        This builder loads V01 station data into memory, and uses them to associate a station with an observation
        lat, lon point.
        In each document the observation data is an array of objects each of which is the obs data
        for a specific station.
        If a station from a metar file does not exist in the couchbase database
        a station document will be created from the metar record data and
        the station document will be added to the document map.
        :param ingest_document: the document from the ingest document
        :param cluster: - a Couchbase cluster object, used for N1QL queries (QueryService)
        :param collection: - essentially a couchbase connection object, used to get documents by id (DataService)
        """
        NetcdfBuilder.__init__(
            self, load_spec, ingest_document, cluster, collection)
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
    # TODO - may not need this - checking
    def meterspersecond_to_milesperhour(self, params_dict):
        # Meters/second to miles/hour
        try:
            value = self.umask_value_transform(params_dict)
            if value is not None and value != "":
                value = str("{0:.4f}".format(float(value) * 2.237))
            return value
        except Exception as e:
            logging.error(self.__class__.__name__ +
                          "handle_data: Exception in named function meterspersecond_to_milesperhour:  error: " + str(e))

    def ceiling_transform(self, params_dict):
        try:
            _skyCover = params_dict['skyCover']
            _skyLayerBase = params_dict['skyLayerBase']
            # code clear as 60,000 ft 
            ceiling = 60000
            mBKN = re.compile('.*BKN.*')  # Broken
            mOVC = re.compile('.*OVC.*')  # Overcast
            mVV = re.compile('.*VV.*')  # Vertical Visibility
            mask_array = ma.getmaskarray(_skyLayerBase)
            _skyCover_array = _skyCover[1:-1].replace("'","").split(" ")
            for index in range(len(_skyLayerBase)):
                if (not mask_array[index]) and (mBKN.match(_skyCover_array[index]) or mOVC.match(_skyCover_array[index]) or mVV.match(_skyCover_array[index])):
                    ceiling = _skyLayerBase[index]
                    break   
            return str(math.floor(ceiling))
        except Exception as e:
            logging.error(self.__class__.__name__ +
                            "handle_data: Exception in named function ceiling_transform:  error: " + str(e))

    def kelvin_to_farenheight(self, params_dict):
        try:
            value = self.umask_value_transform(params_dict)
            if value is not None and value != "":
                value = "{0:.4f}".format((float(value) - 273.15) * 1.8 + 32) 
            return str(value)
        except Exception as e:
            logging.error(self.__class__.__name__ +
                            "handle_data: Exception in named function kelvin_to_farenheight:  error: " + str(e))

    def umask_value_transform(self, params_dict):
        # Probably need more here....
        try:
            _key = None
            _recNum = params_dict['recNum']
            for _key in params_dict.keys():
                if _key != "recNum":
                    break
            _ncValue = self.ncdf_data_set[_key][_recNum]
            if not ma.getmask(_ncValue):
                value = ma.compressed(_ncValue)[0]
                return str("{0:.4f}".format(value)) # trim to four digits for all our data
            else:
                return ""
        except Exception as e:
            logging.error(self.__class__.__name__ +
                            "umask_value_transform: Exception in named function umask_value_transform for key " + _key + ":  error: " + str(e))

    def handle_pressure(self, params_dict):
        try:
            value = self.umask_value_transform(params_dict)
            if value is not None and value != "":
                value = math.floor(float(value) / 100) # convert to millibars (from pascals) and round
            return str(value)
        except Exception as e:
            logging.error(self.__class__.__name__ +
                            "handle_pressure: Exception in named function:  error: " + str(e))

    def handle_visibility(self, params_dict):
        try:
            value = self.umask_value_transform(params_dict)
            if  value is not None and value != "":
                value = math.floor(float(value)) # round
            return str(value)
        except Exception as e:
            logging.error(self.__class__.__name__ +
                            "handle_pressure: Exception in named function:  error: " + str(e))

    def interpolate_time(self, params_dict):
        """
        Rounds to nearest hour by adding a timedelta hour if minute >= 30
        """
        try:
            time = None
            _timeObs = params_dict['timeObs']
            if not ma.getmask(_timeObs):
                _time = int(ma.compressed(_timeObs)[0])
            else:
                return ""
            time = datetime.fromtimestamp(_time)
            time = time.replace(second=0, microsecond=0, minute=0,
                            hour=time.hour) + timedelta(hours=time.minute//30)
            return str(calendar.timegm(time.timetuple()))
        except Exception as e:
            logging.error(self.__class__.__name__ +
                            "handle_data: Exception in named function interpolate_time:  error: " + str(e))

    def interpolate_time_iso(self, params_dict):
        """
        Rounds to nearest hour by adding a timedelta hour if minute >= 30
        """
        try:
            time = None
            _timeObs = params_dict['timeObs']
            if not ma.getmask(_timeObs):
                _time = int(ma.compressed(_timeObs)[0])
            else:
                return ""
            time = datetime.fromtimestamp(_time)
            time = time.replace(second=0, microsecond=0, minute=0,
                            hour=time.hour) + timedelta(hours=time.minute//30)
            # convert this iso
            return str(time.isoformat())
        except Exception as e:
            logging.error(self.__class__.__name__ +
                            "handle_data: Exception in named function interpolate_time_iso:  error: " + str(e))


    def fill_from_netcdf(self, _recNum, _netcdf):
        """
        Used by handle_station to get the records from netcdf for comparing with the 
        records from the database.
        """
        _netcdf = {}
        if not ma.getmask(self.ncdf_data_set['latitude'][_recNum]):
            _netcdf['latitude'] = ma.compressed(self.ncdf_data_set['latitude'][_recNum])[0]
        else:
            _netcdf['latitude'] = None
        if not ma.getmask(self.ncdf_data_set['longitude'][_recNum]):
            _netcdf['longitude'] = ma.compressed(self.ncdf_data_set['longitude'][_recNum])[0]
        else:
            _netcdf['longitude'] = None
        if not ma.getmask(self.ncdf_data_set['elevation'][_recNum]):
            _netcdf['elevation'] = ma.compressed(self.ncdf_data_set['elevation'][_recNum])[0]
        else:
            _netcdf['elevation'] = None
        _netcdf['description'] = str(nc.chartostring(self.ncdf_data_set['locationName'][_recNum]))
        _netcdf['name'] = str(nc.chartostring(self.ncdf_data_set['stationName'][_recNum]))
        return _netcdf

    def handle_station(self, params_dict):
        """
         This method uses the station name in the params_dict
         and a full text search to find a station with that name.
         If the station does not exist it will be created with data from the 
         netcdf file. If it does exist, data from the netcdf file will be compared to what is in the database.
         If the data does not match, the database will be updated.
        For the moment I do not know how to retrieve the elevation from the 
        full text search fields. It is probably possible by modifying the station_geo query.
        I just know how to get the geopoint [lon,lat], the descripttion, and the name, and so that
        is what this code is using to do the comparison. Not the elevation or any of the other fields. 
        It is possible to retrive the document like this..
        _db_station = self.collection.get(rows[0].id).content
        and then all the data could be compared, but I don't think that is necessary at the moment
        unless we decide that elevation is important to compare.
         :param params_dict: {station_name:a_station_name}
         :return: 
         """
        _recNum = params_dict['recNum']
        _station_name = params_dict['stationName']
        _id = None
        _add_station = False
        _netcdf = {}
        _existing = {}

        # noinspection PyBroadException
        try:
            result = self.cluster.search_query(
                "station_geo", QueryStringQuery(_station_name), fields=["*"])
            rows = result.rows()

            if len(rows) == 0: # too cold
                _add_station = True

            if len(rows) > 1: # too hot
                raise Exception(
                    "netcdfObsBuilderV01.handle_station: There are more than one station with the name " + _station_name + "! FIX THAT!")

            # get the netcdf fields for comparing or adding new
            _netcdf = self.fill_from_netcdf(_recNum, _netcdf)

            if len(rows) == 1: # just right
                # compare the existing record from the query to the netcdf record
                _existing['name'] = rows[0].fields['name']
                _existing['description'] = rows[0].fields['description']
                if 'geo' in rows[0].fields:
                    _existing['latitude'] = round(rows[0].fields['geo'][1],2)
                    _existing['longitude'] = round(rows[0].fields['geo'][0], 2)
                else:    
                    _existing['latitude'] = None
                    _existing['longitude'] = None
                          
                for _key in ['latitude','longitude']:
                    if not math.isclose(_existing[_key], _netcdf[_key],abs_tol=.001):
                        _add_station =True
                        break
                if not _add_station:    
                    for _key in ['description','name']:
                        if _existing[_key] != _netcdf[_key]:
                            _add_station =True
                            break

            if _add_station:
                # got to add a station either because it didn't exist in the database, or it didn't match
                # what was in the database
                logging.info(
                    "netcdfObsBuilderV01.handle_station - adding station " + _netcdf['name'])
                _id = "MD:V01:METAR:station:" + _netcdf['name']
                _new_station = {
                    "id": "MD:V01:METAR:station:" + _netcdf['name'],
                    "description": _netcdf['description'],
                    "docType": "station",
                    "firstTime": 0,
                    "geo": {
                        "elev": round(float(_netcdf['elevation']), 4),
                        "lat": round(float(_netcdf['latitude']), 4),
                        "lon": round(float(_netcdf['longitude']), 4)
                    },
                    "lastTime": 0,
                    "name": _netcdf['name'],
                    "subset": "METAR",
                    "type": "MD",
                    "updateTime": int(time.time()),
                    "version": "V01"
                }
                # add the station to the document map
                if not _id in self.document_map:
                     self.document_map[_id] = _new_station
            return params_dict['stationName']        
        except Exception as e:
            logging.error(
                self.__class__.__name__ +
                "netcdfObsBuilderV01.handle_station: Exception finding or creating station to match station_name  "
                "error: ".format(e), " params: " + str(params_dict))
            return ""
