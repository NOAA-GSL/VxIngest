"""
Program Name: Class grib_builder.py
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
import pyproj
import pygrib
import grib2_to_cb.get_grid as gg

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


class GribBuilder:
    def __init__(self, load_spec, ingest_document, cluster, collection):
        self.template = ingest_document['template']
        self.load_spec = load_spec
        self.cluster = cluster
        self.collection = collection
        self.id = None
        self.document_map = {}
        self.projection = None
        self.grbs = None
        self.grbm = None
        self.spacing, max_x, max_y = None
        self.in_proj = None
        self.out_proj = None
        self.transformer = None
        self.transformer_reverse = None
        self.domain_stations = []

    def load_data(self, doc, key, element):
        pass

    def get_document_map(self):
        pass

    def handlestation(self, row):
        pass

    def derive_id(self, template_id, recNum):
        # Private method to derive a document id from the current recNum,
        # substituting *values from the corresponding grib fields as necessary.
        # noinspection PyBroadException
        try:
            _parts = template_id.split(':')
            new_parts = []
            for _part in _parts:
                if _part.startswith('&'):
                    value = str(self.handle_named_function(_part, recNum))
                else:
                    if _part.startswith("*"):
                        value = str(
                            self.translate_template_item(_part, recNum))
                    else:
                        value = str(_part)
                new_parts.append(value)
            _new_id = ":".join(new_parts)
            return _new_id
        except:
            e = sys.exc_info()
            logging.error("GribBuilder.derive_id: Exception  error: " + str(e))

    def translate_template_item(self, variable, station):
        """
        This method translates template replacements (*item or *item1*item2).
        It can translate keys or values. 
        :param variable: a value from the template - should be a grib2 variable
        :param station: the current station
        :return:
        """
        _replacements = []
        # noinspection PyBroadException
        try:
            if isinstance(variable, str):
                # skip the first replacement, its never
                # really a replacement. It is either '' or not a
                # replacement
                _replacements = variable.split('*')[1:]
            value = variable  # in case it isn't a replacement - makes it easier
            if len(_replacements) > 0:
                for _ri in _replacements:
                    _gribVals = self.grbs.select(name=_ri)[0]
                    _values = _gribVals['values']
                    _value = _values[round(station['y_gridpoint']), round(
                        station['x_gridpoint'])]
                    if _ri.startswith("{ISO}"):
                        value = value.replace(
                            "*{ISO}", convert_to_iso("*{ISO}" + _value))
                    else:
                        value = value.replace("*", _value)
                return value
        except Exception as e:
            logging.error(
                "GribBuilder.translate_template_item: Exception  error: " + str(e))
        return value

    def handle_document(self):
        """
        :return: The modified document_map
        """
        # noinspection PyBroadException
        try:
            new_document = copy.deepcopy(self.template)
            station_data_size = self.projection.dimensions['recNum'].size
            if station_data_size == 0:
                return
            # make a copy of the template, which will become the new document
            # once all the translations have occured
            new_document = initialize_data(new_document)
            for station in self.domain_stations:
                for _key in self.template.keys():
                    if _key == "data":
                        new_document = self.handle_data(new_document, station)
                        continue
                    new_document = self.handle_key(new_document, station, _key)
            # put document into document map
            if new_document['id']:
                logging.info(
                    "GribBuilder.handle_document - adding document " + new_document['id'])
                self.document_map[new_document['id']] = new_document
            else:
                logging.info(
                    "GribBuilder.handle_document - cannot add document with key " + str(new_document['id']))
        except Exception as e:
            logging.error(self.__class__.__name__ + "GribBuilder.handle_document: Exception instantiating "
                                                    "builder: " + self.__class__.__name__ + " error: " + str(e))
            raise e

    def handle_key(self, doc, station, _key):
        """
        This routine handles keys by substituting 
        the grib variables that correspond to the key into the values
        in the template that begin with *
        :param doc: the current document
        :param station: The current station
        :param _key: A key to be processed, This can be a key to a primitive,
        or to another dictionary, or to a named function
        """
        # noinspection PyBroadException
        try:
            if _key == 'id':
                _id = self.derive_id(self.template['id'], station)
                if not _id in doc:
                    doc['id'] = _id
                return doc
            if isinstance(doc[_key], dict):
                # process an embedded dictionary
                _tmp_doc = copy.deepcopy(self.template[_key])
                for _sub_key in _tmp_doc.keys():
                    _tmp_doc = self.handle_key(
                        _tmp_doc, station, _sub_key)  # recursion
                doc[_key] = _tmp_doc
            if not isinstance(doc[_key], dict) and isinstance(doc[_key], str) and doc[_key].startswith('&'):
                doc[_key] = self.handle_named_function(doc[_key], station)
            else:
                doc[_key] = self.translate_template_item(doc[_key], station)
            return doc
        except Exception as e:
            logging.error(
                self.__class__.__name__ + "GribBuilder.handle_key: Exception in builder:  error: " + str(e))
        return doc

    def handle_named_function(self, _named_function_def, station):
        """
        This routine processes a named function entry from a template.
        :param _named_function_def - this can be either a template key or a template value.
        The _named_function_def looks like "&named_function|*field1,*field2,*field3..."
        where named_function is the literal function name of a defined function.
        The name of the function and the function parameters are seperated by a ":" and
        the parameters are seperated by a ','.
        It is expected that field1, field2, and field3 etc are all valid variable names.
        Each field will be translated from the netcdf file into value1, value2 etc. 
        The method "named_function" will be called like...
        named_function({field1:value1, field2:value2, ... fieldn:valuen}) and the return value from named_function
        will be substituted into the document.
        :station the recNum being processed.
        """
        # noinspection PyBroadException
        try:
            _func = _named_function_def.split('|')[0].replace('&', '')
            _params = _named_function_def.split('|')[1].split(',')
            _dict_params = {"recNum": station}
            for _p in _params:
                # be sure to slice the * off of the front of the param
                _dict_params[_p[1:]] = self.translate_template_item(
                    _p, station)
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
                                    "GribBuilder.handle_data - value is None")
                _data_elem[key] = value
            if _data_key.startswith('&'):
                _data_key = self.handle_named_function(_data_key, recNum)
            else:
                _data_key = self.translate_template_item(_data_key, recNum)
            if _data_key is None:
                logging.warning(self.__class__.__name__ +
                                "GribBuilder.handle_data - _data_key is None")
            doc = self.load_data(doc, _data_key, _data_elem)
            return doc
        except Exception as e:
            logging.error(self.__class__.__name__ +
                          "handle_data: Exception instantiating builder:  error: " + str(e))
        return doc

    def build_document(self, file_name):
        """
        This is the entry point for the gribBuilders from the ingestManager.
        These documents are id'd by time and fcstLen. The data section is an array 
        each element of which contains variable data and a station name. To process this
        file we need to itterate the domain_stations list and process the station name along
        with all the other variables in the variableList.
        """
        # noinspection PyBroadException
        try:
            # TODO determine if this projection stuff changes file to file
            # If not, use lazy instantiation to just do it once for all the files
            self.projection = gg.getGrid(file_name)
            self.grbs = pygrib.open(file_name)
            self.grbm = self.grbs.message(1)
            self.spacing, max_x, max_y = gg.getAttributes(file_name)
            # Set the two projections to be used during the transformation (nearest neighbor method, what we use for everything with METARS)
            self.in_proj = pyproj.Proj(proj='latlon')
            self.out_proj = self.projection
            self.transformer = pyproj.Transformer.from_proj(
                proj_from=self.in_proj, proj_to=self.out_proj)
            self.transformer_reverse = pyproj.Transformer.from_proj(
                proj_from=self.out_proj, proj_to=self.in_proj)
            # get stations from couchbase
            self.domain_stations = []
            result = self.cluster.query(
                "SELECT mdata.geo.lat, mdata.geo.lon, name from mdata where type='MD' and docType='station' and subset='METAR' and version='V01'")
            for row in result:
                x, y = self.transformer.transform(
                    row['lon'], row['lat'], radians=False)
                x_gridpoint, y_gridpoint = x/self.spacing, y/self.spacing
                if x_gridpoint < 0 or x_gridpoint > max_x or y_gridpoint < 0 or y_gridpoint > max_y:
                    continue
                station = copy.deepcopy(row)
                station['x_gridpoint'] = x_gridpoint
                station['y_gridpoint'] = y_gridpoint
                self.domain_stations.append(station)
            self.handle_document()
            _document_map = self.get_document_map()
            return _document_map
        except Exception as e:
            logging.error(self.__class__.__name__ +
                          ": Exception with builder build_document: error: " + str(e))
            self.close()
            return {}

# Concrete builders


class GribModelBuilderV01(GribBuilder):
    def __init__(self, load_spec, ingest_document, cluster, collection):
        """
        This builder creates a set of V01 model documents using the stations in the station list.
        This builder loads domain qualified station data into memory, and uses the domain_station 
        list to associate a station with a grid value at an x_lat, x_lon point.
        In each document the data is an array of objects each of which is the model variable data
        for specific variables at a point associated with a specific station at the time and fcstLen of
        the document.
        :param load spec used to init the parent
        :param ingest_document: the document from the ingest document
        :param cluster: - a Couchbase cluster object, used for N1QL queries (QueryService)
        :param collection: - essentially a couchbase connection object, used to get documents by id (DataService)
        """
        GribBuilder.__init__(
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

    def handle_ceiling(self, params_dict):
        station = params_dict['station']
        x_gridpoint = station['x_gridpoint']
        y_gridpoint = station['y_gridpoint']
        _key = None
        for _key in params_dict.keys():
            if _key != "station":
                break
        # Convert from pascals to milibars
        type = params_dict[_key]
        ceil = self.grbs.select(
            name='Geopotential Height', typeOfFirstFixedSurface=type)[0]
        ceil_values = ceil['values']
        ceil_msl = ceil_values[round(y_gridpoint), round(x_gridpoint)]
        # Convert to ceiling AGL and from meters to tens of feet (what is currently inside SQL, we'll leave it as just feet in CB)
        ceil_agl = (ceil_msl - params_dict['Orography']) * 0.32808
        return ceil_agl

        # SURFACE PRESSURE
    def handle_surface_pressure(self, params_dict):
        _key = None
        for _key in params_dict.keys():
            if _key != "station":
                break
        # Convert from pascals to milibars
        pres = params_dict[_key]
        pres_mb = pres * 100
        return pres_mb

    def kelvin_to_farenheight(self, params_dict):
        """
            param:params_dict expects {'station':{},'*variable name':variable_value}
            Used for temperature and dewpoint
        """
        _key = None
        for _key in params_dict.keys():
            if _key != "station":
                break
        # Convert from Kelvin to Farenheit
        tempk = params_dict[_key]
        value = ((tempk-273.15)*9)/5 + 32
        return value

        # WIND SPEED AND DIRECTION
    def handle_wind_speed(self, params_dict):
        uwind_ms = params_dict['10 metre U wind component']
        vwind_ms = params_dict['10 metre V wind component']
        # Convert from U-V components to speed and direction (requires rotation if grid is not earth relative)
        # wind speed then convert to mph
        ws_ms = math.sqrt((uwind_ms*uwind_ms)+(vwind_ms*vwind_ms))
        ws_mph = (ws_ms/0.447) + 0.5
        return ws_mph

        # wind direction
    def handle_wind_direction(self, params_dict):
        station = params_dict['station']
        x_gridpoint = station['x_gridpoint']
        y_gridpoint = station['y_gridpoint']
        longitude = station['lon']
        uwind = self.grbs.select(name='10 metre U wind component')[0]
        uwind_values = uwind['values']
        uwind_ms = gg.interpGridBox(uwind_values, x_gridpoint, y_gridpoint)
        vwind = self.grbs.select(name='10 metre V wind component')[0]
        vwind_values = vwind['values']
        vwind_ms = gg.interpGridBox(vwind_values, x_gridpoint, y_gridpoint)
        theta = gg.getWindTheta(vwind, longitude)
        radians = math.atan2(uwind_ms, vwind_ms)
        wd = (radians*57.2958) + theta + 180
        return wd
