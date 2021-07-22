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
import time
import math
from couchbase.cluster import Cluster, ClusterOptions, PasswordAuthenticator
from couchbase.exceptions import CouchbaseException
from couchbase.search import QueryStringQuery, SearchQuery, SearchOptions, PrefixQuery, HighlightStyle, SortField, SortScore, TermFacet
from couchbase.mutation_state import MutationState
from datetime import datetime, timedelta
import pyproj
import pygrib
import grib2_to_cb.get_grid as gg
import cProfile
from pstats import Stats

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
    def __init__(self, load_spec, ingest_document, cluster, collection, number_stations=sys.maxsize):
        self.template = ingest_document['template']
        self.load_spec = load_spec
        self.cluster = cluster
        self.collection = collection
        self.number_stations = number_stations
        self.id = None
        self.document_map = {}
        self.projection = None
        self.grbs = None
        self.grbm = None
        self.spacing = None
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

    def derive_id(self, template_id, station):
        # Private method to derive a document id from the current station,
        # substituting *values from the corresponding grib fields as necessary.
        # noinspection PyBroadException
        try:
            _parts = template_id.split(':')
            new_parts = []
            for _part in _parts:
                if _part.startswith('&'):
                    value = str(self.handle_named_function(_part, station))
                else:
                    if _part.startswith("*"):
                        _v, _interp_v = self.translate_template_item(_part, station)
                        value = str(_v)
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
                    _message = self.grbs.select(name=_ri)[0]
                    _values = _message['values']
                    _value = _values[round(station['y_gridpoint']), round(station['x_gridpoint'])]
                    # interpolated gridpoints cannot be rounded
                    _interpolated_value = gg.interpGridBox(_values, station['y_gridpoint'], station['x_gridpoint'])

                    if _ri.startswith("{ISO}"):
                        value = value.replace(
                            "*" + _ri, convert_to_iso(_ri))
                    else:
                        value = value.replace("*" + _ri, str(_value))
                return value, _interpolated_value
        except Exception as e:
            logging.error(
                "GribBuilder.translate_template_item: Exception  error: " + str(e))
        return value, []

    def handle_document(self):
        """
        :return: The modified document_map
        """
        # noinspection PyBroadException
        try:
            new_document = copy.deepcopy(self.template)
            station_data_size = len(self.domain_stations)
            if station_data_size == 0:
                return
            # make a copy of the template, which will become the new document
            # once all the translations have occured
            new_document = initialize_data(new_document)
            for station in self.domain_stations:
                logging.info("GribBuilder.handle_document - processing station " + station['name'])
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
                doc[_key], _interp_v = self.translate_template_item(doc[_key], station)
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
        :station the station being processed.
        """
        # noinspection PyBroadException
        _func = None
        try:
            _parts = _named_function_def.split('|') 
            _func = _parts[0].replace('&', '')
            _params = []
            if len(_parts) > 1:
                _params = _parts[1].split(',')
            _dict_params = {"station": station}
            for _p in _params:
                # be sure to slice the * off of the front of the param
                # translate_template_item returns a tuple - value,interp_value
                _dict_params[_p[1:]] = self.translate_template_item(_p, station)
            # call the named function using getattr
            _replace_with = getattr(self, _func)(_dict_params)
        except Exception as e:
            logging.error(
                self.__class__.__name__ + " handle_named_function: " + _func + " Exception instantiating builder:  error: " + str(e))
        return _replace_with

    def handle_data(self, doc, station):
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
                        value = self.handle_named_function(value, station)
                    else:
                        value, interp_value = self.translate_template_item(value, station)
                except Exception as e:
                    value = None
                    logging.warning(self.__class__.__name__ +
                                    "GribBuilder.handle_data - value is None")
                _data_elem[key] = value
            if _data_key.startswith('&'):
                _data_key = self.handle_named_function(_data_key, station)
            else:
                # _ ignore the interp_value part of the returned tuple
                _data_key, _interp_value = self.translate_template_item(_data_key, station)
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
            logging.getLogger().setLevel(logging.INFO)
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
            station_limit = self.number_stations
            count = 1
            for row in result:
                if count > station_limit:
                    break
                x, y = self.transformer.transform(
                    row['lon'], row['lat'], radians=False)
                x_gridpoint, y_gridpoint = x/self.spacing, y/self.spacing
                if x_gridpoint < 0 or x_gridpoint > max_x or y_gridpoint < 0 or y_gridpoint > max_y:
                    continue
                station = copy.deepcopy(row)
                station['x_gridpoint'] = x_gridpoint
                station['y_gridpoint'] = y_gridpoint
                self.domain_stations.append(station)
                count = count + 1
            
            if self.do_profiling:
                with cProfile.Profile() as pr:
                    self.handle_document()
                    with open('profiling_stats.txt', 'w') as stream:
                        stats = Stats(pr, stream=stream)
                        stats.strip_dirs()
                        stats.sort_stats('time')
                        stats.dump_stats('profiling_stats.prof')
                        stats.print_stats()
            else:
                self.handle_document()
            _document_map = self.get_document_map()
            return _document_map
        except Exception as e:
            logging.error(self.__class__.__name__ +
                          ": Exception with builder build_document: error: " + str(e))
            return {}
        finally:
            self.grbs.close()
# Concrete builders


class GribModelBuilderV01(GribBuilder):
    def __init__(self, load_spec, ingest_document, cluster, collection, number_stations=sys.maxsize):
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
        :param number_stations - the maximum number of stations to process
        """
        GribBuilder.__init__(self, load_spec, ingest_document, cluster, collection, number_stations=sys.maxsize)
        self.cluster = cluster
        self.collection = collection
        self.number_stations = number_stations
        self.same_time_rows = []
        self.time = 0
        self.interpolated_time = 0
        self.delta = ingest_document['validTimeDelta']
        self.cadence = ingest_document['validTimeInterval']
        self.variableList = ingest_document['variableList']
        self.template = ingest_document['template']
        self.do_profiling = True  # set to True to enable build_document profiling
        #self.do_profiling = False  # set to True to enable build_document profiling

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
        x_gridpoint = round(station['x_gridpoint'])
        y_gridpoint = round(station['y_gridpoint'])

        _message = self.grbs.select(name='Orography')[0]
        _values = _message['values']
        surface = _values[y_gridpoint,x_gridpoint]

        _message = self.grbs.select(name='Geopotential Height', typeOfFirstFixedSurface='215')[0]
        _values = _message['values']
        ceil_msl = _values[y_gridpoint,x_gridpoint]

        # Convert to ceiling AGL and from meters to tens of feet (what is currently inside SQL, we'll leave it as just feet in CB)
        ceil_agl = (ceil_msl - surface) * 0.32808
        return ceil_agl

        # SURFACE PRESSURE
    def handle_surface_pressure(self, params_dict):
        _key = None
        for _key in params_dict.keys():
            if _key != "station":
                break
        _v, _v_interpolated = params_dict[_key]
        # Convert from pascals to milibars
        pres = _v_interpolated
        pres_mb = pres * 100
        return pres_mb

        # Visibility - convert to float
    def handle_visibility(self, params_dict):
        _key = None
        for _key in params_dict.keys():
            if _key != "station":
                break
        _v, _v_interpolated = params_dict[_key]
        return float(_v)

        # relative humidity - convert to float
    def handle_RH(self, params_dict):
        _key = None
        for _key in params_dict.keys():
            if _key != "station":
                break
        _v, _v_interpolated = params_dict[_key]
        return float(_v_interpolated)

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
        _v, _v_interpolated = params_dict[_key]
        tempk = float(_v_interpolated)
        value = ((tempk-273.15)*9)/5 + 32
        return value

        # WIND SPEED
    def handle_wind_speed(self, params_dict):
        station = params_dict['station']
        # interpolated value cannot use rounded gridpoints
        x_gridpoint = station['x_gridpoint']
        y_gridpoint = station['y_gridpoint']

        _message = self.grbs.select(name='10 metre U wind component')[0]
        _values = _message['values']
        uwind_ms = gg.interpGridBox(_values,y_gridpoint,x_gridpoint)
        
        _message = self.grbs.select(name='10 metre V wind component')[0]
        _values = _message['values']
        vwind_ms = gg.interpGridBox(_values,y_gridpoint,x_gridpoint)
        # Convert from U-V components to speed and direction (requires rotation if grid is not earth relative)
        #wind speed then convert to mph
        ws_ms = math.sqrt((uwind_ms*uwind_ms)+(vwind_ms*vwind_ms))
        ws_mph = (ws_ms/0.447) + 0.5
        return ws_mph

        # wind direction
    def handle_wind_direction(self, params_dict):
        station = params_dict['station']
        # interpolated value cannot use rounded gridpoints
        x_gridpoint = station['x_gridpoint']
        y_gridpoint = station['y_gridpoint']
        longitude = station['lon']

        message = self.grbs.select(name='10 metre U wind component')[0]
        _values = message['values']
        uwind_ms = gg.interpGridBox(_values,y_gridpoint,x_gridpoint)
        
        message = self.grbs.select(name='10 metre V wind component')[0]
        _values = message['values']
        vwind_ms = gg.interpGridBox(_values,y_gridpoint,x_gridpoint)
        theta = gg.getWindTheta(message, longitude)
        radians = math.atan2(uwind_ms, vwind_ms)
        wd = (radians*57.2958) + theta + 180
        return wd

    def getName(self, params_dict):
        return params_dict['station']['name']

    def handle_time(self, params_dict):
        # validTime = grbs[1].validate -> 2021-07-12 15:00:00
        _valid_time = self.grbm.analDate
        return round(_valid_time.timestamp())

    def handle_iso_time(self, params_dict):
        _valid_time = _valid_time = self.grbm.analDate
        return _valid_time.isoformat()

    def handle_fcst_len(self, params_dict):
        _fcst_len = self.grbm.forecastTime
        return _fcst_len
