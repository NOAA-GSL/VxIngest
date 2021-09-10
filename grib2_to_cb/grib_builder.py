"""
Program Name: Class grib_builder.py
Contact(s): Randy Pierce
History Log:  Initial version
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSL
"""

import copy
import cProfile
import datetime
import logging
import math
import os.path
import sys
from pstats import Stats

import numpy
import pygrib
import pyproj

import grib2_to_cb.get_grid as gg

TS_OUT_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def convert_to_iso(an_epoch):
    if not isinstance(an_epoch, int):
        an_epoch = int(an_epoch)
    valid_time_str = datetime.datetime.utcfromtimestamp(
        an_epoch).strftime(TS_OUT_FORMAT)
    return valid_time_str


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

    def initialize_document_map(self):
        pass

    def load_data(self, doc, key, element):
        pass

    def get_document_map(self):
        pass

    def handlestation(self, row):
        pass

    def derive_id(self, template_id):
        # Private method to derive a document id from the current station,
        # substituting *values from the corresponding grib fields as necessary.
        # noinspection PyBroadException
        try:
            parts = template_id.split(':')
            new_parts = []
            for part in parts:
                if part.startswith('&'):
                    value = str(self.handle_named_function(part))
                else:
                    if part.startswith("*"):
                        v, _interp_v = self.translate_template_item(part)
                        value = str(v)
                    else:
                        value = str(part)
                new_parts.append(value)
            new_id = ":".join(new_parts)
            return new_id
        except:
            e = sys.exc_info()
            logging.error("GribBuilder.derive_id: Exception  error: " + str(e))

    def translate_template_item(self, variable, single_return=False):
        """
        This method translates template replacements (*item or *item1*item2).
        It can translate keys or values.
        :param variable: a value from the template - should be a grib2 variable or a constant
        :param single_return: if this is True on one value is returned, otherwise an array.
        single_returns are always constants (no replacement).
        :return: It returns an array of values, ordered by domain_stations, or a single value
        depending on the single_return parameter.
        """
        replacements = []
        # noinspection PyBroadException
        try:
            if single_return:
                return (variable, variable)
            if isinstance(variable, str):
                # skip the first replacement, its never
                # really a replacement. It is either '' or not a
                # replacement
                replacements = variable.split('*')[1:]
            # pre assign these in case it isn't a replacement - makes it easier
            station_value = variable
            interpolated_value = variable
            if len(replacements) > 0:
                station_values = []
                for ri in replacements:
                    message = self.grbs.select(name=ri)[0]
                    values = message['values']
                    for station in self.domain_stations:
                        # get the individual station value and interpolated value
                        station_value = values[round(
                            station['y_gridpoint']), round(station['x_gridpoint'])]
                        # interpolated gridpoints cannot be rounded
                        interpolated_value = gg.interpGridBox(
                            values, station['y_gridpoint'], station['x_gridpoint'])
                        # convert each station value to iso if necessary
                        if ri.startswith("{ISO}"):
                            station_value = variable.replace(
                                "*" + ri, convert_to_iso(station_value))
                            interpolated_value = variable.replace(
                                "*" + ri, convert_to_iso(station_value))
                        else:
                            station_value = variable.replace(
                                "*" + ri, str(station_value))
                            interpolated_value = variable.replace(
                                "*" + ri, str(interpolated_value))
                        # add it onto the list of tupples
                        station_values.append(
                            (station_value, interpolated_value))
                return station_values
            # it is a constant, no replacements but we still need a tuple for each station
            return [(station_value, interpolated_value) for i in range(len(self.domain_stations))]
        except Exception as e:
            logging.error(
                "GribBuilder.translate_template_item: Exception  error: " + str(e))

    def handle_document(self):
        """
        This routine processes the complete document (essentially a complete grib file)
        Each template key or value that corresponds to a variable will be selected from
        the grib file into a pygrib message and then
        each station will get values from the grib message.
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
            for key in self.template.keys():
                if key == "data":
                    new_document = self.handle_data(new_document)
                    continue
                new_document = self.handle_key(new_document, key)
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

    def handle_key(self, doc, key):
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
            if key == 'id':
                id = self.derive_id(self.template['id'])
                if not id in doc:
                    doc['id'] = id
                return doc
            if isinstance(doc[key], dict):
                # process an embedded dictionary
                tmp_doc = copy.deepcopy(self.template[key])
                for sub_key in tmp_doc.keys():
                    tmp_doc = self.handle_key(tmp_doc, sub_key)  # recursion
                doc[key] = tmp_doc
            if not isinstance(doc[key], dict) and isinstance(doc[key], str) and doc[key].startswith('&'):
                doc[key] = self.handle_named_function(doc[key])
            else:
                doc[key], _interp_v = self.translate_template_item(
                    doc[key], True)
            return doc
        except Exception as e:
            logging.error(
                self.__class__.__name__ + "GribBuilder.handle_key: Exception in builder:  error: " + str(e))
        return doc

    def handle_named_function(self, _named_function_def):
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
        func = None
        try:
            parts = _named_function_def.split('|')
            func = parts[0].replace('&', '')
            params = []
            if len(parts) > 1:
                params = parts[1].split(',')
            dict_params = {}
            for p in params:
                # be sure to slice the * off of the front of the param
                # translate_template_item returns an array of tuples - value,interp_value, one for each station
                # ordered by domain_stations.
                dict_params[p[1:]] = self.translate_template_item(p)
            # call the named function using getattr
            replace_with = getattr(self, func)(dict_params)
        except Exception as e:
            logging.error(
                self.__class__.__name__ + " handle_named_function: " + func + " Exception instantiating builder:  error: " + str(e))
        return replace_with

    def handle_data(self, doc):
        # noinspection PyBroadException
        try:
            data_elem = {}
            data_key = next(iter(self.template['data']))
            data_template = self.template['data'][data_key]
            for key in data_template.keys():
                try:
                    value = data_template[key]
                    # values can be null...
                    if value and value.startswith('&'):
                        value = self.handle_named_function(value)
                    else:
                        value = self.translate_template_item(value)
                except Exception as e:
                    value = [(None, None)]
                    logging.warning(self.__class__.__name__ +
                                    "GribBuilder.handle_data Exception: " + str(e) + " - setting value to (None,None)")
                data_elem[key] = value
            if data_key.startswith('&'):
                data_key = self.handle_named_function(data_key)
            else:
                # _ ignore the interp_value part of the returned tuple
                data_key, _interp_ignore_value = self.translate_template_item(
                    data_key)
            if data_key is None:
                logging.warning(self.__class__.__name__ +
                                "GribBuilder.handle_data - _data_key is None")

            doc = self.load_data(doc, data_key, data_elem)
            return doc
        except Exception as e:
            logging.error(self.__class__.__name__ +
                          "handle_data: Exception instantiating builder:  error: " + str(e))
        return doc

    def build_document(self, file_name):
        """
        This is the entry point for the gribBuilders from the ingestManager.
        The ingest manager is giving us a grib file to process from the queue.
        These documents are id'd by time and fcstLen. The data section is an array
        each element of which contains variable data and a station name. To process this
        file we need to itterate the domain_stations list and process the station name along
        with all the required variables.
        1) get the first epoch - if none was specified get the latest one from the db
        2) transform the projection from the grib file
        3) determine the stations for this domain, adding gridpoints to each station - build a station list
        4) enable profiling if requested
        5) handle_document - iterate the template and process all the keys and values
        """
        # noinspection PyBroadException
        try:
            # resolve the first epoch
            if self.load_spec['first_last_params']['first_epoch'] == 0:
                # need to find first_epoch from the database - only do this once for all the files
                result = self.cluster.query(
                    "SELECT raw max(mdata.fcstValidEpoch) FROM mdata WHERE type='DD' AND docType='model' AND model=$model AND version='V01' AND subset='METAR';", model=self.template['model'])
                epoch = list(result)[0]
                if epoch is not None:
                    self.load_spec['first_last_params']['first_epoch'] = epoch
            # translate the projection from the grib file
            file_utc_time = datetime.datetime.strptime(
                os.path.basename(file_name), self.load_spec['fmask'])
            file_time = (file_utc_time - datetime.datetime(1970, 1, 1)).total_seconds()
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

            # reset the builders document_map for a new file
            self.initialize_document_map()
            # get stations from couchbase and filter them so
            # that we retain only the ones for this models domain which is derived from the projection
            self.domain_stations = []
            result = self.cluster.query(
                "SELECT mdata.geo.lat, mdata.geo.lon, name from mdata where type='MD' and docType='station' and subset='METAR' and version='V01'")
            station_limit = self.number_stations
            count = 1
            for row in result:
                if count > station_limit:
                    break
                if row['lat'] == -90 and row['lon'] == 180:
                    # TODO need to fix this
                    continue  # don't know how to transform that station
                x, y = self.transformer.transform(
                    row['lon'], row['lat'], radians=False)
                x_gridpoint, y_gridpoint = x/self.spacing, y/self.spacing
                try:
                    if math.floor(x_gridpoint) < 0 or math.ceil(x_gridpoint) >= max_x or math.floor(y_gridpoint) < 0 or math.ceil(y_gridpoint) >= max_y:
                        continue
                except Exception as e:
                    logging.error(
                        self.__class__.__name__ + ": Exception with builder build_document: error: " + str(e))
                    continue
                station = copy.deepcopy(row)
                station['x_gridpoint'] = x_gridpoint
                station['y_gridpoint'] = y_gridpoint
                self.domain_stations.append(station)
                count = count + 1

            # if we have asked for profiling go ahead and do it
            if self.do_profiling:
                with cProfile.Profile() as pr:
            # check to see if it is within first and last epoch (default is 0 and maxsize)
                    if file_time >= self.load_spec['first_last_params']['first_epoch']:
                        self.handle_document()
                    with open('profiling_stats.txt', 'w') as stream:
                        stats = Stats(pr, stream=stream)
                        stats.strip_dirs()
                        stats.sort_stats('time')
                        stats.dump_stats('profiling_stats.prof')
                        stats.print_stats()
            else:
            # check to see if it is within first and last epoch (default is 0 and maxsize)
                if file_time >= self.load_spec['first_last_params']['first_epoch']:
                    self.handle_document()
            document_map = self.get_document_map()
            return document_map
        except Exception as e:
            logging.error(self.__class__.__name__ +
                          ": Exception with builder build_document: error: " + str(e))
            return {}
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
        GribBuilder.__init__(self, load_spec, ingest_document,
                             cluster, collection, number_stations=sys.maxsize)
        self.cluster = cluster
        self.collection = collection
        self.number_stations = number_stations
        self.same_time_rows = []
        self.time = 0
        self.interpolated_time = 0
        self.delta = ingest_document['validTimeDelta']
        self.cadence = ingest_document['validTimeInterval']
        self.template = ingest_document['template']
        # self.do_profiling = True  # set to True to enable build_document profiling
        self.do_profiling = False  # set to True to enable build_document profiling

    def initialize_document_map(self):
        """
        reset the document_map for a new file
        """
        self.document_map = {}

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
        This method builds the data array. It gets the data key ('data') and the data element
        which in this case is a set of arrays. This routine has to create the data array from these
        arrays which are lists of values ordered by domain_station.
        :param doc: The document being created
        :param key: Not used
        :param element: the observation data
        :return: the document being created
        """
        if 'data' not in doc.keys() or doc['data'] is None:
            keys = list(element.keys())
            doc['data'] = []
            for i in range(len(self.domain_stations)):
                elem = {}
                for key in keys:
                    elem[key] = element[key][i]
                doc['data'].append(elem)
        return doc

    # named functions

    def handle_ceiling(self, params_dict):
        """
        the dict_params aren't used here since we need to
        select two messages (self.grbs.select is expensive since it scans the whole grib file).
        Each message is selected once and the station location data saved in an array,
        then all the domain_stations are iterated (in memory operation)
        to make the ceiling calculations for each station location.
        """
        # This is the original 'C' algorithm for grib
        # if(ceil_msl < -1000 ||
        #    ceil_msl > 1e10) {
        #   /* printf("setting ceil_agl for x/y %d/%d from %0f to 6000\n",xi,yj,ceil_msl); */
        #   ceil_agl = 6000;
        #   n_forced_clear++;
        # } else if(ceil_msl < 0) {
        #   /* weird '-1's in the grib files */
        #   printf("strange ceiling: %f. setting to 0.\n",ceil_msl);
        #   ceil_agl = 0;
        #   n_zero_ceils++;
        #  } else {
        #     ceil_agl = (ceil_msl - sfc_hgt)*0.32808; /* m -> tens of ft */
        #   }
        #   n_good_ceils++;
        #   if(ceil_agl < 0) {
        #     if(DEBUG == 1) {
        #       printf("negative AGL ceiling for %d: ceil (MSL?): %.0f sfc: %.0f (ft)\n",
        #              sp->sta_id,ceil_msl*3.2808,sfc_hgt*3.2808);
        #     }
        #     ceil_agl = 0;
        #     n_zero_ceils++;
        #   }
        # }

        message = self.grbs.select(name='Orography')[0]
        values = message['values']
        surface_values = []
        for station in self.domain_stations:
            x_gridpoint = round(station['x_gridpoint'])
            y_gridpoint = round(station['y_gridpoint'])
            surface_values.append(values[y_gridpoint, x_gridpoint])

        message = self.grbs.select(
            name='Geopotential Height', typeOfFirstFixedSurface='215')[0]
        values = message['values']

        ceil_msl_values = []
        for station in self.domain_stations:
            x_gridpoint = round(station['x_gridpoint'])
            y_gridpoint = round(station['y_gridpoint'])
            # what do we do with a masked ceiling value?
            if not numpy.ma.is_masked(values[y_gridpoint, x_gridpoint]):
                ceil_msl_values.append(values[y_gridpoint, x_gridpoint])
            else:
                # masked values should be treated as all clear i.e. 60000
                ceil_msl_values.append(60000)
        ceil_agl = []
        for i in range(len(self.domain_stations)):
            if ceil_msl_values[i] == 60000:
                ceil_agl.append(60000)
            else:
                if ceil_msl_values[i] is None or surface_values[i] is None:
                    ceil_agl.append(None)
                else:
                    if(ceil_msl_values[i] < -1000 or ceil_msl_values[i] > 1e10):
                        ceil_agl.append(60000)
                    else:
                        if ceil_msl_values[i] < 0:
                            # weird '-1's in the grib files??? (from legacy code)
                            ceil_agl.append(0)
                        else:
                            tmp_ceil = (ceil_msl_values[i] - surface_values[i]) * 3.281
                            if tmp_ceil < 0:
                                ceil_agl.append(0)
                            else:
                                ceil_agl.append(tmp_ceil)
        return ceil_agl

        # SURFACE PRESSURE
    def handle_surface_pressure(self, params_dict):
        """
        translate all the pressures(one per station location) to milibars
        """
        pressures = []
        for v, v_intrp_pressure in list(params_dict.values())[0]:
            # Convert from pascals to milibars
            pressures.append(float(v_intrp_pressure) / 100)
        return pressures

        # Visibility - convert to float
    def handle_visibility(self, params_dict):
        # convert all the values to a float
        vis_values = []
        for v, v_intrp_ignore in list(params_dict.values())[0]:
            vis_values.append(float(v) / 1609.344 if v is not None else None)
        return vis_values

        # relative humidity - convert to float
    def handle_RH(self, params_dict):
        # convert all the values to a float
        rh_interpolated_values = []
        for v, v_intrp_pressure in list(params_dict.values())[0]:
            rh_interpolated_values.append(
                float(v_intrp_pressure) if v_intrp_pressure is not None else None)
        return rh_interpolated_values

    def kelvin_to_farenheight(self, params_dict):
        """
            param:params_dict expects {'station':{},'*variable name':variable_value}
            Used for temperature and dewpoint
        """
        # Convert each station value from Kelvin to Farenheit
        tempf_values = []
        for v, v_intrp_tempf in list(params_dict.values())[0]:
            tempf_values.append(((float(v_intrp_tempf)-273.15)*9) /
                                5 + 32 if v_intrp_tempf is not None else None)
        return tempf_values

        # WIND SPEED
    def handle_wind_speed(self, params_dict):
        """
        the params_dict aren't used here since we need to
        select two messages (self.grbs.select is expensive since it scans the whole grib file).
        Each message is selected once and the station location data saved in an array,
        then all the domain_stations are iterated (in memory operation)
        to make the wind speed calculations for each station location.
        """
        # interpolated value cannot use rounded gridpoints

        message = self.grbs.select(name='10 metre U wind component')[0]
        values = message['values']
        uwind_ms_values = []
        for station in self.domain_stations:
            x_gridpoint = station['x_gridpoint']
            y_gridpoint = station['y_gridpoint']
            uwind_ms_values.append(gg.interpGridBox(
                values, y_gridpoint, x_gridpoint))

        message = self.grbs.select(name='10 metre V wind component')[0]
        values = message['values']
        vwind_ms_values = []
        for station in self.domain_stations:
            x_gridpoint = station['x_gridpoint']
            y_gridpoint = station['y_gridpoint']
            vwind_ms_values.append(gg.interpGridBox(
                values, y_gridpoint, x_gridpoint))
        # Convert from U-V components to speed and direction (requires rotation if grid is not earth relative)
        # wind speed then convert to mph
        ws_mph = []
        for i in range(len(uwind_ms_values)):
            uwind_ms = uwind_ms_values[i]
            vwind_ms = vwind_ms_values[i]
            ws_ms = math.sqrt((uwind_ms*uwind_ms)+(vwind_ms*vwind_ms))
            ws_mph.append((ws_ms/0.447) + 0.5)
        return ws_mph

        # wind direction
    def handle_wind_direction(self, params_dict):
        """
        the params_dict aren't used here since we need to
        select two messages (self.grbs.select is expensive since it scans the whole grib file).
        Each message is selected once and the station location data saved in an array,
        then all the domain_stations are iterated (in memory operation)
        to make the wind direction calculations for each station location.
        Each individual station longitude is used to rotate the wind direction.
        """

        message = self.grbs.select(name='10 metre U wind component')[0]
        values = message['values']
        uwind_ms = []
        for station in self.domain_stations:
            x_gridpoint = station['x_gridpoint']
            y_gridpoint = station['y_gridpoint']
            longitude = station['lon']
            # interpolated value cannot use rounded gridpoints
            uwind_ms.append(gg.interpGridBox(values, y_gridpoint, x_gridpoint))

        message = self.grbs.select(name='10 metre V wind component')[0]
        values = message['values']
        vwind_ms = []
        theta = []
        wd = []
        for station in self.domain_stations:
            x_gridpoint = station['x_gridpoint']
            y_gridpoint = station['y_gridpoint']
            longitude = station['lon']
            vwind_ms.append(gg.interpGridBox(values, y_gridpoint, x_gridpoint))
            theta.append(gg.getWindTheta(message, longitude))

        for i in range(len(uwind_ms)):
            radians = math.atan2(uwind_ms[i], vwind_ms[i])
            wd.append((radians*57.2958) + theta[i] + 180)
        return wd

    def getName(self, params_dict):
        station_names = []
        for station in self.domain_stations:
            station_names.append(station['name'])
        return station_names

    def handle_time(self, params_dict):
        # validTime = grbs[1].validate -> 2021-07-12 15:00:00
        valid_time = self.grbm.validDate
        return round(valid_time.timestamp())

    def handle_iso_time(self, params_dict):
        valid_time = valid_time = self.grbm.validDate
        return valid_time.isoformat()

    def handle_fcst_len(self, params_dict):
        fcst_len = self.grbm.forecastTime
        return fcst_len
