"""
Program Name: Class ctc_builder.py
Contact(s): Randy Pierce
History Log:  Initial version
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSL
"""

import copy
import cProfile
import datetime as dt
import logging
import math
import sys
import numpy
from pstats import Stats

from couchbase.cluster import Cluster, ClusterOptions, PasswordAuthenticator
from couchbase.exceptions import CouchbaseException
from couchbase.mutation_state import MutationState
from couchbase.search import (HighlightStyle, PrefixQuery, QueryStringQuery,
                              SearchOptions, SearchQuery, SortField, SortScore,
                              TermFacet)


TS_OUT_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def convert_to_iso(an_epoch):
    if not isinstance(an_epoch, int):
        an_epoch = int(an_epoch)
    valid_time_str = dt.datetime.utcfromtimestamp(
        an_epoch).strftime(TS_OUT_FORMAT)
    return valid_time_str


def initialize_data(doc):
    """ initialize the data by just making sure the template data element has been removed.
    All the data elements are going to be top level elements"""
    if 'data' in doc.keys():
        del doc['data']
    return doc


class CTCBuilder:
    """
    1) find all the stations for the region for this ingest (model and region)
    select raw geo from mdata where type="MD" and docType="station" and subset='METAR' and version='V01'
    Use the region metadata document....
    SELECT 
        geo.bottom_right.lat as br_lat,
        geo.bottom_right.lon as br_lon,
        geo.top_left.lat as tl_lat,
        geo.top_left.lon as tl_lon
    FROM mdata 
    WHERE type="MD" and docType="region" and subset='COMMON' and version='V01' and name="ALL_HRRR"
    use the boubnding box to select stations for the region
    [
        {
            "bottom_right": {
            "lat": 21.7692,
            "lon": -61.7802
            },
            "top_left": {
            "lat": 52.3516,
            "lon": -126.5495
            }
        }
    ]
    SELECT name,
       geo.lat,
       geo.lon
    FROM mdata
    WHERE type="MD"
    AND docType="station"
    AND subset='METAR'
    AND version='V01'
    AND geo.lat BETWEEN 21.7692 and 52.3516
    AND geo.lon BETWEEN -126.5495 AND -61.7802

    This can be done with a join but it it is probably better to do it with two queries and code.
    This is the join.
    SELECT RAW s.name
    FROM mdata s
        JOIN mdata bb ON s.geo.lat BETWEEN bb.geo.bottom_right.lat AND bb.geo.top_left.lat
        AND s.geo.lon BETWEEN bb.geo.top_left.lon AND bb.geo.bottom_right.lon
    WHERE bb.type="MD"
        AND bb.docType="region"
        AND bb.subset='COMMON'
        AND bb.version='V01'
        AND bb.name="ALL_HRRR"
        AND s.type="MD"
        AND s.docType="station"
        AND s.subset='METAR'
        AND s.version='V01'
    2) find the minimum of the maximum valid times of corresponding CTC documents currently in the database.
    select raw min (mdata.fcstValidEpoch) from mdata where type="DD" and docType="obs" and subset='METAR' and version='V01'  limit 10
    3) find the maximum of the minimum valid times of the obs and corresponding models that are currently in the database. This is
    the data for ALL the stations. What delineates a region is the subset of station names that are in a region. This corresponds to a
    subset of the data portion of each model.
    select raw min (mdata.fcstValidEpoch) from mdata where type="DD" and docType="model" and subset='METAR' and version='V01' and model="HRRR_OPS" 
    select raw min (mdata.fcstValidEpoch) from mdata where type="DD" and docType="obs" and subset='METAR' and version='V01'  limit 10
    4) using the minimum valid time and the domain station list query for model and obs pairs within the station list.

    5) iterate that batch of data by valid time and fcstLen creating corresponding CTC documents.
    """

    def __init__(self, load_spec, ingest_document, cluster, collection, number_stations=sys.maxsize):
        self.template = ingest_document['template']
        self.load_spec = load_spec
        self.cluster = cluster
        self.collection = collection
        self.id = None
        self.document_map = {}
        self.domain_stations = []
        self.model= self.template['model']
        self.region= self.template['region']

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
                                "*" + ri, str(station_value))
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
        This routine processes the complete document
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
        These documents are id'd by time and fcstLen. The data section is an array 
        each element of which contains variable data and a station name. To process this
        file we need to itterate the domain_stations list and process the station name along
        with all the required variables.
        """
        # noinspection PyBroadException
        try:
            logging.getLogger().setLevel(logging.INFO)
            # reset the builders document_map for a new file
            self.initialize_document_map()

            # get stations from couchbase and filter them so
            # that we retain only the ones for this models domain which is defined by the region boundingbox
            try:
                result = self.cluster.query(
                    "SELECT  geo.bottom_right.lat as br_lat, geo.bottom_right.lon as br_lon, geo.top_left.lat as tl_lat, geo.top_left.lon as tl_lon FROM mdata  WHERE type='MD' and docType='region' and subset='COMMON' and version='V01' and name=$region", region=self.region)
                boundingbox = list(result)[0]
                self.domain_stations = []
                result = self.cluster .query(
                    "SELECT mdata.geo.lat, mdata.geo.lon, name from mdata where type='MD' and docType='station' and subset='METAR' and version='V01'")
                for row in result:
                    if row['lat'] >= boundingbox.br_lat and \
                            row['lat'] <= boundingbox.tl_lat and \
                            row['lon'] >= boundingbox.br_lon and \
                            row['lon'] <= boundingbox.tl_lon:
                        self.domain_stations.append(row['name'])
            except Exception as e:
                logging.error(self.__class__.__name__ +
                              ": Exception with builder build_document: error: " + str(e))

            # Get the valid list of fcstValidEpochs for this operation.
            # First get the latest fcstValidEpoch for the ctc's for this model and region.
            # Second get the intersection of the fcstValidEpochs that correspond for this
            # model and the obs for all fcstValidEpochs greater than the first ctc.
            
            result = self.cluster.query(
                "SELECT MAX(mdata.fcstValidEpoch) FROM mdata WHERE type='DD' AND docType='CTC' AND model=$model AND region=$region AND version='V01' AND subset='METAR'", model=self.model, region=self.region, read_only=True)
            max_ctc_fcstValidEpochs = list(result)[0]

            result = self.cluster.query(
                "SELECT raw mdata.fcstValidEpoch FROM mdata WHERE type='DD' AND docType='model' AND model=$model AND region=$region AND version='V01' AND subset='METAR' and fcstValidEpoch > $max_fcst_epoch", model=self.model, region=self.region, max_fcst_epoch=max_ctc_fcstValidEpochs, read_only=True)
            model_fcstValidEpochs = list(result)

            result = self.cluster.query(
                "SELECT raw mdata.fcstValidEpoch FROM mdata WHERE type='DD' AND docType='obs' AND version='V01' AND subset='METAR' and fcstValidEpoch > $max_fcst_epoch", model=self.model, region=self.region, max_fcst_epoch=max_ctc_fcstValidEpochs, read_only=True)
            obs_fcstValidEpochs = list(result)
            self.fcstValidEpochs = set(model_fcstValidEpochs).intersection(obs_fcstValidEpochs)

            # if we have asked for profiling go ahead and do it
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
            document_map = self.get_document_map()
            return document_map
        except Exception as e:
            logging.error(self.__class__.__name__ +
                          ": Exception with builder build_document: error: " + str(e))
            return {}
        finally:
            self.grbs.close()
# Concrete builders


class CTCModelObsBuilderV01(CTCBuilder):
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
        CTCModelObsBuilderV01.__init__(
            self, load_spec, ingest_document, cluster, collection)
        self.cluster = cluster
        self.collection = collection
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

    def handle_time(self, params_dict):
        # validTime = grbs[1].validate -> 2021-07-12 15:00:00
        valid_time = self.grbm.analDate
        return round(valid_time.timestamp())

    def handle_iso_time(self, params_dict):
        valid_time = valid_time = self.grbm.analDate
        return valid_time.isoformat()

    def handle_fcst_len(self, params_dict):
        fcst_len = self.grbm.forecastTime
        return fcst_len
