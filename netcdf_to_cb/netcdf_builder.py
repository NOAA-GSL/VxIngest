"""
Program Name: Class netcdf_builder.py
Contact(s): Randy Pierce
History Log:  Initial version
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSL
"""

import calendar
import copy
import datetime as dt
import logging
import math
import os.path
import re
import sys
import time
from datetime import datetime, timedelta

import netCDF4 as nc
import numpy.ma as ma

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


class NetcdfBuilder:
    def __init__(self, load_spec, ingest_document, cluster, collection):
        self.template = ingest_document['template']
        self.load_spec = load_spec
        self.cluster = cluster
        self.collection = collection
        self.id = None
        self.document_map = {}
        self.ncdf_data_set = None
        self.station_names = []

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
            parts = template_id.split(':')
            new_parts = []
            for part in parts:
                if part.startswith('&'):
                    value = str(self.handle_named_function(part, recNum))
                else:
                    if part.startswith("*"):
                        value = str(self.translate_template_item(part, recNum))
                    else:
                        value = str(part)
                new_parts.append(value)
            new_id = ":".join(new_parts)
            return new_id
        except:
            e = sys.exc_info()
            logging.error("NetcdfBuilder.derive_id: Exception  error: %s", str(e))

    def translate_template_item(self, variable, recNum):
        """
        This method translates template replacements (*item).
        It can translate keys or values.
        :param variable: a value from the template - should be a netcdf variable
        :param recNum: the current recNum
        :return:
        """
        replacements = []
        # noinspection PyBroadException
        try:
            if isinstance(variable, str):
                replacements = variable.split('*')[1:]
            # skip the first replacement, its never
            # really a replacement. It is either '' or not a
            # replacement

            make_str = False
            value = variable
            Smatch = re.compile(".*S.*")
            Umatch = re.compile(".*U.*")
            if len(replacements) > 0:
                for ri in replacements:
                    vtype = str(self.ncdf_data_set.variables[ri].dtype)
                    if Smatch.match(vtype) or Umatch.match(vtype):
                        make_str = True
                        chartostring = True
                        break
                for ri in replacements:
                    if ri.startswith("{ISO}"):
                        variable = value.replace("*{ISO}", "")
                        if chartostring:
                            # for these we have to convert the character array AND convert to ISO (it is probably a string date)
                            value = convert_to_iso(
                                # pylint: disable=maybe-no-member
                                "*{ISO}" + nc.chartostring(self.ncdf_data_set[variable][recNum]))
                        else:
                            # for these we have to convert convert to ISO (it is probably an epoch)
                            value = convert_to_iso(
                                "*{ISO}" + self.ncdf_data_set[variable][recNum])
                    else:
                        variable = value.replace("*", "")
                        if make_str:
                            if chartostring:
                                # it is a char array of something
                                value = value.replace(
                                    # pylint: disable=maybe-no-member
                                    '*' + ri, str(nc.chartostring(self.ncdf_data_set[variable][recNum])))
                                return value
                            else:
                                # it is probably a number
                                value = str(
                                    self.ncdf_data_set[variable][recNum])
                                return value
                        else:
                            # it desn't need to be a string
                            return self.ncdf_data_set[variable][recNum]
        except Exception as e:
            logging.error("NetcdfBuilder.translate_template_item: Exception  error: %s", str(e))
        return value

    def handle_document(self):
        """
        :return: The modified document_map
        """
        # noinspection PyBroadException
        try:
            new_document = copy.deepcopy(self.template)
            recNum_data_size = self.ncdf_data_set.dimensions['recNum'].size
            if recNum_data_size == 0:
                return
            # make a copy of the template, which will become the new document
            # once all the translations have occured
            new_document = initialize_data(new_document)
            for recNum in range(recNum_data_size):
                for key in self.template.keys():
                    if key == "data":
                        new_document = self.handle_data(new_document, recNum)
                        continue
                    new_document = self.handle_key(new_document, recNum, key)
            # put document into document map
            if new_document['id']:
                logging.info("NetcdfBuilder.handle_document - adding document %s", new_document['id'])
                self.document_map[new_document['id']] = new_document
            else:
                logging.info("NetcdfBuilder.handle_document - cannot add document with key %s", str(new_document['id']))
        except Exception as e:
            logging.error("NetcdfBuilder.handle_document: Exception instantiating builder: %s error: %s", self.__class__.__name__,  str(e))
            raise e

    def handle_key(self, doc, _recNum, key):
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
            if key == 'id':
                an_id = self.derive_id(self.template['id'], _recNum)
                if not an_id == doc['id']:
                    doc['id'] = an_id
                return doc
            if isinstance(doc[key], dict):
                # process an embedded dictionary
                tmp_doc = copy.deepcopy(self.template[key])
                for _sub_key in tmp_doc.keys():
                    tmp_doc = self.handle_key(
                        tmp_doc, _recNum, _sub_key)  # recursion
                doc[key] = tmp_doc
            if not isinstance(doc[key], dict) and isinstance(doc[key], str) and doc[key].startswith('&'):
                doc[key] = self.handle_named_function(doc[key], _recNum)
            else:
                doc[key] = self.translate_template_item(doc[key], _recNum)
            return doc
        except Exception as e:
            logging.error("%s NetcdfBuilder.handle_key: Exception in builder:  error: %s", self.__class__.__name__, str(e))
        return doc

    def handle_named_function(self, named_function_def, _recNum):
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
            func = named_function_def.split('|')[0].replace('&', '')
            params = named_function_def.split('|')[1].split(',')
            dict_params = {"recNum": _recNum}
            for _p in params:
                # be sure to slice the * off of the front of the param
                dict_params[_p[1:]] = self.translate_template_item(_p, _recNum)
            # call the named function using getattr
            replace_with = getattr(self, func)(dict_params)
        except Exception as e:
            logging.error("%s handle_named_function: Exception instantiating builder:  error: %s", self.__class__.__name__, str(e))
        return replace_with

    def handle_data(self, doc, recNum):
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
                        value = self.handle_named_function(value, recNum)
                    else:
                        value = self.translate_template_item(value, recNum)
                except Exception as e:
                    value = None
                    logging.warning("%s NetcdfBuilder.handle_data - value is None", self.__class__.__name__)
                data_elem[key] = value
            if data_key.startswith('&'):
                data_key = self.handle_named_function(data_key, recNum)
            else:
                data_key = self.translate_template_item(data_key, recNum)
            if data_key is None:
                logging.warning("%s NetcdfBuilder.handle_data - _data_key is None", self.__class__.__name__)
            # pylint: disable=assignment-from-no-return
            doc = self.load_data(doc, data_key, data_elem)
            return doc
        except Exception as e:
            logging.error("%s handle_data: Exception instantiating builder:  error: %s", self.__class__.__name__, str(e))
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
            # pylint: disable=no-member
            self.ncdf_data_set = nc.Dataset(file_name)
            if len(self.station_names) == 0:
                result = self.cluster.query("""SELECT raw name FROM mdata
                    WHERE
                    type = 'MD'
                    AND docType = 'station'
                    AND subset = 'METAR'
                    AND version = 'V01';
                """)
                self.station_names = list(result)

            if self.load_spec['first_last_params']['first_epoch'] == 0:
                # need to find first_epoch from the database - only do this once for all the files
                result = self.cluster.query(
                    "SELECT raw max(mdata.fcstValidEpoch) FROM mdata WHERE type='DD' AND docType='obs' AND version='V01' AND subset='METAR';")
                epoch = list(result)[0]
                if epoch is not None:
                    self.load_spec['first_last_params']['first_epoch'] = epoch
            file_utc_time = datetime.strptime(
                os.path.basename(file_name), self.load_spec['fmask'])
            file_time = (file_utc_time - datetime(1970, 1, 1)).total_seconds()
            # check to see if it is within first and last epoch (default is 0 and maxsize)
            if file_time >= float(self.load_spec['first_last_params']['first_epoch']):
                logging.info("%s building documents for file %s", self.__class__.__name__, file_name)
                self.handle_document()
            # pylint: disable=assignment-from-no-return
            document_map = self.get_document_map()
            return document_map
        except Exception as e:
            logging.error("%s: Exception with builder build_document: error: %s", self.__class__.__name__, str(e))
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
        try:
            if len(self.same_time_rows) != 0:
                self.handle_document()
            # convert data map to a list
            # document_map might be None
            if self.document_map and (type(self.document_map) == dict):
                for d in self.document_map.values():
                    try:
                        if 'data' in d.keys() and type(d['data']) == dict:
                            data_map = d['data']
                            data_list = list(data_map.values())
                            d['data'] = sorted(data_list, key=lambda data_elem: data_elem['name'])
                    except Exception as e1:
                        logging.error("%s get_document_map list conversion: Exception processing%s:  error: %s", self.__class__.__name__, str(d['data']), str(e1))
            return self.document_map
        except Exception:
            logging.exception("%s get_document_map: Exception in get_document_map", self.__class__.__name__)
    def load_data(self, doc, key, element):
        """
        This method appends an observation to the data array -
        in fact we use a dict to hold data elems to ensure
        the data elements are unique per station name, the map is converted
        back to a list in get_document_map. Using a map ensures that the last 
        entry in the netcdf file is the one that gets captured.
        :param doc: The document being created
        :param key: Not used
        :param element: the observation data
        :return: the document being created
        """
        if 'data' not in doc.keys() or doc['data'] is None:
            doc['data'] = {}
        doc['data'][element['name']] = element
        return doc

    # named functions
    # TODO - may not need this - checking
    def meterspersecond_to_milesperhour(self, params_dict):
        # Meters/second to miles/hour
        try:
            value = self.umask_value_transform(params_dict)
            if value is not None and value != "":
                value = value * 2.237
            return value
        except Exception as e:
            logging.error("%s handle_data: Exception in named function meterspersecond_to_milesperhour:  error: %s", self.__class__.__name__, str(e))

    def ceiling_transform(self, params_dict):
        try:
            skyCover = params_dict['skyCover']
            skyLayerBase = params_dict['skyLayerBase']
            # code clear as 60,000 ft 
            mCLR = re.compile('.*CLR.*')
            mSKC = re.compile('.*SKC.*')
            mNSC = re.compile('.*NSC.*')
            mFEW = re.compile('.*FEW.*')
            mSCT = re.compile('.*SCT.*')
            mBKN = re.compile('.*BKN.*')  # Broken
            mOVC = re.compile('.*OVC.*')  # Overcast
            mVV = re.compile('.*VV.*')  # Vertical Visibility
            mask_array = ma.getmaskarray(skyLayerBase)
            skyCover_array = skyCover[1:-1].replace("'", "").split(" ")
            # check for unmasked ceiling values - broken, overcast, vertical visibility - return associated skyLayerBase
            for index in range(len(skyLayerBase)):
                # also convert meters to feet (* 3.281)
                if (not mask_array[index]) and (mBKN.match(skyCover_array[index]) or mOVC.match(skyCover_array[index]) or mVV.match(skyCover_array[index])):
                    return math.floor(skyLayerBase[index]) * 3.281
            # check for unmasked ceiling values - all the others - CLR, SKC, NSC, FEW, SCT - return 60000
            for index in range(len(skyLayerBase)):
                # 60000 is aldready feet
                if (not mask_array[index]) and (mCLR.match(skyCover_array[index]) or mSKC.match(skyCover_array[index]) or mNSC.match(skyCover_array[index]) or mFEW.match(skyCover_array[index]) or mSCT.match(skyCover_array[index])):                 
                    return 60000
            # nothing was unmasked - return None
            return None
        except Exception as e:
            logging.error("%s handle_data: Exception in named function ceiling_transform:  error: %s", self.__class__.__name__, str(e))

    def kelvin_to_farenheight(self, params_dict):
        try:
            value = self.umask_value_transform(params_dict)
            if value is not None and value != "":
                value = (float(value) - 273.15) * 1.8 + 32
            return value
        except Exception as e:
            logging.error("%s handle_data: Exception in named function kelvin_to_farenheight:  error: %s", self.__class__.__name__, str(e))

    def umask_value_transform(self, params_dict):
        # Probably need more here....
        try:
            key = None
            recNum = params_dict['recNum']
            for key in params_dict.keys():
                if key != "recNum":
                    break
            ncValue = self.ncdf_data_set[key][recNum]
            if not ma.getmask(ncValue):
                value = ma.compressed(ncValue)[0]
                return float(value)
            else:
                return None
        except Exception as e:
            logging.error("%s umask_value_transform: Exception in named function umask_value_transform for key %s:  error: %s", self.__class__.__name__, key, str(e))

    def handle_pressure(self, params_dict):
        try:
            value = self.umask_value_transform(params_dict)
            if value is not None:
                # convert to millibars (from pascals) and round
                value = math.floor(float(value) / 100)
            return value
        except Exception as e:
            logging.error("%s handle_pressure: Exception in named function:  error: %s", self.__class__.__name__, str(e))

    def handle_visibility(self, params_dict):
        #vis_sm = vis_m / 1609.344
        try:
            value = self.umask_value_transform(params_dict)
            logging.info("converting %s to nautical miles", str(value))
            if value is not None:
                value = math.floor(float(value)/ 1609.344)
            return float(value)
        except Exception as e:
            logging.error("%s handle_visibility: Exception in named function:  error: %s", self.__class__.__name__, str(e))

    def interpolate_time(self, params_dict):
        """
        Rounds to nearest hour by adding a timedelta hour if minute >= 30
        """
        try:
            time = None
            timeObs = params_dict['timeObs']
            if not ma.getmask(timeObs):
                time = int(ma.compressed(timeObs)[0])
            else:
                return ""
            time = datetime.fromtimestamp(time)
            time = time.replace(second=0, microsecond=0, minute=0,
                                hour=time.hour) + timedelta(hours=time.minute//30)
            return calendar.timegm(time.timetuple())
        except Exception as e:
            logging.error("%s handle_data: Exception in named function interpolate_time:  error: %s", self.__class__.__name__, str(e))

    def interpolate_time_iso(self, params_dict):
        """
        Rounds to nearest hour by adding a timedelta hour if minute >= 30
        """
        try:
            time = None
            timeObs = params_dict['timeObs']
            if not ma.getmask(timeObs):
                time = int(ma.compressed(timeObs)[0])
            else:
                return ""
            time = datetime.fromtimestamp(time)
            time = time.replace(second=0, microsecond=0, minute=0,
                                hour=time.hour) + timedelta(hours=time.minute//30)
            # convert this iso
            return str(time.isoformat())
        except Exception as e:
            logging.error("%s handle_data: Exception in named function interpolate_time_iso:  error: %s", self.__class__.__name__, str(e))

    def fill_from_netcdf(self, _recNum, netcdf):
        """
        Used by handle_station to get the records from netcdf for comparing with the
        records from the database.
        """
        netcdf = {}
        if not ma.getmask(self.ncdf_data_set['latitude'][_recNum]):
            netcdf['latitude'] = ma.compressed(
                self.ncdf_data_set['latitude'][_recNum])[0]
        else:
            netcdf['latitude'] = None
        if not ma.getmask(self.ncdf_data_set['longitude'][_recNum]):
            netcdf['longitude'] = ma.compressed(
                self.ncdf_data_set['longitude'][_recNum])[0]
        else:
            netcdf['longitude'] = None
        if not ma.getmask(self.ncdf_data_set['elevation'][_recNum]):
            netcdf['elevation'] = ma.compressed(
                self.ncdf_data_set['elevation'][_recNum])[0]
        else:
            netcdf['elevation'] = None
        # pylint: disable=no-member
        netcdf['description'] = str(nc.chartostring(
            self.ncdf_data_set['locationName'][_recNum]))
        netcdf['name'] = str(nc.chartostring(
            self.ncdf_data_set['stationName'][_recNum]))
        return netcdf

    def handle_station(self, params_dict):
        """
         This method uses the station name in the params_dict
         to find a station with that name.
         If the station does not exist it will be created with data from the
         netcdf file.
         :param params_dict: {station_name:a_station_name}
         :return:
         """
        recNum = params_dict['recNum']
        station_name = params_dict['stationName']
        id = None
        netcdf = {}

        # noinspection PyBroadException
        try:
            if station_name not in self.station_names:
                # get the netcdf fields for comparing or adding new
                netcdf = self.fill_from_netcdf(recNum, netcdf)
                logging.info("netcdfObsBuilderV01.handle_station - adding station %s", netcdf['name'])
                id = "MD:V01:METAR:station:" + netcdf['name']
                new_station = {
                    "id": "MD:V01:METAR:station:" + netcdf['name'],
                    "description": netcdf['description'],
                    "docType": "station",
                    "firstTime": 0,
                    "geo": {
                        "elev": round(float(netcdf['elevation']), 4),
                        "lat": round(float(netcdf['latitude']), 4),
                        "lon": round(float(netcdf['longitude']), 4)
                    },
                    "lastTime": 0,
                    "name": netcdf['name'],
                    "subset": "METAR",
                    "type": "MD",
                    "updateTime": int(time.time()),
                    "version": "V01"
                }
                # add the station to the document map
                if not id in self.document_map.keys():
                    self.document_map[id] = new_station
                self.station_names.append(station_name)
            return params_dict['stationName']
        except Exception as e:
            logging.error("%s netcdfObsBuilderV01.handle_station: Exception finding or creating station to match station_name error: %s params: %s", self.__class__.__name__,
                str(e), str(params_dict))
            return ""
