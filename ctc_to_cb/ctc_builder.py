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
import sys
import re
from couchbase.exceptions import DocumentNotFoundException

from pstats import Stats

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

    def __init__(self, load_spec, ingest_document, cluster, collection):
        self.template = ingest_document['template']
        self.load_spec = load_spec
        self.cluster = cluster
        self.collection = collection
        self.id = None
        self.document_map = {}
        self.domain_stations = []
        self.model = self.template['model']
        self.region = self.template['region']
        self.subDocType = self.template['subDocType']
        self.model_fcstValidEpochs = []
        self.model_data = {} # used to stash each fcstValidEpoch model_data for the handlers
        self.obs_data = {} # used to stash each fcstValidEpoch obs_data for the handlers
        self.obs_station_names = [] # used to stash sorted obs names for the handlers
        self.thresholds = None
    def initialize_document_map(self):
        pass

    def get_document_map(self):
        pass

    def handle_data(self):
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

    def translate_template_item(self, variable):
        """
        This method translates template replacements (*item or *item1*item2).
        It can translate keys or values. 
        :param variable: a value from the template - should be a variable or a constant
        :return: It returns an array of values, ordered by domain_stations
        """
        replacements = []
        # noinspection PyBroadException
        try:
            if isinstance(variable, str):
                # skip the first replacement, its never
                # really a replacement. It is either '' or not a
                # replacement
                replacements = variable.split('*')[1:]
            # pre assign these in case it isn't a replacement - makes it easier
            value = variable
            if len(replacements) > 0:
                station_values = []
                for ri in replacements:
                        value = self.model_data[ri]
                        # convert each station value to iso if necessary
                        if ri.startswith("{ISO}"):
                            value = variable.replace("*" + ri, convert_to_iso(value))
                        else:
                            value = variable.replace("*" + ri, str(value))
            return value
        except Exception as e:
            logging.error(
                "GribBuilder.translate_template_item: Exception  error: " + str(e))

    def handle_document(self):
        """
        This routine processes the complete document matching template items to 
        the self.modelData and self.obsData
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
        the data that correspond to the key into the values
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
                doc[key] = self.translate_template_item(doc[key])
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


    def handle_fcstValidEpochs(self):
        try:
            for fve in self.model_fcstValidEpochs:
                self.obs_data = {}
                self.obs_station_names = []
                # get the models and obs for this fve
                obs_id = re.sub(':' + str(fve['fcstLen']) +"$", '',fve['id'] ) # remove the fcstLen part
                obs_id = re.sub(self.model,'obs',obs_id) # substitute the model part for obs
                self.model_data = self.collection.get(fve['id']).content
                _obs_data = self.collection.get(obs_id).content
                for entry in _obs_data['data']:
                    self.obs_data[entry['name']] = entry
                    self.obs_station_names.append(entry['name'])
                self.obs_station_names.sort()
                self.handle_document()
        except DocumentNotFoundException:
            logging.info(self.__class__.__name__ + " handle_fcstValidEpochs: document " + fve['id'] + " was not found! ")
        except Exception as e:
            logging.error(
                self.__class__.__name__ + " handle_fcstValidEpochs: Exception instantiating builder:  error: " + str(e))

    def build_document(self):
        """
        This is the entry point for the ctcBuilders from the ingestManager.
        These documents are id'd by time and fcstLen. The data section is an array 
        each element of which contains a map keyed by thresholds. The values are the
        hits, misses, false_alarms, adn correct_negatives for the stations in the region
        that is specified in the ingest_document. 
        To process this file we need to itterate the list of valid fcstValidEpochs
        and process the region station list for each fcstValidEpoch and fcstLen.

        1) get stations from couchbase and filter them so that we retain only the ones for this models region
        2) get the latest fcstValidEpoch for the ctc's for this model and region.
        3) get the intersection of the fcstValidEpochs that correspond for this model and the obs 
        for all fcstValidEpochs greater than the first ctc.
        4) if we have asked for profiling go ahead and do it
        5) iterate the fcstValidEpochs an get the models and obs for each fcstValidEpoch
        6) Within the fcstValidEpoch loop iterate the models and handle a document for each 
        fcstValidEpoch. This will result in a document for each fcstLen within a fcstValidEpoch.
        5) and 6) are enclosed in the handle_document()
        """
        # noinspection PyBroadException
        try:
            logging.getLogger().setLevel(logging.INFO)
            # reset the builders document_map for a new file
            self.initialize_document_map()

            # get stations from couchbase and filter them so
            # that we retain only the ones for this models domain which is defined by the region boundingbox
            try:
                # get the bounding box for this region
                result = self.cluster.query(
                    "SELECT  geo.bottom_right.lat as br_lat, geo.bottom_right.lon as br_lon, geo.top_left.lat as tl_lat, geo.top_left.lon as tl_lon FROM mdata  WHERE type='MD' and docType='region' and subset='COMMON' and version='V01' and name=$region", region=self.region, read_only=True)
                boundingbox = list(result)[0]
                self.domain_stations = []
                # get the stations that are within this boundingbox
                result = self.cluster .query(
                    "SELECT mdata.geo.lat, mdata.geo.lon, name from mdata where type='MD' and docType='station' and subset='METAR' and version='V01'", read_only=True)
                for row in result:
                    rlat = row['lat']
                    bb_br_lat = boundingbox['br_lat']
                    bb_tl_lat = boundingbox['tl_lat']
                    rlon = row['lon'] if row['lon'] >= 0 else 180 - row['lon']
                    bb_br_lon = boundingbox['br_lon'] if boundingbox['br_lon'] >= 0 else 180 - \
                        boundingbox['br_lon']
                    bb_tl_lon = boundingbox['tl_lon'] if boundingbox['tl_lon'] >= 0 else 180 - \
                        boundingbox['tl_lon']
                    if rlat >= bb_br_lat and rlat <= bb_tl_lat and rlon >= bb_br_lon and rlon <= bb_tl_lon:
                        self.domain_stations.append(row['name'])
                self.domain_stations.sort()        
            except Exception as e:
                logging.error(self.__class__.__name__ +
                              ": Exception with builder build_document: error: " + str(e))

            # First get the latest fcstValidEpoch for the ctc's for this model and region.
            result = self.cluster.query(
                "SELECT RAW MAX(mdata.fcstValidEpoch) FROM mdata WHERE type='DD' AND docType='CTC' AND subDocType=$subDocType AND model=$model AND region=$region AND version='V01' AND subset='METAR'", model=self.model, region=self.region, subDocType=self.subDocType, read_only=True)
            max_ctc_fcstValidEpochs = 0
            if list(result)[0] is not None:
                max_ctc_fcstValidEpochs = list(result)[0]

            # Second get the intersection of the fcstValidEpochs that correspond for this
            # model and the obs for all fcstValidEpochs greater than the first_epoch ctc.
            # this could be done with implicit join but this seems to be faster when the results are large.
            result = self.cluster.query(
                """SELECT fve.fcstValidEpoch, fve.fcstLen, meta().id
                    FROM mdata fve
                    WHERE fve.type='DD'
                        AND fve.docType='model'
                        AND fve.model=$model
                        AND fve.version='V01'
                        AND fve.subset='METAR'
                        AND fve.fcstValidEpoch > $max_fcst_epoch
                    ORDER BY fve.fcstValidEpoch, fcstLen""",
                        model=self.model, max_fcst_epoch=max_ctc_fcstValidEpochs)
            _tmp_model_fve = list(result)

            result1 = self.cluster.query(
                """SELECT raw obs.fcstValidEpoch
                        FROM mdata obs
                        WHERE obs.type='DD'
                            AND obs.docType='obs'
                            AND obs.version='V01'
                            AND obs.subset='METAR'
                            AND obs.fcstValidEpoch > $max_fcst_epoch
                    ORDER BY obs.fcstValidEpoch""", max_fcst_epoch=max_ctc_fcstValidEpochs)
            _tmp_obs_fve = list(result1)        
            # for row in result:        
            #     _tmp_obs_fve.append(row.fcstValidEpoch)

            for fve in _tmp_model_fve:
                if fve['fcstValidEpoch'] in _tmp_obs_fve:
                    self.model_fcstValidEpochs.append(fve)
            
            # if we have asked for profiling go ahead and do it
            if self.do_profiling:
                with cProfile.Profile() as pr:
                    self.handle_fcstValidEpochs()
                    with open('profiling_stats.txt', 'w') as stream:
                        stats = Stats(pr, stream=stream)
                        stats.strip_dirs()
                        stats.sort_stats('time')
                        stats.dump_stats('profiling_stats.prof')
                        stats.print_stats()
            else:
                self.handle_fcstValidEpochs()
            document_map = self.get_document_map()
            return document_map
        except Exception as e:
            logging.error(self.__class__.__name__ +
                          ": Exception with builder build_document: error: " + str(e))
            return {}

# Concrete builders


class CTCModelObsBuilderV01(CTCBuilder):
    def __init__(self, load_spec, ingest_document, cluster, collection):
        """
        This builder creates a set of V01 ctc documents using the data from associated
        model and obs data for the model and the region defined in the ingest document.
        Each document is indexed by the &handle_time:&handle_fcst_len" where the
        handle_time returns the valid time of a model and the handle_fcst_len returns the
        fcst_len of the model. 
        The minimum valid time that is available to be ingested for the specified model
        and the minimum valid time for the obs that is available to be ingested,
        where both are greater than what already exists in the database,
        will be matched against the prescribed thresholds from the ingest metadata in
        the MD:matsAux:COMMON:V01 metadata document in the thresholdDescriptions map.
        :param load spec used to init the parent
        :param ingest_document: the document from the ingest document
        :param cluster: - a Couchbase cluster object, used for N1QL queries (QueryService)
        :param collection: - essentially a couchbase connection object, used to get documents by id (DataService)
        """
        CTCBuilder.__init__(
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
        return self.document_map

    # named functions
    def handle_data(self, doc):
        # noinspection PyBroadException
        """
        This routine processes the ctc data element. The data elements are all the same and always have the 
        same keys which are thresholds, therefore this routine does not implement handlers.
        :return: The modified document_map
        """
        try:
            data_elem = {}
            # get the thresholds
            if self.thresholds is None:
                result = self.cluster.query("""
                    SELECT RAW mdata.thresholdDescriptions
                    FROM mdata
                    WHERE type="MD"
                        AND docType="matsAux"
                """, read_only=True)
                self.thresholds = list(map(int,list((list(result)[0])['ceiling'].keys())))
            for threshold in self.thresholds:
                hits = 0
                misses = 0
                false_alarms = 0
                correct_negatives = 0
                
                for station in self.model_data['data']:
                    # only count the ones that are in our region
                    if station['name'] not in self.domain_stations:
                        continue
                    if station['name'] not in self.obs_station_names:
                        logging.info(self.__class__.__name__ +
                          "handle_data: model station " + station['name'] + " was not found in the available observations.")
                        continue
                    if station['Ceiling'] is None:
                        continue
                    if station['Ceiling'] < threshold and self.obs_data[station['name']]['Ceiling'] < threshold:
                        hits = hits + 1
                    if station['Ceiling'] < threshold and not self.obs_data[station['name']]['Ceiling'] < threshold:
                        false_alarms = false_alarms + 1
                    if not station['Ceiling'] < threshold and self.obs_data[station['name']]['Ceiling'] < threshold:
                        misses = misses + 1
                    if not station['Ceiling'] < threshold and not self.obs_data[station['name']]['Ceiling'] < threshold:
                        correct_negatives = correct_negatives + 1
                data_elem[threshold] = data_elem[threshold] if threshold in data_elem.keys() else {}
                data_elem[threshold]['hits'] = hits
                data_elem[threshold]['false_alarms'] = false_alarms
                data_elem[threshold]['misses'] = misses
                data_elem[threshold]['correct_negatives'] = correct_negatives

                doc['data'] = data_elem
                return doc
        except Exception as e:
            logging.error(self.__class__.__name__ +
                          "handle_data: Exception :  error: " + str(e))
        return doc

    def handle_time(self, params_dict):
        return self.model_data['fcstValidEpoch']

    def handle_iso_time(self, params_dict):
        return (dt.datetime.utcfromtimestamp(self.model_data['fcstValidEpoch']).isoformat())

    def handle_fcst_len(self, params_dict):
        return self.model_data['fcstLen']

    """
        How CTC tables are derived....
        ARRAY_SUM(ARRAY CASE WHEN (pair.modelValue < 300
                AND pair.observationValue < 300) THEN 1 ELSE 0 END FOR pair IN pairs END) AS hits,
        ARRAY_SUM(ARRAY CASE WHEN (pair.modelValue < 300
                AND NOT pair.observationValue < 300) THEN 1 ELSE 0 END FOR pair IN pairs END) AS false_alarms,
        ARRAY_SUM(ARRAY CASE WHEN (NOT pair.modelValue < 300
                AND pair.observationValue < 300) THEN 1 ELSE 0 END FOR pair IN pairs END) AS misses,
        ARRAY_SUM(ARRAY CASE WHEN (NOT pair.modelValue < 300
                AND NOT pair.observationValue < 300) THEN 1 ELSE 0 END FOR pair IN pairs END) AS correct_negatives
    """
