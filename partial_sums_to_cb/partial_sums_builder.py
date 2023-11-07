"""
Program Name: Class partialsums_builder.py
Contact(s): Randy Pierce
History Log:  Initial version
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSL
"""

import copy
import cProfile
import logging
import datetime as dt
import re
import time
from pstats import Stats

from couchbase.exceptions import DocumentNotFoundException, TimeoutException
from couchbase.search import GeoBoundingBoxQuery, SearchOptions
from builder_common.builder_utilities import convert_to_iso
from builder_common.builder_utilities import get_geo_index
from builder_common.builder_utilities import initialize_data_array
from builder_common.builder import Builder


class PartialSumsBuilder(Builder):  # pylint:disable=too-many-instance-attributes
    """
    Parent class for PARTIALSUMS builders
    1) find all the stations for the region for this ingest (model and region)
    select raw geo from `{self.bucket}`.{self.scope}.{self.collection} where type="MD" and docType="station" and subset='METAR' and version='V01'
    Use the region metadata document....
    SELECT
        geo.bottom_right.lat as br_lat,
        geo.bottom_right.lon as br_lon,
        geo.top_left.lat as tl_lat,
        geo.top_left.lon as tl_lon
    FROM `{self.bucket}`.{self.scope}.{self.collection}
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
    FROM `{self.bucket}`.{self.scope}.{self.collection}
    WHERE type="MD"
    AND docType="station"
    AND subset='METAR'
    AND version='V01'
    AND geo.lat BETWEEN 21.7692 and 52.3516
    AND geo.lon BETWEEN -126.5495 AND -61.7802

    This can be done with a join but it it is probably better to do it with two queries and code.
    This is the join.
    SELECT RAW s.name
    FROM `{self.bucket}`.{self.scope}.{self.collection} s
        JOIN `{self.bucket}`.{self.scope}.{self.collection} bb ON s.geo.lat BETWEEN bb.geo.bottom_right.lat AND bb.geo.top_left.lat
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
    2) find the minimum of the maximum valid times of corresponding PARTIALSUMS documents currently in the database.
    select raw min (METAR.fcstValidEpoch) from `{self.bucket}`.{self.scope}.{self.collection} where type="DD" and docType="obs" and subset='METAR' and version='V01'  limit 10
    3) find the maximum of the minimum valid times of the obs and corresponding models that are currently in the database. This is
    the data for ALL the stations. What delineates a region is the subset of station names that are in a region. This corresponds to a
    subset of the data portion of each model.
    select raw min (METAR.fcstValidEpoch) from `{self.bucket}`.{self.scope}.{self.collection} where type="DD" and docType="model" and subset='METAR' and version='V01' and model="HRRR_OPS"
    select raw min (METAR.fcstValidEpoch) from `{self.bucket}`.{self.scope}.{self.collection} where type="DD" and docType="obs" and subset='METAR' and version='V01'  limit 10
    4) using the minimum valid time and the domain station list query for model and obs pairs within the station list.
    5) iterate that batch of data by valid time and fcstLen creating corresponding PARTIALSUMS documents.
    """

    def __init__(self, load_spec, ingest_document):
        # PARTIALSUMS builders do not init the ingest_document. That happens in build_document
        super().__init__(load_spec, ingest_document)

        self.load_spec = load_spec
        self.domain_stations = []
        # PARTIALSUMS builder specific
        # These following need to be declared here but assigned in
        # build_document because each ingest_id might redifne them
        self.ingest_document = None
        self.template = None
        self.subset = None
        self.model = None
        self.region = None
        self.sub_doc_type = None
        self.variable = None
        self.model_fcst_valid_epochs = []
        self.model_data = (
            {}
        )  # used to stash each fcstValidEpoch model_data for the handlers
        self.obs_data = (
            {}
        )  # used to stash each fcstValidEpoch obs_data for the handlers
        self.obs_station_names = []  # used to stash sorted obs names for the handlers
        self.thresholds = None
        self.not_found_stations = set()
        self.not_found_station_count = 0
        self.bucket = None
        self.scope = None
        self.collection = None

    def derive_id(self, **kwargs):
        """
        This is a private method to derive a document id from the current station,
        substituting *values from the corresponding grib fields as necessary. A *field
        represents a direct substitution and a &function|params...
        represents a handler function. There is a kwargs because different builders may
        require a different argument list.
        Args:
            template_id (string): this is an id template string
        Returns:
            [string]: The processed id with substitutions made for elements in the id template
        """
        try:
            template_id = kwargs["template_id"]
            parts = template_id.split(":")
            new_parts = []
            for part in parts:
                if part.startswith("&"):
                    value = str(self.handle_named_function(part))
                else:
                    if part.startswith("*"):
                        value = str(self.translate_template_item(part))
                    else:
                        value = str(part)
                new_parts.append(value)
            new_id = ":".join(new_parts)
            return new_id
        except Exception as _e:  # pylint:disable=broad-except
            logging.exception("PARTIALSUMSBuilder.derive_id")
            return None

    def translate_template_item(self, variable):
        """This method translates template replacements (*item or *item1*item2).
        It can translate keys or values.
        Args:
            variable ([object]): a value from the template - should be a variable or a constant
        Returns:
            [string]: It returns a value
        """
        replacements = []
        try:
            if isinstance(variable, str):
                replacements = variable.split("*")[1:]
            # this is a literal, doesn't need to be returned
            if len(replacements) == 0:
                return variable
            # pre assign these in case it isn't a replacement - makes it easier
            value = variable
            if len(replacements) > 0:
                for _ri in replacements:
                    value = self.model_data[_ri]
                    # convert each station value to iso if necessary
                    if _ri.startswith("{ISO}"):
                        value = variable.replace("*" + _ri, convert_to_iso(value))
                    else:
                        value = variable.replace("*" + _ri, str(value))
            return value
        except Exception as _e:  # pylint: disable=broad-except
            logging.error(
                "PartialSumsBuilder.translate_template_item: Exception  error: %s", str(_e)
            )
            return None

    def handle_document(self):
        """
        This routine processes the complete document matching template items to
        the self.modelData and self.obsData
        :return: The modified document_map
        """
        # noinspection PyBroadException
        try:
            new_document = copy.deepcopy(self.template)
            if self.domain_stations is None:
                return
            station_data_size = len(self.domain_stations)
            if station_data_size == 0:
                return
            # make a copy of the template, which will become the new document
            # once all the translations have occured
            new_document = initialize_data_array(new_document)
            for key in self.template.keys():
                if key == "data":
                    # pylint: disable=assignment-from-no-return
                    new_document = self.handle_data(doc=new_document)
                    continue
                new_document = self.handle_key(new_document, key)
            # put document into document map
            if new_document["id"]:
                logging.info(
                    "PARTIALSUMSBuilder.handle_document - adding document %s",
                    new_document["id"],
                )
                self.document_map[new_document["id"]] = new_document
            else:
                logging.info(
                    "PartialSumsBuilder.handle_document - cannot add document with key %s",
                    str(new_document["id"]),
                )
        except Exception as _e:  # pylint: disable=broad-except
            logging.error(
                "%s PartialSumsBuilder.handle_document: Exception instantiating builder:  error %s",
                self.__class__.__name__,
                str(_e),
            )
            raise _e

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
            if key == "id":
                an_id = self.derive_id(template_id=self.template["id"])
                if not an_id in doc:
                    doc["id"] = an_id
                return doc
            if isinstance(doc[key], dict):
                # process an embedded dictionary
                tmp_doc = copy.deepcopy(self.template[key])
                for sub_key in tmp_doc.keys():
                    tmp_doc = self.handle_key(tmp_doc, sub_key)  # recursion
                doc[key] = tmp_doc
            if (
                not isinstance(doc[key], dict)
                and isinstance(doc[key], str)
                and doc[key].startswith("&")
            ):
                doc[key] = self.handle_named_function(doc[key])
            else:
                doc[key] = self.translate_template_item(doc[key])
            return doc
        except Exception as _e:  # pylint:disable=broad-except
            logging.exception(
                "%s PartialSumsBuilder.handle_key: Exception in builder",
                self.__class__.__name__,
            )
        return doc

    def handle_named_function(self, named_function_def):
        """
        This method processes a named function entry from a template.
        Args:
        _named_function_def ([string]): this can be either a template key or a template value.
        The _named_function_def looks like "&named_function|*field1,*field2,*field3..."
        where named_function is the literal function name of a defined function.
        The name of the function and the function parameters are seperated by a "|" and
        the parameters are seperated by a ','.
        It is expected that field1, field2, and field3 etc are all valid variable names.
        Each field will be translated from the data into value1, value2 etc.
        The method "named_function" will be called like...
        named_function({field1:value1, field2:value2, ... fieldn:valuen}) and the return value from named_function
        will be substituted into the document.
        station ([string]) the station name being processed.
        Returns:
            [string]: processed template item
        """
        func = None
        try:
            parts = named_function_def.split("|")
            func = parts[0].replace("&", "")
            params = []
            if len(parts) > 1:
                params = parts[1].split(",")
            dict_params = {}
            for _p in params:
                # be sure to slice the * off of the front of the param
                # translate_template_item returns an array of tuples - value,interp_value, one for each station
                # ordered by domain_stations.
                if _p[0] == "&" or _p[0] == "*":
                    dict_params[_p[1:]] = self.translate_template_item(_p)
                else:
                    dict_params[_p] = self.translate_template_item(_p)
            # call the named function using getattr
            replace_with = getattr(self, func)(dict_params)
        except Exception as _e:  # pylint:disable=broad-except
            logging.exception(
                "%s handle_named_function: %s params %s: Exception instantiating builder:",
                self.__class__.__name__,
                func,
                params,
            )
        return replace_with

    def handle_fcstValidEpochs(self):  # pylint: disable=invalid-name
        """iterate through all the fcstValidEpochs for which we have both model data and observation data.
        For each entry in the data section, i.e for each station build a data element that
        has model and observation data, then handle the document.
        """
        try:  # pylint: disable=too-many-nested-blocks
            _obs_data = {}
            for fve in self.model_fcst_valid_epochs:
                try:
                    self.obs_data = {}
                    self.obs_station_names = []
                    try:
                        # get_stations_for_region_by_geosearch is broken for geo losts untill late 2022
                        # full_station_name_list = self.get_stations_for_region_by_geosearch(self.region, fve)
                        full_station_name_list = self.get_stations_for_region_by_sort(
                            self.region, fve["fcstValidEpoch"]
                        )
                        self.domain_stations = full_station_name_list
                    except Exception as _e:  # pylint: disable=broad-except
                        logging.error(
                            "%s: Exception with builder build_document: error: %s",
                            self.__class__.__name__,
                            str(_e),
                        )

                    # get the models and obs for this fve
                    # remove the fcstLen part
                    obs_id = re.sub(":" + str(fve["fcstLen"]) + "$", "", fve["id"])
                    # substitute the model part for obs
                    obs_id = re.sub(self.model, "obs", obs_id)
                    logging.info("Looking up model document: %s", fve["id"])
                    try:
                        _model_doc = self.load_spec["collection"].get(fve["id"])
                        self.model_data = _model_doc.content_as[dict]
                        if not self.model_data["data"]:
                            logging.info(
                                "%s handle_fcstValidEpochs: model document %s has no data! ",
                                self.__class__.__name__,
                                fve["id"],
                            )
                            continue
                    except DocumentNotFoundException:
                        logging.info(
                            "%s handle_fcstValidEpochs: model document %s was not found! ",
                            self.__class__.__name__,
                            fve["id"],
                        )
                    except Exception as _e:  # pylint: disable=broad-except
                        logging.error(
                            "%s Error getting model document: %s",
                            self.__class__.__name__,
                            str(_e),
                        )

                    logging.info("Looking up observation document: %s", obs_id)
                    try:
                        # I don't really know how I can get here with _obs_data AND
                        # _obs_data['id'] != obs_id and still no self.obs_data
                        # but it does happen and it results in documents
                        # that have 0 hits, misses, false_alarms etc
                        # It might be from duplicate ids but it must be handled
                        # so  "or not self.obs_data"
                        if (
                            not _obs_data
                            or (_obs_data["id"] != obs_id)
                            or not self.obs_data
                        ):
                            _obs_doc = self.load_spec["collection"].get(obs_id)
                            _obs_data = _obs_doc.content_as[dict]
                            if not _obs_data["data"]:
                                logging.info(
                                    "%s handle_fcstValidEpochs: obs document %s has no data! ",
                                    self.__class__.__name__,
                                    obs_id,
                                )
                                continue
                            for key in _obs_data["data"].keys():
                                self.obs_data[key] = _obs_data["data"][key]
                                self.obs_station_names.append(key)
                            self.obs_station_names.sort()
                        self.handle_document()
                    except DocumentNotFoundException:
                        logging.info(
                            "%s handle_fcstValidEpochs: obs document %s was not found! ",
                            self.__class__.__name__,
                            fve["id"],
                        )
                except Exception as _e:  # pylint: disable=broad-except
                    logging.exception(
                        "%s problem getting obs document: %s",
                        self.__class__.__name__,
                        str(_e),
                    )

        except Exception as _e:  # pylint: disable=broad-except
            logging.error(
                "%s handle_fcstValidEpochs: Exception instantiating builder:  error: %s",
                self.__class__.__name__,
                str(_e),
            )

    def build_document(self, queue_element):
        """
        This is the entry point for the partialsumsBuilders from the ingestManager.
        These documents are id'd by model, region, fcstValidEpoch and fcstLen. The data section is a map
        each element of which contains a sum value keyed by a variable name. The values are the
        {
	        “num_recs”: # matched pairs,
 	        “sum_obs”: sum of all the observation values for each matched pair,
            “sum_model”: sum of all the model values for each matched pair,
            “sum_diff”: sum of all variables differences, obs-model,
            “sum2_diff”: sum of all variable differences squared, obs-model,
        }

        1) get stations from couchbase and filter them so that we retain only the ones for this models region
        2) get the latest fcstValidEpoch for the partialsums's for this model and region.
        3) get the intersection of the fcstValidEpochs that correspond for this model and the obs
        for all fcstValidEpochs greater than the first partialsums.
        4) if we have asked for profiling go ahead and do it
        5) iterate the fcstValidEpochs an get the models and obs for each fcstValidEpoch
        6) Within the fcstValidEpoch loop iterate the model fcstLen's and handle a document for each
        fcstValidEpoch and fcstLen. This will result in a document for each fcstLen within a fcstValidEpoch.
        5) and 6) are enclosed in the handle_document()
        """
        # noinspection PyBroadException
        try:
            logging.getLogger().setLevel(logging.INFO)
            # reset the builders document_map for a new file
            self.initialize_document_map()
            self.not_found_station_count = 0
            # PARTIALSUMS builder specific
            self.domain_stations = []
            # queue_element is an ingest document id
            # get the ingest document

            self.ingest_document = self.load_spec["ingest_documents"][queue_element]
            self.model = self.ingest_document["model"]
            self.region = self.ingest_document["region"]
            self.sub_doc_type = self.ingest_document["subDocType"]
            self.variable = self.ingest_document["subDocType"].lower()
            self.subset = self.ingest_document["subset"]
            self.template = self.ingest_document["template"]
            self.bucket = self.load_spec["cb_connection"]["bucket"]
            self.scope = self.load_spec["cb_connection"]["scope"]
            self.collection = self.load_spec["cb_connection"]["collection"]
            logging.info(
                "%s.build_document queue_element:%s model:%s region:%s variable:%s subset:%s",
                self.__class__.__name__,
                queue_element,
                self.model,
                self.region,
                self.variable,
                self.subset,
            )

            # First get the latest fcstValidEpoch for the partialsums's for this model and region.
            stmnt = ""
            error_count = 0
            success = False
            while error_count < 3 and success is False:
                try:
                    stmnt = f"""SELECT RAW MAX(METAR.fcstValidEpoch)
                            FROM `{self.bucket}`.{self.scope}.{self.collection}
                            WHERE type='DD'
                            AND docType='PARTIALSUMS'
                            AND subDocType='{self.sub_doc_type}'
                            AND model='{self.model}'
                            AND region='{self.region}'
                            AND version='V01'
                            AND subset='{self.subset}'"""
                    # logging.info("build_document start query %s", stmnt)
                    result = self.load_spec["cluster"].query(stmnt, read_only=True)
                    success = True
                    # logging.info("build_document finished query %s", stmnt)
                except TimeoutException:
                    logging.info(
                        "%s.build_document TimeoutException retrying %s: %s",
                        self.__class__.__name__,
                        error_count,
                        stmnt,
                    )
                    if error_count > 2:
                        raise
                    time.sleep(2)  # don't hammer the server too hard
                    error_count = error_count + 1
            # initial value for the max epoch
            max_partialsums_fcst_valid_epochs = self.load_spec["first_last_params"]["first_epoch"]
            max_partialsums_fcst_valid_epochs_result = list(result)
            # if there are partialsums's for this model and region then get the max epoch from the query
            max_partialsums_fcst_valid_epochs = max_partialsums_fcst_valid_epochs_result[0] if max_partialsums_fcst_valid_epochs_result[0] is not None  else 0

            # Second get the intersection of the fcstValidEpochs that correspond for this
            # model and the obs for all fcstValidEpochs greater than the first_epoch partialsums
            # and less than the last_epoch.
            # this could be done with implicit join but this seems to be faster when the results are large.
            # get the model fcstValidEpochs (models don't have regions) that are > the last partialsums epoch
            error_count = 0
            success = False
            while error_count < 3 and success is False:
                try:
                    stmnt = f"""SELECT fve.fcstValidEpoch, fve.fcstLen, meta().id
                            FROM `{self.bucket}`.{self.scope}.{self.collection} fve
                            WHERE fve.type='DD'
                                AND fve.docType='model'
                                AND fve.model='{self.model}'
                                AND fve.version='V01'
                                AND fve.subset='{self.subset}'
                                AND fve.fcstValidEpoch >= {self.load_spec["first_last_params"]["first_epoch"]}
                                AND fve.fcstValidEpoch >= {max_partialsums_fcst_valid_epochs}
                                AND fve.fcstValidEpoch <= {self.load_spec["first_last_params"]["last_epoch"]}
                            ORDER BY fve.fcstValidEpoch, fve.fcstLen"""
                    # logging.info("build_document start query %s", stmnt)
                    result = self.load_spec["cluster"].query(stmnt,read_only=True)
                    success = True
                    # logging.info("build_document finished query %s", stmnt)
                except TimeoutException:
                    logging.info(
                        "%s.build_document TimeoutException retrying %s: %s",
                        self.__class__.__name__,
                        error_count,
                        stmnt,
                    )
                    if error_count > 2:
                        raise
                    time.sleep(2)  # don't hammer the server too hard
                    error_count = error_count + 1
            _tmp_model_fve = list(result)

            # get the obs fcstValidEpochs (obs don't have regions) that are > the last partialsums epoch
            error_count = 0
            success = False
            while error_count < 3 and success is False:
                try:
                    stmnt = f"""SELECT raw obs.fcstValidEpoch
                                FROM `{self.bucket}`.{self.scope}.{self.collection} obs
                                WHERE obs.type='DD'
                                    AND obs.docType='obs'
                                    AND obs.version='V01'
                                    AND obs.subset='{self.subset}'
                                    AND obs.fcstValidEpoch >= {max_partialsums_fcst_valid_epochs}
                                    AND obs.fcstValidEpoch <= {self.load_spec["first_last_params"]["last_epoch"]}
                            ORDER BY obs.fcstValidEpoch"""
                    # logging.info("build_document start query %s", stmnt)
                    result1 = self.load_spec["cluster"].query(stmnt, read_only=True)
                    success = True
                    # logging.info("build_document finished query %s", stmnt)
                except TimeoutException:
                    logging.info(
                        "%s.build_document TimeoutException retrying %s: %s",
                        self.__class__.__name__,
                        error_count,
                        stmnt,
                    )
                    if error_count > 2:
                        raise
                    time.sleep(2)  # don't hammer the server too hard
                    error_count = error_count + 1
            _tmp_obs_fve = list(result1)

            # this will give us a list of {fcstValidEpoch:fve, fcslLen:fl, id:an_id}
            # where we know that each entry has a corresponding valid observation
            for fve in _tmp_model_fve:
                if fve["fcstValidEpoch"] in _tmp_obs_fve:
                    self.model_fcst_valid_epochs.append(fve)

            # if we have asked for profiling go ahead and do it
            # pylint: disable=no-member
            if self.do_profiling:
                with cProfile.Profile() as _pr:
                    # process the fcstValidEpochs with profiling
                    self.handle_fcstValidEpochs()
                    with open("profiling_stats.txt", "w", encoding="utf-8") as stream:
                        stats = Stats(_pr, stream=stream)
                        stats.strip_dirs()
                        stats.sort_stats("time")
                        stats.dump_stats("profiling_stats.prof")
                        stats.print_stats()
            else:
                # process the fcstValidEpochs without profiling
                self.handle_fcstValidEpochs()
            # pylint: disable=assignment-from-no-return
            logging.info(
                "There were %s stations not found", self.not_found_station_count
            )
            document_map = self.get_document_map()
            return document_map
        except Exception as _e:  # pylint: disable=broad-except
            logging.error(
                "%s: Exception with builder build_document: error: %s for element %s",
                self.__class__.__name__,
                str(_e),
                queue_element,
            )
            return {}

    def get_stations_for_region_by_geosearch(
        self, region_name, valid_epoch
    ):  # pylint: disable=unused-argument
        # NOTE: this is currently broken because we have to modify this query to
        # work woth the data model that has data elements as a MAP indexed by station name
        """Using a geosearh return all the stations within the defined region
        Args:
            region_name (string): the name of the region.
        Returns:
            list: the list of stations within this region
        """
        try:
            stmnt = f"""SELECT
                    geo.bottom_right.lat as br_lat,
                    geo.bottom_right.lon as br_lon,
                    geo.top_left.lat as tl_lat,
                    geo.top_left.lon as tl_lon
                    FROM `{self.bucket}`.{self.scope}.{self.collection}
                    WHERE type='MD'
                    and docType='region'
                    and subset='COMMON'
                    and version='V01'
                    and name='{region_name}'"""
            result = self.load_spec["cluster"].query(stmnt, read_only=True)
            _boundingbox = list(result)[0]
            _domain_stations = []
            _result1 = self.load_spec["cluster"].search_query(
                "station_geo",
                GeoBoundingBoxQuery(
                    top_left=(_boundingbox["tl_lon"], _boundingbox["tl_lat"]),
                    bottom_right=(_boundingbox["br_lon"], _boundingbox["br_lat"]),
                    field="geo",
                ),
                SearchOptions(fields=["name"], limit=10000),
            )
            for elem in list(_result1):
                _domain_stations.append(elem.fields["name"])
            _domain_stations.sort()
            return _domain_stations
        except Exception as _e:  # pylint: disable=broad-except
            logging.error(
                "%s: Exception with builder: error: %s",
                self.__class__.__name__,
                str(_e),
            )
            return []

    def get_legacy_stations_for_region(self, region_name):
        """Using the corresponding legacy list from a document return all the stations within the defined region
        NOTE: this has nothing to do with "_LEGACY" subset obs or PARTIALSUMS's.
        Args:
            region_name (string): the name of the region.
        Returns:
            list: the list of stations within this region
        """
        try:
            classic_station_id = "MD-TEST:V01:CLASSIC_STATIONS:" + region_name
            doc = self.load_spec["collection"].get(classic_station_id.strip())
            classic_stations = doc.content_as[dict]["stations"]
            classic_stations.sort()
            return classic_stations
        except Exception as _e:  # pylint: disable=broad-except
            logging.error(
                "%s: Exception with builder: error: %s",
                self.__class__.__name__,
                str(_e),
            )
            return []

    def get_stations_for_region_by_sort(self, region_name, valid_epoch):
        """Using a lat/lon filter return all the stations within the defined region
        Args:
            region_name (string): the name of the region.
        Returns:
            list: the list of stations within this region
        """
        # get the bounding box for this region
        try:
            stmnt = f"""SELECT  geo.bottom_right.lat as br_lat,
                    geo.bottom_right.lon as br_lon,
                    geo.top_left.lat as tl_lat,
                    geo.top_left.lon as tl_lon
                    FROM `{self.bucket}`.{self.scope}.{self.collection}
                    WHERE type='MD'
                    and docType='region'
                    and subset='COMMON'
                    and version='V01'
                    and name='{region_name}'"""
            result = self.load_spec["cluster"].query(stmnt, read_only=True)
            _boundingbox = list(result)[0]
            _domain_stations = []
            # get the stations that are within this boundingbox
            stmnt = f"""SELECT
                    geo, name
                    from `{self.bucket}`.{self.scope}.{self.collection}
                    where type='MD'
                    and docType='station'
                    and subset='{self.subset}'
                    and version='V01'"""
            result = self.load_spec["cluster"].query(stmnt, read_only=True)
            for row in result:
                geo_index = get_geo_index(valid_epoch, row["geo"])
                rlat = row["geo"][geo_index]["lat"]
                bb_br_lat = _boundingbox["br_lat"]
                bb_tl_lat = _boundingbox["tl_lat"]

                rlon = (
                    row["geo"][geo_index]["lon"]
                    if row["geo"][geo_index]["lon"] <= 180
                    else row["geo"][geo_index]["lon"] - 360
                )
                bb_br_lon = (
                    _boundingbox["br_lon"]
                    if _boundingbox["br_lon"] <= 180
                    else _boundingbox["br_lon"] - 360
                )
                bb_tl_lon = (
                    _boundingbox["tl_lon"]
                    if _boundingbox["tl_lon"] <= 180
                    else _boundingbox["tl_lon"] - 360
                )
                if (
                    rlat >= bb_br_lat
                    and rlat <= bb_tl_lat
                    and rlon >= bb_tl_lon
                    and rlon <= bb_br_lon
                ):
                    _domain_stations.append(row["name"])
                else:
                    continue
            _domain_stations.sort()
            return _domain_stations
        except Exception as _e:  # pylint: disable=broad-except
            logging.error(
                "%s: Exception with builder: error: %s",
                self.__class__.__name__,
                str(_e),
            )
            return None


# Concrete builders
class PartialSumsSurfaceModelObsBuilderV01(PartialSumsBuilder):
    """This builder creates a set of V01 partialsums documents using the data from associated
        model and obs data for the model and the region defined in the ingest document.
        Each document is indexed by the &handle_time:&handle_fcst_len" where the
        handle_time returns the valid time of a model and the handle_fcst_len returns the
        fcst_len of the model.
        The minimum valid time that is available to be ingested for the specified model
        and the minimum valid time for the obs that is available to be ingested,
        where both are greater than what already exists in the database,
        will be matched against the prescribed thresholds from the ingest metadata in
        the MD:matsAux:COMMON:V01 metadata document in the thresholdDescriptions map.
    Args:
        PartialSumsSurfaceModelObsBuilderV01 (Class): parent class PartialSumsBuilder
    """

    def __init__(self, load_spec, ingest_document):
        """This builder creates a set of V01 partialsums documents using the data from associated
        model and obs data for the model and the region defined in the ingest document.
        Each document is indexed by the &handle_time:&handle_fcst_len" where the
        handle_time returns the valid time of a model and the handle_fcst_len returns the
        fcst_len of the model.
        The minimum valid time that is available to be ingested for the specified model
        and the minimum valid time for the obs that is available to be ingested,
        where both are greater than what already exists in the database,
        will be matched against the prescribed thresholds from the ingest metadata in
        the MD:matsAux:COMMON:V01 metadata document in the thresholdDescriptions map.
        Args:
            load_spec (dict): used to init the parent
            ingest_document (dict): the document from the ingest document
            cluster (Cluster): couchbase cluster object (used for queries)
            collection ([type]): couchbase collection object (used for data fetch operations)
        """
        PartialSumsBuilder.__init__(self, load_spec, ingest_document)
        self.template = ingest_document["template"]
        self.ingest_document = None
        self.template = None
        self.subset = None
        self.model = None
        self.region = None
        self.sub_doc_type = None
        self.variable = None

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

    def handle_sum(self, params_dict):
        """ calculate sums for a given data set - i.e. model, region, fcstValidEpoch, fcstLen"""
        try:
            keys = list(params_dict.keys())
            variable = keys[0]
            obs_vals = []
            model_vals = []
            diff_vals = []
            diff_vals_squared = []
            for name in self.domain_stations:
                if name in self.obs_data and name in self.model_data["data"]:
                    obs_elem = self.obs_data[name]
                    model_elem = self.model_data["data"][name]
                    if variable in obs_elem and variable in model_elem:
                        if obs_elem[variable] is not None and model_elem[variable] is not  None:
                            obs_vals.append(obs_elem[variable])
                            model_vals.append(model_elem[variable])
                            diff = obs_elem[variable] - model_elem[variable]
                            diff_vals.append(diff)
                            diff_vals_squared.append(diff * diff)
            sum_elem = {
                "num_recs": len(obs_vals),
                "sum_obs": sum(obs_vals),
                "sum_model": sum(model_vals),
                "sum_diff": sum(diff_vals),
                "sum2_diff": diff_vals_squared,
            }
            return sum_elem
        except Exception as _e:  # pylint:disable=broad-except
            logging.error(
                "%s handle_sum: Exception :  error: %s",
                self.__class__.__name__,
                str(_e),
            )
            return None

    def handle_data(self, **kwargs):  # pylint:disable=too-many-branches
        """
        This routine processes the partialsums data element. The data elements are
        variables for which we will derive a set of sums. The sums are
        derived from the model and observation data for the specific time, model, fcstLen
        and region. A region might be a vegitation type, or a lat/lon bounding box.
        :return: The modified document_map data section that looks like ...
         "data": {
            Temperature: "
            {
	            “num_recs”: # matched pairs,
 	            “sum_obs”: sum of all the observation values for each matched pair,
                “sum_model”: sum of all the model values for each matched pair,
                “sum_diff”: sum of all variables differences, obs-model,
                “sum2_diff”: sum of all variable differences squared, obs-model,
            },
            WindSpeed: {...},
            WindU: {...},
            WindV: {...},
            DewPoint: {...},
            RelativeHumidity: {...},
            SurfacePressure: {...}
        }
        """
        try:  # pylint: disable=too-many-nested-blocks
            doc = kwargs["doc"]
            template_data = self.template["data"]
            data_elem = {}
            # it is expected that the template data section be comprised of named functions
            for variable in template_data.keys():
                data_elem[variable] = self.handle_named_function(template_data[variable])
            doc["data"] = data_elem
            return doc
        except Exception as _e:  # pylint: disable=broad-except
            logging.error(
                "%s handle_data: Exception :  error: %s",
                self.__class__.__name__,
                str(_e),
            )
        return doc

    def handle_time(self, params_dict):  # pylint: disable=unused-argument
        """return the fcstValidTime for the current model in epoch
        Args:
            params_dict (dict): contains named_function parameters
        Returns:
            int: epoch
        """
        return self.model_data["fcstValidEpoch"]

    def handle_iso_time(self, params_dict):  # pylint: disable=unused-argument
        """return the fcstValidTime for the current model in ISO
        Args:
            params_dict (dict): contains named_function parameters
        Returns:
            string: ISO time string
        """
        return dt.datetime.utcfromtimestamp(
            self.model_data["fcstValidEpoch"]
        ).isoformat()

    def handle_fcst_len(self, params_dict):  # pylint: disable=unused-argument
        """returns the fcst lead time in hours for this document
        Args:
            params_dict (dict): contains named_function parameters
        Returns:
            int: a fcst lead time in hours
        """
        return self.model_data["fcstLen"]

    def handleWindDirU(self, params_dict):  # pylint: disable=unused-argument, invalid-name
        """returns the wind direction U component for this document
        Args:
            params_dict (dict): contains named_function parameters
        Returns:
            float: wind direction U component
        """
        return self.model_data["windDirU"]

    def handleWindDirV(self, params_dict):  # pylint: disable=unused-argument, invalid-name
        """returns the wind direction V component for this document
        Args:
            params_dict (dict): contains named_function parameters
        Returns:
            float: wind direction V component
        """
        return self.model_data["windDirV"]

    def handle_specific_humidity(self, params_dict):  # pylint: disable=unused-argument
        """returns the specific humidity for this document
        Args:
            params_dict (dict): contains named_function parameters
        Returns:
            float: specific humidity
        """
        return self.model_data["specificHumidity"]
