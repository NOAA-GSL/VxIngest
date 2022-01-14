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
import re
import sys
from pstats import Stats
from couchbase.search import GeoBoundingBoxQuery, SearchOptions
from couchbase.exceptions import DocumentNotFoundException

TS_OUT_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def convert_to_iso(an_epoch):
    """
    simple conversion of an epoch to an iso string
    """
    if not isinstance(an_epoch, int):
        an_epoch = int(an_epoch)
    valid_time_str = dt.datetime.utcfromtimestamp(an_epoch).strftime(TS_OUT_FORMAT)
    return valid_time_str


def initialize_data(doc):
    """initialize the data by just making sure the template data element has been removed.
    All the data elements are going to be top level elements"""
    if "data" in doc.keys():
        del doc["data"]
    return doc


class CTCBuilder:  # pylint:disable=too-many-instance-attributes
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
        self.template = ingest_document["template"]
        self.load_spec = load_spec
        self.cluster = cluster
        self.collection = collection
        self.an_id = None
        self.document_map = {}
        self.domain_stations = []
        self.model = ingest_document["model"]
        self.region = ingest_document["region"]
        self.subset = ingest_document["subset"]
        self.sub_doc_type = self.template["subDocType"]
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

    def initialize_document_map(self):  # pylint: disable=missing-function-docstring
        pass

    def get_document_map(self):  # pylint: disable=missing-function-docstring
        pass

    def handle_data(self, doc):  # pylint: disable=missing-function-docstring
        pass

    def derive_id(self, template_id):
        """
        This is a private method to derive a document id from the current station,
        substituting *values from the corresponding grib fields as necessary. A *field
        represents a direct substitution and a &function|params...
        represents a handler function.
        Args:
            template_id (string): this is an id template string
        Returns:
            [string]: The processed id with substitutions made for elements in the id template
        """
        try:
            parts = template_id.split(":")
            new_parts = []
            for part in parts:
                if part.startswith("&"):
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
        except Exception as _e:  # pylint: disable=broad-except
            _e = sys.exc_info()
            logging.error("GribBuilder.derive_id: Exception  error: %s", str(_e))
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
                "GribBuilder.translate_template_item: Exception  error: %s", str(_e)
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
            station_data_size = len(self.domain_stations)
            if station_data_size == 0:
                return
            # make a copy of the template, which will become the new document
            # once all the translations have occured
            new_document = initialize_data(new_document)
            for key in self.template.keys():
                if key == "data":
                    # pylint: disable=assignment-from-no-return
                    new_document = self.handle_data(new_document)
                    continue
                new_document = self.handle_key(new_document, key)
            # put document into document map
            if new_document["id"]:
                logging.info(
                    "GribBuilder.handle_document - adding document %s",
                    new_document["id"],
                )
                self.document_map[new_document["id"]] = new_document
            else:
                logging.info(
                    "GribBuilder.handle_document - cannot add document with key %s",
                    str(new_document["id"]),
                )
        except Exception as _e:  # pylint: disable=broad-except
            logging.error(
                "%s GribBuilder.handle_document: Exception instantiating builder:  error %s",
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
                an_id = self.derive_id(self.template["id"])
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
        except Exception as _e:  # pylint: disable=broad-except
            logging.error(
                "%s GribBuilder.handle_key: Exception in builder:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
        return doc

    def handle_named_function(self, _named_function_def):
        """
        This method processes a named function entry from a template.
        Args:
        _named_function_def ([string]): this can be either a template key or a template value.
        The _named_function_def looks like "&named_function|*field1,*field2,*field3..."
        where named_function is the literal function name of a defined function.
        The name of the function and the function parameters are seperated by a ":" and
        the parameters are seperated by a ','.
        It is expected that field1, field2, and field3 etc are all valid variable names.
        Each field will be translated from the netcdf file into value1, value2 etc.
        The method "named_function" will be called like...
        named_function({field1:value1, field2:value2, ... fieldn:valuen}) and the return value from named_function
        will be substituted into the document.
        station ([string]) the station name being processed.
        Returns:
            [string]: processed template item
        """
        func = None
        try:
            parts = _named_function_def.split("|")
            func = parts[0].replace("&", "")
            params = []
            if len(parts) > 1:
                params = parts[1].split(",")
            dict_params = {}
            for _p in params:
                # be sure to slice the * off of the front of the param
                # translate_template_item returns an array of tuples - value,interp_value, one for each station
                # ordered by domain_stations.
                dict_params[_p[1:]] = self.translate_template_item(_p)
            # call the named function using getattr
            replace_with = getattr(self, func)(dict_params)
        except Exception as _e:  # pylint: disable=broad-except
            logging.error(
                "%s handle_named_function: %s Exception instantiating builder:  error: %s",
                self.__class__.__name__,
                func,
                str(_e),
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
                    # get the models and obs for this fve
                    # remove the fcstLen part
                    obs_id = re.sub(":" + str(fve["fcstLen"]) + "$", "", fve["id"])
                    # substitute the model part for obs
                    obs_id = re.sub(self.model, "obs", obs_id)
                    logging.info("Looking up model document: %s", fve["id"])
                    try:
                        _model_doc = self.collection.get(fve["id"])
                        self.model_data = _model_doc.content
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
                            _obs_doc = self.collection.get(obs_id)
                            _obs_data = _obs_doc.content
                            for entry in _obs_data["data"]:
                                self.obs_data[entry["name"]] = entry
                                self.obs_station_names.append(entry["name"])
                            self.obs_station_names.sort()
                        self.handle_document()
                    except Exception as _e:  # pylint: disable=broad-except
                        logging.error(
                            "%s Error getting obs document: %s",
                            self.__class__.__name__,
                            str(_e),
                        )

                except DocumentNotFoundException:
                    logging.info(
                        "%s handle_fcstValidEpochs: document %s was not found! ",
                        self.__class__.__name__,
                        fve["id"],
                    )
        except Exception as _e:  # pylint: disable=broad-except
            logging.error(
                "%s handle_fcstValidEpochs: Exception instantiating builder:  error: %s",
                self.__class__.__name__,
                str(_e),
            )

    def build_document(self):
        """
        This is the entry point for the ctcBuilders from the ingestManager.
        These documents are id'd by fcstValidEpoch and fcstLen. The data section is an array
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
            try:
                full_station_name_list = self.get_stations_for_region_by_geosearch(self.region)
                self.domain_stations = full_station_name_list
            except Exception as _e:  # pylint: disable=broad-except
                logging.error(
                    "%s: Exception with builder build_document: error: %s",
                    self.__class__.__name__,
                    str(_e),
                )

            # First get the latest fcstValidEpoch for the ctc's for this model and region.
            result = self.cluster.query(
                """SELECT RAW MAX(mdata.fcstValidEpoch)
                    FROM mdata
                    WHERE type='DD'
                    AND docType='CTC'
                    AND subDocType=$subDocType
                    AND model=$model
                    AND region=$region
                    AND version='V01'
                    AND subset=$subset""",
                model=self.model,
                region=self.region,
                subDocType=self.sub_doc_type,
                subset=self.subset,
                read_only=True
            )
            max_ctc_fcst_valid_epochs = self.load_spec["first_last_params"][
                "first_epoch"
            ]
            if list(result)[0] is not None:
                max_ctc_fcst_valid_epochs = list(result)[0]

            # Second get the intersection of the fcstValidEpochs that correspond for this
            # model and the obs for all fcstValidEpochs greater than the first_epoch ctc
            # and less than the last_epoch.
            # this could be done with implicit join but this seems to be faster when the results are large.
            result = self.cluster.query(
                """SELECT fve.fcstValidEpoch, fve.fcstLen, meta().id
                    FROM mdata fve
                    WHERE fve.type='DD'
                        AND fve.docType='model'
                        AND fve.model='{model}'
                        AND fve.version='V01'
                        AND fve.subset='{subset}'
                        AND fve.fcstValidEpoch >= {first_epoch}
                        AND fve.fcstValidEpoch <= {last_epoch}
                    ORDER BY fve.fcstValidEpoch, fcstLen""".format(
                        model=self.model,
                        subset=self.subset,
                        first_epoch=self.load_spec["first_last_params"]["first_epoch"],
                        last_epoch=self.load_spec["first_last_params"]["last_epoch"]),
              read_only=True
            )
            _tmp_model_fve = list(result)
            result1 = self.cluster.query(
                """SELECT raw obs.fcstValidEpoch
                        FROM mdata obs
                        WHERE obs.type='DD'
                            AND obs.docType='obs'
                            AND obs.version='V01'
                            AND obs.subset='{subset}'
                            AND obs.fcstValidEpoch >= {max_fcst_epoch}
                            AND obs.fcstValidEpoch <= {last_epoch}
                    ORDER BY obs.fcstValidEpoch""".format(
                max_fcst_epoch=max_ctc_fcst_valid_epochs,
                last_epoch=self.load_spec["first_last_params"]["last_epoch"],
                subset=self.subset ),
                read_only=True
            )
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
                    self.handle_fcstValidEpochs()
                    with open("profiling_stats.txt", "w") as stream:
                        stats = Stats(_pr, stream=stream)
                        stats.strip_dirs()
                        stats.sort_stats("time")
                        stats.dump_stats("profiling_stats.prof")
                        stats.print_stats()
            else:
                self.handle_fcstValidEpochs()
            # pylint: disable=assignment-from-no-return
            logging.info(
                "There were %s stations not found", self.not_found_station_count
            )
            document_map = self.get_document_map()
            return document_map
        except Exception as _e:  # pylint: disable=broad-except
            logging.error(
                "%s: Exception with builder build_document: error: %s",
                self.__class__.__name__,
                str(_e),
            )
            return {}

    def get_stations_for_region_by_geosearch(self, region_name):
        """Using a geosearh return all the stations within the defined region
        Args:
            region_name (string): the name of the region.
        Returns:
            list: the list of stations within this region
        """
        try:
            result = self.cluster.query(
                """SELECT
                    geo.bottom_right.lat as br_lat,
                    geo.bottom_right.lon as br_lon,
                    geo.top_left.lat as tl_lat,
                    geo.top_left.lon as tl_lon
                    FROM mdata
                    WHERE type='MD'
                    and docType='region'
                    and subset='COMMON'
                    and version='V01'
                    and name=$region""",
                    region=region_name,
                    read_only=True,
            )
            _boundingbox = list(result)[0]
            _domain_stations = []
            _result1 = self.cluster.search_query(
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
        NOTE: this has nothing to do with "-LEGACY" subset obs or CTC's.
        Args:
            region_name (string): the name of the region.
        Returns:
            list: the list of stations within this region
        """
        try:
            classic_station_id = "MD-TEST:V01:CLASSIC_STATIONS:" + region_name
            doc = self.collection.get(classic_station_id.strip())
            classic_stations = doc.content["stations"]
            classic_stations.sort()
            return classic_stations
        except Exception as _e:  # pylint: disable=broad-except
            logging.error(
                "%s: Exception with builder: error: %s",
                self.__class__.__name__,
                str(_e),
            )
            return []

    def get_stations_for_region_by_sort(self, region_name):
        """Using a lat/lon filter return all the stations within the defined region
        Args:
            region_name (string): the name of the region.
        Returns:
            list: the list of stations within this region
        """
        # get the bounding box for this region
        try:
            result = self.cluster.query(
                """SELECT  geo.bottom_right.lat as br_lat,
                    geo.bottom_right.lon as br_lon,
                    geo.top_left.lat as tl_lat,
                    geo.top_left.lon as tl_lon
                    FROM mdata
                    WHERE type='MD'
                    and docType='region'
                    and subset='COMMON'
                    and version='V01'
                    and name=$region""",
                    region=region_name,
                    read_only=True,
            )
            _boundingbox = list(result)[0]
            _domain_stations = []
            # get the stations that are within this boundingbox - this metadata is always subset METAR
            result = self.cluster.query(
                """SELECT
                    mdata.geo.lat,
                    mdata.geo.lon,
                    name
                    from mdata
                    where type='MD'
                    and docType='station'
                    and subset='METAR'
                    and version='V01'""",
                read_only=True,
            )
            for row in result:
                rlat = row["lat"]
                bb_br_lat = _boundingbox["br_lat"]
                bb_tl_lat = _boundingbox["tl_lat"]

                rlon = row["lon"] if row["lon"] <= 180 else row["lon"] - 360
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
class CTCModelObsBuilderV01(CTCBuilder):
    """This builder creates a set of V01 ctc documents using the data from associated
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
        CTCBuilder (Class): parent class CTCBuilder
    """

    def __init__(self, load_spec, ingest_document, cluster, collection):
        """This builder creates a set of V01 ctc documents using the data from associated
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
        CTCBuilder.__init__(self, load_spec, ingest_document, cluster, collection)
        self.cluster = cluster
        self.collection = collection
        self.template = ingest_document["template"]
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
    def handle_data(self, doc):  # pylint:disable=too-many-branches
        """
        This routine processes the ctc data element. The data elements are all the same and always have the
        same keys which are thresholds, therefore this class does not implement handlers.
        :return: The modified document_map
        """
        try:  # pylint: disable=too-many-nested-blocks
            data_elem = {}
            # get the thresholds
            if self.thresholds is None:
                result = self.cluster.query(
                    """
                    SELECT RAW mdata.thresholdDescriptions
                    FROM mdata
                    WHERE type="MD"
                        AND docType="matsAux"
                """,
                    read_only=True,
                )
                self.thresholds = list(
                    map(int, list((list(result)[0])["ceiling"].keys()))
                )
            for threshold in self.thresholds:
                hits = 0
                misses = 0
                false_alarms = 0
                correct_negatives = 0
                none_count = 0
                for model_station in self.model_data["data"]:
                    try:
                        # only count the ones that are in our region
                        if model_station["name"] not in self.domain_stations:
                            continue
                        if model_station["name"] not in self.obs_station_names:
                            self.not_found_station_count = (
                                self.not_found_station_count + 1
                            )
                            if model_station["name"] not in self.not_found_stations:
                                logging.info(
                                    "%s handle_data: model station %s was not found in the available observations.",
                                    self.__class__.__name__,
                                    model_station["name"],
                                )
                                self.not_found_stations.add(model_station["name"])
                            continue
                        if (
                            model_station["Ceiling"] is None
                            or self.obs_data[model_station["name"]]["Ceiling"] is None
                        ):
                            none_count = none_count + 1
                            continue
                        if (
                            model_station["Ceiling"] < threshold
                            and self.obs_data[model_station["name"]]["Ceiling"]
                            < threshold
                        ):
                            hits = hits + 1
                        if (
                            model_station["Ceiling"] < threshold
                            and not self.obs_data[model_station["name"]]["Ceiling"]
                            < threshold
                        ):
                            false_alarms = false_alarms + 1
                        if (
                            not model_station["Ceiling"] < threshold
                            and self.obs_data[model_station["name"]]["Ceiling"]
                            < threshold
                        ):
                            misses = misses + 1
                        if (
                            not model_station["Ceiling"] < threshold
                            and not self.obs_data[model_station["name"]]["Ceiling"]
                            < threshold
                        ):
                            correct_negatives = correct_negatives + 1
                    except Exception as _e:  # pylint: disable=broad-except
                        logging.info("unexpected exception:%s", str(_e))
                data_elem[threshold] = (
                    data_elem[threshold] if threshold in data_elem.keys() else {}
                )
                data_elem[threshold]["hits"] = hits
                data_elem[threshold]["false_alarms"] = false_alarms
                data_elem[threshold]["misses"] = misses
                data_elem[threshold]["correct_negatives"] = correct_negatives
                data_elem[threshold]["none_count"] = none_count
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

    def handle_iso_time(self, params_dict):
        """return the fcstValidTime for the current model in ISO
        Args:
            params_dict (dict): contains named_function parameters
        Returns:
            string: ISO time string
        """
        return dt.datetime.utcfromtimestamp(
            self.model_data["fcstValidEpoch"]
        ).isoformat()

    def handle_fcst_len(self, params_dict):
        """returns the fcst lead time in hours for this document
        Args:
            params_dict (dict): contains named_function parameters
        Returns:
            int: a fcst lead time in hours
        """
        return self.model_data["fcstLen"]

        # How CTC tables are derived....
        # ARRAY_SUM(ARRAY CASE WHEN (pair.modelValue < 300
        #         AND pair.observationValue < 300) THEN 1 ELSE 0 END FOR pair IN pairs END) AS hits,
        # ARRAY_SUM(ARRAY CASE WHEN (pair.modelValue < 300
        #         AND NOT pair.observationValue < 300) THEN 1 ELSE 0 END FOR pair IN pairs END) AS false_alarms,
        # ARRAY_SUM(ARRAY CASE WHEN (NOT pair.modelValue < 300
        #         AND pair.observationValue < 300) THEN 1 ELSE 0 END FOR pair IN pairs END) AS misses,
        # ARRAY_SUM(ARRAY CASE WHEN (NOT pair.modelValue < 300
        #         AND NOT pair.observationValue < 300) THEN 1 ELSE 0 END FOR pair IN pairs END) AS correct_negatives

class CTCModelObsBuilderLegacyV01(CTCModelObsBuilderV01):
    """This builder creates a set of V01 ctc documents using the data from associated
        model and obs data for the model and the region defined in the ingest document.
        Each document is indexed by the &handle_time:&handle_fcst_len" where the
        handle_time returns the valid time of a model and the handle_fcst_len returns the
        fcst_len of the model.
        The minimum valid time that is available to be ingested for the specified model
        and the minimum valid time for the obs that is available to be ingested,
        where both are greater than what already exists in the database,
        will be matched against the prescribed thresholds from the ingest metadata in
        the MD:matsAux:COMMON:V01 metadata document in the thresholdDescriptions map.
        This class extends the CTCModelObsBuilderV01 by allowing for regions that have
        problematic stations deleted and also use the METAR-LEGACY obs.
        The problematic stations are stations that get projected to
        grid points differently than the legacy projection, making them get different model values
        from the legacy sql ingest. The "METAR-LEGACY" obs are ones that use the closest ceiling value
        to the fcstValidEpoch that is not None even if the reported time is not the same fcstValidTime.
        So, a "METAR-LEGACY" subset CTC
        1) is derived from "METAR-LEGACY" subset obs that have the closest non-None ceiling values
        regardless of the fcstValidEpoch that corresponds to the ceiling value reported time.
        and
        2) is derived from a domain (region) that has a rejected station list stripped out of the domain
        in order to accomodate a projection discrepancy for a limited set of stations.
        3) use the legacy station list which is an explicit list from a document that has been manually derived
        in order to exactly match the station list that the legacy SQL code uses.
    Args:
        CTCModelObsBuilderV01 (Class): parent class CTCModelObsBuilderV01
    """

    def handle_legacy_model(self, params_dict):  # pylint: disable=unused-argument
        """return the model name with -legacy attached for the current model in epoch
        Args:
            params_dict (dict): contains named_function parameters
        Returns:
            string: model_name + "_LEGACY"
        """
        return self.model + "_LEGACY"

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
                    # get the models and obs for this fve
                    # remove the fcstLen part
                    obs_id = re.sub(":" + str(fve["fcstLen"]) + "$", "", fve["id"])
                    # substitute the model part for obs (if it is a LEGACY model for CTC's remove the -LEGACY part)
                    # LEGACY CTC's have to do with the CTC regions, the model data is all the same
                    # there are no special LEGACY model values - only LEGACY regions that have a rejected
                    # station list removed from the region because of projection problems related to the
                    # stations in the LEGACY rejected station list - and LEGACY obs.
                    # To differentiate regular CTC documents from LEGACY CTC documents we use a model name
                    # FOR THE CTC's ONLY that has the "_LEGACY" appended. To retieve the actual model data
                    # or related elements, or ids we need to strip the "_LEGACY" off.
                    model_str = self.model.replace('_LEGACY','')
                    obs_id = re.sub(model_str, "obs", obs_id)
                    logging.info("Looking up model document: %s", fve["id"])
                    try:
                        _model_doc = self.collection.get(fve["id"])
                        self.model_data = _model_doc.content
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
                            _obs_doc = self.collection.get(obs_id)
                            _obs_data = _obs_doc.content
                            for entry in _obs_data["data"]:
                                self.obs_data[entry["name"]] = entry
                                self.obs_station_names.append(entry["name"])
                            self.obs_station_names.sort()
                        self.handle_document()
                    except Exception as _e:  # pylint: disable=broad-except
                        logging.error(
                            "%s Error getting obs document: %s",
                            self.__class__.__name__,
                            str(_e),
                        )

                except DocumentNotFoundException:
                    logging.info(
                        "%s handle_fcstValidEpochs: document %s was not found! ",
                        self.__class__.__name__,
                        fve["id"],
                    )
        except Exception as _e:  # pylint: disable=broad-except
            logging.error(
                "%s handle_fcstValidEpochs: Exception instantiating builder:  error: %s",
                self.__class__.__name__,
                str(_e),
            )

    def build_document(self):
        """
        This is the entry point for the ctcBuilders from the ingestManager.
        These documents are id'd by fcstValidEpoch and fcstLen. The data section is an array
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
        6) Within the fcstValidEpoch loop iterate the model fcstLen's and handle a document for each
        fcstValidEpoch and fcstLen. This will result in a document for each fcstLen within a fcstValidEpoch.
        5) and 6) are enclosed in the handle_document()

        So, a "METAR-LEGACY" subset CTC
        1) is derived from "METAR-LEGACY" subset obs that have the closest non-None ceiling values
        regardless of the fcstValidEpoch that corresponds to the ceiling value reported time.
        and
        2) is derived from a domain (region) that has a rejected station list stripped out of the domain
        in order to accomodate a projection discrepancy for a limited set of stations.
        3) use the legacy station list which is an explicit list from a document that has been manually derived
        in order to exactly match the station list that the legacy SQL code uses.
        """
        # noinspection PyBroadException
        try:
            logging.getLogger().setLevel(logging.INFO)
            # reset the builders document_map for a new file
            self.initialize_document_map()
            self.not_found_station_count = 0
            try:
                full_station_name_list = self.get_legacy_stations_for_region(self.region)
                # get the reject station list - legacy station list has to have rejected stations removed.
                results = self.collection.get("MD-TEST:V01:LEGACY_REJECTED_STATIONS").content
                rejected_stations = results['stations']
                rejected_station_names = [s['name'] for s in rejected_stations]
                # prune out the rejected stations
                self.domain_stations = [s for s in full_station_name_list if s not in rejected_station_names]
            except Exception as _e:  # pylint: disable=broad-except
                logging.error(
                    "%s: Exception with builder build_document: error: %s",
                    self.__class__.__name__,
                    str(_e),
                )

            # First get the latest fcstValidEpoch for the ctc's for this model and region.
            # LEGACY CTC's should have model values and subset values (and therefore ids) that have "-LEGACY"
            # appended to the subset, and "_LEGACY" appended to the model. This happens when the "LEGACY" CTC is built.
            result = self.cluster.query(
                """SELECT RAW MAX(mdata.fcstValidEpoch)
                    FROM mdata
                    WHERE type='DD'
                    AND docType='CTC'
                    AND subDocType=$subDocType
                    AND model=$model
                    AND region=$region
                    AND version='V01'
                    AND subset=$subset""",
                model=self.model,
                region=self.region,
                subDocType=self.sub_doc_type,
                subset=self.subset,
                read_only=True
            )
            max_ctc_fcst_valid_epochs = self.load_spec["first_last_params"][
                "first_epoch"
            ]
            if list(result)[0] is not None:
                max_ctc_fcst_valid_epochs = list(result)[0]

            # Second get the intersection of the fcstValidEpochs that correspond for this
            # model and the obs for all fcstValidEpochs greater than the first_epoch
            # and less than the last_epoch for the model and obs that support the ctc documents.
            # The model data are not the model name from the ingest document. There are no model data
            # that ends in "_LEGACY" or has a subset ending with "-LEGACY". LEGACY CTC's get those when the CTC is built
            # and they only serve the purpose to differentiate them from regular CTC's.
            # so the "_LEGACY" and -LEGACY" must be stripped to get the underlying model name.
            # This could be done with implicit join but this way seems to be faster when the results are large.
            strp_model=self.model.replace('_LEGACY','') # always remove the LEGACY part - CB models are not LEGACY - only CTC's
            strp_subset=self.subset.replace('-LEGACY','') # always remove the RETRO part - CB models are not LEGACY - only CTC's
            strp_model=strp_model.replace('_RETRO','') # always remove the LEGACY part - CB models are not LEGACY - only CTC's
            strp_subset=strp_subset.replace('-RETRO','') # always remove the RETRO part - CB models are not LEGACY - only CTC's

            result = self.cluster.query(
                """SELECT fve.fcstValidEpoch, fve.fcstLen, meta().id
                    FROM mdata fve
                    WHERE fve.type='DD'
                        AND fve.docType='model'
                        AND fve.model='{model}'
                        AND fve.version='V01'
                        AND fve.subset='{subset}'
                        AND fve.fcstValidEpoch >= {first_epoch}
                        AND fve.fcstValidEpoch <= {last_epoch}
                    ORDER BY fve.fcstValidEpoch, fcstLen""".format(
                model=strp_model, # always remove the LEGACY part - CB models are not LEGACY - only CTC's
                subset=strp_subset, # always remove the RETRO part - CB models are not LEGACY - only CTC's
                first_epoch=self.load_spec["first_last_params"]["first_epoch"],
                last_epoch=self.load_spec["first_last_params"]["last_epoch"]),
                read_only=True
            )
            _tmp_model_fve = list(result)

            # METAR-LEGACY observations should have a special subset (METAR-LEGACY), we want that.
            result1 = self.cluster.query(
                """SELECT raw obs.fcstValidEpoch
                        FROM mdata obs
                        WHERE obs.type='DD'
                            AND obs.docType='obs'
                            AND obs.version='V01'
                            AND obs.subset='{subset}'
                            AND obs.fcstValidEpoch >= {max_fcst_epoch}
                            AND obs.fcstValidEpoch <= {last_epoch}
                    ORDER BY obs.fcstValidEpoch""".format(
                max_fcst_epoch=max_ctc_fcst_valid_epochs,
                last_epoch=self.load_spec["first_last_params"]["last_epoch"],
                subset=self.subset ),
                read_only=True
            )
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
                    self.handle_fcstValidEpochs()
                    with open("profiling_stats.txt", "w") as stream:
                        stats = Stats(_pr, stream=stream)
                        stats.strip_dirs()
                        stats.sort_stats("time")
                        stats.dump_stats("profiling_stats.prof")
                        stats.print_stats()
            else:
                self.handle_fcstValidEpochs()
            # pylint: disable=assignment-from-no-return
            logging.info(
                "There were %s stations not found", self.not_found_station_count
            )
            document_map = self.get_document_map()
            return document_map
        except Exception as _e:  # pylint: disable=broad-except
            logging.error(
                "%s: Exception with builder build_document: error: %s",
                self.__class__.__name__,
                str(_e),
            )
            return {}
