"""
Program Name: Class netcdf_builder.py
Contact(s): Randy Pierce
History Log:  Initial version
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSL
"""

import calendar
import copy
import cProfile
import logging
import math
import re
import os
from pstats import Stats

import time
import traceback
from datetime import datetime, timedelta

import netCDF4 as nc
import numpy.ma as ma

def truncate_round(n, decimals=0):
    """
    Round a float to a specific number of places in an expected manner
    Args:
        n (int): the number of decimal places to use as a multiplier and divider
        decimals (int, optional): [description]. Defaults to 0.
    Returns:
        float: The number multiplied by n and then divided by n
    """
    multiplier = 10 ** decimals
    return int(n * multiplier) / multiplier

def convert_to_iso(an_epoch):
    """
    convert an epoch to ISO format
    """
    if not isinstance(an_epoch, int):
        an_epoch = int(an_epoch)
    valid_time_str = datetime.utcfromtimestamp(an_epoch).strftime("%Y-%m-%dT%H:%M:%SZ")
    return valid_time_str


def initialize_data(doc):
    """initialize the data by just making sure the template data element has been removed.
    All the data elements are going to be top level elements"""
    if "data" in doc.keys():
        del doc["data"]
    return doc


class NetcdfBuilder:  # pylint disable=too-many-instance-attributes
    """parent class for netcdf builders"""

    def __init__(self, load_spec, ingest_document, cluster, collection):
        self.ingest_document = ingest_document
        self.template = ingest_document["template"]
        self.load_spec = load_spec
        self.cluster = cluster
        self.collection = collection
        self.an_id = None
        self.document_map = {}
        self.ncdf_data_set = None
        self.station_names = []
        self.file_name = None
        # self.do_profiling = True  # set to True to enable build_document profiling
        self.do_profiling = False

    def initialize_document_map(self):  # pylint: disable=missing-function-docstring
        pass

    def load_data(
        self, doc, key, element
    ):  # pylint: disable=missing-function-docstring
        pass

    def get_document_map(self):  # pylint: disable=missing-function-docstring
        pass

    def handle_recNum(
        self, row
    ):  # pylint: disable=missing-function-docstring, disable=invalid-name
        pass

    def build_datafile_doc(
        self, file_name, data_file_id
    ):  # pylint: disable=missing-function-docstring
        pass

    def create_data_file_id(
        self, file_name
    ):  # pylint: disable=missing-function-docstring
        pass

    def derive_id(self, template_id, rec_num):
        """
        This is a private method to derive a document id from the current recNum,
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
                    value = str(self.handle_named_function(part, rec_num))
                else:
                    if part.startswith("*"):
                        value = str(self.translate_template_item(part, rec_num))
                    else:
                        value = str(part)
                new_parts.append(value)
            new_id = ":".join(new_parts)
            return new_id
        except Exception as _e:  # pylint:disable=broad-except
            logging.error("NetcdfBuilder.derive_id: Exception  error: %s", str(_e))
            return None

    def translate_template_item(self, variable, rec_num):
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
                replacements = variable.split("*")[1:]
            if len(replacements) == 0:
                # it is a literal, not a replacement (doesn't start with *)
                return variable
            make_str = False
            value = variable
            Smatch = re.compile(".*S.*")  # pylint:disable=invalid-name
            Umatch = re.compile(".*U.*")  # pylint:disable=invalid-name
            if len(replacements) > 0:
                for _ri in replacements:
                    vtype = str(self.ncdf_data_set.variables[_ri].dtype)
                    if Smatch.match(vtype) or Umatch.match(vtype):
                        make_str = True
                        chartostring = True
                        break
                for _ri in replacements:
                    if _ri.startswith("{ISO}"):
                        variable = value.replace("*{ISO}", "")
                        if chartostring:
                            # for these we have to convert the character array AND convert to ISO (it is probably a string date)
                            value = convert_to_iso(
                                # pylint: disable=maybe-no-member
                                "*{ISO}"
                                + nc.chartostring(self.ncdf_data_set[variable][rec_num])
                            )
                        else:
                            # for these we have to convert convert to ISO (it is probably an epoch)
                            value = convert_to_iso(
                                "*{ISO}" + self.ncdf_data_set[variable][rec_num]
                            )
                    else:
                        variable = value.replace("*", "")
                        if make_str:
                            if chartostring:
                                # it is a char array of something
                                value = value.replace(
                                    # pylint: disable=maybe-no-member
                                    "*" + _ri,
                                    str(
                                        nc.chartostring(
                                            self.ncdf_data_set[variable][rec_num]
                                        )
                                    ),
                                )
                                return value
                            else:
                                # it is probably a number
                                value = str(self.ncdf_data_set[variable][rec_num])
                                return value
                        else:
                            # it desn't need to be a string
                            return self.ncdf_data_set[variable][rec_num]
        except Exception as _e:  # pylint:disable=broad-except
            logging.error(
                "NetcdfBuilder.translate_template_item: Exception  error: %s", str(_e)
            )
        return value

    def handle_document(self):
        """
        :return: The modified document_map
        """
        # noinspection PyBroadException
        try:
            new_document = copy.deepcopy(self.template)
            rec_num_data_size = self.ncdf_data_set.dimensions["recNum"].size
            if rec_num_data_size == 0:
                return
            # make a copy of the template, which will become the new document
            # once all the translations have occured
            new_document = initialize_data(new_document)
            for rec_num in range(rec_num_data_size):
                for key in self.template.keys():
                    if key == "data":
                        new_document = self.handle_data(new_document, rec_num)
                        continue
                    new_document = self.handle_key(new_document, rec_num, key)
            # put document into document map
            if new_document["id"]:
                logging.info(
                    "NetcdfBuilder.handle_document - adding document %s",
                    new_document["id"],
                )
                self.document_map[new_document["id"]] = new_document
            else:
                logging.info(
                    "NetcdfBuilder.handle_document - cannot add document with key %s",
                    str(new_document["id"]),
                )
        except Exception as _e:  # pylint:disable=broad-except
            logging.error(
                "NetcdfBuilder.handle_document: Exception instantiating builder: %s error: %s",
                self.__class__.__name__,
                str(_e),
            )
            raise _e

    def handle_key(self, doc, _rec_num, key):
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
            if key == "id":
                an_id = self.derive_id(self.template["id"], _rec_num)
                if not an_id in doc:
                    doc["id"] = an_id
                return doc
            if isinstance(doc[key], dict):
                # process an embedded dictionary
                tmp_doc = copy.deepcopy(self.template[key])
                for sub_key in tmp_doc.keys():
                    tmp_doc = self.handle_key(tmp_doc, _rec_num, sub_key)  # recursion
                doc[key] = tmp_doc
            if (
                not isinstance(doc[key], dict)
                and isinstance(doc[key], str)
                and doc[key].startswith("&")
            ):
                doc[key] = self.handle_named_function(doc[key], _rec_num)
            else:
                doc[key] = self.translate_template_item(doc[key], _rec_num)
            return doc
        except Exception as _e:  # pylint:disable=broad-except
            logging.error(
                "%s NetcdfBuilder.handle_key: Exception in builder:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
        return doc

    def handle_named_function(self, named_function_def, rec_num):
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
            func = named_function_def.split("|")[0].replace("&", "")
            params = named_function_def.split("|")[1].split(",")
            dict_params = {"recNum": rec_num}
            for _p in params:
                # be sure to slice the * off of the front of the param
                dict_params[_p[1:]] = self.translate_template_item(_p, rec_num)
            # call the named function using getattr
            replace_with = getattr(self, func)(dict_params)
        except Exception as _e:  # pylint:disable=broad-except
            logging.error(
                "%s handle_named_function: Exception instantiating builder:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
        return replace_with

    def handle_data(self, doc, rec_num):
        """This method iterates the template entries, deciding for each entry to either
        handle_named_function (if the entry starts with a '&') or to translate_template_item
        if it starts with an '*'. It handles both keys and values for each template entry.
        Args:
            doc (Object): this is the data document that is being built
        Returns:
            (Object): this is the data document that is being built
        """
        try:
            data_elem = {}
            data_key = next(iter(self.template["data"]))
            data_template = self.template["data"][data_key]
            for key in data_template.keys():
                try:
                    value = data_template[key]
                    # values can be null...
                    if value and value.startswith("&"):
                        value = self.handle_named_function(value, rec_num)
                    else:
                        value = self.translate_template_item(value, rec_num)
                except Exception as _e:  # pylint:disable=broad-except
                    value = None
                    logging.warning(
                        "%s NetcdfBuilder.handle_data - value is None",
                        self.__class__.__name__,
                    )
                data_elem[key] = value
            if data_key.startswith("&"):
                data_key = self.handle_named_function(data_key, rec_num)
            else:
                data_key = self.translate_template_item(data_key, rec_num)
            if data_key is None:
                logging.warning(
                    "%s NetcdfBuilder.handle_data - _data_key is None",
                    self.__class__.__name__,
                )
            # pylint: disable=assignment-from-no-return
            doc = self.load_data(doc, data_key, data_elem)
            return doc
        except Exception as _e:  # pylint:disable=broad-except
            logging.error(
                "%s handle_data: Exception instantiating builder:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
        return doc

    def build_document(self, file_name):
        """This is the entry point for the NetcfBuilders from the ingestManager.
        These documents are id'd by fcstValidEpoch. The data section is an array
        each element of which contains variable data and a station name. To process this
        file we need to itterate the document by recNum and process the station name along
        with all the other variables in the variableList.
        Args:
            file_name (str): the name of the file being processed
        Returns:
            [dict]: document
        """
        # noinspection PyBroadException
        try:
            # stash the file_name so that it can be used later
            self.file_name = os.path.basename(file_name)
            # pylint: disable=no-member
            self.ncdf_data_set = nc.Dataset(file_name)
            if len(self.station_names) == 0:
                result = self.cluster.query(
                    """SELECT raw name FROM mdata
                    WHERE
                    type = 'MD'
                    AND docType = 'station'
                    AND subset = 'METAR'
                    AND version = 'V01';
                """
                )
                self.station_names = list(result)
            self.initialize_document_map()
            logging.info(
                "%s building documents for file %s", self.__class__.__name__, file_name
            )
            if self.do_profiling:
                with cProfile.Profile() as _pr:
                    self.handle_document()
                    with open("profiling_stats.txt", "w") as stream:
                        stats = Stats(_pr, stream=stream)
                        stats.strip_dirs()
                        stats.sort_stats("time")
                        stats.dump_stats("profiling_stats.prof")
                        stats.print_stats()
            else:
                self.handle_document()
            # pylint: disable=assignment-from-no-return
            document_map = self.get_document_map()
            data_file_id = self.create_data_file_id(file_name=file_name)
            data_file_doc = self.build_datafile_doc(
                file_name=file_name,
                data_file_id=data_file_id,
            )
            document_map[data_file_doc["id"]] = data_file_doc
            return document_map
        except Exception as _e:  # pylint:disable=broad-except
            logging.error(
                "%s: Exception with builder build_document: error: %s",
                self.__class__.__name__,
                str(_e),
            )
            return {}


# Concrete builders
class NetcdfMetarObsBuilderV01(NetcdfBuilder): # pylint: disable=too-many-instance-attributes
    """
    This is the builder for observation data that is ingested from netcdf (madis) files
    """

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
        NetcdfBuilder.__init__(self, load_spec, ingest_document, cluster, collection)
        self.cluster = cluster
        self.collection = collection
        self.same_time_rows = []
        self.time = 0
        self.interpolated_time = 0
        self.delta = ingest_document["validTimeDelta"]
        self.cadence = ingest_document["validTimeInterval"]
        self.template = ingest_document["template"]
        self.subset = self.template["subset"]
        # self.do_profiling = True  # set to True to enable build_document profiling
        self.do_profiling = False  # set to True to enable build_document profiling

    def create_data_file_id(self, file_name):
        """
        This method creates a metar netcdf_to_cb datafile id from the parameters
        """
        base_name = os.path.basename(file_name)
        an_id = "DF:" + self.subset + ":obs:netcdf:{n}".format(n=base_name)
        return an_id

    def build_datafile_doc(self, file_name, data_file_id):
        """
        This method will build a dataFile document for GribBuilder. The dataFile
        document will represent the file that is ingested by the GribBuilder. The document
        is intended to be added to the output folder and imported with the other documents.
        The VxIngest will examine the existing dataFile documents to determine if a psecific file
        has already been ingested.
        """
        mtime = os.path.getmtime(file_name)
        df_doc = {
            "id": data_file_id,
            "mtime": mtime,
            "subset": self.subset,
            "type": "DF",
            "fileType": "netcdf",
            "originType": "madis",
            "loadJobId": self.load_spec["load_job_doc"]["id"],
            "dataSourceId": "madis3",
            "url": file_name,
            "projection": "lambert_conformal_conic",
            "interpolation": "nearest 4 weighted average",
        }
        return df_doc

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
        try:
            if len(self.same_time_rows) != 0:
                self.handle_document()
            # convert data map to a list
            # document_map might be None
            if self.document_map and isinstance(self.document_map, dict):
                for _d in self.document_map.values():
                    try:
                        if "data" in _d.keys() and isinstance(_d["data"], dict):
                            data_map = _d["data"]
                            data_list = list(data_map.values())
                            _d["data"] = sorted(
                                data_list, key=lambda data_elem: data_elem["name"]
                            )
                    except Exception as _e1:  # pylint:disable=broad-except
                        logging.error(
                            "%s get_document_map list conversion: Exception processing%s:  error: %s",
                            self.__class__.__name__,
                            str(_d["data"]),
                            str(_e1),
                        )
            return self.document_map
        except Exception as _e:  # pylint:disable=broad-except
            logging.exception(
                "%s get_document_map: Exception in get_document_map: %s",
                self.__class__.__name__,
                str(_e),
            )
            return None

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
        if "data" not in doc.keys() or doc["data"] is None:
            doc["data"] = {}
        if element["name"] not in doc["data"].keys():
            # we only want the closest record (to match the legacy data)
            doc["data"][element["name"]] = element
        else:
            # is this one closer to the target time?
            top_of_hour = doc["fcstValidEpoch"]
            if abs(top_of_hour - element["Reported Time"]) < abs(
                top_of_hour - doc["data"][element["name"]]["Reported Time"]
            ):
                doc["data"][element["name"]] = element
        return doc

    # named functions
    def meterspersecond_to_milesperhour(self, params_dict):
        """Converts meters per second to mile per hour performing any translations that are necessary
        Args:
            params_dict (dict): named function parameters
        Returns:
            [type]: [description]
        """
        # Meters/second to miles/hour
        try:
            value = self.umask_value_transform(params_dict)
            if value is not None and value != "":
                value = value * 2.237
            return value
        except Exception as _e:  # pylint:disable=broad-except
            logging.error(
                "%s handle_data: Exception in named function meterspersecond_to_milesperhour:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
            return None

    def ceiling_transform(self, params_dict): # pylint: disable=too-many-locals
        """retrieves skyCover and skyLayerBase data and transforms it into a Ceiling value
        Args:
            params_dict (dict): named function parameters
        Returns:
            [type]: [description]
        """
        try:
            skyCover = params_dict["skyCover"]  # pylint:disable=invalid-name
            skyLayerBase = params_dict["skyLayerBase"]  # pylint:disable=invalid-name
            # code clear as 60,000 ft
            mCLR = re.compile(".*CLR.*")  # pylint:disable=invalid-name
            mSKC = re.compile(".*SKC.*")  # pylint:disable=invalid-name
            mNSC = re.compile(".*NSC.*")  # pylint:disable=invalid-name
            mFEW = re.compile(".*FEW.*")  # pylint:disable=invalid-name
            mSCT = re.compile(".*SCT.*")  # pylint:disable=invalid-name
            mBKN = re.compile(".*BKN.*")  # Broken pylint:disable=invalid-name
            mOVC = re.compile(".*OVC.*")  # Overcast pylint:disable=invalid-name
            mVV = re.compile(".*VV.*")  # Vertical Visibility pylint:disable=invalid-name
            mask_array = ma.getmaskarray(skyLayerBase)
            skyCover_array = ( # pylint:disable=invalid-name
                skyCover[1:-1].replace("'", "").split(" ")
            )
            # check for unmasked ceiling values - broken, overcast, vertical visibility - return associated skyLayerBase
            # name = str(nc.chartostring(self.ncdf_data_set['stationName'][params_dict['recNum']]))
            for index in range(  # pylint:disable=consider-using-enumerate
                len(skyCover_array)
            ):
                # also convert meters to feet (* 3.281)
                if (not mask_array[index]) and (
                    mBKN.match(skyCover_array[index])
                    or mOVC.match(skyCover_array[index])
                    or mVV.match(skyCover_array[index])
                ):
                    return math.floor( # pylint: disable=c-extension-no-member
                        skyLayerBase[index] * 3.281
                    )  # pylint:disable=c-extension-no-member
            # check for unmasked ceiling values - all the others - CLR, SKC, NSC, FEW, SCT - return 60000
            for index in range( #pylint:disable=consider-using-enumerate
                len(skyCover_array)
            ):  # pylint:disable=consider-using-enumerate
                # 60000 is aldready feet
                if (not mask_array[index]) and (
                    mCLR.match(skyCover_array[index])
                    or mSKC.match(skyCover_array[index])
                    or mNSC.match(skyCover_array[index])
                    or mFEW.match(skyCover_array[index])
                    or mSCT.match(skyCover_array[index])
                ):
                    return 60000
            # nothing was unmasked - return 60000 if there is a ceiling value in skycover array (legacy)
            for index in range( #pylint:disable=consider-using-enumerate
                len(skyCover_array)
            ):  # pylint:disable=consider-using-enumerate
                if (
                    mCLR.match(skyCover_array[index])
                    or mSKC.match(skyCover_array[index])
                    or mNSC.match(skyCover_array[index])
                    or mFEW.match(skyCover_array[index])
                    or mSCT.match(skyCover_array[index])
                ):
                    return 60000
            #  masked and no ceiling value in skyCover_array
            return None
        except Exception as _e:  # pylint:disable=broad-except
            logging.error(
                "%s handle_data: Exception in named function ceiling_transform:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
            logging.error(
                "ceiling_transform skyCover_array: %s skyLayerBase %s",
                str(skyCover_array),
                str(skyLayerBase),
            )
            logging.error(
                "ceiling_transform stacktrace %s", str(traceback.format_exc())
            )
            return None

    def kelvin_to_farenheight(self, params_dict):
        """Converts kelvin to farenheight performing any translations that are necessary
        Args:
            params_dict (dict): named function parameters
        Returns:
            [type]: [description]
        """
        try:
            value = self.umask_value_transform(params_dict)
            if value is not None and value != "":
                value = (float(value) - 273.15) * 1.8 + 32
            return value
        except Exception as _e:  # pylint:disable=broad-except
            logging.error(
                "%s handle_data: Exception in named function kelvin_to_farenheight:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
            return None

    def umask_value_transform(self, params_dict):
        """Retrieves a netcdf value, checking for masking and retrieves the value as a float
        Args:
            params_dict (dict): named function parameters
        Returns:
            float: the corresponding value
        """
        # Probably need more here....
        try:
            key = None
            rec_num = params_dict["recNum"]
            for key in params_dict.keys():
                if key != "recNum":
                    break
            nc_value = self.ncdf_data_set[key][rec_num]
            if not ma.getmask(nc_value):
                value = ma.compressed(nc_value)[0]
                return float(value)
            else:
                return None
        except Exception as _e:  # pylint:disable=broad-except
            logging.error(
                "%s umask_value_transform: Exception in named function umask_value_transform for key %s:  error: %s",
                self.__class__.__name__,
                key,
                str(_e),
            )
            return None

    def handle_pressure(self, params_dict):
        """Retrieves a pressure value and converts it to millibars from pascals
        Args:
            params_dict (dict): named function parameters
        Returns:
            float: the pressure in millibars
        """
        try:
            value = self.umask_value_transform(params_dict)
            if value is not None:
                # convert to millibars (from pascals) and round
                value = float(value) / 100
            return value
        except Exception as _e:  # pylint:disable=broad-except
            logging.error(
                "%s handle_pressure: Exception in named function:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
            return None

    def handle_visibility(self, params_dict):
        """Retrieves a visibility value and performs data transformations
        Args:
            params_dict (dict): named function parameters
        Returns:
            float: the visibility in miles
        """
        # vis_sm = vis_m / 1609.344
        try:
            value = self.umask_value_transform(params_dict)
            if value is not None:
                value = float(value) / 1609.344
            return value
        except Exception as _e:  # pylint:disable=broad-except
            logging.error(
                "%s handle_visibility: Exception in named function:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
            return None

    def derive_valid_time_iso(self, params_dict):
        """
        This routine accepts a pattern parameter like '%Y%m%d_%H%M'
        which it applies against the current file name to derive the
        expected validTime and convert it to an iso
        """
        # convert the file name to an epoch using the mask
        try:
            key = None
            for key in params_dict.keys():
                if key != "recNum":
                    break
            _file_utc_time = datetime.strptime(self.file_name, params_dict[key])
            epoch = (_file_utc_time - datetime(1970, 1, 1)).total_seconds()
            iso = convert_to_iso(epoch)
            return iso
        except Exception as _e:  # pylint:disable=broad-except
            logging.error(
                "%s : Exception in named function derive_valid_time_iso:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
        return None

    def derive_valid_time_epoch(self, params_dict):
        """
        This routine accepts a pattern parameter like '%Y%m%d_%H%M'
        which it applies against the current file name to derive the
        expected validTime and convert it to an epoch
        """
        # convert the file name to an epoch using the mask
        try:
            key = None
            for key in params_dict.keys():
                if key != "recNum":
                    break
            _file_utc_time = datetime.strptime(self.file_name, params_dict[key])
            epoch = (_file_utc_time - datetime(1970, 1, 1)).total_seconds()
            return int(epoch)
        except Exception as _e:  # pylint:disable=broad-except
            logging.error(
                "%s : Exception in named function derive_valid_time_epoch:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
        return None

    def interpolate_time(self, params_dict):
        """
        Rounds to nearest hour by adding a timedelta hour if minute >= delta (from the template)
        """
        try:
            _thistime = None
            _time_obs = params_dict["timeObs"]
            if not ma.getmask(_time_obs):
                _thistime = int(ma.compressed(_time_obs)[0])
            else:
                return None
            # if I get here process the _thistime
            delta_minutes = self.delta / 60
            _ret_time = datetime.utcfromtimestamp(_thistime)
            _ret_time = _ret_time.replace(
                second=0, microsecond=0, minute=0, hour=_ret_time.hour
            ) + timedelta(hours=_ret_time.minute // delta_minutes)
            return calendar.timegm(_ret_time.timetuple())

        except Exception as _e:  # pylint:disable=broad-except
            logging.error(
                "%s handle_data: Exception in named function interpolate_time:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
            return None

    def interpolate_time_iso(self, params_dict):
        """
        Rounds to nearest hour by adding a timedelta hour if minute >= delta_minutes
        """
        try:
            _time = None
            _time = self.interpolate_time(params_dict)
            _time = datetime.utcfromtimestamp(_time)
            # convert this iso
            if _time is None:
                return None
            return str(_time.isoformat())
        except Exception as _e:  # pylint:disable=broad-except
            logging.error(
                "%s handle_data: Exception in named function interpolate_time_iso:  error: %s",
                self.__class__.__name__,
                str(_e),
            )

    def fill_from_netcdf(self, rec_num, netcdf):
        """
        Used by handle_station to get the records from netcdf for comparing with the
        records from the database.
        """
        netcdf = {}
        if not ma.getmask(self.ncdf_data_set["latitude"][rec_num]):
            netcdf["latitude"] = ma.compressed(self.ncdf_data_set["latitude"][rec_num])[
                0
            ]
        else:
            netcdf["latitude"] = None
        if not ma.getmask(self.ncdf_data_set["longitude"][rec_num]):
            netcdf["longitude"] = ma.compressed(
                self.ncdf_data_set["longitude"][rec_num]
            )[0]
        else:
            netcdf["longitude"] = None
        if not ma.getmask(self.ncdf_data_set["elevation"][rec_num]):
            netcdf["elevation"] = ma.compressed(
                self.ncdf_data_set["elevation"][rec_num]
            )[0]
        else:
            netcdf["elevation"] = None
        # pylint: disable=no-member
        netcdf["description"] = str(
            nc.chartostring(self.ncdf_data_set["locationName"][rec_num])
        )
        netcdf["name"] = str(
            nc.chartostring(self.ncdf_data_set["stationName"][rec_num])
        )
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
        rec_num = params_dict["recNum"]
        station_name = params_dict["stationName"]
        an_id = None
        netcdf = {}

        # noinspection PyBroadException
        try:
            if station_name not in self.station_names:
                # get the netcdf fields for comparing or adding new
                netcdf = self.fill_from_netcdf(rec_num, netcdf)
                logging.info(
                    "netcdfObsBuilderV01.handle_station - adding station %s",
                    netcdf["name"],
                )
                an_id = "MD:V01:METAR:station:" + netcdf["name"]
                new_station = {
                    "id": an_id,
                    "description": netcdf["description"],
                    "docType": "station",
                    "firstTime": 0,
                    "geo": {
                        "elev": truncate_round(float(netcdf["elevation"]), 5),
                        "lat": truncate_round(float(netcdf["latitude"]), 5),
                        "lon": truncate_round(float(netcdf["longitude"]), 5),
                    },
                    "lastTime": 0,
                    "name": netcdf["name"],
                    "subset": "METAR",
                    "type": "MD",
                    "updateTime": int(time.time()),
                    "version": "V01",
                }
                # add the station to the document map
                if not an_id in self.document_map.keys():
                    self.document_map[an_id] = new_station
                self.station_names.append(station_name)
            return params_dict["stationName"]
        except Exception as _e:  # pylint:disable=broad-except
            logging.error(
                "%s netcdfObsBuilderV01.handle_station: Exception finding or creating station to match station_name error: %s params: %s",
                self.__class__.__name__,
                str(_e),
                str(params_dict),
            )
            return ""

class NetcdfMetarLegacyObsBuilderV01(NetcdfMetarObsBuilderV01):# pylint: disable=too-many-instance-attributes
    """
    This is the builder for observation data that is ingested from netcdf (madis) files
    with special regard for how data is loaded. The special data loading concerns the
    ceiling value which might or might not come from the closest data record for a given station,
    which is how the legacy ingest does it.
    """

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
        NetcdfMetarObsBuilderV01.__init__(self, load_spec, ingest_document, cluster, collection)
        self.cluster = cluster
        self.collection = collection
        self.same_time_rows = []
        self.time = 0
        self.interpolated_time = 0
        self.delta = ingest_document["validTimeDelta"]
        self.cadence = ingest_document["validTimeInterval"]
        self.template = ingest_document["template"]
        self.subset = self.template["subset"]
        # We want the subset to be derived from the template because the ids will reflect
        # the subset and the subset will be set in the template i.e. "metar-legacy".
        # self.do_profiling = True  # set to True to enable build_document profiling
        self.do_profiling = False  # set to True to enable build_document profiling

    # override load_data to choose closest data record and the closest ceiling value
    # (which might have a different recorded time than the data element)
    def load_data(self, doc, key, element):
        """
        This method appends an observation to the data array -
        in fact we use a dict to hold data elems to ensure
        the data elements are unique per station name, the map is converted
        back to a list in get_document_map. Using a map ensures that the last
        entry in the netcdf file is the one that gets captured.
        The data element with the recorded time that is closest to the fcstValidEpoch
        is the one that is saved, and that recorded time is saved in the data element
        as well. The Ceiling value might not come from the data element with the recorded
        time that is closest to the fcstValidEpoch, it might come from a different data element
        that is not closest to the fcstValidEpoch, if the closest one has a None ceiling value.
        That is how the legacy ingest worked.
        :param doc: The document being created
        :param key: Not used
        :param element: the observation data
        :return: the document being created
        """
        if "data" not in doc.keys() or doc["data"] is None:
            doc["data"] = {}
        # we only want the closest record (to match the legacy data)
        # but we need a valid ceiling value if one exists that isn't in the closest record
        if element["name"] not in doc["data"].keys():
            element["Ceiling Reported Time"] = int(element["Reported Time"])
            doc["data"][element["name"]] = element
        else:
            # One already exists in the doc for this station,
            # Determine the ceiling value to use
            # if the existing ceiling value is None
            #   then the ceiling value is the new element ceiling value
            # else
            #     if the new element "Reported Time" is closer to the fcstValidEpoch
            #            AND the new element ceiling value is not None
            #        use the new element ceiling value and update the "Ceiling Reported Time"
            #     else keep the existing ceiling value and "Ceiling Reported Time"
            # NOTE: the new data element doesn't have a "Ceiling Reported Time" since
            # there is no such variable in the netcdf file.
            # We generate it here from a new element "Reported Time" and stuff it into the document for later comparison against
            # a new element["Reported Time"].
            top_of_hour = doc["fcstValidEpoch"]
            if doc["data"][element["name"]]["Ceiling"] is None:
                # the existing one is None so use the new one
                ceiling_value = element['Ceiling']
                ceiling_reported_time = int(element["Reported Time"])
            else:
                # existing is not None so we have to compare
                if element["Ceiling"] is not None and abs(
                    top_of_hour - element["Reported Time"]) < abs(
                        top_of_hour - doc["data"][element["name"]]["Ceiling Reported Time"]):
                    # new element ceiling is closer than the existing one so use the new one
                    ceiling_value = element["Ceiling"]
                    ceiling_reported_time = int(element["Reported Time"])
                else:
                    # existing ceiling value is closer than the new one so use the existing one
                    ceiling_value = doc["data"][element["name"]]["Ceiling"]
                    ceiling_reported_time = doc["data"][element["name"]]["Ceiling Reported Time"]
            # is this data element closer to the fcstValidEpoch?
            if abs(top_of_hour - element["Reported Time"]) < abs(
                top_of_hour - doc["data"][element["name"]]["Reported Time"]
            ):
                # the new element is closer to fcstValisEpoch than the existing one so replace the existing one
                doc["data"][element["name"]] = element
            # update the ceiling values regardless of if the data element is replaced
            # we might be keeping the existing data element and updating the Ceiling value
            # or vice versa
            doc["data"][element["name"]]["Ceiling"] = ceiling_value
            doc["data"][element["name"]]["Ceiling Reported Time"] = ceiling_reported_time
        return doc
