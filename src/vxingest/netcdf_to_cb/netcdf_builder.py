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
import datetime as dt
import logging
import math
import os
import re
import time
import traceback
from pstats import Stats

import netCDF4 as nc
import numpy.ma as ma
from metpy.calc import relative_humidity_from_dewpoint, wind_components
from metpy.units import units
from vxingest.builder_common.builder import Builder
from vxingest.builder_common.builder_utilities import (
    convert_to_iso,
    initialize_data_array,
    truncate_round,
)

# Get a logger with this module's name to help with debugging
logger = logging.getLogger(__name__)


class NetcdfBuilder(Builder):  # pylint disable=too-many-instance-attributes
    """parent class for netcdf builders"""

    def __init__(self, load_spec, ingest_document):
        super().__init__(load_spec, ingest_document)

        self.ingest_document = ingest_document
        self.template = ingest_document["template"]
        self.subset = self.template["subset"]
        self.load_spec = load_spec
        # NetcdfBuilder specific
        self.ncdf_data_set = None
        self.stations = []
        self.file_name = None

        # self.do_profiling = False  - in super
        # set to True to enable build_document profiling

    def derive_id(self, **kwargs):
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
            template_id = kwargs["template_id"]
            rec_num = kwargs["rec_num"]
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
            logger.exception("NetcdfBuilder.derive_id: Exception  error: %s")
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
            logger.exception(
                "Builder.translate_template_item for variable %s: replacements: %s",
                str(variable),
                str(replacements),
            )
        return value

    def handle_document(self):
        """
        This routine processes the complete document (essentially a complete netcdf file)
        Each template key or value that corresponds to a variable will be selected from
        the netcdf file into a netcdf data set and then
        each station will get values from the record.
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
            new_document = initialize_data_array(new_document)
            for rec_num in range(rec_num_data_size):
                for key in self.template.keys():
                    if key == "data":
                        new_document = self.handle_data(
                            doc=new_document, rec_num=rec_num
                        )
                        continue
                    new_document = self.handle_key(new_document, rec_num, key)
            # put document into document map
            if new_document["id"]:
                logger.info(
                    "NetcdfBuilder.handle_document - adding document %s",
                    new_document["id"],
                )
                self.document_map[new_document["id"]] = new_document
            else:
                logger.info(
                    "NetcdfBuilder.handle_document - cannot add document with key %s",
                    str(new_document["id"]),
                )
        except Exception as _e:  # pylint:disable=broad-except
            logger.error(
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
                an_id = self.derive_id(
                    template_id=self.template["id"], rec_num=_rec_num
                )
                if an_id not in doc:
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
            logger.exception(
                "%s NetcdfBuilder.handle_key: Exception in builder",
                self.__class__.__name__,
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
        func = None
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
            logger.exception(
                "%s handle_named_function: %s params %s: Exception instantiating builder:",
                self.__class__.__name__,
                func,
                params,
            )
        return replace_with

    def handle_data(self, **kwargs):
        """This method iterates the template entries, deciding for each entry to either
        handle_named_function (if the entry starts with a '&') or to translate_template_item
        if it starts with an '*'. It handles both keys and values for each template entry.
        Args:
            doc (Object): this is the data document that is being built
        Returns:
            (Object): this is the data document that is being built
        """
        try:
            doc = kwargs["doc"]
            rec_num = kwargs["rec_num"]
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
                    logger.warning(
                        "%s Builder.handle_data - value is None",
                        self.__class__.__name__,
                    )
                data_elem[key] = value
            if data_key.startswith("&"):
                data_key = self.handle_named_function(data_key, rec_num)
            else:
                data_key = self.translate_template_item(data_key, rec_num)
            if data_key is None:
                logger.warning(
                    "%s Builder.handle_data - _data_key is None",
                    self.__class__.__name__,
                )
            self.load_data(doc, data_key, data_elem)
            return doc
        except Exception as _e:  # pylint:disable=broad-except
            logger.exception(
                "%s handle_data: Exception instantiating builder",
                self.__class__.__name__,
            )
        return doc

    def build_document(self, queue_element):
        """This is the entry point for the NetcfBuilders from the ingestManager.
        These documents are id'd by fcstValidEpoch. The data section is an array
        each element of which contains variable data and a station name. To process this
        file we need to itterate the document by recNum and process the station name along
        with all the other variables in the template.
        Args:
            file_name (str): the name of the file being processed
        Returns:
            [dict]: document
        """
        # noinspection PyBroadException
        try:
            bucket = self.load_spec["cb_connection"]["bucket"]
            scope = self.load_spec["cb_connection"]["scope"]
            collection = self.load_spec["cb_connection"]["collection"]

            # stash the file_name so that it can be used later
            self.file_name = os.path.basename(queue_element)
            # pylint: disable=no-member
            self.ncdf_data_set = nc.Dataset(queue_element)
            if len(self.stations) == 0:
                stmnt = f"""SELECT {self.subset}.*
                    FROM `{bucket}`.{scope}.{collection}
                    WHERE type = 'MD'
                    AND docType = 'station'
                    AND subset = '{self.subset}'
                    AND version = 'V01';"""
                result = self.load_spec["cluster"].query(stmnt)
                self.stations = list(result)

            self.initialize_document_map()
            logger.info(
                "%s building documents for file %s",
                self.__class__.__name__,
                queue_element,
            )
            if self.do_profiling:
                with cProfile.Profile() as _pr:
                    self.handle_document()
                    with open("profiling_stats.txt", "w", encoding="utf-8") as stream:
                        stats = Stats(_pr, stream=stream)
                        stats.strip_dirs()
                        stats.sort_stats("time")
                        stats.dump_stats("profiling_stats.prof")
                        stats.print_stats()
            else:
                self.handle_document()
            # pylint: disable=assignment-from-no-return
            document_map = self.get_document_map()
            data_file_id = self.create_data_file_id(
                self.subset, "netcdf", "madis", queue_element
            )
            data_file_doc = self.build_datafile_doc(
                file_name=queue_element, data_file_id=data_file_id, origin_type="madis"
            )
            document_map[data_file_doc["id"]] = data_file_doc
            return document_map
        except Exception as _e:  # pylint:disable=broad-except
            logger.exception(
                "%s: Exception with builder build_document: file_name: %s",
                self.__class__.__name__,
                queue_element,
            )
            return {}


# Concrete builders
class NetcdfMetarObsBuilderV01(NetcdfBuilder):  # pylint: disable=too-many-instance-attributes
    """
    This is the builder for observation data that is ingested from netcdf (madis) files
    """

    def __init__(self, load_spec, ingest_document):
        """
        This builder creates a set of V01 obs documents using the V01 station documents.
        This builder loads V01 station data into memory, and uses them to associate a station with an observation
        lat, lon point.
        In each document the observation data is an array of objects each of which is the obs data
        for a specific station.
        If a station from a metar file does not exist in the couchbase database
        a station document will be created from the metar record data and
        the station document will be added to the document map. If a station location has changed
        the geo element will be updated to have an additional geo element that has the new location
        and time bracket for the location.
        :param ingest_document: the document from the ingest document
        :param cluster: - a Couchbase cluster object, used for N1QL queries (QueryService)
        :param collection: - essentially a couchbase connection object, used to get documents by id (DataService)
        """
        NetcdfBuilder.__init__(self, load_spec, ingest_document)
        self.same_time_rows = []
        self.time = 0
        self.interpolated_time = 0
        self.delta = ingest_document["validTimeDelta"]
        self.cadence = ingest_document["validTimeInterval"]
        self.template = ingest_document["template"]
        self.subset = self.template["subset"]
        # self.do_profiling = True  # set to True to enable build_document profiling
        self.do_profiling = False  # set to True to enable build_document profiling

    def build_datafile_doc(self, file_name, data_file_id, origin_type):
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
            "originType": origin_type,
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
            return self.document_map
        except Exception as _e:  # pylint:disable=broad-except
            logger.exception(
                "%s get_document_map: Exception in get_document_map: %s",
                self.__class__.__name__,
                str(_e),
            )
            return None

    def load_data(self, doc, key, element):
        """
        This method adds an observation to the data dict -
        in fact we use a dict to hold data elems to ensure
        the data elements are unique per station name.
        Using a map ensures that the last
        entry in the netcdf file is the one that gets captured.
        :param doc: The document being created
        :param key: Not used
        :param element: the observation data
        :return: the document being created
        """
        if "data" not in doc.keys() or doc["data"] is None:
            doc["data"] = {}
        if element["name"] not in doc["data"].keys():
            # we only want the closest record (to match the legacy-sql data)
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
            logger.error(
                "%s handle_data: Exception in named function meterspersecond_to_milesperhour:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
            return None

    def ceiling_transform(self, params_dict):  # pylint: disable=too-many-locals
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
            mVV = re.compile(  # pylint: disable=invalid-name
                ".*VV.*"
            )  # Vertical Visibility pylint:disable=invalid-name
            mask_array = ma.getmaskarray(skyLayerBase)
            skyCover_array = (  # pylint:disable=invalid-name
                skyCover[1:-1].replace("'", "").split(" ")
            )
            # check for unmasked ceiling values - broken, overcast, vertical visibility - return associated skyLayerBase
            # name = str(nc.chartostring(self.ncdf_data_set['stationName'][params_dict['recNum']]))
            for index, sca_val in enumerate(skyCover_array):
                # also convert meters to feet (* 3.281)
                if (not mask_array[index]) and (
                    mBKN.match(sca_val) or mOVC.match(sca_val) or mVV.match(sca_val)
                ):
                    return math.floor(  # pylint: disable=c-extension-no-member
                        skyLayerBase[index] * 3.281
                    )  # pylint:disable=c-extension-no-member
            # check for unmasked ceiling values - all the others - CLR, SKC, NSC, FEW, SCT - return 60000
            for index, sca_val in enumerate(skyCover_array):
                # 60000 is aldready feet
                if (not mask_array[index]) and (
                    mCLR.match(sca_val)
                    or mSKC.match(sca_val)
                    or mNSC.match(sca_val)
                    or mFEW.match(sca_val)
                    or mSCT.match(sca_val)
                ):
                    return 60000
            # nothing was unmasked - return 60000 if there is a ceiling value in skycover array
            for index, sca_val in enumerate(skyCover_array):
                if (
                    mCLR.match(sca_val)
                    or mSKC.match(sca_val)
                    or mNSC.match(sca_val)
                    or mFEW.match(sca_val)
                    or mSCT.match(sca_val)
                ):
                    return 60000
            #  masked and no ceiling value in skyCover_array
            return None
        except Exception as _e:  # pylint:disable=broad-except
            logger.error(
                "%s handle_data: Exception in named function ceiling_transform:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
            logger.error(
                "ceiling_transform skyCover_array: %s skyLayerBase %s",
                str(skyCover_array),
                str(skyLayerBase),
            )
            logger.error("ceiling_transform stacktrace %s", str(traceback.format_exc()))
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
            logger.error(
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
            logger.error(
                "%s umask_value_transform: Exception in named function umask_value_transform for key %s:  error: %s",
                self.__class__.__name__,
                key,
                str(_e),
            )
            return None

    def handle_rh(self, params_dict):
        """Derives the RH from the dewpoint and temperature.
        Args:
            params_dict (dict): named function parameters
        Returns:
            float: the RH
        """
        try:
            # dewpoint = self.umask_value_transform({"recNum": params_dict["recNum"], "dewpoint":"dewpoint"})
            # temperature = self.umask_value_transform({"recNum": params_dict["recNum"], "temperature":"temperature"})
            dewpoint = self.umask_value_transform(
                {"recNum": params_dict["recNum"], "dewpoint": params_dict["dewpoint"]}
            )
            temperature = self.umask_value_transform(
                {
                    "recNum": params_dict["recNum"],
                    "temperature": params_dict["temperature"],
                }
            )
            _q = (
                relative_humidity_from_dewpoint(
                    temperature * units.kelvin, dewpoint * units.kelvin
                ).magnitude
            ) * 100
            return _q
        except Exception as _e:  # pylint:disable=broad-except
            # there must not have been one
            return None

    def handle_wind_dir_u(self, params_dict):
        """Derives the U component from the wind direction and speed.
           expects wind speed and wind direction to be in the params_dict.
        Args:
            params_dict (dict): named function parameters
        Returns:
            float: the wind direction
        """
        try:
            _wind_dir = self.umask_value_transform(
                {"recNum": params_dict["recNum"], "windDir": params_dict["windDir"]}
            )
            _wind_speed = self.umask_value_transform(
                {"recNum": params_dict["recNum"], "windSpeed": params_dict["windSpeed"]}
            )
            if _wind_dir is None:
                return None
            if _wind_speed is None:
                return None
            # wind speed is in meters per second and windDir is in degrees from netcdf file
            u = wind_components(_wind_speed * units("m/s"), _wind_dir * units.deg)[
                0
            ].magnitude
            return u
        except Exception as _e:  # pylint:disable=broad-exception-caught
            logger.error(
                "%s handle_wind_dir_v: Exception in named function:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
            return None

    def handle_wind_dir_v(self, params_dict):
        """Derives the V component from the wind direction and speed.
        Args:
            params_dict (dict): named function parameters
        Returns:
            float: the wind direction
        """
        try:
            _wind_dir = self.umask_value_transform(
                {"recNum": params_dict["recNum"], "windDir": params_dict["windDir"]}
            )
            _wind_speed = self.umask_value_transform(
                {"recNum": params_dict["recNum"], "windSpeed": params_dict["windSpeed"]}
            )
            if _wind_dir is None:
                return None
            if _wind_speed is None:
                return None
            # wind speed is in meters per second and windDir is in degrees from netcdf file
            v = wind_components(_wind_speed * units("m/s"), _wind_dir * units.deg)[
                1
            ].magnitude
            return v
        except Exception as _e:  # pylint:disable=broad-exception-caught
            logger.error(
                "%s handle_wind_dir_v: Exception in named function:  error: %s",
                self.__class__.__name__,
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
            logger.error(
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
            logger.error(
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
            _file_utc_time = dt.datetime.strptime(self.file_name, params_dict[key])
            epoch = (_file_utc_time - dt.datetime(1970, 1, 1)).total_seconds()
            iso = convert_to_iso(epoch)
            return iso
        except Exception as _e:  # pylint:disable=broad-except
            logger.error(
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
            _file_utc_time = dt.datetime.strptime(self.file_name, params_dict[key])
            epoch = (_file_utc_time - dt.datetime(1970, 1, 1)).total_seconds()
            return int(epoch)
        except Exception as _e:  # pylint:disable=broad-except
            logger.error(
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
            _ret_time = dt.datetime.utcfromtimestamp(_thistime)
            _ret_time = _ret_time.replace(
                second=0, microsecond=0, minute=0, hour=_ret_time.hour
            ) + dt.timedelta(hours=_ret_time.minute // delta_minutes)
            return calendar.timegm(_ret_time.timetuple())

        except Exception as _e:  # pylint:disable=broad-except
            logger.error(
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
            _time = dt.datetime.utcfromtimestamp(_time)
            # convert this iso
            if _time is None:
                return None
            return str(_time.isoformat())
        except Exception as _e:  # pylint:disable=broad-except
            logger.error(
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
        to find a station with that name from self.stations (which are all the station documents
        from couchbase).
        If the station does not exist it will be created with data from the
        netcdf file. If the station exists the lat, lon, and elev from the netcdf file
        will be compared to that in the existing station and if an update of the geo list is required it will be updated.
        Any modified or newly created stations get added to the document_map and automatically upserted.
        :param params_dict: {station_name:a_station_name}
        :return:
        """
        rec_num = params_dict["recNum"]
        station_name = params_dict["stationName"]
        an_id = None
        netcdf = {}
        fcst_valid_epoch = self.derive_valid_time_epoch(
            {"file_name_pattern": self.load_spec["fmask"]}
        )
        # noinspection PyBroadException
        try:
            # get the netcdf fields for comparing or adding new
            netcdf = self.fill_from_netcdf(rec_num, netcdf)
            elev = truncate_round(float(netcdf["elevation"]), 5)
            lat = truncate_round(float(netcdf["latitude"]), 5)
            lon = truncate_round(float(netcdf["longitude"]), 5)
            station = None
            station_index = None
            for station_index, a_station in enumerate(self.stations):
                if a_station["name"] == station_name:
                    station = a_station
                    break
            if station is None:
                # get the netcdf fields for comparing or adding new
                an_id = "MD:V01:METAR:station:" + netcdf["name"]
                new_station = {
                    "id": an_id,
                    "description": netcdf["description"],
                    "docType": "station",
                    "geo": [
                        {
                            "firstTime": fcst_valid_epoch,
                            "elev": elev,
                            "lat": lat,
                            "lon": lon,
                            "lastTime": fcst_valid_epoch,
                        }
                    ],
                    "name": netcdf["name"],
                    "subset": "METAR",
                    "type": "MD",
                    "updateTime": int(time.time()),
                    "version": "V01",
                }
                # add the new station to the document map with the new id
                if an_id not in self.document_map:
                    self.document_map[an_id] = new_station
                self.stations.append(new_station)
            else:
                # station does exist but is there a matching geo?
                # if there is not a matching geo create a new geo
                # if there is a matching geo then update the matching geo time range
                matching_location = False
                requires_new_geo = False
                for geo_index in range(len(self.stations[station_index]["geo"])):
                    geo = self.stations[station_index]["geo"][geo_index]
                    if geo["lat"] == lat and geo["lon"] == lon and geo["elev"] == elev:
                        matching_location = True
                        break
                if matching_location:
                    if (
                        fcst_valid_epoch
                        <= self.stations[station_index]["geo"][geo_index]["firstTime"]
                    ):
                        self.stations[station_index]["geo"][geo_index][
                            "firstTime"
                        ] = fcst_valid_epoch
                    else:
                        self.stations[station_index]["geo"][geo_index][
                            "lastTime"
                        ] = fcst_valid_epoch
                else:
                    # This station requires a new geo because there are no matching locations i.e. the location has changed
                    requires_new_geo = True
                if requires_new_geo:
                    self.stations[station_index]["geo"].append(
                        {
                            "firstTime": fcst_valid_epoch,
                            "elev": elev,
                            "lat": lat,
                            "lon": lon,
                            "lastTime": fcst_valid_epoch,
                        }
                    )
                # add the modified station to the document map with its existing id
                self.stations[station_index]["updateTime"] = int(time.time())
                an_id = self.stations[station_index]["id"]
                self.document_map[an_id] = self.stations[station_index]
            return params_dict["stationName"]
        except Exception as _e:  # pylint:disable=broad-except
            logger.exception(
                "%s netcdfObsBuilderV01.handle_station: Exception finding or creating station to match station_name: params: %s",
                self.__class__.__name__,
                str(params_dict),
            )
            return ""
