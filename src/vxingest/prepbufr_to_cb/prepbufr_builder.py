"""
Program Name: Class PrepbufrBuilder.py
Contact(s): Randy Pierce
History Log:  Initial version
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSL
"""

import abc
import collections
import contextlib
import copy
import cProfile
import datetime
import logging
import math
import pathlib
from pathlib import Path
from pstats import Stats

import metpy.calc
import ncepbufr
import numpy as np
import numpy.ma as ma
from metpy.units import units

from vxingest.builder_common.builder import Builder
from vxingest.builder_common.builder_utilities import (
    convert_to_iso,
)

# Get a logger with this module's name to help with debugging
logger = logging.getLogger(__name__)


# custom validation Exception for all masked data in a given level
class AllMaskedException(Exception):
    def __init__(self, message):
        super().__init__(message)


#  ApiBuilder← RaobObsBuilder ← RaobsGslObsBuilder
class PrepbufrBuilder(Builder):
    """parent class for Prepbufr builders"""

    def __init__(self, load_spec, ingest_document):
        #  builders do not init the ingest_document. That happens in build_document
        super().__init__(load_spec, ingest_document)

        self.load_spec = load_spec
        self.domain_stations = []
        self.ingest_document = None
        self.template = None
        self.subset = None
        self.model = None
        self.sub_doc_type = None
        self.model_fcst_valid_epochs = []
        self.stations = {}
        self.interpolated_data = {}
        self.levels = []
        # used to stash each fcstValidEpoch obs_data for the handlers
        self.obs_station_names = []  # used to stash sorted obs names for the handlers
        self.thresholds = None
        self.not_found_stations = set()
        self.not_found_station_count = 0
        self.bucket = None
        self.scope = None
        self.collection = None

    @abc.abstractmethod
    def read_data_from_file(self, queue_element, templates):
        """read data from the prepbufr file, filter messages for appropriate ones,
        and load them raw into a dictionary structure, so that they can be post processed
        for interpolations."""
        return

    def derive_id(self, **kwargs):
        """
        This is a private method to derive a document id from the current valid_fcst_time and level.
        A *field represents a direct substitution and a &function|params...
        represents a handler function.
        Args:
            template_id (string): this is an id template string
        Returns:
            [string]: The processed id with substitutions made for elements in the id template
        """
        try:
            template_id = kwargs["template_id"]
            level = round(kwargs["level"])
            stationName = kwargs["stationName"]
            parts = template_id.split(":")
            new_parts = []
            for part in parts:
                if "level" in part:
                    value = str(level)
                else:
                    if "stationName" in part:
                        value = stationName
                    else:
                        if part.startswith("&"):
                            value = str(
                                self.handle_named_function(stationName, level, part)
                            )
                        else:
                            if part.startswith("*"):
                                value = str(
                                    self.translate_template_item(
                                        stationName, level, part
                                    )
                                )
                            else:
                                value = str(part)
                new_parts.append(value)
            new_id = ":".join(new_parts)
            return new_id
        except Exception as _e:
            logging.exception("ApiBuilder.derive_id: Exception  error: %s")
            return None

    def translate_template_item(self, stationName, level, variable):
        """
        This method translates template replacements (*item).
        It can translate keys or values.
        :param stationName: the station name
        :param level: the level
        :param variable: a value from the template - should be a record field
        :param api_record
        :return:
        """
        replacements = []
        # noinspection PyBroadException
        try:
            # level_idx = self.get_mandatory_levels().index(level)
            if isinstance(variable, str):
                replacements = variable.split("*")[1:]
            # this is a literal, doesn't need to be returned
            if len(replacements) == 0:
                return variable
            # pre assign these in case it isn't a replacement - makes it easier
            value = variable
            if len(replacements) > 0:
                for _ri in replacements:
                    # _ri is the template field name.
                    # this is the place where it happens
                    # we need to find the value in the raw_obs_data
                    # looking in report_type 120 for non wind data
                    # and in report_type 220 for wind data.

                    if _ri == "stationName":
                        return stationName
                    if "wind" in _ri.lower():
                        # look in report_type 220
                        if (
                            220 not in self.interpolated_data[stationName]
                            or not self.interpolated_data[stationName][220]["data"]
                        ):
                            # this one has no data
                            value = None
                        else:
                            try:
                                value = (
                                    self.interpolated_data[stationName][220]["data"][
                                        _ri
                                    ][level]
                                    if self.interpolated_data[stationName][220]["data"][
                                        _ri
                                    ]
                                    else None
                                )
                            except KeyError as _ke:
                                # this level doesn't exist for this variable
                                value = None
                    else:
                        # look in report_type 120
                        if (
                            120 not in self.interpolated_data[stationName]
                            or not self.interpolated_data[stationName][120]["data"]
                        ):
                            # this one has no data
                            return None
                        else:
                            try:
                                value = (
                                    self.interpolated_data[stationName][120]["data"][
                                        _ri
                                    ][level]
                                    if self.interpolated_data[stationName][120]["data"][
                                        _ri
                                    ]
                                    else None
                                )

                            except KeyError as _ke:
                                # this level doesn't exist for this variable
                                return None
                        # convert each station value to iso if necessary
                    if _ri.startswith("{ISO}"):
                        value = variable.replace("*" + _ri, convert_to_iso(value))
                    else:
                        value = (
                            variable.replace("*" + _ri, str(value))
                            if value is not None
                            else None
                        )
                    try:  # make sure we have a number, if possible - except for stationNames
                        value = float(value) if value is not None else None
                    except ValueError:
                        return value
            return value
        except Exception as _e:
            logging.error(
                "PrepBufrBuilder.translate_template_item: Exception  error: %s", str(_e)
            )
            return None

    def interpolate_variable_for_level(
        self,
        variable,
        nearest_higher_pressure_index,
        nearest_lower_pressure_index,
        obs_data,
        wanted_pressure_level_mb,
    ):
        """
        This method interpolates the data for a given variable to the wanted level
        using the nearest higher and lower pressure indexes. This method assumes that the variables
        are masked arrays with the same shape. The wanted pressure is a mandatory level provided in mb
        and the pressure masked array is also assumed to be in mb.
        :param variable: the variable to interpolate
        :param nearest_higher_pressure_index: the nearest higher pressure index
        :param nearest_lower_pressure_index: the nearest lower pressure index
        :param wanted_pressure_level_mb: the wanted pressure level in mb
        :return: the interpolated value for the variable at the wanted pressure level
        """
        try:
            value = None
            nearest_higher_pressure = obs_data["pressure"][
                nearest_higher_pressure_index
            ]
            nearest_lower_pressure = obs_data["pressure"][nearest_lower_pressure_index]
            try:
                weight = (float)(
                    (
                        math.log(nearest_higher_pressure)
                        - math.log(wanted_pressure_level_mb)
                    )
                    / (
                        math.log(nearest_higher_pressure)
                        - math.log(nearest_lower_pressure)
                    )
                )
            except Exception as _e1:
                if not isinstance(_e1, ZeroDivisionError):
                    # don't log divide by zero (two adjacent levels with same)
                    logging.error(
                        "PrepBufrBuilder.interpolate_variable_for_level: Exception  error: %s",
                        str(_e1),
                    )
                return None
            if variable == "wind_direction":  # if it is a wind_direction do this
                # interpolates wind directions in the range 0 - 359 degrees
                if not self.is_a_number(nearest_lower_pressure) or not self.is_a_number(
                    nearest_higher_pressure
                ):
                    return None

                next_higher_pressure_direction = obs_data["wind_direction"][
                    nearest_higher_pressure_index
                ]
                next_lower_pressure_direction = obs_data["wind_direction"][
                    nearest_lower_pressure_index
                ]
                if not self.is_a_number(
                    next_lower_pressure_direction
                ) or not self.is_a_number(next_higher_pressure_direction):
                    return None

                dir_dif = next_lower_pressure_direction - next_higher_pressure_direction
                if dir_dif > 180:
                    dir_dif -= 360
                else:
                    if dir_dif < -180:
                        dir_dif += 360
                value = next_higher_pressure_direction + weight * (dir_dif)
                if value < 0:
                    value += 360
                else:
                    if value > 360:
                        value -= 360
                return value
            else:  # if it isn't a wind_direction do this
                next_higher_pressure_variable_value = obs_data[variable][
                    nearest_higher_pressure_index
                ]
                next_lower_pressure_variable_value = obs_data[variable][
                    nearest_lower_pressure_index
                ]
                if not self.is_a_number(
                    next_higher_pressure_variable_value
                ) or not self.is_a_number(next_lower_pressure_variable_value):
                    return None
                else:
                    value = next_higher_pressure_variable_value + weight * (
                        next_lower_pressure_variable_value
                        - next_higher_pressure_variable_value
                    )
                    return value
        except Exception as _e:
            logging.error(
                "PrepBufrBuilder.interpolate_level: Exception  error: %s", str(_e)
            )
            return None

    def interpolate_data(self, raw_obs_data):
        """fill in the mandatory levels with interpolated data using the log difference interpolation method.
        For each pressure level in the mandatory levels, find the nearest higher and lower pressure levels
        and interpolate the data for each variable at the mandatory level. Set the pressure level to the mandatory level.
        Args:
            raw_data (): this is the raw data from the prepbufr file with missing heights having been interpolated
            using the hypsometric equation for thickness.
        Returns: the interpolated_data
        """
        try:
            interpolated_data = {}
            mandatory_levels = self.get_mandatory_levels()
            for station in raw_obs_data:
                if station not in interpolated_data:
                    interpolated_data[station] = {}
                for report in raw_obs_data[station]:
                    if report not in interpolated_data[station]:
                        interpolated_data[station][report] = {}
                        if "data" not in interpolated_data[station][report]:
                            interpolated_data[station][report]["data"] = {}
                    if raw_obs_data[station][report][
                        "obs_data"
                    ] is None or not isinstance(
                        raw_obs_data[station][report]["obs_data"]["pressure"],
                        list,
                    ):
                        # I cannot process this station - there is no array of pressure data
                        del interpolated_data[station]
                        break
                    for variable in raw_obs_data[station][report]["obs_data"]:
                        # create masked array for the variable with ALL the mandatory levels
                        # though the levels below the bottom level and above the top level will be masked
                        if report == 120 and "wind" in variable.lower():
                            # skip this one - it is handled in the 220 report
                            continue
                        if (
                            report == 220
                            and variable.lower() != "pressure"
                            and "wind" not in variable.lower()
                        ):
                            # skip this one - it is handled in the 120 report - except for pressure
                            continue
                        if variable not in interpolated_data[station][report]["data"]:
                            interpolated_data[station][report]["data"][variable] = {}
                        if (
                            raw_obs_data[station][report]["obs_data"][variable] is None
                            or len(raw_obs_data[station][report]["obs_data"][variable])
                            == 0
                        ):
                            # can't do this, there is no raw data for this variable
                            interpolated_data[station][report]["data"][variable] = None
                            continue
                        # now we can interpolate the levels for each variable
                        for level in mandatory_levels:
                            # find the nearest higher and lower pressure to this level
                            p_arr = np.asarray(
                                raw_obs_data[station][report]["obs_data"]["pressure"]
                            )
                            if level > p_arr.max() or level < p_arr.min():
                                # this level is outside the range of the data - have to skip it
                                interpolated_data[station][report]["data"][variable][
                                    level
                                ] = None
                                continue
                            p_no_nan_arr = p_arr[~np.isnan(p_arr)]
                            if level > p_no_nan_arr.max() or level < p_no_nan_arr.min():
                                # this level is outside the range of the data - have to skip it
                                interpolated_data[station][report]["data"][variable][
                                    level
                                ] = None
                                continue
                            nearest_higher_pressure = p_no_nan_arr[
                                p_no_nan_arr >= level
                            ].min()
                            nearest_higher_i = raw_obs_data[station][report][
                                "obs_data"
                            ]["pressure"].index(nearest_higher_pressure)
                            nearest_lower_pressure = p_no_nan_arr[
                                p_no_nan_arr <= level
                            ].max()
                            nearest_lower_i = raw_obs_data[station][report]["obs_data"][
                                "pressure"
                            ].index(nearest_lower_pressure)

                            if (
                                nearest_higher_i == nearest_lower_i
                                and nearest_higher_pressure == level
                            ):
                                # this is the level we want - it matches the mandatory level
                                interpolated_data[station][report]["data"][variable][
                                    level
                                ] = raw_obs_data[station][report]["obs_data"][variable][
                                    nearest_lower_i
                                ]
                                continue
                            # have to interpolate the data for this variable and level
                            try:
                                interpolated_data[station][report]["data"][variable][
                                    level
                                ] = self.interpolate_variable_for_level(
                                    variable,
                                    nearest_higher_i,
                                    nearest_lower_i,
                                    raw_obs_data[station][report]["obs_data"],
                                    level,
                                )
                            except Exception as _e:
                                logging.error(
                                    "PrepBufrBuilder.interpolate_data: Exception  error: %s",
                                    str(_e),
                                )
                                interpolated_data[station][report]["data"][variable][
                                    level
                                ] = None
        except Exception as _e:
            logging.error(
                "PrepBufrBuilder.interpolate_data: Exception  error: %s", str(_e)
            )
        # set the pressure levels to the mandatory levels now that the data has all been interpolated to mandatory levels
        for station in raw_obs_data:
            for report in raw_obs_data[station]:
                for _l in mandatory_levels:
                    if station not in interpolated_data:
                        continue
                    interpolated_data[station][report]["data"]["pressure"][_l] = _l
        return interpolated_data

    def handle_document(self):
        """
        This routine processes the complete document (essentially a complete bufr file)
        which includes a new document for each mandatory level. The data section of each document
        is a dictionary keyed by the station name. The handle_data method is called and it will
        process each station in the interpolated_data and reconcile station locations with the
        couchbase station documents. If a station is not found in the couchbase database
        a new station document will be created and added to the document map.
        :return: The modified document_map
        The document map should be a dictionary keyed by the document id
        and each document id should look like DD:V01:RAOB:obs:prepbufr:500:1625097600

        where the type is "DD", the version is "V01", the subset is "RAOB", the docType is "obs",
        the docSubType is "prepbufr", the level is "500 (in mb)", and the valid time epoch is "1625097600".

        Each Document shall have a data dictionary that is keyed by the station name. The data section is defined by
        the template in the ingest document.

        The existence of the level key in the template indicates that the template is a multilevel template.
        """
        # noinspection PyBroadException
        try:
            for level in self.get_mandatory_levels():
                new_document = copy.deepcopy(self.template)
                # make a copy of the template, which will become the new document
                # once all the translations have occurred
                # set the level right away (it is needed for the handle_data)
                # clean out the data template from the data portion of the newDocument
                new_document["data"] = {}
                for key in self.template:
                    if key == "level":
                        new_document["level"] = level
                        continue
                    if key == "data":
                        try:
                            self.handle_data(level, doc=new_document)
                        except AllMaskedException as _ame:
                            # this data is all masked at this level. Cannot use this document.
                            break
                        continue
                    # handle the key for this level that isn't data and isn't level
                    # level is the same for all the variables and all the stations
                    # variables will be handled in the data section for every station
                    new_document = self.handle_key(new_document, level, key)
                    # put new document into document map
                    if new_document["id"]:
                        logging.info(
                            "PrepbufrBuilder.handle_document - adding document %s",
                            new_document["id"],
                        )
                        self.document_map[new_document["id"]] = new_document
                    else:
                        logging.info(
                            "PrepbufrBuilder.handle_document - cannot add document with key %s",
                            str(new_document["id"]),
                        )
                        self.document_map[new_document["id"]] = new_document
        except Exception as _e:
            logging.error(
                "PrepbufrBuilder.handle_document: Exception instantiating builder: %s error: %s",
                self.__class__.__name__,
                str(_e),
            )
            raise _e

    def handle_key(self, doc, level, key, stationName=None):
        """
        This routine handles keys by substituting
        the data that correspond to the key into the values
        in the template that begin with *
        :param doc: the current document
        :param level: the current level
        :param stationName: The current station - only necessary for the data section
        :param _key: A key to be processed, This can be a key to a primitive,
        or to another dictionary, or to a named function
        """
        # noinspection PyBroadException
        try:
            if key == "id":
                an_id = self.derive_id(
                    template_id=self.template["id"],
                    level=level,
                    stationName=stationName,
                )
                if an_id not in doc:
                    doc["id"] = an_id
                return doc
            if isinstance(doc[key], dict):
                # process an embedded dictionary
                # find the key in the template and process it (might not be a key in the top level of the template)
                if key in self.template:
                    val = self.template[key]
                else:
                    for k in self.template:
                        if key in self.template[k]:
                            val = self.template[k][key]
                            break
                tmp_doc = copy.deepcopy(val)
                for sub_key in tmp_doc:
                    tmp_doc = self.handle_key(
                        tmp_doc, level, sub_key, stationName
                    )  # recursion
                doc[key] = tmp_doc
            if (
                not isinstance(doc[key], dict)
                and isinstance(doc[key], str)
                and doc[key].startswith("&")
            ):
                doc[key] = self.handle_named_function(stationName, level, doc[key])
            else:
                doc[key] = self.translate_template_item(stationName, level, doc[key])
            return doc
        except Exception as _e:
            logging.exception(
                "%s ApiBuilder.handle_key: Exception in builder",
                self.__class__.__name__,
            )
        return doc

    def handle_named_function(self, stationName, level, named_function_def):
        """
        This routine processes a named function entry from a template.
        :param _named_function_def - this can be either a template key or a template value.
        :param stationName - the current station
        :param level - the current level
        The _named_function_def looks like "&named_function:*field1,*field2,*field3..."
        where named_function is the literal function name of a defined function.
        The name of the function and the function parameters are separated by a ":" and
        the parameters are separated by a ','.
        It is expected that field1, field2, and field3 etc are all valid variable names.
        Each field will be translated from the interpolated_data for every mandatory level
        into value1, value2 etc. The method "named_function" will be called like...
        named_function({field1:value1, field2:value2, ... field_n:value_n}) and the return value from named_function
        will be substituted into each level document.
        """

        func = None
        try:
            func = named_function_def.split("|")[0].replace("&", "")
            if len(named_function_def.split("|")) > 1:
                params = named_function_def.split("|")[1].split(",")
            else:
                params = []
            dict_params = {}
            for _p in params:
                # be sure to slice the * off of the front of the param
                dict_params[_p[1:]] = self.translate_template_item(
                    stationName, level, _p
                )
            dict_params["level"] = level
            dict_params["stationName"] = stationName
            # call the named function using getattr
            if not dict_params:
                replace_with = getattr(self, func)()
            else:
                replace_with = getattr(self, func)(dict_params)
        except ValueError as _ve:
            if not isinstance(_ve, ValueError):
                logging.error(
                    "%s handle_named_function: %s params %s: ValueError instantiating builder: %s",
                    self.__class__.__name__,
                    func,
                    params,
                    str(_ve),
                )
            raise _ve
        except Exception as _e:
            logging.exception(
                "%s handle_named_function: %s params %s: Exception instantiating builder:",
                self.__class__.__name__,
                func,
                params,
            )
        return replace_with

    def handle_data(self, level, **kwargs):
        """This method must handle each station. For each station this method iterates
        the template entries, deciding for each entry to either
        handle_named_function (if the entry starts with a '&') or to translate_template_item
        if it starts with an '*'. It handles both keys and values for each template entry. The level
        and the station are included in the params for the named function or the template item.
        The inclusion of level and station is what allows the proper access to the interpolated data for the station.
        Args:
            doc (Object): this is the data document that is being built
        Returns:
            (Object): this is the data document that is being built
        Raises: AllMaskedException if all the data is masked for a given level
                ValueError if the data is not valid.
                Either exception will cause the document to be skipped.
        """
        try:
            doc = kwargs["doc"]
            for _station_name in self.interpolated_data:
                data_elem = {}
                data_template = self.template["data"]["*stationName"]
                try:
                    for _data_key in data_template:
                        try:
                            value = data_template[_data_key]
                            # values can be null...
                            if (
                                value
                                and not isinstance(value, dict)
                                and value.startswith("&")
                            ):
                                replace_value = self.handle_named_function(
                                    _station_name, level, value
                                )
                            else:
                                replace_value = self.translate_template_item(
                                    _station_name, level, value
                                )
                            data_elem[_data_key] = replace_value
                        except ValueError as _ve:
                            # this was logged already - dont log it again
                            raise _ve
                        except Exception as _e:
                            replace_value = None
                            logging.warning(
                                "%s Builder.handle_data - value is None",
                                self.__class__.__name__,
                            )
                            raise _e  # probably cannot use this document - throw it away
                except ValueError as _ve:
                    continue  # do not use this one - we didn't have enough data to create a new station document
                doc["data"][_station_name] = data_elem
            return doc
        except AllMaskedException as _ame:
            raise _ame
        except Exception as _e:
            logging.exception(
                "%s handle_data: Exception instantiating builder",
                self.__class__.__name__,
            )
        return doc

    def build_document(self, queue_element):
        """This is the entry point for the Builders from the ingestManager.
        These documents are id'd by fcstValidEpoch and level. The data section is a dictionary
        keyed by station name each element of which contains variable data and the station name.
        To process this raob_data object we need to iterate the data and process the station
        name along with all the other variables in the template.
        Args:
            queue_element - a prepbufr file name
        Returns:
            [dict]: document

        1) read the prepbufr file to get all the obs data into a raw data dict interpolating missing heights with the hypsometric equation for thickness.
        2) add the raw data to the document map
        2) interpolate the data into mandatory 10mb levels
        see https://docs.google.com/document/d/1-L-1FMKGDRXNGmAZhdZb3_vZT41kOWte8ufqd2c_CvQ/edit?usp=sharing
        For each level interpolate the data to the mandatory levels, if necessary and build a document for each level
        that has all the data for that level. Add the document to the document map.
        """
        # noinspection PyBroadException
        try:
            # read the api for all data for this valid fcst hour.
            bucket = self.load_spec["cb_connection"]["bucket"]
            scope = self.load_spec["cb_connection"]["scope"]
            # collection is set to "RAOB" in the run_ingest
            collection = self.load_spec["cb_connection"]["collection"]
            mnemonic_mapping = self.mnemonic_mapping
            self.raw_obs_data = self.read_data_from_file(
                queue_element, mnemonic_mapping
            )
            try:
                self.interpolated_data = self.interpolate_data(self.raw_obs_data)
            except Exception as _e:
                logger.error(
                    "PrepBufrBuilder.build_document: Exception  error: %s", str(_e)
                )
                return {}
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
                    # derive and add the documents (one per mandatory level) to the document map with profiling
                    self.handle_document()
                    with Path.open("profiling_stats.txt", "w") as stream:
                        stats = Stats(_pr, stream=stream)
                        stats.strip_dirs()
                        stats.sort_stats("time")
                        stats.dump_stats("profiling_stats.prof")
                        stats.print_stats()
            else:
                # derive and add the documents (one per mandatory level) to the document map without profiling
                self.handle_document()
            document_map = self.get_document_map()
            # add the datafile doc to the document map
            data_file_id = self.create_data_file_id(
                self.subset, "GDAS", "prepbufr", queue_element
            )
            data_file_doc = self.build_datafile_doc(
                file_name=queue_element, data_file_id=data_file_id, origin_type="GDAS"
            )
            document_map[data_file_doc["id"]] = data_file_doc
            return document_map
        except Exception as _e:
            logger.exception(
                "%s: Exception with builder build_document: file_name: %s",
                self.__class__.__name__,
                queue_element,
            )
            return {}


# Concrete builders
class PrepbufrRaobsObsBuilderV01(PrepbufrBuilder):
    """
    This is the builder for RAOBS observation data that is ingested from prepbufr files
    """

    def __init__(self, load_spec, ingest_document):
        """
        This builder creates a set of V01 obs documents using the V01 raob station documents.
        This builder loads V01 station data into memory, and associates a station with an observation
        lat, lon, point.
        In each document the observation data is an array of objects each of which is the obs data
        for a specific station.
        If a station from a prepbufr file does not exist in the couchbase database
        a station document will be created from the prepbufr record data and
        the station document will be added to the document map. If a station location has changed
        the geo element will be updated to have an additional geo element that has the new location
        and time bracket for the location.
        :param ingest_document: the document from the ingest document
        :param load_spec: the load specification
        """
        PrepbufrBuilder.__init__(self, load_spec, ingest_document)
        self.same_time_rows = []
        self.time = 0
        self.interpolated_time = 0
        self.template = ingest_document["template"]
        self.mnemonic_mapping = ingest_document["mnemonic_mapping"]
        self.subset = self.template["subset"]
        self.raw_obs_data = {}
        self.interpolated_data = {}
        self.mandatory_levels = []
        self.station_reference = {}

        self.print_debug_station_report = False
        # self.print_debug_station_report = True
        self.report_file = None
        self.report_station_file_name = "/tmp/station_report.txt"
        if self.print_debug_station_report:
            with contextlib.suppress(OSError):
                Path(self.station_report_file_name).unlink()
            print(f"debug station report is in {self.station_report_file_name}")
        # self.do_profiling = True  # set to True to enable build_document profiling
        self.do_profiling = False  # set to True to enable build_document profiling

        self.write_data_for_debug_station_list = None  # used for debugging
        self.current_station = None  # used for debugging

    # used for debugging - mostly for setting breakpoints
    def set_write_data_for_debug_station_list(self, station_list):
        self.write_data_for_debug_station_list = station_list

    def get_mandatory_levels(self):
        """
        This method gets the mandatory levels for the raw data set.
        :param report: the bufr report i.e. the subset.report_type
        :return: the mandatory levels
        """
        if not self.mandatory_levels:
            self.mandatory_levels = list(range(1010, 10, -10))
        return self.mandatory_levels

    def get_svpWobus(self, tx):
        """_summary_
            From legacy code: svpWobus
            static public float svpWobus(double tk) {
                double tx = tk-273.15;
                double pol = 0.99999683       + tx*(-0.90826951e-02 +
                    tx*(0.78736169e-04   + tx*(-0.61117958e-06 +
                    tx*(0.43884187e-08   + tx*(-0.29883885e-10 +
                    tx*(0.21874425e-12   + tx*(-0.17892321e-14 +
                    tx*(0.11112018e-16   + tx*(-0.30994571e-19)))))))));
                double  esw_pascals = 6.1078/Math.pow(pol,8.) *100.;
                return((float)esw_pascals);
                }

        Args:
            t (float): either temp or dewpoint in degC
        """
        tx = math.nan if tx is ma.masked else tx
        _pol = 0.99999683 + tx * (
            -0.90826951e-02
            + tx
            * (
                0.78736169e-04
                + tx
                * (
                    -0.61117958e-06
                    + tx
                    * (
                        0.43884187e-08
                        + tx
                        * (
                            -0.29883885e-10
                            + tx
                            * (
                                0.21874425e-12
                                + tx
                                * (
                                    -0.17892321e-14
                                    + tx * (0.11112018e-16 + tx * (-0.30994571e-19))
                                )
                            )
                        )
                    )
                )
            )
        )
        esw_pascals = 6.1078 / math.pow(_pol, 8.0) * 100.0
        return esw_pascals

    def get_relative_humidity_wobus(self, temperature, dewpoint):
        """_summary_
            From legacy code: svpWobusRH
            rh[i] = svpWobus(tdk)/svpWobus(tk) * 100;  NOTE tdk is dewpoint and tk is temperature IN KELVIN
            where tdk is the dewpoint and tk is the temperature.
        Args:
            temperature (list temp): list of temperatures degC
            dewpoint (list dp): list of dewpoint  deg C
        """
        if temperature is None or dewpoint is None:
            return None
        else:
            return [
                round((self.get_svpWobus(dp) / self.get_svpWobus(t)) * 100, 4)
                for dp, t in zip(dewpoint, temperature)
            ]

    def get_relative_humidity(self, pressure, temperature, specific_humidity):
        """
        This method calculates the relative humidity from the specific humidity, if necessary
        :param pressure: the pressure data (list)
        :param temperature: the temperature data (list)
        :param specific_humidity: the specific humidity data (list)
        :return: the relative humidity data

        example:
        The list parameters must be converted to masked arrays.
        relative_humidity_from_specific_humidity(pressure, temperature, specific_humidity)  all pint.Quantity
        relative_humidity_from_specific_humidity(1013.25 * units.hPa, 30 * units.degC, 18/1000).to('percent')

        WOBUS values:
        The legacy svpWobus value is derived from saturationVaporPressure and temperature using the Wobus formula.
        It is provided here for reference only. The svpWobusRH will be included in the raw data.
        """
        try:
            if pressure is None or temperature is None or specific_humidity is None:
                # cannot process this
                return None
            relative_humidity = [
                None
                if p is not self.is_a_number(ma.masked) or t is not self.is_a_number(ma.masked) or s is not self.is_a_number(ma.masked)
                else metpy.calc.relative_humidity_from_specific_humidity(
                    p * units.hPa,
                    t * units.degC,
                    s / 1000 * units("g/kg"),
                )
                .to("percent")
                .to_tuple()[0]
                for p, t, s in zip(pressure, temperature, specific_humidity)
            ]
            return relative_humidity
        except Exception as _e:
            logger.error(
                "PrepBufrBuilder.get_relative_humidity: Exception  error: %s", str(_e)
            )
            return None

    def interpolate_heights_hypsometric(
        self, height, pressure, temperature, specific_humidity
    ):
        """
        This method interpolates the heights using the hypsometric thickness equation.
        There is the possibility that a height value is missing for a given pressure level.
        There is the assumption that the pressure levels are more correct than the heights.
        We will use the highest pressure level as the bottom and the lowest pressure level as the top,
        making sure that the temperature, pressure, and mixing_ratio lists are all homogeneous.
        Using that data we will use the metpy.calc.thickness_hydrostatic to calculate the thickness of each layer
        adding it to the previous height to get the interpolated height.

        :param height: list of float - the height data (may have elements that are ma.masked or None) in meters.
        :param pressure: list of float - the pressure data atmospheric profile in units.hPa.
        :param temperature: list of float - the temperature data atmospheric profile in units.degC.
        :param specific_humidity - list of float - the specific_humidity atmospheric profile in units("mg/kg").
        :return: the interpolated heights nd_array (in meters) or None if the interpolation fails.

        examples: (from https://unidata.github.io/MetPy/latest/api/generated/metpy.calc.thickness_hydrostatic.html)

        mixing ratio from specific humidity:
            sh = [4.77, 12.14, 6.16, 15.29, 12.25] * units('g/kg')
            mixing_ratio_from_specific_humidity(sh).to('g/kg')
            <Quantity([ 4.79286195 12.28919078  6.19818079 15.52741416 12.40192356],
            'gram / kilogram')>

        thickness_hydrostatic with mixing ratio:
            # pressure
            p = [1008., 1000., 950., 900., 850., 800., 750., 700., 650., 600.,
                550., 500., 450., 400., 350., 300., 250., 200.,
                175., 150., 125., 100., 80., 70., 60., 50.,
                40., 30., 25., 20.] * units.hPa
            # temperature
            T = [29.3, 28.1, 23.5, 20.9, 18.4, 15.9, 13.1, 10.1, 6.7, 3.1,
                -0.5, -4.5, -9.0, -14.8, -21.5, -29.7, -40.0, -52.4,
                -59.2, -66.5, -74.1, -78.5, -76.0, -71.6, -66.7, -61.3,
                -56.3, -51.7, -50.7, -47.5] * units.degC
            # specify a layer
            layer = (p <= 1000 * units.hPa) & (p >= 500 * units.hPa)
            # compute the hydrostatic thickness
            mpcalc.thickness_hydrostatic(p[layer], T[layer])
            <Quantity(5755.94719, 'meter')>
        """
        if height is None or pressure is None or temperature is None or specific_humidity is None:
            # if there aren't data I don't know what to do.
            return None
        try:
            # don't use invalid data at the top or bottom of the profile
            # if any of the needed values are invalid make them all math.nan at that position.
            # Make invalid values math.nan because the metpy.calc routine likes them that way.
            i = 0
            while (
                not self.is_a_number(pressure[i])
                or not self.is_a_number(temperature[i])
                or not self.is_a_number(specific_humidity[i])
            ):
                temperature[i] = math.nan
                pressure[i] = math.nan
                specific_humidity[i] = math.nan
                i = i + 1
            _first_bottom_i = i
            i = len(pressure) - 1
            while (
                not self.is_a_number(pressure[i])
                or not self.is_a_number(temperature[i])
                or not self.is_a_number(specific_humidity[i])
            ):
                temperature[i] = math.nan
                pressure[i] = math.nan
                specific_humidity[i] = math.nan
                i = i - 1
            _last_top_i = i

            # create pint.Quantity sequences for the data
            sh = [
                s if self.is_a_number(s) else math.nan for s in specific_humidity
            ] * units("mg/kg")
            mr = metpy.calc.mixing_ratio_from_specific_humidity(sh).to("g/kg")
            p = [p1 if self.is_a_number(p1) else math.nan for p1 in pressure] * units.hPa
            t = [
                t1 if self.is_a_number(t1) else math.nan for t1 in temperature
            ] * units.degC
            h = [h1 if self.is_a_number(h1) else math.nan for h1 in height] * units.meter

            # now determine the layer by finding the bottom valid pressure that has corresponding valid data for
            # temperature, pressure, and mixing ratio.
            _bottom_i = _first_bottom_i
            _top_i = _bottom_i + 1
            while _top_i < _last_top_i:
                if self.is_a_number(height[_top_i]):
                    # we have a valid height - so use it
                    h[_top_i] = height[_top_i] * units.meter
                    _bottom_i = _top_i
                    _top_i = _top_i + 1
                    continue
                while math.isnan(pressure[_top_i]):
                    _top_i = _top_i + 1
                layer = (p <= pressure[_bottom_i] * units.hPa) & (
                    p >= pressure[_top_i] * units.hPa
                )
                _thickness = metpy.calc.thickness_hydrostatic(
                    p[layer],
                    t[layer],
                    mr[layer],
                    molecular_weight_ratio=0.6219569100577033,
                )
                if not self.is_a_number(_thickness.magnitude):
                    # Could not derive the thickness from the hypsometric equation
                    if self.is_a_number(height[_top_i]):
                        # The provided height is valid - so use that
                        h[_top_i] = height[_top_i] * units.meter
                    else:
                        # cannot do anything - the derived thickness is invalid and so is the provided
                        # radiosonde height - so invalidate this level
                        h[_top_i] = math.nan
                else:
                    # Thickness is valid - could derive the thickness from the hypsometric equation
                    # check the previous derived height - is it a valid number?
                    if self.is_a_number(h[_top_i - 1].magnitude):
                        # The previous derived height was also valid so use the previous derived height as a base reference
                        # for adding to the thickness
                        h[_top_i] = _thickness + h[_top_i - 1]
                    else:
                        # the previous derived height is not a valid number - is the previous provided (radiosonde) height?
                        if self.is_a_number(height[_top_i - 1]):
                            # The previous provided (radiosonde) height is valid - so
                            # use it as a base reference for adding to the thickness
                            h[_top_i] = _thickness + height[_top_i - 1] * units.meter
                        else:
                            # we have a valid thickness but no valid valid base reference for
                            # the height to add to the thickness
                            # so we have to invalidate this height
                            h[_top_i] = math.nan
                _bottom_i = _top_i
                _top_i = _top_i + 1
            return [_h1 if self.is_a_number(_h1) else None for _h1 in list(h.magnitude)]
        except Exception as _e1:
            logger.error(
                "PrepBufrBuilder.interpolate_heights: RuntimeWarning  error: %s",
                str(_e1),
            )
            return None

    def get_data_from_bufr_for_field(
        self,
        events,
        bufr_data,
        mnemonics,  # mnemonics is a multidimensional np.array of variable, program_code, q_marker mnemonics
        mnemonic=None,
        event_value=None,
        q_marker_keep_values=None,
    ):
        """
        This method gets the value from the bufr data at the index for the specific field
        :param events: Bool - whether the bufr has events or not
        :param bufr_data: the bufr data
        :param mnemonics: the variable mnemonic list
        :param mnemonic: the specific mnemonic
        :param event_value: the specific event value
        :return: If events are False the variable value will be read from the bufr_data at the mnemonic_index.
        If events are True the variable value will be read from the bufr_data at the mnemonic_index and the event dimension
        will be used to find the value that corresponds to the event_value that is passed in, e.g. 1 is 'Initial' and 8 is 'Virtual'
        see https://www.emc.ncep.noaa.gov/mmb/data_processing/table_local_await-val.htm#0-12-247
        An event_value of None will cause the value of the first event to be returned, regardless of the events actual value.
        An event_value is the value of the corresponding event program code at the event index. The event program code is the
        value for the event_program_code mnemonic.
        example:
        as an example consider the following bufr data decoded from a prepbufr file 241570000.gdas.t00z.prepbufr.nr which
        is the 0 hour UTC readings for the GDAS data set from 2024 June 5th.
        The data was loaded in read_data_from_file with bufr = ncepbufr.open(queue_element), bufr.load_subset(), and bufr.advance().
        In this routine the data for a subset is read with the following code...
        For example purposes consider using two different read_subset calls, one for events-False and one for events=True.
        bdne = bufr.read_subset(mnemonics_str, events=False).squeeze()
        bdwe = bufr.read_subset(mnemonics_str, events=True).squeeze()
        consider the following mnemonics_str for debugging purposes
        - no events mnemonics_str = 'TOB TDO RHO QOB POB ZOB' (temp, dewpoint, relative humidity, specific humidity, pressure, height)
        - with events mnemonics_str = 'TOB TPC TDO RHO QOB QPC POB PPC ZOB ZPC' (temp, temp program code, dewpoint, relative humidity, specific humidity, specific humidity program code, pressure, pressure program code, height, height program code)

        bdne=bufr.read_subset('TOB TDO RHO QOB POB ZOB', events=False)
        bdwe=bufr.read_subset('TOB TPC TDO RHO QOB QPC POB PPC ZOB ZPC', events=True)
        bdne.shape
         (6, 43)
        > bdwe.shape
         (6, 43, 255)
        bdne is a masked array for the temperature TOB i.e. the first mnemonic i.e. variable_index==0. There are 43 elements i.e. levels
        > bdne[0]
          masked_array(data=[--, --, --, 26.8, 25.200000000000003, 16.7,
          11.200000000000001, -4.4, -5.4, -12.200000000000001,
           ....,
          -51.7, -49.1],
          mask=[ True,  True,  True, False, False, False, False, False,
          .....,
           False, False, False],
           fill_value=100000000000.0)
        the 26th element is -76.7 degrees C
        > bdne[0][26]
        -76.7
        bdwe is a masked array for the temperature TOB i.e. the first mnemonic. There are 43 elements i.e. levels, and an additional element for each of 255 potential events.
         > bdwe.shape
         (6, 43, 255)
        there are two events for the 26th level and all the others are masked
        > bdwe[0][26].count()
         2
        for this example the first event is 0 and the second event is 1 and the corresponding data values are identical
        > bdwe[0][26][0]
        -76.7
        > bdwe[0][26][1]
        -76.7
        > bdwe[0][26][2]
        masked
        to get all the first event pressures...
        > [i[0] for i in bdwe[6]]
        > [1000.0, 925.0, 850.0, 773.0, 772.0, 700.0, 654.0, 515.0, 500.0, 440.0, 431.0, 428.0, 400.0, 383.0, 342.0, 329.0, 326.0, 320.0, 300.0, 264.0, 250.0, 208.0, 200.0, 185.0, 150.0, 149.0, 100.0, 97.5, 71.9, 70.0, 68.60000000000001, 64.4, 56.6, 50.0, 42.300000000000004, 36.5, 32.300000000000004, 30.900000000000002, 30.0, 25.3, 22.3, 20.0, 16.7]
        to get all the second event pressures (note that all but the first two are masked)...
        > i[1] for i in bdwe[6]]
        [1000.0, 925.0, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked, masked]
        to find the index of the 500 mb level (it is the ninth element - index 8) ...
        [i[0] for i in bdwe[6]].index(500)
        8
        so to find the temperature that corresponds to the pressure level 500 mb use the index 8...
        > bdwe[0][8][0]
        -5.4     (this is the first event value)
        bdwe[0][8][1]
        -6.1000000000000005 (this is the second event value)
        now to find out what the temperature event program code is for the first or second event use the TPC value i.e. 1 ...
        > bdwe[1][8][0]
        The program event code table is at https://www.emc.ncep.noaa.gov/mmb/data_processing/table_local_await-val.htm#0-12-247
          8.0    (from the code table we know that this is the temperature event program code for Virtual Temp)
        > bdwe[1][8][1]
          1.0    (from the code table we know that this is the temperature event program code for Initial Temp reading)
        So the initial temp reading is -6.1 degrees C - bdwe[0][8][1] and the virtual temp is -5.4 degrees C - bdwe[0][8][0]

        For some of the templates the events variable will be set to false and for others it will be set to true.
        For those set to true we will look for the initial (event program code value == 1) data values not the virtual ones.
        For those set to false we will look for the first data value.

        """
        # _mnemonics = mnemonics[0]
        # event_program_code_mnemonics = _mnemonics[1]
        # q_marker_mnemonics = _mnemonics[2]

        # have to remember that the mnemonics are a 3-d array with the first dimension being the variable,
        # 2nd the event_program_code mnemonics, and 3rd the q_marker_mnemonics
        # The bufr was read with a concatenation of these three arrays so the bufr data index for the variable mnemonic
        # is offset by 0, the bufr data for the event_program_code mnemonic is offset by the length of the variable mnemonic array,
        # and the bufr data q_marker_mnemonic index is offset by the length of the variable and event_program_code mnemonics.
        # Those lengths should all be the same i.e. mnemonics.shape[0]. Missing elements in the event_program_code and q_marker mnemonics
        # are None.

        mnemonic_index = list(mnemonics[0]).index(mnemonic)
        # if it isn't a masked array just return the data
        if not ma.isMaskedArray(bufr_data[mnemonic_index]):
            return bufr_data[mnemonic_index]
        # if it is all masked return None
        if np.ma.array(bufr_data[mnemonic_index]).mask.all():
            return None
        # do we need to check for events
        if (
            events is False
            or event_value is None
            or mnemonics[1][mnemonic_index] is None
        ):
            # don't consider events just return the data for the field
            if len(bufr_data[mnemonic_index].shape) == 1:
                # no events present
                return [
                    i if i is not ma.masked else np.nan for i in bufr_data[mnemonic_index]
                ]
            if len(bufr_data[mnemonic_index].shape) > 1:
                # there is an event dimension but we are ignoring it - just return the first event
                return [i[0] for i in bufr_data[mnemonic_index]]
            else:
                if bufr_data[mnemonic_index].shape == ():
                    # this data is not multidimensional
                    return [bufr_data[mnemonic_index].item()]
        else:
            # Need to consider events and q_markers.
            # Go through each level and find the value that corresponds to the event_value,
            # then use the event program code mnemonic to find the index of the desired event program code value,
            # then use that event index to find the corresponding value for the variable. These bufr
            # events should have the variable value, and the q_marker in the same event index.
            # TODO USE SHAPE HERE!
            event_program_code_mnemonic_index = len(mnemonics[0]) + mnemonic_index
            q_marker_mnemonic_index = len(mnemonics[0]) * 2 + mnemonic_index
            # Make a copy of the bufr_data for the mnemonic - the program will modify the data
            # in the copy to reflect the correct event value and qualify it with the q_marker value
            # the copy wont have multiple events, just the data for the correct event mnemonic value
            bufr_data_for_mnemonic = bufr_data[mnemonic_index].copy()
            for level_index in range(0, bufr_data.shape[1]):
                if bufr_data[mnemonic_index][level_index].shape == ():  # scalar
                    return [bufr_data[mnemonic_index][level_index].item()]
                if (
                    len(
                        bufr_data[event_program_code_mnemonic_index][
                            level_index
                        ].compressed()
                    )
                    == 1
                ):
                    # ignore the events, there is only one event anyway, just return like there weren't events.
                    # This deserves explanation. The bufr data is a 3-d array with the first dimension being the variable (or q_marker)
                    # the second dimension being the level and the third dimension being the event. If there is only one event
                    # then the third dimension is irrelevant.
                    bufr_data_for_mnemonic[level_index] = (
                        bufr_data[mnemonic_index][level_index]
                        if q_marker_keep_values is None
                        or bufr_data[q_marker_mnemonic_index][level_index][0]
                        in q_marker_keep_values
                        else np.nan
                    )
                    continue
                try:
                    # now we have multiple events so we have to consider them - so find the index of the expected event_value
                    _event_value_found = False
                    for e_index in range(0, bufr_data.shape[2]):
                        if (
                            bufr_data[event_program_code_mnemonic_index][level_index][
                                e_index
                            ]
                            is ma.masked
                        ):
                            # do not consider this masked event - just use the bufr_data for the mnemonic
                            bufr_data_for_mnemonic[level_index] = bufr_data[
                                mnemonic_index
                            ][level_index]  # is masked
                            continue
                        if (
                            bufr_data[event_program_code_mnemonic_index][level_index][
                                e_index
                            ]
                            == event_value
                        ):
                            _event_value_found = True
                            break
                    # using the found event value index find the correct value for this field and level
                    # qualify the value by the corresponding q_marker value
                    # if the q_marker value is not in the q_marker_keep_values then set the field value to None i.e. disqualified
                    if _event_value_found is True:
                        if (
                            bufr_data[mnemonic_index][level_index][e_index]
                            is not ma.masked
                        ):
                            if (
                                q_marker_keep_values is None
                                or bufr_data[q_marker_mnemonic_index][level_index][
                                    e_index
                                ]
                                in q_marker_keep_values
                            ):
                                # for the copy we only have one value for the mnemonic[level] so we can just set it
                                # with the correct event value (lose the event dimension)
                                bufr_data_for_mnemonic[level_index][0] = bufr_data[
                                    mnemonic_index
                                ][level_index][e_index]
                            else:
                                # the correct event value failed the q_marker test - gets a nan for that level
                                # This means the the interpolation will have to interpolate the value for this level
                                bufr_data_for_mnemonic[level_index][0] = np.nan
                    else:
                        logger.info(
                            f"PrepBufrBuilder.get_data_from_bufr_for_field: event_value not found for mnemonic {mnemonic}",
                        )
                        # could not find the desired event value - return None
                        bufr_data_for_mnemonic[level_index][0] = np.nan
                except IndexError as _ie:
                    logger.error(
                        f"PrepBufrBuilder.get_data_from_bufr_for_field: IndexError for mnemonic {mnemonic}",
                    )
                    # bad data - return None
                    return None
            # return the data for the mnemonic converted to a list
            return [i[0] for i in bufr_data_for_mnemonic]

    def read_data_from_bufr(self, bufr, template):
        """
        This method reads the data from the bufr file according to a provided template.
        A template is a dict keyed by the desired field name with a value that is a
        dict with a mnemonic and an intent. The mnemonic is the bufr mnemonic for the field
        and the intent is the datatype of the field in the resulting data document.
        For example station_id "SID" often returns a float but the intent is str.
        :param bufr: the bufr file
        :template: a dictionary of header keys with their corresponding mnemonics and intended types
        refer to https://www.emc.ncep.noaa.gov/emc/pages/infrastructure/bufrlib/tables/bufrtab_tableb.html
        example: see prepbufr_raob_template.json

        :return: a data object: each key is a field name and the value is the data array (not masked) for that field,
        None values should be np.nan in the data array.
        """
        # see read_subset https://github.com/NOAA-EMC/NCEPLIBS-bufr/blob/a8108e591c6cb1e21ddc7ddb6715df1b3801fff8/python/ncepbufr/__init__.py#L449
        variable_mnemonics = []
        event_program_code_mnemonics = []
        q_marker_mnemonics = []
        for o in template.values():
            if isinstance(o, dict):
                variable_mnemonics.append(o["mnemonic"])
                event_program_code_mnemonics.append(
                    o.get("event_program_code_mnemonic", "")
                )
                q_marker_mnemonics.append(o.get("q_marker_mnemonic", ""))
        mnemonics = np.array(
            (variable_mnemonics, event_program_code_mnemonics, q_marker_mnemonics),
            dtype=str,
        )
        mnemonics_str = " ".join(
            variable_mnemonics + event_program_code_mnemonics + q_marker_mnemonics
        ).strip()
        events = template["events"] is True
        # see https://github.com/NOAA-EMC/NCEPLIBS-bufr/blob/a8108e591c6cb1e21ddc7ddb6715df1b3801fff8/python/ncepbufr/__init__.py#L449
        # for reference on read_subset
        # if `events=True`, `ufbevn` is used to read prepbufr "events", and a 3-d array is returned.
        # The shape of the (events=False) array is `(nm,nlevs)`, where where `nm` is the number of elements in the specified
        # mnemonics string, and `nlevs` is the number of levels in the report.
        # If `events=True`, a 3rd dimension representing the prepbufr event codes is added giving a shape of `(nm,nlevs,events)`. The
        # 'events' has the same length as the number of events in the report and has the values of the event program codes, provided
        # that event program code mnemonics are provided in the mnemonic list. If there are no event
        # program codes in the mnemonics list, the events dimension is masked, the values of the data mnemonic are present but you cannot determine which
        # event program code applies to which event or which data value.
        # The q_marker_mnemonics are available to use to ignore values that are not valid.
        bufr_data = bufr.read_subset(mnemonics_str, events=events).squeeze()
        data = {}
        height = None
        temperature = None
        pressure = None
        specific_humidity = None
        relative_humidity = None
        dewpoint = None
        for mnemonic in variable_mnemonics:
            try:
                # uncomment this for debugging - makes a good place for a breakpoint
                # if 'temperature' in template:
                #     print(f"station:{self.current_station} mnemonic:{mnemonic}\n")
                field = self.get_field_for_mnemonic(template, mnemonic)
                match field:
                    case "relative_humidity":
                        pressure = self.get_raw_pressure(
                            template,
                            mnemonics,
                            events,
                            bufr_data,
                            pressure,
                        )
                        temperature = self.get_raw_temperature(
                            template,
                            mnemonics,
                            events,
                            bufr_data,
                            temperature,
                        )
                        specific_humidity = self.get_raw_specific_humidity(
                            template,
                            mnemonics,
                            events,
                            bufr_data,
                            specific_humidity,
                        )
                        dewpoint = self.get_raw_dewpoint(
                            template,
                            mnemonics,
                            events,
                            bufr_data,
                            dewpoint,
                        )
                        relative_humidity = self.get_raw_relative_humidity(
                            data, temperature, pressure, specific_humidity, dewpoint
                        )
                        data["relative_humidity"] = relative_humidity
                    case "height":
                        height = self.get_raw_height(
                            template,
                            mnemonics,
                            events,
                            bufr_data,
                            height,
                        )
                        pressure = self.get_raw_pressure(
                            template,
                            mnemonics,
                            events,
                            bufr_data,
                            pressure,
                        )
                        temperature = self.get_raw_temperature(
                            template,
                            mnemonics,
                            events,
                            bufr_data,
                            temperature,
                        )
                        specific_humidity = self.get_raw_specific_humidity(
                            template,
                            mnemonics,
                            events,
                            bufr_data,
                            specific_humidity,
                        )
                        # how many heights can be masked before this is a useless exercise?
                        if (
                            len(pressure) < 2
                        ):  # cannot interpolate with less than 2 pressure levels - throw this away
                            return None
                        _interpolated_height = self.interpolate_heights_hypsometric(
                            height,
                            pressure,
                            temperature,
                            specific_humidity,
                        )
                        data["height"] = _interpolated_height
                    case "temperature":
                        temperature = self.get_raw_temperature(
                            template,
                            mnemonics,
                            events,
                            bufr_data,
                            temperature,
                        )
                        data["temperature"] = temperature
                    case "pressure":
                        pressure = self.get_raw_pressure(
                            template,
                            mnemonics,
                            events,
                            bufr_data,
                            pressure,
                        )
                        data["pressure"] = pressure
                    case "dewpoint":
                        dewpoint = self.get_raw_dewpoint(
                            template,
                            mnemonics,
                            events,
                            bufr_data,
                            dewpoint,
                        )
                        data["dewpoint"] = dewpoint
                    case "specific_humidity":
                        specific_humidity = self.get_raw_specific_humidity(
                            template,
                            mnemonics,
                            events,
                            bufr_data,
                            specific_humidity,
                        )
                        data["specific_humidity"] = specific_humidity
                    case _:
                        event_value = template[field].get("event_value", None)
                        data[field] = self.get_data_from_bufr_for_type_field(
                            template,
                            events,
                            bufr_data,
                            mnemonics,
                            mnemonic,
                            field,
                            event_value,
                        )
                        # capture the station_id for debugging
                        if field == "station_id":
                            self.current_station = data[field]
            except Exception as _e:
                logger.error(
                    "PrepBufrBuilder.read_data_from_bufr: Exception  error: %s", str(_e)
                )
                data[field] = None
        return data

    def get_field_for_mnemonic(self, template, mnemonic):
        for k, v in template.items():
            if isinstance(v, dict) and mnemonic == v["mnemonic"]:
                return k
        return k

    def get_raw_height(
        self,
        template,
        mnemonics,
        events,
        bufr_data,
        height,
    ):
        if height is None:
            # need to get some specific fields to interpolate the height
            height_mnemonic = template["height"]["mnemonic"]
            height_event_value = template["height"].get("event_value", None)
            height_q_marker_keep_values = template["height"].get("q_marker_keep", None)

            height = self.get_data_from_bufr_for_field(
                events,
                bufr_data,
                mnemonics,
                mnemonic=height_mnemonic,
                event_value=height_event_value,
                q_marker_keep_values=height_q_marker_keep_values,
            )
        return height

    def get_raw_relative_humidity(
        self, data, temperature, pressure, specific_humidity, dewpoint
    ):
        _rh = self.get_relative_humidity(
            pressure,
            temperature,
            specific_humidity,
        )
        #data["rh_wobus"] = self.get_relative_humidity_wobus(temperature, dewpoint)
        return _rh

    def get_raw_dewpoint(
        self,
        template,
        mnemonics,
        events,
        bufr_data,
        dewpoint,
    ):
        if dewpoint is None:
            dewpoint_mnemonic = template["dewpoint"]["mnemonic"]
            dewpoint_event_value = template["dewpoint"].get("event_value", None)
            dewpoint = self.get_data_from_bufr_for_field(
                events,
                bufr_data,
                mnemonics,
                mnemonic=dewpoint_mnemonic,
                event_value=dewpoint_event_value,
            )
        return dewpoint

    def get_raw_specific_humidity(
        self,
        template,
        mnemonics,
        events,
        bufr_data,
        specific_humidity,
    ):
        if specific_humidity is None:
            specific_humidity_mnemonic = template["specific_humidity"]["mnemonic"]
            specific_humidity_event_value = template["specific_humidity"].get(
                "event_value", None
            )
            specific_humidity = self.get_data_from_bufr_for_field(
                events,
                bufr_data,
                mnemonics,
                mnemonic=specific_humidity_mnemonic,
                event_value=specific_humidity_event_value,
            )
        return specific_humidity

    def get_raw_temperature(
        self,
        template,
        mnemonics,
        events,
        bufr_data,
        temperature,
    ):
        if temperature is None:
            temperature_mnemonic = template["temperature"]["mnemonic"]
            temperature_event_value = template["temperature"].get("event_value", None)
            temperature = self.get_data_from_bufr_for_field(
                events,
                bufr_data,
                mnemonics,
                mnemonic=temperature_mnemonic,
                event_value=temperature_event_value,
            )
        return temperature

    def get_raw_pressure(
        self,
        template,
        mnemonics,
        events,
        bufr_data,
        pressure,
    ):
        if pressure is None:
            pressure_mnemonic = template["pressure"]["mnemonic"]
            pressure_event_value = template["pressure"].get("event_value", None)
            pressure = self.get_data_from_bufr_for_field(
                events,
                bufr_data,
                mnemonics,
                mnemonic=pressure_mnemonic,
                event_value=pressure_event_value,
            )
        return pressure

    def get_data_from_bufr_for_type_field(
        self,
        template,
        events,
        bufr_data,
        mnemonics,
        mnemonic,
        field,
        event_value,
    ):
        data = []
        try:
            match template[field]["intent"]:
                case "int":
                    try:
                        b_data = self.get_data_from_bufr_for_field(
                            events,
                            bufr_data,
                            mnemonics,
                            mnemonic=mnemonic,
                            event_value=event_value,
                            q_marker_keep_values=template[field].get(
                                "q_marker_keep", None
                            ),
                        )
                        if b_data is None:
                            return None
                        if not isinstance(
                            b_data, collections.abc.Sequence
                        ) and not isinstance(b_data, np.ndarray):
                            return int(b_data)
                        else:
                            return [int(i) if i is not None else None for i in b_data]
                    except Exception as _e:
                        logger.error(
                            "PrepBufrBuilder.get_data_from_bufr_for_type_field: Exception  error: %s",
                            str(_e),
                        )
                        return None
                case "float":
                    try:
                        b_data = self.get_data_from_bufr_for_field(
                            events,
                            bufr_data,
                            mnemonics,
                            mnemonic=mnemonic,
                            event_value=event_value,
                            q_marker_keep_values=template[field].get(
                                "q_marker_keep", None
                            ),
                        )
                        mnemonic_index = list(mnemonics[0]).index(mnemonic)
                        if b_data is None:
                            return None
                        if not isinstance(
                            b_data, collections.abc.Sequence
                        ) and not isinstance(b_data, np.ndarray):
                            data = round(b_data, 3)
                        else:
                            data = [
                                round(i, 3)
                                if i is not None and i is not ma.masked
                                else None
                                for i in b_data
                            ]
                    except Exception as _e:
                        logger.error(
                            "PrepBufrBuilder.get_data_from_bufr_for_type_field: Exception  error: %s",
                            str(_e),
                        )
                        data = None
                case "str":
                    try:
                        data = str(
                            self.get_data_from_bufr_for_field(
                                events,
                                bufr_data,
                                mnemonics,
                                mnemonic=mnemonic,
                                event_value=event_value,
                                q_marker_keep_values=template[field].get(
                                    "q_marker_keep", None
                                ),
                            ),
                            encoding="utf-8",
                        ).strip()
                    except Exception as _e:
                        logger.error(
                            "PrepBufrBuilder.get_data_from_bufr_for_type_field: Exception  error: %s",
                            str(_e),
                        )
                        data = None
                case _:
                    data = bufr_data[mnemonic_index]
        except Exception as _e:
            logger.error(
                "PrepBufrBuilder.get_data_from_bufr_for_type_field: Exception  error: %s",
                str(_e),
            )
            return None
        return data

    def get_fcst_valid_epoch_from_msg(self, bufr):
        """this method gets the forecast valid epoch from the message

        Args:
            bufr (_type_): _description_

        Returns:
            int: epoch representing the date
        """
        date_str = (
            bufr.msg_date
        )  # date is a datetime object i.e. 2024041012 is 2024-04-10 12:00:00
        _dt = datetime.datetime.strptime(str(date_str), "%Y%m%d%H").replace(
            tzinfo=datetime.timezone.utc
        )
        _epoch = int(_dt.timestamp())
        return _epoch

    def read_data_from_file(self, queue_element, templates):
        """read data from the prepbufr file, filter messages for appropriate ones,
        and load them raw into a raw dictionary structure. Use hypsometric equation to
        calculate heights from pressure for determining unknown heights. Load everything into
        a dictionary structure, so that mandatory levels can be interpolated for every 10mb
         using weighted logarithmic interpolation.
        Args:
            queue_element: the file name to read
        Transformations: 1) pressure to height 2) temperature to dewpoint 3) knots to meters per second
        creates a self raw document_map
        NOTE: for report_type see https://www.emc.ncep.noaa.gov/emc/pages/infrastructure/bufrlib/tables/CodeFlag_0_STDv41_LOC7.html#055007
        120 - MASS REPORT - Rawinsonde
        220 - WIND REPORT - Rawinsonde
        """
        bufr = ncepbufr.open(queue_element)
        raw_bufr_data = {}
        # loop over messages, each new subset is a new station. Each subset has all the recorded levels for that station.
        while bufr.advance() == 0:
            if bufr.msg_type != templates["bufr_msg_type"]:
                continue
            # see load subset https://github.com/NOAA-EMC/NCEPLIBS-bufr/blob/a8108e591c6cb1e21ddc7ddb6715df1b3801fff8/python/ncepbufr/__init__.py#L389
            # we use the bufr.msg_date to get the forecast valid epoch for the entire message
            self.fcst_valid_epoch = self.get_fcst_valid_epoch_from_msg(bufr)
            while bufr.load_subset() == 0:  # loop over subsets in message.
                try:
                    subset_data = {}
                    # read the header data
                    header_data = self.read_data_from_bufr(bufr, templates["header"])
                    # only save report types that are in the template
                    # if we already have data for this station - skip it
                    if header_data["report_type"] not in templates["bufr_report_types"]:
                        continue
                    subset_data["station_id"] = header_data["station_id"]
                    subset_data["report_type"] = round(header_data["report_type"])
                    subset_data["fcst_valid_epoch"] = self.fcst_valid_epoch
                    subset_data["header"] = header_data
                    # read the obs_err data
                    obs_err_data = self.read_data_from_bufr(bufr, templates["obs_err"])
                    subset_data["obs_err"] = obs_err_data
                    # read the obs data
                    logger.debug(
                        f"{subset_data["station_id"]}, {subset_data["report_type"]}"
                    )
                    # use the template for the specific report type to read the obs data
                    # see https://www.emc.ncep.noaa.gov/emc/pages/infrastructure/bufrlib/tables/CodeFlag_0_STDv42_LOC7.html#007246
                    obs_data = self.read_data_from_bufr(
                        bufr, templates["obs_data_" + str(header_data["report_type"])]
                    )
                    subset_data["obs_data"] = obs_data

                    raw_bufr_data[subset_data["station_id"]] = raw_bufr_data.get(
                        subset_data["station_id"], {}
                    )
                    raw_bufr_data[subset_data["station_id"]][
                        subset_data["report_type"]
                    ] = raw_bufr_data[subset_data["station_id"]].get(
                        subset_data["report_type"], {}
                    )
                    raw_bufr_data[subset_data["station_id"]][
                        subset_data["report_type"]
                    ] = subset_data
                except Exception as _e:
                    logger.error(
                        "PrepBufrBuilder.read_data_from_file: Exception  error: %s",
                        str(_e),
                    )
        bufr.close()
        return raw_bufr_data

    def build_datafile_doc(self, file_name, data_file_id, origin_type):
        """
        This method will build a dataFile document for prepbufr builder. The dataFile
        document will represent the file that is ingested by the prepbufr builder. The document
        is intended to be added to the output folder and imported with the other documents.
        The VxIngest will examine the existing dataFile documents to determine if a specific file
        has already been ingested.
        """
        mtime = Path(file_name).stat().st_mtime
        df_doc = {
            "id": data_file_id,
            "mtime": mtime,
            "subset": self.subset,
            "type": "DF",
            "fileType": "prepbufr",
            "originType": origin_type,
            "loadJobId": self.load_spec["load_job_doc"]["id"],
            "dataSourceId": "NCEP",
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
        except Exception as _e:
            logger.exception(
                "%s get_document_map: Exception in get_document_map: %s",
                self.__class__.__name__,
                str(_e),
            )
            return None

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
            value = next(iter(params_dict.items()))[1]
            # value might have been masked (there is probably a better way to deal with this)
            if not self.is_a_number(value):
                return None
            else:
                value = (float(value) - 273.15) * 1.8 + 32
            return value
        except Exception as _e:
            logger.error(
                "%s handle_data: Exception in named function meterspersecond_to_milesperhour:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
            return None

        return None

    def knots_to_meters_per_second(self, params_dict):
        """Converts knots to meters per second performing any translations that are necessary
        Args:
            params_dict (dict): named function parameters
        Returns:
            float: value converted from knots to meters per second
        """
        try:
            value = next(iter(params_dict.items()))[1]
            # value might have been masked (there is probably a better way to deal with this)
            if not self.is_a_number(value):
                return None
            else:
                value = round(float(value) * 0.5144444444, 4)
            return value
        except Exception as _e:
            logger.error(
                "%s handle_data: Exception in named function knots_to_meters_per_second:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
            return None

    def celsius_to_fahrenheit(self, params_dict):
        """Converts celsius to fahrenheit performing any translations that are necessary
        Args:
            params_dict (dict): named function parameters
        Returns:
            [type]: [description]
        """

        try:
            value = next(iter(params_dict.items()))[1]
            # value might have been masked (there is probably a better way to deal with this)
            if not self.is_a_number(value):
                return None
            else:
                value = float(value) * 1.8 + 32
            return value
        except Exception as _e:
            logger.error(
                "%s handle_data: Exception in named function celsius_to_fahrenheit:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
            return None

    def mg_per_kg_to_g_per_kg(self, params_dict):
        """Converts mg per kg to g per kg performing any translations that are necessary
        Args:
            params_dict (dict): named function parameters
        Returns:
            [type]: [description]
        """
        try:
            value = next(iter(params_dict.items()))[1]
            # value might have been masked (there is probably a better way to deal with this)
            if not self.is_a_number(value):
                return None
            else:
                value = float(value) / 1000
            return value
        except Exception as _e:
            logger.error(
                "%s handle_data: Exception in named function mg_per_kg_to_g_per_kg:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
            return None

    def kelvin_to_fahrenheit(self, params_dict):
        """Converts kelvin to fahrenheit performing any translations that are necessary
        Args:
            params_dict (dict): named function parameters
        Returns:
            [type]: [description]
        """
        try:
            value = next(iter(params_dict.items()))[1]
            # value might have been masked (there is probably a better way to deal with this)
            if not self.is_a_number(value):
                return None
            else:
                value = (float(value) - 273.15) * 1.8 + 32
            return value
        except Exception as _e:
            logger.error(
                "%s handle_data: Exception in named function kelvin_to_fahrenheit:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
            return None

    def get_station_reference(self):
        """
        This method returns the station reference metadata from the station Reference document
        :return: the station reference metadata
        """
        if not self.station_reference:
            self.station_reference = (
                self.load_spec["collection"]
                .get("MD:V01:RAOB:stationReference")
                .content_as[dict]
            )
        return self.station_reference

    def handle_station(self, params_dict):
        """
        This method uses the station name in the params_dict
        to find a station with that name from self.stations (which are all the station documents
        from couchbase).
        If the station does not exist it will be created with data from the
        prepbufr file. If the station exists the lat, lon, and elev from the prepbufr file
        will be compared to that in the existing station and if an update of the geo list is required it will be updated.
        Any modified or newly created stations get added to the document_map and automatically upsert'ed.
        :param params_dict: {'bufr_subset': 'bufr_subset_loaded`}
        :return:
        """
        try:
            station_id = params_dict["stationName"]
            level = params_dict["level"]
            if (
                120 not in self.raw_obs_data[station_id]
                or 220 not in self.raw_obs_data[station_id]
            ):
                # we don't need to process the station for lack of data
                raise ValueError("bad wmoid in handle_station - no data")
            fcst_valid_epoch = self.raw_obs_data[station_id][120]["fcst_valid_epoch"]
            header = self.raw_obs_data[station_id][120]["header"]
            highest_interpolated_level = max(
                list(self.interpolated_data[station_id][120]["data"]["pressure"].keys())
            )
            an_id = None
            if level != highest_interpolated_level:
                # we don't need to process the station except at the highest interpolated level
                return station_id
            # process the station to see if the geo is correct etc.
            elev = header["elevation"]
            lat = header["lat"]
            lon = header["lon"]
            if lon > 180:
                lon = round((360 - lon), 4) * -1
                header["lon"] = lon
            station = None
            station_index = None
            for idx, a_station in enumerate(self.stations):
                if str(a_station["wmoid"]) == station_id:
                    station = a_station
                    station_index = idx
                    break
            if station is None:
                # station does not exist in reference but does exist in the prepbufr file
                # Throw this data away. We don't have enough information to create a station
                raise ValueError("bad wmoid in handle_station")
            # station does exist but is there a matching geo for the lowest height lat/lon ?
            # If this is a new location then we need to add a new geo to the station
            # but only if this is a new location close to the ground
            # if there is not a matching geo create a new geo
            # if there is a matching geo then update the matching geo time range
            for geo_index in range(len(self.stations[station_index]["geo"])):
                geo = self.stations[station_index]["geo"][geo_index]
                # is the level close to the station elevation? The lat? The lon?
                # if not then don't use this data (the balloon probably isn't close to the station)
                # 1° = 111 km so 0.01  degrees is about 1.1 kilometers ~ 1 km.
                # assume ~ 2km (0.02 degrees) distance is close to the station if the altitude is under 100 meters
                # this is all totally arbitrary, but math.dist can give us the approximate distance between two points.

                if (
                    math.dist([lat, lon], [geo["lat"], geo["lon"]]) < 0.02
                    and abs(geo["elev"] - elev) <= 100
                ):
                    # update the matching geo time range for this station
                    # with the fcst_valid_epoch.
                    # make firstTime the earliest and lastTime the latest
                    if (
                        fcst_valid_epoch
                        <= self.stations[station_index]["geo"][geo_index]["firstTime"]
                    ):
                        self.stations[station_index]["geo"][geo_index]["firstTime"] = (
                            fcst_valid_epoch
                        )
                    else:
                        self.stations[station_index]["geo"][geo_index]["lastTime"] = (
                            fcst_valid_epoch
                        )
                else:
                    # This station might require a new geo because there are no matching locations i.e. the location has changed
                    # unless the level is not close to the highest pressure level for an accurate location match - close to the ground.
                    # The highest level is actually the highest pressure, the closest to the ground,
                    # apparently sometimes the pressure data is not an array!

                    if self.print_debug_station_report:
                        self.write_dbg_station_rpt(
                            params_dict,
                            station_id,
                            header,
                            elev,
                            lat,
                            lon,
                            station,
                            highest_interpolated_level,
                        )
                    if False:  # we cannot be changing the stations `geo` list until we
                        # have a better understanding of the location differences in the prepbufr data
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
                # the station will be upserted. It might have only had times changed
                # but it might have had a new geo added.
                self.stations[station_index]["updateTime"] = fcst_valid_epoch
                an_id = self.stations[station_index]["id"]
                self.document_map[an_id] = self.stations[station_index]
            return station_id
        except ValueError as _e:
            logger.info(
                "%s PrepbufrRaobsObsBuilderV01.handle_station: Exception bad wmoid in handle_station: params: %s",
                self.__class__.__name__,
                str(params_dict),
            )
            raise _e
        except Exception as _e:
            logger.error(
                "%s PrepbufrRaobsObsBuilderV01.handle_station: Exception finding or creating station to match station_name: params: %s",
                self.__class__.__name__,
                str(params_dict),
                _e,
            )
            raise _e

    def write_dbg_station_rpt(
        self,
        params_dict,
        station_id,
        header,
        elev,
        lat,
        lon,
        station,
        highest_interpolated_level,
    ):
        """
        Used for debugging stations.
        This method writes a debug report to a file. You can turn this on by setting self.print_debug_report to True.
        :param params_dict: the params_dict
        :param station_id: the station_id
        :param header: the header
        :param elev: the elevation
        :param lat: the latitude
        :param lon: the longitude
        :param station: the station
        :param highest_interpolated_level: the highest interpolated level
        :return: None
        The output is a file in {self.station+report_file_name} that will be overwritten each time this is used.
        The output is a series of lines that show the differences between the station and the raob data, with
        the particular columns separated by a space.
        You can grep and sort the output to get more specific views of the data.
        for example:
            grep  header /tmp/station_report.txt | sort -n -k 15     (sort numerically on the 15th space separated column)
            ... will show the header data sorted by the distance of the station to the raob data.
            NOTE: to get data directly from the prepbufr file for comparison you can use the following:
            cd ...VxIngest
            . .venv/bin/activate
            python /Users/randy.pierce/VxIngest/third_party/NCEPLIBS-bufr/tests/dump_by_mnemonic.py /opt/data/prepbufr_to_cb/input_files/241011200.gdas.t12z.prepbufr.nr ADPUPA > /tmp/adpupa-verbose.txt

        """
        try:
            if self.report_file is None:
                with pathlib.Path(self.station_report_file_name).open(
                    "a"
                ) as report_file:
                    self.report_file = report_file
                    self.report_file.write("------\n")
                    self.report_file.write(
                        f"""geo:{station["geo"]} wmoid:{station["wmoid"]} currentYear:{station["currentYear"]} raob-lat:{lat} raob-lon:{lon} raob-elev:{elev}\n"""
                    )
                    self.report_file.write(
                        f"""header: station_id:{header["station_id"]} h-lat:{header["lat"]} h-lon:{header["lon"]} h-elev:{header["elevation"]} station-lat:{station["geo"][-1]["lat"]} station-lon:{station["geo"][-1]["lon"]} station-elev:{station["geo"][-1]["elev"]}"""
                        + f""" highest_interpolated_level:{highest_interpolated_level} level-diff:{round(abs(params_dict["level"] - highest_interpolated_level),4)}"""
                        + f""" lat-diff:{round(abs(lat - station["geo"][0]["lat"]),4)} lon-diff:{round(abs(lon - station["geo"][0]["lon"]),4)} elev-diff:{round(abs(elev - station["geo"][0]["elev"]),4)}"""
                        + f""" distance_km {round(math.dist([lat, lon], [station["geo"][0]["lat"], station["geo"][0]["lon"]]) * 111,4)}\n"""
                    )
                    self.report_file.write("observation pressures:")
                    op_str = []
                op_str = []
                for n in self.raw_obs_data[station_id][120]["obs_data"][
                    "pressure"
                ].compressed():
                    op_str.append(f"{str(n)} ")
                self.report_file.write(f""" {op_str} \n""")

                self.report_file.write("interpolated pressures:")
                op_str = []
                for n in self.interpolated_data[station_id][120]["data"]["pressure"]:
                    op_str.append(f"{str(n)} ")
                self.report_file.write(f""" {op_str} \n""")
        except Exception as _e:
            logger.error(
                "%s PrepbufrRaobsObsBuilderV01.print_dbg_rpt: Exception  error: %s",
                self.__class__.__name__,
                str(_e),
            )

    def get_valid_time_iso(self, params_dict=None):
        """
        This routine returns the valid time epoch converted to an iso string
        """
        # convert the file name to an epoch using the mask
        try:
            epoch = self.fcst_valid_epoch
            iso = convert_to_iso(epoch)
            return iso
        except Exception as _e:
            logger.error(
                "%s : Exception in named function derive_valid_time_iso:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
        return None

    def get_valid_time_epoch(self, params_dict=None):
        """
        This routine returns the valid time epoch
        """
        # convert the file name to an epoch using the mask
        try:
            epoch = self.fcst_valid_epoch
            return int(epoch)
        except Exception as _e:
            logger.error(
                "%s : Exception in named function derive_valid_time_epoch:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
        return None
