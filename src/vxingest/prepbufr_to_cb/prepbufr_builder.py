"""
Program Name: Class ApiBuilder.py
Contact(s): Randy Pierce
History Log:  Initial version
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSL
"""

import abc
import contextlib
import copy
import cProfile
import datetime
import logging
import math
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
    initialize_data_array,
)

# Get a logger with this module's name to help with debugging
logger = logging.getLogger(__name__)


# custom validation Exception for all masked data in a given level
class AllMaskedException(Exception):
    def __init__(self, message):
        super().__init__(message)


#  ApiBuilder← RaobObsBuilder ← RaobsGslObsBuilder
class PrepbufrBuilder(Builder):
    """parent class for API builders"""

    def __init__(self, load_spec, ingest_document):
        # api builders do not init the ingest_document. That happens in build_document
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
            level_idx = self.get_mandatory_levels().index(level)
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
                    # and in report_type 220 for wind data
                    if "stationName" in _ri:
                        value = stationName
                    else:
                        if "wind" in _ri.lower():
                            # look in report_type 220
                            if not self.interpolated_data[stationName][220]["data"]:
                                # this one has no data
                                value = None
                            else:
                                value = self.interpolated_data[stationName][220][
                                    "data"
                                ][_ri][level_idx]
                        else:
                            # look in report_type 120
                            if not self.interpolated_data[stationName][120]["data"]:
                                # this one has no data
                                value = None
                            else:
                                value = self.interpolated_data[stationName][120][
                                    "data"
                                ][_ri][level_idx]
                            # convert each station value to iso if necessary
                    if _ri.startswith("{ISO}"):
                        value = variable.replace("*" + _ri, convert_to_iso(value))
                    else:
                        value = variable.replace("*" + _ri, str(value))
                    try:  # make sure we have a number, if possible
                        value = float(value)
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
            fact = (float)(
                (math.log(nearest_higher_pressure) - math.log(wanted_pressure_level_mb))
                / (math.log(nearest_higher_pressure) - math.log(nearest_lower_pressure))
            )

            if variable == "wind_direction":  # if it is a wind_direction do this
                # interpolates wind directions in the range 0 - 359 degrees
                if (
                    nearest_lower_pressure is ma.masked
                    or nearest_higher_pressure is ma.masked
                ):
                    return ma.masked
                else:
                    next_higher_pressure_direction = obs_data["wind_direction"][
                        nearest_higher_pressure_index
                    ]
                    next_lower_pressure_direction = obs_data["wind_direction"][
                        nearest_lower_pressure_index
                    ]
                    dir_dif = (
                        next_lower_pressure_direction - next_higher_pressure_direction
                    )
                    if dir_dif > 180:
                        dir_dif -= 360
                    else:
                        if dir_dif < -180:
                            dir_dif += 360
                    value = next_higher_pressure_direction + fact * (dir_dif)
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
                if (
                    next_higher_pressure_variable_value is ma.masked
                    or next_lower_pressure_variable_value is ma.masked
                ):
                    return ma.masked
                else:
                    value = next_higher_pressure_variable_value + fact * (
                        nearest_lower_pressure - next_lower_pressure_variable_value
                    )
                    return value
        except Exception as _e:
            logging.error(
                "PrepBufrBuilder.interpolate_level: Exception  error: %s", str(_e)
            )
            return ma.masked

    def interpolate_data(self, raw_obs_data):
        """fill in the missing mandatory levels with interpolated data
            using the log difference interpolation method.
        Args:
            raw_data (): this is the raw data from the prepbufr file with missing heights having been interpolated
            using the hypsometric equation for thickness.
        Returns: the interpolated_data
        """
        try:
            interpolated_data = {}
            for station in raw_obs_data:
                if station not in interpolated_data:
                    interpolated_data[station] = {}
                for report in raw_obs_data[station]:
                    if report not in interpolated_data[station]:
                        interpolated_data[station][report] = {}
                        if "data" not in interpolated_data[station][report]:
                            interpolated_data[station][report]["data"] = {}
                    if not isinstance(
                        raw_obs_data[station][report]["obs_data"]["pressure"],
                        ma.core.MaskedArray,
                    ):
                        # I cannot process this station - there is no array of pressure data
                        del interpolated_data[station]
                        break
                    mandatory_levels = self.get_mandatory_levels()
                    for variable in raw_obs_data[station][report]["obs_data"]:
                        # create masked array for the variable with ALL the mandatory levels
                        # though the levels below the bottom level and above the top level will be masked
                        if variable not in interpolated_data[station][report]["data"]:
                            interpolated_data[station][report]["data"][variable] = (
                                ma.empty(shape=(len(mandatory_levels),))
                            )
                        if (
                            raw_obs_data[station][report]["obs_data"][variable].shape
                            == (0,)
                            or raw_obs_data[station][report]["obs_data"][variable].shape
                            == ()
                            or raw_obs_data[station][report]["obs_data"][
                                variable
                            ].mask.all()
                        ):
                            interpolated_data[station][report]["data"][variable] = (
                                ma.empty(shape=(len(mandatory_levels),))
                            )
                            # can't do this, there is no raw data for this variable - create a masked array of the proper shape
                            continue
                        # now we can interpolate the levels for each variable
                        for level_i, level in enumerate(mandatory_levels):
                            # find the nearest higher and lower pressure to this level
                            # find the nearest raw_obs_data pressure level
                            diff_arr = np.absolute(
                                raw_obs_data[station][report]["obs_data"][
                                    "pressure"
                                ].data
                                - level
                            )
                            nearest_i = diff_arr.argmin()
                            nearest_pressure = raw_obs_data[station][report][
                                "obs_data"
                            ]["pressure"][nearest_i]
                            if nearest_pressure == level:
                                # do not interpolate - this one is on the mandatory level
                                interpolated_data[station][report]["data"][variable][
                                    level_i
                                ] = raw_obs_data[station][report]["obs_data"][variable][
                                    nearest_i
                                ]
                            else:
                                # interpolate these values
                                if (
                                    nearest_i == 0
                                    or nearest_i
                                    == len(
                                        raw_obs_data[station][report]["obs_data"][
                                            "pressure"
                                        ]
                                    )
                                    - 1
                                ):
                                    # there is no higher or lower pressure level - have to mask this one
                                    interpolated_data[station][report]["data"][
                                        variable
                                    ][level_i] = ma.masked
                                    continue
                                if nearest_i >= 0:
                                    nearest_higher_pressure_index = nearest_i
                                    nearest_lower_pressure_index = nearest_i + 1
                                else:
                                    nearest_higher_pressure_index = nearest_i - 1
                                    nearest_lower_pressure_index = nearest_i
                                interpolated_data[station][report]["data"][variable][
                                    level_i
                                ] = self.interpolate_variable_for_level(
                                    variable,
                                    nearest_higher_pressure_index,
                                    nearest_lower_pressure_index,
                                    raw_obs_data[station][report]["obs_data"],
                                    level,
                                )
        except Exception as _e:
            logging.error(
                "PrepBufrBuilder.interpolate_data: Exception  error: %s", str(_e)
            )
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
            if "level" in self.template:
                # this is a multilevel template. We need to process each level in mandatory levels.
                for level in self.get_mandatory_levels():
                    new_document = copy.deepcopy(self.template)
                    # make a copy of the template, which will become the new document
                    # once all the translations have occurred
                    # set the level right away (it is needed for the handle_data)
                    # clean out the data template from the data portion of the newDocument
                    new_document["data"] = {}
                    new_document["level"] = level
                    for key in self.template:
                        if key == "level":
                            continue
                        if key == "data":
                            try:
                                new_document = self.handle_data(level, doc=new_document)
                            except AllMaskedException as _ame:
                                # this data is all masked at this level. Cannot use this document.
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
            else:
                new_document = copy.deepcopy(self.template)
                # make a copy of the template, which will become the new document
                # once all the translations have occurred
                new_document = initialize_data_array(new_document)
                for key in self.template:
                    if key == "data":
                        new_document = self.handle_data(level, doc=new_document)
                        continue
                    # stationName not needed, this is not a 'data' section key
                    new_document = self.handle_key(level, new_document, key)
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
            if key == "*stationName":
                doc[key] = stationName
                return doc
            if isinstance(doc[key], dict):
                # process an embedded dictionary
                tmp_doc = copy.deepcopy(self.template[key])
                for sub_key in tmp_doc:
                    tmp_doc = self.handle_key(tmp_doc, level, sub_key)  # recursion
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
        # noinspection PyBroadException
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
            data_elem = {}
            for _station_name in self.interpolated_data:
                level_index = self.get_mandatory_levels().index(level)
                data_elem["stationName"] = _station_name
                data_key = next(iter(self.template["data"]))
                data_template = self.template["data"][data_key]
                try:
                    all_masked = True
                    for key in data_template:
                        report_type = 220 if "wind" in key.lower() else 120
                        # don't use non data keys (like stationName) as a test for masking
                        if key in self.interpolated_data[_station_name][report_type][
                            "data"
                        ] and not ma.is_masked(
                            self.interpolated_data[_station_name][report_type]["data"][
                                key
                            ][level_index]
                        ):
                            # we actually have un-masked data so we will process this station
                            all_masked = False
                            break
                    if all_masked is True:
                        # don't use this document for this level - all the data is masked
                        raise AllMaskedException("All values are masked")
                    for key in data_template:
                        try:
                            value = data_template[key]
                            # values can be null...
                            if value and value.startswith("&"):
                                value = self.handle_named_function(
                                    _station_name, level, value
                                )
                            else:
                                value = self.translate_template_item(
                                    _station_name, level, value
                                )
                        except ValueError as _ve:
                            # this was logged already - dont log it again
                            raise _ve
                        except Exception as _e:
                            value = None
                            logging.warning(
                                "%s Builder.handle_data - value is None",
                                self.__class__.__name__,
                            )
                            raise _e  # probably cannot use this document - throw it away
                        data_elem[key] = value
                    if data_key.startswith("&"):
                        data_key = self.handle_named_function(
                            _station_name, level, data_key
                        )
                    else:
                        data_key = self.translate_template_item(
                            _station_name, level, data_key
                        )
                    if data_key is None:
                        logging.warning(
                            "%s Builder.handle_data - _data_key is None",
                            self.__class__.__name__,
                        )
                    doc["data"][data_key] = data_elem
                except ValueError as _ve:
                    continue  # do not use this one - we didn't have enough data to create a new station document
            return doc
        except AllMaskedException as _ame:
            raise _ame
        except Exception as _e:
            logging.exception(
                "%s handle_data: Exception instantiating builder",
                self.__class__.__name__,
            )
        return doc

    def create_raw_data_id(self):
        """
        This method will create a raw data id for the raw data document.
        """
        return f"DD:{self.subset}:RAW_OBS:GDAS:prepbufr:V01:{self.fcst_valid_epoch}"

    def de_mask_raw_obs_data(self):
        """
        This method will convert the masked arrays in the raw_obs_data to lists.
        """
        raw_obs_data_de_masked = {}
        for station in self.raw_obs_data:
            raw_obs_data_de_masked[station] = {}
            for report in self.raw_obs_data[station]:
                raw_obs_data_de_masked[station][report] = {}
                raw_obs_data_de_masked[station][report]["obs_data"] = {}
                for variable in self.raw_obs_data[station][report]["obs_data"]:
                    if isinstance(
                        self.raw_obs_data[station][report]["obs_data"][variable],
                        ma.core.MaskedArray,
                    ):
                        raw_obs_data_de_masked[station][report]["obs_data"][
                            variable
                        ] = self.raw_obs_data[station][report]["obs_data"][
                            variable
                        ].data.tolist()
                    else:
                        raw_obs_data_de_masked[station][report]["obs_data"][
                            variable
                        ] = self.raw_obs_data[station][report]["obs_data"][variable]
        return raw_obs_data_de_masked

    def build_raw_data_doc(self, raw_data_id):
        """This method will build a raw data document for the prepbufr builder raw_obs_data.
        The raw data document will contain the raw_obs_data that is read from the prepbufr file.
        The raw_obs_data contains masked arrays which are not suitable for json serialization.
        The masked arrays are converted to lists before being added to the raw data document.
        Args:
            raw_data_id (string): the id that will be used for the raw data document
        Returns:
            json: the raw data document
        """
        de_masked_data = self.de_mask_raw_obs_data()
        rd_doc = {
            "id": raw_data_id,
            "type": "DD",
            "docType": "RAW_OBS",
            "subset": self.subset,
            "dataSourceId": "GDAS",
            "fcstValidISO": self.get_valid_time_iso(),
            "fcstValidEpoch": self.get_valid_time_epoch(),
            "version": "V01",
            "fileType": "prepbufr",
            "originType": "GDAS",
            "data": de_masked_data,
        }
        return rd_doc

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
            collection = self.load_spec["cb_connection"]["collection"]
            # collection is set to "RAOB" in the run_ingest
            template_doc = (
                self.load_spec["collection"]
                .get("MD:V01:RAOB:ingest:mnemonic_mapping:prepbufr")
                .content_as[dict]
            )
            self.raw_obs_data = self.read_data_from_file(queue_element, template_doc)
            try:
                self.interpolated_data = self.interpolate_data(self.raw_obs_data)
            except Exception as _e:
                logger.error(
                    "PrepBufrBuilder.build_document: Exception  error: %s", str(_e)
                )
                return {}
            except RuntimeWarning as rw:
                logger.error(
                    "PrepBufrBuilder.build_document: RuntimeWarning  error: %s", str(rw)
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
            # add the raw data doc to the document map
            raw_data_id = self.create_raw_data_id()
            raw_data_doc = self.build_raw_data_doc(raw_data_id)
            document_map[raw_data_doc["id"]] = raw_data_doc
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
        self.subset = self.template["subset"]
        self.raw_obs_data = {}
        self.interpolated_data = {}
        self.mandatory_levels = []
        self.station_reference = {}
        # self.print_debug_report = False
        self.print_debug_report = True
        self.report_file = None
        if self.print_debug_report:
            with contextlib.suppress(OSError):
                Path("/tmp/report.txt").unlink()
            print("debug report is in /tmp/report.txt")
        # self.do_profiling = True  # set to True to enable build_document profiling
        self.do_profiling = False  # set to True to enable build_document profiling

    def get_mandatory_levels(self):
        """
        This method gets the mandatory levels for the raw data set.
        :param report: the bufr report i.e. the subset.report_type
        :return: the mandatory levels
        """
        if not self.mandatory_levels:
            self.mandatory_levels = list(range(1010, 10, -10))
        return self.mandatory_levels

    def get_relative_humidity(
        self, relative_humidity, pressure, temperature, specific_humidity
    ):
        """
        This method calculates the relative humidity from the specific humidity, if necessary
        :param relative_humidity: the relative humidity data - sometimes is not present
        :param pressure: the pressure data
        :param temperature: the temperature data
        :param specific_humidity: the specific humidity data
        :return: the relative humidity data

        example:
        relative_humidity_from_specific_humidity(pressure, temperature, specific_humidity)  all pint.Quantity
        relative_humidity_from_specific_humidity(1013.25 * units.hPa, 30 * units.degC, 18/1000).to('percent')
        """
        try:
            if (
                (
                    not ma.isMaskedArray(pressure)
                    or ma.all(ma.is_masked(pressure))
                    or pressure.shape == ()
                )
                or (
                    not ma.isMaskedArray(temperature)
                    or ma.all(ma.is_masked(temperature))
                    or temperature.shape == ()
                )
                or (
                    not ma.isMaskedArray(specific_humidity)
                    or ma.all(ma.is_masked(specific_humidity))
                    or specific_humidity.shape == ()
                )
            ):
                _ret_array = np.full(temperature.shape, np.nan)
                _mask = np.full(temperature.shape, True)
                return ma.masked_array(_ret_array, mask=_mask)
            sh_data = specific_humidity.data
            # this is a HACK! - the specific humidity data is sometimes 1000
            # and the metpy function has a divide by zero RuntimeWarning when it is 1000!
            if 1000 in sh_data:
                sh_idx = np.where(sh_data == 1000)
                sh_data[sh_idx] = 999
            _relative_humidity = metpy.calc.relative_humidity_from_specific_humidity(
                pressure.data * units.hPa,
                temperature.data * units.degC,
                sh_data / 1000,
            ).to("percent")
            # make a masked array
            return ma.masked_array(
                data=_relative_humidity, mask=False, fill_value=math.nan
            )
        except Exception as _e:
            logger.error(
                "PrepBufrBuilder.get_relative_humidity: Exception  error: %s", str(_e)
            )
            _ret_array = np.full(temperature.shape, np.nan)
            _mask = np.full(temperature.shape, True)
            return ma.masked_array(_ret_array, mask=_mask)

    def interpolate_heights(self, height, pressure, temperature, specific_humidity):
        """
        This method interpolates the heights that are missing in the height data
        using the hypsometric thickness equation
        :param height: the height data
        :return: the heights nd_array

        examples:

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
        try:
            # if the height is not a masked array - make it one
            if not ma.isMaskedArray(height):
                height = ma.masked_invalid(height)
            # save the original height mask in the raw data
            original_height_mask = height.mask
            # calculate the thickness for each layer and update the masked array
            # if the height is totally masked or the shape is () - it is a scalar - I don't know what to do
            if ma.is_masked(height) or height.mask.all() or height.shape == ():
                return height, original_height_mask
            # interpolate the heights
            # start at the bottom and work up
            # first - calculate the mixing ratio from the specific humidity for the entire array
            try:
                _mixing_ratio = metpy.calc.mixing_ratio_from_specific_humidity(
                    specific_humidity
                ).to("g/kg")
            except Exception as _e:
                logger.error(
                    "PrepBufrBuilder.interpolate_heights: Exception  error: %s", str(_e)
                )
                return height, original_height_mask

            # now determine the missing layers

            i = 0
            len(height)
            while i < len(height):  # iterate the masked heights
                if ma.is_masked(height[i]) or math.isnan(height[i]):
                    # get the height from the hydrostatic thickness using the layer below and the next layer above that has data
                    # what is the next layer above that has data?
                    j = i + 1
                    while j < len(height) and ma.is_masked(height[j]):
                        j = j + 1
                    # now height[i-1] (or height[0]) is the layer below that has data i.e. the bottom
                    # and height[j] is the next layer above that has data i.e. the top
                    top = j if j < len(height) else len(height) - 1
                    bottom = 0 if i == 0 else i - 1
                    p = pressure.data * units.hPa
                    t = temperature.data * units.degC
                    mr = _mixing_ratio.data * units.dimensionless
                    layer = (pressure <= pressure[bottom]) & (pressure >= pressure[top])
                    _height = metpy.calc.thickness_hydrostatic(
                        pressure=p[layer],
                        temperature=t[layer],
                        mixing_ratio=mr[layer],
                        molecular_weight_ratio=0.6219569100577033,
                    )
                    while (
                        i < j
                    ):  # remember i is the bottom masked layer and j is the next layer above that has data
                        height[i] = round(_height.magnitude, 1)
                        # assigning a valid value to height[i] unmasks that value
                        # does this need to be added to the height of the layer below?
                        # i.e. _height.magnitude + height[i - 1]
                        # go to the next one
                        i = i + 1
                else:
                    i = i + 1  # this one was not masked so go to the next one
            return height, original_height_mask
        except RuntimeWarning as rw:
            logger.error(
                "PrepBufrBuilder.interpolate_heights: RuntimeWarning  error: %s",
                str(rw),
            )
            return height, original_height_mask

    def read_data_from_bufr(self, bufr, template):
        """
        This method reads the data from the bufr file according to a provided template.
        A template is a dict keyed by the desired field name with a value that is a
        dict with a mnemonic and an intent. The mnemonic is the bufr mnemonic for the field
        and the intent is the datatype of the field in the resulting data document.
        For example station_id "SID" returns a float but the intent is str.
        :param bufr: the bufr file
        :template: a dictionary of header keys with their corresponding mnemonics and intended types
        refer to https://www.emc.ncep.noaa.gov/emc/pages/infrastructure/bufrlib/tables/bufrtab_tableb.html
        example: see prepbufr_raob_template.json

        :return: the data
        """
        # see read_subset https://github.com/NOAA-EMC/NCEPLIBS-bufr/blob/a8108e591c6cb1e21ddc7ddb6715df1b3801fff8/python/ncepbufr/__init__.py#L449
        mnemonics = [o["mnemonic"] for o in template.values()]
        bufr_data = bufr.read_subset(" ".join(mnemonics)).squeeze()
        data = {}
        for i, mnemonic in enumerate(mnemonics):
            try:
                field = [k for k, v in template.items() if v["mnemonic"] == mnemonic]
                if field[0] == "relative_humidity":
                    # need to get some specific fields to calculate the relative humidity
                    relative_humidity_index = mnemonics.index("RHO")
                    pressure_index = mnemonics.index("POB")
                    temperature_index = mnemonics.index("TOB")
                    specific_humidity_index = mnemonics.index("QOB")
                    data[field[0]] = self.get_relative_humidity(
                        bufr_data[
                            relative_humidity_index
                        ],  # relative_humidity - sometimes is missing
                        bufr_data[pressure_index],  # pressure
                        bufr_data[temperature_index],  # temperature
                        bufr_data[specific_humidity_index],  # specific_humidity
                    )
                else:
                    if field[0] == "height":
                        # need to get some specific fields to interpolate the height
                        height_index = mnemonics.index("ZOB")
                        pressure_index = mnemonics.index("POB")
                        temperature_index = mnemonics.index("TOB")
                        specific_humidity_index = mnemonics.index("QOB")
                        data[field[0]], _original_mask = self.interpolate_heights(
                            bufr_data[
                                height_index
                            ],  # height - fields are sometimes missing
                            bufr_data[pressure_index],  # pressure
                            bufr_data[temperature_index],  # temperature
                            bufr_data[specific_humidity_index],  # specific_humidity
                        )
                    else:
                        match template[field[0]]["intent"]:
                            case "int":
                                if not ma.isMaskedArray(bufr_data[i]):
                                    data[field[0]] = int(bufr_data[i])
                                else:
                                    data[field[0]] = bufr_data[i].astype(int)

                            case "float":
                                if not ma.isMaskedArray(bufr_data[i]):
                                    data[field[0]] = round(bufr_data[i], 3)
                                else:
                                    data[field[0]] = bufr_data[i].round(3)
                            case "str":
                                data[field[0]] = str(
                                    bufr_data[i], encoding="utf-8"
                                ).strip()
                            case _:
                                data[field][0] = bufr_data[i]
            except Exception as _e:
                logger.error(
                    "PrepBufrBuilder.read_data_from_bufr: Exception  error: %s", str(_e)
                )
                data[field[0]] = None
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
        _dt = datetime.datetime.strptime(str(date_str), "%Y%m%d%H")
        _epoch = int(_dt.strftime("%s"))
        return _epoch

    def read_data_from_file(self, queue_element, templates):
        """read data from the prepbufr file, filter messages for appropriate ones,
        and load them raw into a raw dictionary structure. Use hypsometric equation to
        calculate heights from pressure for determining unknown heights. Load everything into
        a dictionary structure, so that mandatory levels can be interpolated for every 10mb
         using weighted logarithmic interpolation.
        Args:
            queue_element: the file name to read
        Transformations: 1) pressure to height 2) temperature to dewpoint 3) meters per second to miles per hour
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
                    if header_data["report_type"] not in templates["bufr_report_types"]:
                        continue
                    # if we already have data for this station - skip it
                    subset_data["station_id"] = header_data["station_id"]
                    subset_data["report_type"] = round(header_data["report_type"])
                    subset_data["fcst_valid_epoch"] = self.fcst_valid_epoch
                    subset_data["header"] = header_data
                    # read the q_marker data
                    q_marker_data = self.read_data_from_bufr(
                        bufr, templates["q_marker"]
                    )
                    subset_data["q_marker"] = q_marker_data
                    # read the obs_err data
                    obs_err_data = self.read_data_from_bufr(bufr, templates["obs_err"])
                    subset_data["obs_err"] = obs_err_data
                    # read the obs data
                    logger.debug(
                        f"{subset_data["station_id"]}, {subset_data["report_type"]}"
                    )
                    obs_data = self.read_data_from_bufr(bufr, templates["obs_data"])
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
            if value == "--" or value == "" or value == "None" or value is None:
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
            if value == "--" or value == "" or value == "None" or value is None:
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
            highest_interpolated_level = self.interpolated_data[station_id][120][
                "data"
            ]["pressure"].compressed()[0]
            fcst_valid_epoch = self.raw_obs_data[station_id][120]["fcst_valid_epoch"]
            header = self.raw_obs_data[station_id][120]["header"]
            an_id = None
        except Exception as _e:
            logger.error(
                "%s handle_station: Exception in handle_station:  error: %s station_id: %s header: %s",
                self.__class__.__name__,
                str(_e),
                station_id,
                header,
            )
            raise ValueError("bad data in handle_station") from _e
        if level != highest_interpolated_level:
            # we don't need to process the station except at the highest interpolated level
            return header["station_id"]
        try:
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

                    if self.print_debug_report:
                        self.write_dbg_rpt(
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
            return params_dict["stationName"]
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

    def write_dbg_rpt(
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
        The output is a file in /tmp/report.txt that will be overwritten each time this is used.
        The output is a series of lines that show the differences between the station and the raob data, with
        the particular columns separated by a space.
        You can grep and sort the output to get more specific views of the data.
        for example:
            grep  header /tmp/report.txt | sort -n -k 15     (sort numerically on the 15th space separated column)
            ... will show the header data sorted by the distance of the station to the raob data.
            NOTE: to get data directly from the prepbufr file for comparison you can use the following:
            cd ...VxIngest
            . .venv/bin/activate
            python /Users/randy.pierce/VxIngest/third_party/NCEPLIBS-bufr/tests/dump_by_mnemonic.py /opt/data/prepbufr_to_cb/input_files/241011200.gdas.t12z.prepbufr.nr ADPUPA > /tmp/adpupa-verbose.txt

        """
        try:
            if self.report_file is None:
                self.report_file = open("/tmp/report.txt", "a")
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
