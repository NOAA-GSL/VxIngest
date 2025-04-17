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
import re
from pathlib import Path
from pstats import Stats

import netCDF4 as nc
import numpy.ma as ma
from metpy.calc import relative_humidity_from_dewpoint, wind_components
from metpy.units import units

from vxingest.builder_common.builder import Builder
from vxingest.builder_common.builder_utilities import (
    convert_to_iso,
    initialize_data_array,
)

# Get a logger with this module's name to help with debugging
logger = logging.getLogger(__name__)


class NetcdfBuilder(Builder):
    """parent class for netcdf builders"""

    def build_document(self, queue_element):
        pass

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

    def get_database_connection_details(self, queue_element):
        bucket = self.load_spec["cb_connection"]["bucket"]
        scope = self.load_spec["cb_connection"]["scope"]
        collection = self.load_spec["cb_connection"]["collection"]

        # stash the file_name so that it can be used later
        self.file_name = Path(queue_element).name
        return bucket, scope, collection

    def build_document_map(self, queue_element, base_var_name, origin_type=None):
        self.initialize_document_map()
        logger.info(
            "%s building documents for file %s",
            self.__class__.__name__,
            queue_element,
        )
        if self.do_profiling:
            with cProfile.Profile() as _pr:
                self.handle_document(base_var_name)
                with Path("profiling_stats.txt").open("w", encoding="utf-8") as stream:
                    stats = Stats(_pr, stream=stream)
                    stats.strip_dirs()
                    stats.sort_stats("time")
                    stats.dump_stats("profiling_stats.prof")
                    stats.print_stats()
        else:
            self.handle_document(base_var_name)

        document_map = self.get_document_map(base_var_name)
        data_file_id = self.create_data_file_id(
            self.subset, "netcdf", origin_type, queue_element
        )
        data_file_doc = self.build_datafile_doc(
            file_name=queue_element, data_file_id=data_file_id, origin_type=origin_type
        )
        document_map[data_file_doc["id"]] = data_file_doc
        return document_map

    def build_3d_document_map(
        self, queue_element, base_var_name, data_key_var_name, origin_type=None
    ):
        # this is a 3D netcdf file
        # we have to process the base_var_name and then
        # process the 3D data
        # for example for the fireweather data the time is the unlimited var
        # for all the documents in the file i.e. one document for each valid time
        # but within a single document the unlimited var is height i.e. there is
        # one data element within a given document for each height level.
        # we will pass in the index of the top level unlimited var 'time' and the name of the data_key_var
        # 'height' which will be the unlimited var for each individual time document.
        self.initialize_document_map()
        logger.info(
            "%s building documents for file %s",
            self.__class__.__name__,
            queue_element,
        )
        base_var_size = self.ncdf_data_set[base_var_name].size
        if base_var_size == 0:
            logger.info(
                "%s building documents for file %s - no data",
                self.__class__.__name__,
                queue_element,
            )
            return {}
        logger.info(
            "%s building documents for file %s - processing unlimited var %s",
            self.__class__.__name__,
            queue_element,
            base_var_name,
        )

        for base_var_index in range(base_var_size):
            # we have to process the base_var_name with the 3D data.
            # for example for the fireweather data the 'time' is the unlimited var
            # for all the documents in the file i.e. one document for each valid time
            # but within a single document the unlimited var is height i.e. there is
            # one data element within a given document for each height level.
            if self.do_profiling:
                with cProfile.Profile() as _pr:
                    self.handle_3d_document(
                        base_var_name, base_var_index, data_key_var_name
                    )
                    with Path("profiling_stats.txt").open(
                        "w", encoding="utf-8"
                    ) as stream:
                        stats = Stats(_pr, stream=stream)
                        stats.strip_dirs()
                        stats.sort_stats("time")
                        stats.dump_stats("profiling_stats.prof")
                        stats.print_stats()
            else:
                self.handle_3d_document(
                    base_var_name, base_var_index, data_key_var_name
                )

            document_map = self.get_document_map(base_var_name)
            data_file_id = self.create_data_file_id(
                self.subset, "netcdf", origin_type, queue_element
            )
            data_file_doc = self.build_datafile_doc(
                file_name=queue_element,
                data_file_id=data_file_id,
                origin_type=origin_type,
            )
        document_map[data_file_doc["id"]] = data_file_doc
        return document_map

    def derive_id(self, **kwargs):
        """
        This is a private method to derive a document id from the current base_var_index,
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
            base_var_index = kwargs["base_var_index"]
            parts = template_id.split(":")
            new_parts = []
            for part in parts:
                if part.startswith("&"):
                    value = str(self.handle_named_function(part, base_var_index))
                else:
                    if part.startswith("*"):
                        value = str(self.translate_template_item(part, base_var_index))
                    else:
                        value = str(part)
                new_parts.append(value)
            new_id = ":".join(new_parts)
            return new_id
        except Exception as _e:
            logger.exception("NetcdfBuilder.derive_id: Exception  error: %s")
            return None

    def translate_template_item(self, variable, base_var_index):
        """
        This method translates template replacements (*item).
        It can translate keys or values.
        :param variable: a value from the template - should be a netcdf variable
        :param recNum: the current recNum
        :return:
        """
        replacements = []

        try:
            if isinstance(variable, str):
                replacements = variable.split("*")[1:]
            if len(replacements) == 0:
                # it is a literal, not a replacement (doesn't start with *)
                return variable
            make_str = False
            value = variable
            Smatch = re.compile(".*S.*")
            Umatch = re.compile(".*U.*")
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
                                "*{ISO}"
                                + nc.chartostring(
                                    self.ncdf_data_set[variable][base_var_index]
                                )
                            )
                        else:
                            # for these we have to convert convert to ISO (it is probably an epoch)
                            value = convert_to_iso(
                                "*{ISO}" + self.ncdf_data_set[variable][base_var_index]
                            )
                    else:
                        variable = value.replace("*", "")
                        if make_str:
                            if chartostring:
                                # it is a char array of something
                                value = value.replace(
                                    "*" + _ri,
                                    str(
                                        nc.chartostring(
                                            self.ncdf_data_set[variable][base_var_index]
                                        )
                                    ),
                                )
                                return value
                            else:
                                # it is probably a number
                                value = str(
                                    self.ncdf_data_set[variable][base_var_index]
                                )
                                return value
                        else:
                            # it desn't need to be a string
                            size = self.ncdf_data_set[variable][base_var_index].size
                            if size <= 1:
                                return self.ncdf_data_set[variable][
                                    base_var_index
                                ].data.item()
                            else:
                                return self.ncdf_data_set[variable][
                                    base_var_index
                                ].tolist()
        except Exception as _e:
            logger.exception(
                "Builder.translate_template_item for variable %s: replacements: %s",
                str(variable),
                str(replacements),
            )
        return value

    def handle_document(self, base_var_name):
        """
        This routine processes the complete document (essentially a complete netcdf file)
        Each template key or value that corresponds to a variable will be selected from
        the netcdf file into a netcdf data set and then
        each station will get values from the record.
        :return: The modified document_map
        """

        try:
            new_document = copy.deepcopy(self.template)
            number_of_docs = self.ncdf_data_set.dimensions[base_var_name].size
            if number_of_docs == 0:
                return
            # make a copy of the template, which will become the new document
            # once all the translations have occured
            new_document = initialize_data_array(new_document)
            for base_var_index in range(number_of_docs):
                for key in self.template:
                    if key == "data":
                        new_document = self.handle_data(
                            base_var_index=base_var_index,
                            doc=new_document,
                            data_key_var=base_var_name,
                        )
                        continue
                    new_document = self.handle_key(
                        new_document, base_var_name, base_var_index, key
                    )
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
        except Exception as _e:
            logger.error(
                "NetcdfBuilder.handle_document: Exception instantiating builder: %s error: %s",
                self.__class__.__name__,
                str(_e),
            )
            raise _e

    def handle_3d_document(self, base_var_name, base_var_index, data_key_var_name):
        """
        This routine processes the complete 3d document (one record of the unlimited var)
        Each template key or value that corresponds to a variable will be selected from
        the netcdf file into a netcdf data set.
        :return: The modified document_map
        """

        try:
            new_document = copy.deepcopy(self.template)
            # get the unlimited var
            data_key_var_data_size = self.ncdf_data_set.dimensions[
                data_key_var_name
            ].size
            if data_key_var_data_size == 0:
                return
            # make a copy of the template, which will become the new document
            # once all the translations have occured
            new_document = initialize_data_array(new_document)
            for data_key_var_index in range(data_key_var_data_size):
                for key in self.template:
                    if key == "data":
                        new_document = self.handle_3d_data(
                            base_var_index=base_var_index,
                            data_key_var_name=data_key_var_name,
                            doc=new_document,
                            data_key_var=data_key_var_index,
                        )
                        continue
                    new_document = self.handle_3d_key(
                        new_document,
                        base_var_index,
                        data_key_var_name,
                        key,
                        data_key_var_index,
                    )
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
        except Exception as _e:
            logger.error(
                "NetcdfBuilder.handle_document: Exception instantiating builder: %s error: %s",
                self.__class__.__name__,
                str(_e),
            )
            raise _e

    def handle_3d_data(self, **kwargs):
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
            base_var_index = kwargs["base_var_index"]
            data_elem = {}
            data_key = next(iter(self.template["data"]))
            data_template = self.template["data"][data_key]
            for key in data_template:
                try:
                    value = data_template[key]
                    # values can be null...
                    if value and value.startswith("&"):
                        value = self.handle_named_function(value, base_var_index)
                    else:
                        value = self.translate_template_item(value, base_var_index)
                except Exception as _e:
                    value = None
                    logger.warning(
                        "%s Builder.handle_3d_data - value is None",
                        self.__class__.__name__,
                    )
                data_elem[key] = value
            if data_key.startswith("&"):
                data_key = self.handle_named_function(data_key, base_var_index)
            else:
                data_key = self.translate_template_item(data_key, base_var_index)
            if data_key is None:
                logger.warning(
                    "%s Builder.handle_3d_data - _data_key is None",
                    self.__class__.__name__,
                )
            # add the height and load the raw data into the document - convert km to m
            data_elem["height"] = [
                i * 1000 for i in self.ncdf_data_set["height"][:].tolist()
            ]
            doc["raw_data"] = data_elem
            # interpolate the data
            interpolated_data = self.interpolate_3d_data(data_elem)
            doc["data"] = interpolated_data
            return doc
        except Exception as _e:
            logger.exception(
                "%s handle_3d_data: Exception instantiating builder",
                self.__class__.__name__,
            )
        return doc

    def handle_3d_key(
        self, doc, base_var_index, base_var_name, key, data_key_var_index
    ):
        """
        This routine handles keys by substituting
        the netcdf variables that correspond to the key into the values
        in the template that begin with *
        :param doc: the current document
        :param base_var_index: The current unlimited variable
        :param _key: A key to be processed, This can be a key to a primitive,
        or to another dictionary, or to a named function
        """

        try:
            if key == "id":
                an_id = self.derive_id(
                    base_var_index=base_var_index,
                    template_id=self.template["id"],
                )
                if an_id not in doc:
                    doc["id"] = an_id
                return doc
            if isinstance(doc[key], dict):
                # process an embedded dictionary
                tmp_doc = copy.deepcopy(self.template[key])
                for sub_key in tmp_doc:
                    tmp_doc = self.handle_3d_key(
                        tmp_doc,
                        base_var_index,
                        base_var_name,
                        sub_key,
                        data_key_var_index,
                    )  # recursion
                doc[key] = tmp_doc
            if (
                not isinstance(doc[key], dict)
                and isinstance(doc[key], str)
                and doc[key].startswith("&")
            ):
                doc[key] = self.handle_named_function(doc[key], base_var_index)
            else:
                doc[key] = self.translate_template_item(doc[key], base_var_index)
            return doc
        except Exception as _e:
            logger.exception(
                "%s NetcdfBuilder.handle_key: Exception in builder",
                self.__class__.__name__,
            )
        return doc

    def interpolate_3d_data(self, raw_data):
        """
        This function is used to interpolate the data to the standard levels.
        Args:
            raw_data (dict): raw data
        Returns:
            [dict]: interpolated data

        The standard levels are:
        [every 20 m, up to 200 m]  (10 levs)
        20, 40, 60, 80, 100, 120, 140, 160, 180, 200,
        [then every 50 m, up to 500 m]  (8 levs)
        250, 300, 250, 300, 350, 400, 450, 500,
        [then every 100 m, up to 2000 m]  (14 levs)
        600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900, 2000
        [then every 200 m, up to 5000 m]  (15 levs)
        2200, 2400, 2600, 2800….
        """
        # The standard levels are:
        standard_levels = [
            20,
            40,
            60,
            80,
            100,
            120,
            140,
            160,
            180,
            200,
            250,
            300,
            250,
            300,
            350,
            400,
            450,
            500,
            600,
            700,
            800,
            900,
            1000,
            1100,
            1200,
            1300,
            1400,
            1500,
            1600,
            1700,
            1800,
            1900,
            2000,
            2200,
            2400,
            2600,
            2800,
            3000,
            3200,
            3400,
            3600,
            3800,
            4000,
            4200,
            4400,
            4600,
            4800,
            5000,
        ]
        try:
            # Interpolate the data to the standard levels
            # raw heights are the valuse of the height variable in the netcdf file
            interpolated_data = {}
            heights = raw_data["height"]
            for level in standard_levels:
                if any(x == level for x in heights):
                    # If the level is already in the data, just copy it
                    for variable in raw_data:
                        if variable != "height":
                            continue
                        interpolated_data[variable][level] = raw_data[variable][
                            heights.index(level)
                        ]
                else:
                    # If the level is not in the data, we need to interpolate
                    # Find the two closest raw heights below and above the level
                    lower_height = max([h for h in heights if h < level])
                    upper_height = min([h for h in heights if h > level])
                    # Interpolate the data for this height
                    for variable in raw_data:
                        if variable == "height":
                            continue
                        # Linear interpolation
                        lower_value = raw_data[variable][heights.index(lower_height)]
                        upper_value = raw_data[variable][heights.index(upper_height)]
                        interpolated_value = lower_value + (
                            upper_value - lower_value
                        ) * ((level - lower_height) / (upper_height - lower_height))
                        if variable not in interpolated_data:
                            interpolated_data[variable] = {}
                        if level not in interpolated_data[variable]:
                            interpolated_data[variable][level] = {}
                        interpolated_data[variable][level] = interpolated_value
        except Exception as _e:
            logger.error(
                "%s : Exception in named function interpolate_data:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
        return interpolated_data

    def handle_key(self, doc, base_var_name, base_var_index, key):
        """
        This routine handles keys by substituting
        the netcdf variables that correspond to the key into the values
        in the template that begin with *
        :param doc: the current document
        :param base_var_index: The current unlimited variable
        :param _key: A key to be processed, This can be a key to a primitive,
        or to another dictionary, or to a named function
        """

        try:
            if key == "id":
                an_id = self.derive_id(
                    base_var_index=base_var_index,
                    template_id=self.template["id"],
                )
                if an_id not in doc:
                    doc["id"] = an_id
                return doc
            if isinstance(doc[key], dict):
                # process an embedded dictionary
                tmp_doc = copy.deepcopy(self.template[key])
                for sub_key in tmp_doc:
                    tmp_doc = self.handle_key(
                        tmp_doc, base_var_name, base_var_index, sub_key
                    )  # recursion
                doc[key] = tmp_doc
            if (
                not isinstance(doc[key], dict)
                and isinstance(doc[key], str)
                and doc[key].startswith("&")
            ):
                doc[key] = self.handle_named_function(doc[key], base_var_index)
            else:
                doc[key] = self.translate_template_item(doc[key], base_var_index)
            return doc
        except Exception as _e:
            logger.exception(
                "%s NetcdfBuilder.handle_key: Exception in builder",
                self.__class__.__name__,
            )
        return doc

    def handle_named_function(self, named_function_def, base_var_index):
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
        :base_var_index the base_var_index being processed.
        """

        func = None
        try:
            func = named_function_def.split("|")[0].replace("&", "")
            params = named_function_def.split("|")[1].split(",")
            dict_params = {"base_var_index": base_var_index}
            for _p in params:
                # be sure to slice the * off of the front of the param
                dict_params[_p[1:]] = self.translate_template_item(_p, base_var_index)
            # call the named function using getattr
            replace_with = getattr(self, func)(dict_params)
        except Exception as _e:
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
            base_var_index = kwargs["base_var_index"]
            data_elem = {}
            data_key = next(iter(self.template["data"]))
            data_template = self.template["data"][data_key]
            for key in data_template:
                try:
                    value = data_template[key]
                    # values can be null...
                    if value and value.startswith("&"):
                        value = self.handle_named_function(value, base_var_index)
                    else:
                        value = self.translate_template_item(value, base_var_index)
                except Exception as _e:
                    value = None
                    logger.warning(
                        "%s Builder.handle_data - value is None",
                        self.__class__.__name__,
                    )
                data_elem[key] = value
            if data_key.startswith("&"):
                data_key = self.handle_named_function(data_key)
            else:
                data_key = self.translate_template_item(data_key, base_var_index)
            if data_key is None:
                logger.warning(
                    "%s Builder.handle_data - _data_key is None",
                    self.__class__.__name__,
                )
            self.load_data(doc, data_key, data_elem)
            return doc
        except Exception as _e:
            logger.exception(
                "%s handle_data: Exception instantiating builder",
                self.__class__.__name__,
            )
        return doc

    def build_datafile_doc(self, file_name, data_file_id, origin_type):
        """
        This method will build a dataFile document for NetcdfBuilder. The dataFile
        document will represent the file that is ingested by the Builder. The document
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
            "fileType": "netcdf",
            "originType": origin_type,
            "loadJobId": self.load_spec["load_job_doc"]["id"],
            "dataSourceId": origin_type,
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

    def get_document_map(self, base_var_name):
        """
        In case there are leftovers we have to process them first so call the handle_document method again.
        :return: the document_map
        """
        try:
            if len(self.same_time_rows) != 0:
                self.handle_document(base_var_name)
            return self.document_map
        except Exception as _e:
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
        if "data" not in doc or doc["data"] is None:
            doc["data"] = {}
        if element["name"] not in doc["data"]:
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
        except Exception as _e:
            logger.error(
                "%s meterspersecond_to_milesperhour: Exception in named function meterspersecond_to_milesperhour:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
            return None

    def kelvin_to_fahrenheit(self, params_dict):
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
        except Exception as _e:
            logger.error(
                "%s kelvin_to_fahrenheit: Exception in named function kelvin_to_fahrenheit:  error: %s",
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
            base_var_index = params_dict["base_var_index"]
            for key in params_dict:
                if key != "base_var_index":
                    break
            nc_value = self.ncdf_data_set[key][base_var_index]
            if not ma.getmask(nc_value):
                value = ma.compressed(nc_value)[0]
                return float(value)
            else:
                return None
        except Exception as _e:
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
            # dewpoint = self.umask_value_transform({"base_var_index": params_dict["base_var_index"], "dewpoint":"dewpoint"})
            # temperature = self.umask_value_transform({"base_var_index": params_dict["base_var_index"], "temperature":"temperature"})
            dewpoint = self.umask_value_transform(
                {
                    "base_var_index": params_dict["base_var_index"],
                    "dewpoint": params_dict["dewpoint"],
                }
            )
            temperature = self.umask_value_transform(
                {
                    "base_var_index": params_dict["base_var_index"],
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
                {
                    "base_var_index": params_dict["base_var_index"],
                    "windDir": params_dict["windDir"],
                }
            )
            _wind_speed = self.umask_value_transform(
                {
                    "base_var_index": params_dict["base_var_index"],
                    "windSpeed": params_dict["windSpeed"],
                }
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
                {
                    "base_var_index": params_dict["base_var_index"],
                    "windDir": params_dict["windDir"],
                }
            )
            _wind_speed = self.umask_value_transform(
                {
                    "base_var_index": params_dict["base_var_index"],
                    "windSpeed": params_dict["windSpeed"],
                }
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
        except Exception as _e:
            logger.error(
                "%s handle_pressure: Exception in named function:  error: %s",
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
            for key in params_dict:
                if key != "base_var_index":
                    break
            _file_utc_time = dt.datetime.strptime(self.file_name, params_dict[key])
            epoch = (_file_utc_time - dt.datetime(1970, 1, 1)).total_seconds()
            iso = convert_to_iso(epoch)
            return iso
        except Exception as _e:
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
            for key in params_dict:
                if key != "base_var_index":
                    break
            _file_utc_time = dt.datetime.strptime(self.file_name, params_dict[key])
            epoch = (_file_utc_time - dt.datetime(1970, 1, 1)).total_seconds()
            return int(epoch)
        except Exception as _e:
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

        except Exception as _e:
            logger.error(
                "%s interpolate_time: Exception in named function interpolate_time:  error: %s",
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
        except Exception as _e:
            logger.error(
                "%s handle_data: Exception in named function interpolate_time_iso:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
