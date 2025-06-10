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

# Removed deprecated typing.List; using built-in list type instead
import couchbase.subdocument as SD
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
        self.standard_levels = None

        # self.do_profiling = False  - in super
        # set to True to enable build_document profiling

    def get_database_connection_details(self, queue_element):
        bucket = self.load_spec["cb_connection"]["bucket"]
        scope = self.load_spec["cb_connection"]["scope"]
        collection = self.load_spec["cb_connection"]["collection"]
        common_collection = self.load_spec["cb_connection"]["common_collection"]
        # stash the file_name so that it can be used later
        self.file_name = Path(queue_element).name
        return bucket, scope, collection, common_collection

    def build_document_map(
        self, queue_element: str, base_var_name: str, origin_type: str = None
    ) -> dict:
        # Type checks
        if not isinstance(queue_element, str):
            raise TypeError(
                f"Expected 'queue_element' to be a string, got {type(queue_element).__name__}"
            )
        if not isinstance(base_var_name, str):
            raise TypeError(
                f"Expected 'base_var_name' to be a string, got {type(base_var_name).__name__}"
            )
        if origin_type is not None and not isinstance(origin_type, str):
            raise TypeError(
                f"Expected 'origin_type' to be a string or None, got {type(origin_type).__name__}"
            )

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
        self,
        queue_element: str,
        base_var_name: str,
        origin_type: str = None,
    ) -> dict:
        """
        Builds a document map for a 3D NetCDF file by processing the base variable
        and associated 3D data.

        This method processes the unlimited variable (e.g., 'time') to create one
        document for each valid time. Within each document, the unlimited variable
        (e.g., 'time') is processed to produce a document for each valid time.

        Args:
            queue_element (str): The file path or identifier for the NetCDF file.
            base_var_name (str): The name of the base variable (e.g., 'time')
                representing the top-level unlimited dimension.
            origin_type (str, optional): The origin type of the data (e.g.,
                'satellite', 'model'). Defaults to None.

        Returns:
            dict: A dictionary representing the document map. Each key is a
            document ID, and the value is the corresponding document metadata.

        Raises:
            TypeError: If any of the input arguments are not of the expected type.

        Notes:
            - This method initializes the document map before processing.
            - If the base variable has no data (size is 0), an empty dictionary is returned.
            - Profiling can be enabled to analyze performance using cProfile.
            - The method logs various stages of processing for debugging and monitoring.
        """

        # Type checks
        if not isinstance(queue_element, str):
            raise TypeError(
                f"Expected 'queue_element' to be a string, got {type(queue_element).__name__}"
            )
        if not isinstance(base_var_name, str):
            raise TypeError(
                f"Expected 'base_var_name' to be a string, got {type(base_var_name).__name__}"
            )
        if origin_type is not None and not isinstance(origin_type, str):
            raise TypeError(
                f"Expected 'origin_type' to be a string or None, got {type(origin_type).__name__}"
            )

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
                    self.handle_document(base_var_name, base_var_index)
                    with Path("profiling_stats.txt").open(
                        "w", encoding="utf-8"
                    ) as stream:
                        stats = Stats(_pr, stream=stream)
                        stats.strip_dirs()
                        stats.sort_stats("time")
                        stats.dump_stats("profiling_stats.prof")
                        stats.print_stats()
            else:
                self.handle_document(base_var_name, base_var_index)
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

    def derive_id(self, **kwargs: dict) -> str:
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

    def translate_template_item(self, variable, base_var_index: int) -> str:
        """
        This method translates template replacements (*item).
        It can translate keys or values.
        :param variable: a value from the template - should be a netcdf variable
        :param recNum: the current recNum
        :return:
        """
        # Type checks
        # variable is not checked on purpose - it can be different things
        if not isinstance(base_var_index, int):
            raise TypeError(
                f"translate_template_item - Expected 'base_var_index' to be an int, got {type(base_var_index).__name__}"
            )

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
                    if _ri in self.ncdf_data_set.ncattrs():
                        # it is a global attribute - replace ' '  with '_'
                        tmp_value = self.ncdf_data_set.getncattr(_ri)
                        value = str(tmp_value).replace(" ", "_")
                        return value
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
                                if ma.isMaskedArray(value):
                                    # it is a masked array
                                    if value.size == 1:
                                        value = value.data.item()
                                    else:
                                        # it is a masked array of more than one value
                                        value = value.data.tolist()
                                return value
                            else:
                                # it is probably a number
                                value = str(
                                    self.ncdf_data_set[variable][base_var_index]
                                )
                                if ma.isMaskedArray(value):
                                    # it is a masked array
                                    if value.size == 1:
                                        value = value.data.item()
                                    else:
                                        # it is a masked array of more than one value
                                        value = value.data.tolist()
                                return value
                        else:
                            # it doesn't need to be a string
                            value = self.ncdf_data_set[variable][base_var_index]
                            if ma.isMaskedArray(value):
                                # it is a masked array
                                if value.size == 1:
                                    value = value.data.item()
                                else:
                                    # it is a masked array of more than one value
                                    value = value.data.tolist()
                            return value
        except Exception as _e:
            logger.exception(
                "Builder.translate_template_item for variable %s: replacements: %s",
                str(variable),
                str(replacements),
            )
        return value

    def handle_document(self, base_var_name: str, base_var_index: int = None) -> None:
        """
        This routine processes the complete document (essentially a complete netcdf file)
        Each template key or value that corresponds to a variable will be selected from
        the netcdf file into a netcdf data set and then
        each station will get values from the record.
        :return: The modified document_map
        """

        if not isinstance(base_var_name, str):
            raise TypeError(
                f"Expected base_var_name to be a string, got {type(base_var_name).__name__}"
            )
        if base_var_index is not None and not isinstance(base_var_index, int):
            raise TypeError(
                f"Expected base_var_index to be an int, got {type(base_var_index).__name__}"
            )

        try:
            new_document = copy.deepcopy(self.template)
            if base_var_index is not None:
                number_of_docs = 1
                docs = [base_var_index]
            else:
                number_of_docs = self.ncdf_data_set.dimensions[base_var_name].size
                docs = range(number_of_docs)

            if number_of_docs == 0:
                return
            # make a copy of the template, which will become the new document
            # once all the translations have occured
            new_document = initialize_data_array(new_document)
            for base_var_index in docs:
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

    def getBoundary_heights_for_level(
        self, levels: list, heights: list[int], level: int
    ) -> dict:
        """
        This function is used to get the boundary heights for the levels.
        Args:
            levels (list): list of levels
            heights (list): list of heights
        Returns:
            map: map of boundary heights for this level
        """
        if type(levels) is not list:
            raise TypeError("levels must be a list")
        if type(heights) is not list:
            raise TypeError("heights must be a list")
        if type(level) is not int:
            raise TypeError("level must be an int")
        boundary_heights = {"lower": None, "upper": None, "exact": None}
        if level in heights:
            boundary_heights["exact"] = level
        else:
            # If the level is not in the data, we need to interpolate
            # Find the two closest raw heights below and above the level
            lower_height = max([h for h in heights if h < level])
            upper_height = min([h for h in heights if h > level])
        boundary_heights["lower"] = lower_height
        boundary_heights["upper"] = upper_height
        return boundary_heights

    def calculate_interpolated_values(
        self,
        raw_data: dict,
        standard_levels: list,
        interpolated_data: dict,
        heights: list,
        level: int,
    ) -> dict:
        """
        Calculate interpolated values for a given level based on raw data and standard levels.

        This method interpolates data for a specified level using the provided raw data,
        standard levels, and heights. If the level exists in the raw data, the corresponding
        value is directly copied. Otherwise, linear interpolation is performed using the
        two closest heights.

        Args:
            raw_data (dict): A dictionary containing raw data values for various variables.
                             Each key represents a variable, and its value is a list of
                             corresponding data points.
            standard_levels (list): A list of standard levels to which the data should be
                                    interpolated.
            interpolated_data (dict): A dictionary to store the interpolated data. This will
                                      be updated with the calculated values.
            heights (list): A list of heights corresponding to the raw data.
            level (int): The specific level for which interpolated values are to be calculated.

        Returns:
            dict: The updated `interpolated_data` dictionary containing interpolated values
                  for the specified level.

        Raises:
            TypeError: If any of the input arguments are of incorrect type.
            ValueError: If the specified level is not in the `standard_levels`.
            Exception: If an error occurs during interpolation or boundary height calculation.

        Notes:
            - The method relies on `getBoundary_heights_for_level` to determine the boundary
              heights for interpolation.
            - Linear interpolation is used to calculate values for levels not present in the
              raw data.
        """

        # type checks
        if not isinstance(raw_data, dict):
            raise TypeError(
                f"Expected 'raw_data' to be a dictionary, got {type(raw_data).__name__}"
            )
        if not isinstance(standard_levels, list):
            raise TypeError(
                f"Expected 'standard_levels' to be a list, got {type(standard_levels).__name__}"
            )
        if not isinstance(interpolated_data, dict):
            raise TypeError(
                f"Expected 'interpolated_data' to be a dictionary, got {type(interpolated_data).__name__}"
            )
        if not isinstance(heights, list):
            raise TypeError(
                f"Expected 'heights' to be a list, got {type(heights).__name__}"
            )
        if not isinstance(level, int):
            raise TypeError(
                f"Expected 'level' to be an int, got {type(level).__name__}"
            )
        if level not in self.get_standard_levels():
            raise ValueError(
                f"level {level} is not in standard_levels {self.standard_levels}"
            )

        try:
            boundary_heights = self.getBoundary_heights_for_level(
                self.get_standard_levels(), heights, level
            )
        except TypeError as _e:
            raise TypeError(
                "%s : Exception in getBoundary_heights:  error: %s",
                self.__class__.__name__,
                str(_e),
            ) from _e
        try:
            if boundary_heights["exact"] is not None:
                # If the level is already in the data, just copy it
                for variable in raw_data:
                    if variable != "height":
                        continue
                    interpolated_data[variable] = raw_data[variable][
                        heights.index(boundary_heights["exact"])
                    ]
            else:
                # If the level is not in the data, we need to interpolate
                # Find the two closest raw heights below and above the level
                lower_height = boundary_heights["lower"]
                upper_height = boundary_heights["upper"]
                # Interpolate the data for this height
                for variable in raw_data:
                    if variable == "height":
                        continue
                        # Linear interpolation
                    lower_value = raw_data[variable][heights.index(lower_height)]
                    upper_value = raw_data[variable][heights.index(upper_height)]
                    interpolated_value = lower_value + (upper_value - lower_value) * (
                        (level - lower_height) / (upper_height - lower_height)
                    )
                    if variable not in interpolated_data:
                        interpolated_data[variable] = {}
                    if level not in interpolated_data[variable]:
                        interpolated_data[variable][level] = {}
                    interpolated_data[variable][level] = interpolated_value
        except Exception as _e:
            raise TypeError(
                "%s : Exception in calculate_interpolated_values:  error: %s",
                self.__class__.__name__,
                str(_e),
            ) from _e
        return interpolated_data

    def interpolate_3d_data(self, raw_data: dict) -> dict:
        """
        Interpolates 3D data to standard levels.

        This method takes raw 3D data and interpolates it to a predefined set of
        standard levels. The raw data is expected to include a "height" variable
        which represents the raw heights from the NetCDF file.

        Args:
            raw_data (dict): A dictionary containing the raw 3D data. It must
                             include a "height" key with corresponding height values.

        Returns:
            dict: A dictionary containing the interpolated data at the standard levels.

        Raises:
            Exception: Logs an error message if an exception occurs during the
                       interpolation process.
        """
        # Type checks
        if not isinstance(raw_data, dict):
            raise TypeError(
                f"Expected 'raw_data' to be a dictionary, got {type(raw_data).__name__}"
            )

        try:
            # Interpolate the data to the standard levels
            # raw heights are the valuse of the height variable in the netcdf file
            interpolated_data = {}
            heights = raw_data["height"]
            # Get the standard levels from the metadata
            standard_levels = self.get_standard_levels()
            # Get the boundary heights for the levels
            for level in standard_levels:
                self.calculate_interpolated_values(
                    raw_data, standard_levels, interpolated_data, heights, level
                )
        except Exception as _e:
            logger.error(
                "%s : Exception in named function interpolate_data:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
        return interpolated_data

    def handle_key(self, doc: dict, base_var_name: str, base_var_index: int, key: str):
        """
        Processes a key within a document by substituting NetCDF variables or handling
        nested dictionaries and named functions based on the provided template.

        This method modifies the input document (`doc`) by processing the specified key
        and replacing its value based on the template and the current NetCDF variable index.

        Args:
            doc (dict): The current document being processed. Must be a dictionary.
            base_var_name (str): The name of the base NetCDF variable.
            base_var_index (int): The current index of the unlimited NetCDF variable.
            key (str): The key to be processed. Can refer to a primitive, another dictionary,
                       or a named function.

        Returns:
            dict: The updated document after processing the specified key.

        Raises:
            TypeError: If any of the arguments (`doc`, `base_var_name`, `base_var_index`, `key`)
                       are not of the expected type.

        Notes:
            - If the key is "id", it derives an ID using the `derive_id` method and adds it
              to the document if it doesn't already exist.
            - If the value of the key is a dictionary, it recursively processes the nested
              dictionary.
            - If the value of the key is a string starting with "&", it processes it as a
              named function using the `handle_named_function` method.
            - Otherwise, it translates the template item using the `translate_template_item` method.
            - Logs exceptions if any errors occur during processing.
        """
        # Type checks
        if not isinstance(doc, dict):
            raise TypeError(
                f"netcdf_builder_parent:handle_key: Expected 'doc' to be a dictionary, got {type(doc).__name__}"
            )
        if not isinstance(base_var_name, str):
            raise TypeError(
                f"netcdf_builder_parent:handle_key: Expected 'base_var_name' to be a string, got {type(base_var_name).__name__}"
            )
        if not isinstance(base_var_index, int):
            raise TypeError(
                f"netcdf_builder_parent:handle_key: Expected 'base_var_index' to be an int, got {type(base_var_index).__name__}"
            )
        if not isinstance(key, str):
            raise TypeError(
                f"netcdf_builder_parent:handle_key: Expected 'key' to be a string, got {type(key).__name__}"
            )

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

    def handle_named_function(
        self, named_function_def: str, base_var_index: int
    ) -> str:
        """
        Processes a named function entry from a template and substitutes its return value into a document.
        This method takes a named function definition string, extracts the function name and its parameters,
        translates the parameters from the NetCDF file, and calls the corresponding function with the translated
        parameters. The return value of the function is then used as a replacement in the document.
        Args:
            named_function_def (str): A string representing the named function definition. It is expected to
                follow the format "&named_function:*field1,*field2,*field3...". The function name and its
                parameters are separated by a colon (":") and the parameters are separated by commas (",").
                Each parameter is prefixed with an asterisk ("*").
            base_var_index (int): The base variable index being processed.
        Returns:
            str: The result of the named function call, which is substituted into the document.
        Raises:
            Exception: Logs an exception if there is an error during the processing of the named function.
        Notes:
            - The method assumes that the parameters (e.g., field1, field2, field3) are valid variable names
              and translates them into corresponding values (e.g., value1, value2, value3) using the
              `translate_template_item` method.
            - The named function is dynamically called using `getattr` on the current instance.
            - If the function name or parameters are not valid, an exception is logged."""

        # Type checks
        if not isinstance(named_function_def, str):
            raise TypeError(
                f"handle_named_function - Expected 'named_function_def' to be a string, got {type(named_function_def).__name__}"
            )
        if not isinstance(base_var_index, int):
            raise TypeError(
                f"handle_named_function - Expected 'base_var_index' to be an int, got {type(base_var_index).__name__}"
            )
        # Split the named function definition into function name and parameters
        func = None
        try:
            func = named_function_def.split("|")[0].replace("&", "")
            params = named_function_def.split("|")[1].split(",")
            dict_params = {"base_var_index": base_var_index}
            for _p in params:
                # be sure to slice the * off of the front of the param - if it is there
                if _p.startswith("*"):
                    # this is a template item
                    dict_params[_p[1:]] = self.translate_template_item(
                        _p, base_var_index
                    )
                else:
                    dict_params[_p] = self.translate_template_item(_p, base_var_index)
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
        """
        Processes a data template by iterating through its entries and applying specific
        handling logic based on the entry type. This method supports handling named
        functions and translating template items for both keys and values in the template.
            **kwargs: Arbitrary keyword arguments.
                - doc (Object): The data document that is being built.
                - base_var_index (int): An index used for variable substitution in the template.
            Object: The updated data document after processing the template.
            Returns:
                (Object): this is the data document that is being built
        Raises:
            Exception: Logs and handles any exceptions that occur during processing.
        Notes:
            - Template entries starting with '&' are processed using `handle_named_function`.
            - Template entries starting with '*' are processed using `translate_template_item`.
            - Logs warnings if a value or key cannot be processed.
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
                data_key = self.handle_named_function(data_key, base_var_index)
            else:
                data_key = self.translate_template_item(data_key, base_var_index)
            if data_key is None:
                logger.warning(
                    "%s Builder.handle_data - _data_key is None",
                    self.__class__.__name__,
                )
            if "name" not in data_elem:
                data_elem["name"] = data_key
            self.load_data(doc, data_elem)
            return doc
        except Exception as _e:
            logger.exception(
                "%s handle_data: Exception instantiating builder",
                self.__class__.__name__,
            )
        return doc

    def build_datafile_doc(
        self, file_name: str, data_file_id: str, origin_type: str
    ) -> dict:
        """
        Builds a dataFile document for the NetcdfBuilder.

        This method creates a dictionary representing a dataFile document, which
        contains metadata about a NetCDF file being ingested. The document is
        intended to be added to the output folder and imported with other documents.
        The VxIngest system uses these documents to determine if a specific file
        has already been processed.

        Args:
            file_name (str): The path to the NetCDF file.
            data_file_id (str): A unique identifier for the data file.
            origin_type (str): The origin type of the data file.

        Returns:
            dict: A dictionary containing metadata about the NetCDF file.
        """
        # Type checks
        if not isinstance(file_name, str):
            raise TypeError(
                f"Expected 'file_name' to be a string, got {type(file_name).__name__}"
            )
        if not isinstance(data_file_id, str):
            raise TypeError(
                f"Expected 'data_file_id' to be a string, got {type(data_file_id).__name__}"
            )
        if not isinstance(origin_type, str):
            raise TypeError(
                f"Expected 'origin_type' to be a string, got {type(origin_type).__name__}"
            )
        # Build the dataFile document
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

    def get_document_map(self, base_var_name: str) -> dict:
        """
        dict:
        Retrieve the document map for the current file.
        This method is responsible for returning the `document_map` attribute, which
        contains processed data for the current file. If there are any unprocessed
        rows remaining in `same_time_rows`, it will invoke the `handle_document`
        method to process them before returning the `document_map`.
        Args:
            base_var_name (str): The base variable name used for processing.
        Returns:
            dict: The document map containing processed data for the current file.
                    Returns `None` if an exception occurs during processing.
        Raises:
            Exception: Logs any exception that occurs
            during the execution of the method.
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

    def load_data(self, doc: dict, element: dict) -> dict:
        """
        Adds an observation to the provided document dictionary, ensuring that
        data elements are unique per station name. If multiple observations exist
        for the same station, the one closest to the target time is retained.
        This method uses a dictionary to hold data elements, ensuring uniqueness
        and capturing the most relevant observation for each station.
        Args:
            doc (dict): The document being created or updated. It must contain a
                        "data" key to store observations, and a "fcstValidEpoch"
                        key representing the target time.
            element (dict): The observation data to be added. It must contain a
                            "name" key for the station name and a "Reported Time"
                            key for the observation timestamp.
        Returns:
            dict: The updated document with the observation added or replaced.
        Raises:
            TypeError: If `doc` or `element` is not a dictionary.
        """
        # Type checks
        if not isinstance(doc, dict):
            raise TypeError(
                f"Expected 'doc' to be a dictionary, got {type(doc).__name__}"
            )
        if not isinstance(element, dict):
            raise TypeError(
                f"Expected 'element' to be a dictionary, got {type(element).__name__}"
            )
        # Check if the element is None
        if element is None:
            logger.warning(
                "%s load_data: element is None",
                self.__class__.__name__,
            )
            return doc
        # Check if the element is empty
        if not element:
            logger.warning(
                "%s load_data: element is empty",
                self.__class__.__name__,
            )
            return doc
        if "data" not in doc or doc["data"] is None:
            doc["data"] = {}
        if "name" not in element or element["name"] not in doc["data"]:
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
    def get_standard_levels(self):
        """Returns the standard levels for the data
        Returns:
            list: the standard levels
        """
        if not self.standard_levels:
            try:
                docid = "MD:STANDARD_LEVELS:COMMON:V01"
                self.standard_levels = (
                    self.load_spec["common_collection"]
                    .lookup_in(docid, (SD.get("TROPOE"),))
                    .content_as[list](0)
                )
            except Exception as _e:
                logger.error(
                    "%s get_standard_levels: Exception in named function get_standard_levels:  error: %s",
                    self.__class__.__name__,
                    str(_e),
                )
                raise TypeError(
                    f"{self.__class__.__name__} get_standard_levels: Exception in named function get_standard_levels"
                ) from _e
        return self.standard_levels

    def meterspersecond_to_milesperhour(self, params_dict):
        """Converts meters per second to mile per hour performing any translations that are necessary
        Args:
            params_dict (dict): named function parameters
        Returns:
            [type]: [description]
        """
        # Meters/second to miles/hour
        try:
            value = self.retrieve_from_netcdf(params_dict)
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
            value = self.retrieve_from_netcdf(params_dict)
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

    def retrieve_from_netcdf(self, params_dict):
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
                "%s retrieve_from_netcdf: Exception in named function retrieve_from_netcdf for key %s:  error: %s",
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
            # dewpoint = self.retrieve_from_netcdf({"base_var_index": params_dict["base_var_index"], "dewpoint":"dewpoint"})
            # temperature = self.retrieve_from_netcdf({"base_var_index": params_dict["base_var_index"], "temperature":"temperature"})
            dewpoint = self.retrieve_from_netcdf(
                {
                    "base_var_index": params_dict["base_var_index"],
                    "dewpoint": params_dict["dewpoint"],
                }
            )
            temperature = self.retrieve_from_netcdf(
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
            _wind_dir = self.retrieve_from_netcdf(
                {
                    "base_var_index": params_dict["base_var_index"],
                    "windDir": params_dict["windDir"],
                }
            )
            _wind_speed = self.retrieve_from_netcdf(
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
            _wind_dir = self.retrieve_from_netcdf(
                {
                    "base_var_index": params_dict["base_var_index"],
                    "windDir": params_dict["windDir"],
                }
            )
            _wind_speed = self.retrieve_from_netcdf(
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
            value = self.retrieve_from_netcdf(params_dict)
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
