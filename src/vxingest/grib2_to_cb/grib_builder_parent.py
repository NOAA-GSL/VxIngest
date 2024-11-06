"""
Program Name: Class grib_builder.py
Contact(s): Randy Pierce
History Log:  Initial version
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSL
"""

import copy
import logging
import math
import os
import sys
from pathlib import Path

import pyproj

from vxingest.builder_common.builder import Builder
from vxingest.builder_common.builder_utilities import (
    convert_to_iso,
    get_geo_index,
    initialize_data_array,
)

# Get a logger with this module's name to help with debugging
logger = logging.getLogger(__name__)


class GribBuilder(Builder):
    """parent class for grib builders. This class contains methods that are
    common to all the grib builders. The entry point for every builder is the build_document(self, queue_element)
    which is common to all grib2 builders and is in this class."""

    def __init__(
        self,
        load_spec,
        ingest_document,
        number_stations=sys.maxsize,
    ):
        super().__init__(load_spec, ingest_document)

        self.ingest_document = ingest_document
        self.template = ingest_document["template"]
        self.subset = self.template["subset"]
        self.load_spec = load_spec
        # GribBuilder specific
        self.number_stations = number_stations
        self.domain_stations = []
        self.ds_translate_item_variables_map = None

        # self.do_profiling = False - in super
        self.do_profiling = os.getenv("PROFILE")
        if self.do_profiling:
            self.profile_output_path = os.getenv("PROFILE_OUTPUT_DIR")
        # set to True to enable build_document profiling

    def get_proj_params_from_string(self, proj_string):
        """Convert the proj string to a dictionary of parameters
        Args:
            proj_string (string): from the cfgrib variable attrs i.e. ds_height_above_ground_2m.r2.attrs['GRIB_projString']
        Returns:
            dict: dictionary of projection parameters
        """
        proj_params = {}
        for _v in proj_string.replace(" ", "").split("+")[1:]:
            elem = _v.split("=")
            proj_params[elem[0]] = elem[1]
        return proj_params

    def get_grid(
        self,
        proj_params,
        latitude_of_first_grid_point_in_degrees,
        longitude_of_first_grid_point_in_degrees,
    ):
        """get the grid for the projection
        Args:
            proj_params (dict): the parameters for the projection
            latitudeOfFirstGridPointInDegrees (float): latitude
            longitudeOfFirstGridPointInDegrees (float): longitude

        Returns:
            projection: projection object
        """
        init_projection = pyproj.Proj(proj_params)
        latlon_proj = pyproj.Proj(proj="latlon")
        lat_0 = latitude_of_first_grid_point_in_degrees
        lon_0 = longitude_of_first_grid_point_in_degrees

        init_transformer = pyproj.Transformer.from_proj(
            proj_from=latlon_proj, proj_to=init_projection
        )
        _x, _y = init_transformer.transform(
            lon_0, lat_0, radians=False
        )  # the lower left coordinates in the projection space

        # Add the proper conversion to 'fool' Proj into setting 0,0 in the lower left corner of the domain
        # NOTE: It doesn't actually do this, but it will be necessary to find x,y coordinates relative to the lower left corner
        proj_params["x_0"] = abs(_x)
        # offset the x,y points in the projection so that we get points oriented to bottom left
        proj_params["y_0"] = abs(_y)
        # Create Proj object
        grid_projection = pyproj.Proj(proj_params)
        return grid_projection

    def get_wind_theta(self, proj_params, lad_in_degrees, lov_in_degrees, lon):
        """
        Calculate the rotation angle for the wind vector
        :param proj_params: the projection parameters
        :param lon: the longitude
        :return: the rotation angle
        """
        theta = 0
        if lon < 0:
            lon = lon + 360
        if proj_params["proj"] == "lcc":
            alattan = lad_in_degrees
            elonv = lov_in_degrees

            dlon = elonv - lon
            rotation = math.sin(math.radians(alattan))

            if lon > 180:
                lon -= 360
            if lon < -180:
                lon += 360
            theta = -rotation * dlon
        else:
            print(f"Projection {proj_params['proj']} not yet supported")
        return theta

    def interp_grid_box(self, values, _y, _x):
        """
        Interpolate the value at a given point in the grid
        :param values: the grid of values
        :param _y: the y coordinate
        :param _x: the x coordinate
        :return: the interpolated value
        """
        try:
            xmin, xmax = math.floor(_x), math.ceil(_x)
            ymin, ymax = math.floor(_y), math.ceil(_y)
            xmin_ymin_value = values[ymin, xmin]
            xmax_ymin_value = values[ymin, xmax]
            xmin_ymax_value = values[ymax, xmin]
            xmax_ymax_value = values[ymax, xmax]
            remainder_x = _x - xmin
            remainder_y = _y - ymin
            interpolated_value = (
                (remainder_x * remainder_y * xmax_ymax_value)
                + (remainder_x * (1 - remainder_y) * xmax_ymin_value)
                + ((1 - remainder_x) * remainder_y * xmin_ymax_value)
                + ((1 - remainder_x) * (1 - remainder_y) * xmin_ymin_value)
            )
            return interpolated_value
        except Exception as _e:
            raise Exception(f"Error in get_grid.interpGridBox - {str(_e)}") from _e

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
                        _v, _interp_v = self.translate_template_item(part)
                        value = str(_v)
                    else:
                        value = str(part)
                new_parts.append(value)
            new_id = ":".join(new_parts)
            return new_id
        except Exception as _e:
            logger.exception("GribBuilder.derive_id")
            return None

    def translate_template_item(self, variable, single_return=False):
        """This method translates template replacements (*item or *item1*item2).
        It can translate keys or values.
        If the variable is a variable to be translated (not a constant)
        it should literally be the long name attribute of the variable from the
        proper cfgrib dataset. For example "2 metre temperature" or "2 metre dewpoint temperature"
        from the
            ds_height_above_ground_2m dataset.
                ds_height_above_ground_2m.filter_by_attrs(long_name="2 metre temperature")
        Args:
            variable (string): a value from the template - should be a grib2 variable or a constant.
            single_return (boolean): if this is True only one value is returned, otherwise an array.
        Returns:
            [list]: It returns an array of values, ordered by domain_stations, or a single value.
            The array will have values and interpreted values. The values are the actual values
            from the grib2 file. The interpreted values are the values interpolated to the
            station location. The array will have one or two values. Each handler decides whether to use the
            values or the interpreted values or none at all (directly access the grib2 file variables)
            depending on the single_return parameter.
        """
        replacements = []
        try:
            if single_return:
                return (variable, variable)
            if isinstance(variable, str):
                # skip the first replacement, its never
                # really a replacement. It is either '' or not a
                # replacement
                replacements = variable.split("*")[1:]
            # pre assign these in case it isn't a replacement - makes it easier
            station_value = variable
            interpolated_value = variable
            if len(replacements) > 0:
                station_values = []
                for _ri in replacements:
                    values = self.ds_translate_item_variables_map[_ri].values
                    for station in self.domain_stations:
                        # get the individual station value and interpolated value
                        geo_index = get_geo_index(
                            self.ds_translate_item_variables_map["fcst_valid_epoch"],
                            station["geo"],
                        )
                        station_value = values[
                            round(station["geo"][geo_index]["y_gridpoint"]),
                            round(station["geo"][geo_index]["x_gridpoint"]),
                        ]
                        # interpolated grid points cannot be rounded
                        interpolated_value = self.interp_grid_box(
                            values,
                            station["geo"][geo_index]["y_gridpoint"],
                            station["geo"][geo_index]["x_gridpoint"],
                        )
                        # convert each station value to iso if necessary
                        if _ri.startswith("{ISO}"):
                            station_value = variable.replace(
                                "*" + _ri, convert_to_iso(station_value)
                            )
                            interpolated_value = variable.replace(
                                "*" + _ri, convert_to_iso(station_value)
                            )
                        else:
                            station_value = variable.replace(
                                "*" + _ri, str(station_value)
                            )
                            interpolated_value = variable.replace(
                                "*" + _ri, str(interpolated_value)
                            )
                        # add it onto the list of tupples
                        station_values.append((station_value, interpolated_value))
                return station_values
            # it is a constant, no replacements but we still need a tuple for each station
            return [
                (station_value, interpolated_value)
                for i in range(len(self.domain_stations))
            ]
        except Exception as _e:
            logger.exception(
                "Builder.translate_template_item for variable %s: replacements: %s",
                str(variable),
                str(replacements),
            )

    def handle_document(self):
        """
        This routine processes the complete document (essentially a complete grib file)
        Each template key or value that corresponds to a variable will be selected from
        the grib file into an xarray/cfgrib dataset and then
        each station will get values from the dataset.
        :return: The modified document_map
        """
        try:
            new_document = copy.deepcopy(self.template)
            station_data_size = len(self.domain_stations)

            # save the domain_stations to a file for debugging - this is a lot of data
            # and it is not necessary to write it every time. It provides a way to
            # get the domain_stations for a given grib file - which include the lat/lons
            # and the interpolated grid points for each station. Uncomment the following
            # if you need the data for debugging.
            # debugging=True
            # if debugging:
            #     json_object = json.dumps(self.domain_stations, indent=4)
            #     # Writing to sample.json
            #     with open("/tmp/domain_stations.json", "w", encoding='utf-8') as outfile:
            #         outfile.write(json_object)

            if station_data_size == 0:
                return
            # make a copy of the template, which will become the new document
            # once all the translations have occured
            new_document = initialize_data_array(new_document)
            for key in self.template:
                if key == "data":
                    new_document = self.handle_data(doc=new_document)
                    continue
                new_document = self.handle_key(new_document, key)
            # put document into document map
            if new_document["id"]:
                logger.info(
                    "GribBuilder.handle_document - adding document %s",
                    new_document["id"],
                )
                self.document_map[new_document["id"]] = new_document
            else:
                logger.info(
                    "GribBuilder.handle_document - cannot add document with key %s",
                    str(new_document["id"]),
                )
        except Exception as _e:
            logger.error(
                "%s GribBuilder.handle_document: Exception instantiating builder: %s",
                self.__class__.__name__,
                str(_e),
            )
            raise _e

    def handle_key(self, doc, key):
        """
        This routine handles keys by substituting
        the grib variables that correspond to the key into the values
        in the template that begin with *
        :param doc: the current document
        :param station: The current station
        :param _key: A key to be processed, This can be a key to a primitive,
        or to another dictionary, or to a named function
        """

        try:
            if key == "id":
                an_id = self.derive_id(template_id=self.template["id"])
                if an_id not in doc:
                    doc["id"] = an_id
                return doc
            if isinstance(doc[key], dict):
                # process an embedded dictionary
                tmp_doc = copy.deepcopy(self.template[key])
                for sub_key in tmp_doc:
                    tmp_doc = self.handle_key(tmp_doc, sub_key)  # recursion
                doc[key] = tmp_doc
            if (
                not isinstance(doc[key], dict)
                and isinstance(doc[key], str)
                and doc[key].startswith("&")
            ):
                doc[key] = self.handle_named_function(doc[key])
            else:
                doc[key], _interp_v = self.translate_template_item(doc[key], True)
            return doc
        except Exception as _e:
            logger.exception(
                "%s GribBuilder.handle_key: Exception in builder:",
                self.__class__.__name__,
            )
        return doc

    def handle_named_function(self, named_function_def):
        """
        This routine processes a named function entry from a template.
        :param _named_function_def - this can be either a template key or a template value.
        The _named_function_def looks like "&named_function|*field1,*field2,*field3..."
        where named_function is the literal function name of a defined function.
        The name of the function and the function parameters are separated by a ":" and
        the parameters are separated by a ','.
        It is expected that field1, field2, and field3 etc are all valid variable names.
        Each field will be translated from the netcdf file into value1, value2 etc.
        The method "named_function" will be called like...
        named_function({field1:value1, field2:value2, ... fieldn:valuen}) and the return value from named_function
        will be substituted into the document.
        :station the station being processed.
        """

        func = None
        replace_with = None
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
                dict_params[_p[1:]] = self.translate_template_item(_p)
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
            data_elem = {}
            data_key = next(iter(self.template["data"]))
            data_template = self.template["data"][data_key]
            for key in data_template:
                try:
                    value = data_template[key]
                    # values can be null...
                    if value and value.startswith("&"):
                        value = self.handle_named_function(value)
                    else:
                        value = self.translate_template_item(value)
                except Exception as _e:
                    value = [(None, None)]
                    logger.warning(
                        "%s Builder.handle_data - value is (None,None)",
                        self.__class__.__name__,
                    )
                data_elem[key] = value
            if data_key.startswith("&"):
                data_key = self.handle_named_function(data_key)
            else:
                # _ ignore the interp_value part of the returned tuple
                data_key, _interp_ignore_value = self.translate_template_item(data_key)
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

    def delete_idx_file(self, queue_element):
        """
        cfgrib leaves .idx files in the directory - delete the .idx file
        """
        queue_element = Path(queue_element)
        basepath = queue_element.parent
        idx_pattern = queue_element.name.replace(".grib2", "") + ".*.idx"
        file_list = basepath.glob(idx_pattern)

        # Iterate over the list of filepaths & remove each file.
        for file in file_list:
            try:
                file.unlink()
            except OSError as _e:
                logger.warning(
                    "%s Builder.build_document Error - cannot delete idx file %s - %s",
                    self.__class__.__name__,
                    file,
                    _e,
                )

