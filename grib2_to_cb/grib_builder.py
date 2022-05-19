"""
Program Name: Class grib_builder.py
Contact(s): Randy Pierce
History Log:  Initial version
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSL
"""

import copy
import cProfile
import logging
import math
import os
import sys
from pstats import Stats
import numpy
import pygrib
import pyproj
import grib2_to_cb.get_grid as gg
from builder_common.builder_utilities import convert_to_iso
from builder_common.builder_utilities import get_geo_index
from builder_common.builder_utilities import initialize_data_array
from builder_common.builder import Builder


class GribBuilder(Builder):  # pylint: disable=too-many-arguments
    """parent class for grib builders"""

    def __init__(
        self,
        load_spec,
        ingest_document,
        cluster,
        collection,
        number_stations=sys.maxsize,
    ):
        super().__init__(load_spec, ingest_document, cluster, collection)

        self.ingest_document = ingest_document
        self.template = ingest_document["template"]
        self.subset = self.template["subset"]
        self.load_spec = load_spec
        self.cluster = cluster
        self.collection = collection
        # GribBuilder specific
        self.number_stations = number_stations
        self.projection = None
        self.grbs = None
        self.grbm = None
        self.spacing = None
        self.in_proj = None
        self.out_proj = None
        self.transformer = None
        self.transformer_reverse = None
        self.domain_stations = []

        # self.do_profiling = False - in super
        # set to True to enable build_document profiling

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
                        _v, _interp_v = self.translate_template_item(
                            part
                        )  # pylint:disable=unused-variable
                        value = str(_v)
                    else:
                        value = str(part)
                new_parts.append(value)
            new_id = ":".join(new_parts)
            return new_id
        except Exception as _e:  # pylint:disable=broad-except
            logging.exception("GribBuilder.derive_id")
            return None

    def translate_template_item(self, variable, single_return=False):
        """This method translates template replacements (*item or *item1*item2).
        It can translate keys or values.
        Args:
            variable (string): a value from the template - should be a grib2 variable or a constant
            single_return (boolean): if this is True on one value is returned, otherwise an array.
        Returns:
            [list]: It returns an array of values, ordered by domain_stations, or a single value
        depending on the single_return parameter.
        """
        replacements = []
        # noinspection PyBroadException
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
                    message = self.grbs.select(name=_ri)[0]
                    values = message["values"]
                    for station in self.domain_stations:
                        # get the individual station value and interpolated value
                        fcst_valid_epoch = round(message.validDate.timestamp())
                        geo_index = get_geo_index(fcst_valid_epoch, station["geo"])
                        station_value = values[
                            round(station["geo"][geo_index]["y_gridpoint"]),
                            round(station["geo"][geo_index]["x_gridpoint"]),
                        ]
                        # interpolated gridpoints cannot be rounded
                        interpolated_value = gg.interpGridBox(
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
        except Exception as _e:  # pylint:disable=broad-except
            logging.exception(
                "Builder.translate_template_item for variable %s: replacements: %s",
                str(variable),
                str(replacements),
            )

    def handle_document(self):
        """
        This routine processes the complete document (essentially a complete grib file)
        Each template key or value that corresponds to a variable will be selected from
        the grib file into a pygrib message and then
        each station will get values from the grib message.
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
            new_document = initialize_data_array(new_document)
            for key in self.template.keys():
                if key == "data":
                    new_document = self.handle_data(doc=new_document)
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
                doc[key], _interp_v = self.translate_template_item(doc[key], True)
            return doc
        except Exception as _e:  # pylint:disable=broad-except
            logging.exception(
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
        The name of the function and the function parameters are seperated by a ":" and
        the parameters are seperated by a ','.
        It is expected that field1, field2, and field3 etc are all valid variable names.
        Each field will be translated from the netcdf file into value1, value2 etc.
        The method "named_function" will be called like...
        named_function({field1:value1, field2:value2, ... fieldn:valuen}) and the return value from named_function
        will be substituted into the document.
        :station the station being processed.
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
                dict_params[_p[1:]] = self.translate_template_item(_p)
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
            for key in data_template.keys():
                try:
                    value = data_template[key]
                    # values can be null...
                    if value and value.startswith("&"):
                        value = self.handle_named_function(value)
                    else:
                        value = self.translate_template_item(value)
                except Exception as _e:  # pylint:disable=broad-except
                    value = [(None, None)]
                    logging.warning(
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
                logging.warning(
                    "%s Builder.handle_data - _data_key is None",
                    self.__class__.__name__,
                )
            self.load_data(doc, data_key, data_elem)
            return doc
        except Exception as _e:  # pylint:disable=broad-except
            logging.exception(
                "%s handle_data: Exception instantiating builder",
                self.__class__.__name__,
            )
        return doc

    def build_document(
        self, queue_element
    ):  # pylint:disable=too-many-statements, disable=too-many-locals
        """
        This is the entry point for the gribBuilders from the ingestManager.
        The ingest manager is giving us a grib file to process from the queue.
        These documents are id'd by time and fcstLen. The data section is an array
        each element of which contains variable data and a station name. To process this
        file we need to itterate the domain_stations list and process the station name along
        with all the required variables.
        1) get the first epoch - if none was specified get the latest one from the db
        2) transform the projection from the grib file
        3) determine the stations for this domain, adding gridpoints to each station - build a station list
        4) enable profiling if requested
        5) handle_document - iterate the template and process all the keys and values
        6) build a datafile document to record that this file has been processed
        """
        try:
            # translate the projection from the grib file
            logging.getLogger().setLevel(logging.INFO)
            self.projection = gg.getGrid(queue_element)
            self.grbs = pygrib.open(queue_element)  # pylint:disable=no-member
            self.grbm = self.grbs.message(1)
            self.spacing, max_x, max_y = gg.getAttributes(queue_element)
            # Set the two projections to be used during the transformation (nearest neighbor method, what we use for everything with METARS)
            self.in_proj = pyproj.Proj(proj="latlon")
            self.out_proj = self.projection
            self.transformer = pyproj.Transformer.from_proj(
                proj_from=self.in_proj, proj_to=self.out_proj
            )
            self.transformer_reverse = pyproj.Transformer.from_proj(
                proj_from=self.out_proj, proj_to=self.in_proj
            )

            # reset the builders document_map for a new file
            self.initialize_document_map()
            # get stations from couchbase and filter them so
            # that we retain only the ones for this models domain which is derived from the projection
            # NOTE: this is not about regions, this is about models
            self.domain_stations = []
            limit_clause = ";"
            if self.number_stations != sys.maxsize:
                limit_clause = " limit {l};".format(l=self.number_stations)
            result = self.cluster.query(
                """SELECT mdata.geo, name
                    from mdata
                    where type='MD'
                    and docType='station'
                    and subset='METAR'
                    and version='V01'
                    {limit_clause}
                    """.format(
                    limit_clause=limit_clause
                )
            )
            for row in result:
                for geo_index in range(len(row["geo"])):
                    lat = row["geo"][geo_index]["lat"]
                    lon = row["geo"][geo_index]["lon"]
                    if lat == -90 and lon == 180:
                        continue  # don't know how to transform that station
                    _x, _y = self.transformer.transform(lon, lat, radians=False)
                    x_gridpoint, y_gridpoint = _x / self.spacing, _y / self.spacing
                    try:
                        # pylint: disable=c-extension-no-member
                        if (
                            math.floor(x_gridpoint) < 0
                            or math.ceil(x_gridpoint) >= max_x
                            or math.floor(y_gridpoint) < 0
                            or math.ceil(y_gridpoint) >= max_y
                        ):
                            continue
                    except Exception as _e:  # pylint: disable=broad-except
                        logging.error(
                            "%s: Exception with builder build_document processing station: error: %s",
                            self.__class__.__name__,
                            str(_e),
                        )
                        continue
                    station = copy.deepcopy(row)
                    station["geo"][geo_index]["x_gridpoint"] = x_gridpoint
                    station["geo"][geo_index]["y_gridpoint"] = y_gridpoint
                    self.domain_stations.append(station)

            # if we have asked for profiling go ahead and do it
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
            data_file_id = self.create_data_file_id(
                self.subset, "grib2", self.template["model"], queue_element
            )
            if data_file_id is None:
                logging.error(
                    "%s: Failed to create DataFile ID:", self.__class__.__name__
                )
            data_file_doc = self.build_datafile_doc(
                file_name=queue_element,
                data_file_id=data_file_id,
                origin_type=self.template["model"],
            )
            document_map[data_file_doc["id"]] = data_file_doc
            return document_map
        except Exception as _e:  # pylint:disable=broad-except
            logging.exception(
                "%s: Exception with builder build_document: file_name: %s",
                self.__class__.__name__,
                queue_element,
            )
            return {}


# Concrete builders
class GribModelBuilderV01(GribBuilder):  # pylint:disable=too-many-instance-attributes
    """
    This is the builder for model data that is ingested from grib2 files.
    """

    def __init__(
        self,
        load_spec,
        ingest_document,
        cluster,
        collection,
        number_stations=sys.maxsize,
    ):  # pylint:disable=too-many-arguments
        """This builder creates a set of V01 model documents using the stations in the station list.
        This builder loads domain qualified station data into memory, and uses the domain_station
        list to associate a station with a grid value at an x_lat, x_lon point.
        In each document the data is an array of objects each of which is the model variable data
        for specific variables at a point associated with a specific station at the time and fcstLen of
        the document.
        Args:
            load_spec (Object): The load spec used to init the parent
            ingest_document (Object): the ingest document
            cluster (Object): a Couchbase cluster object, used for N1QL queries (QueryService)
            collection (Object): a couchbase collection object, used to get fetch documents by id (DataService)
            number_stations (int, optional): the maximum number of stations to process (for debugging). Defaults to sys.maxsize.
        """
        GribBuilder.__init__(
            self,
            load_spec,
            ingest_document,
            cluster,
            collection,
            number_stations=sys.maxsize,
        )
        self.cluster = cluster
        self.collection = collection
        self.number_stations = number_stations
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
            "fileType": "grib2",
            "originType": origin_type,
            "loadJobId": self.load_spec["load_job_doc"]["id"],
            "dataSourceId": "GSL",
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
        if len(self.same_time_rows) != 0:
            self.handle_document()
        return self.document_map

    def load_data(self, doc, key, element):
        """This method builds the data array. It gets the data key ('data') and the data element
        which in this case is a set of arrays. This routine has to create the data array from these
        arrays which are lists of values ordered by domain_station.
        Args:
            doc (Object): The document being created
            key (string): Not used
            element (Object): the observation data

        Returns:
            doc (Object): The document being created
        """
        if "data" not in doc.keys() or doc["data"] is None:
            keys = list(element.keys())
            doc["data"] = []
            for i in range(len(self.domain_stations)):
                elem = {}
                for key in keys:
                    elem[key] = element[key][i]
                doc["data"].append(elem)
        return doc

    # named functions
    # pylint: disable=no-self-use
    def handle_ceiling(
        self, params_dict
    ):  # pylint: disable=unused-argument, disable=too-many-branches
        """
        the dict_params aren't used here since we need to
        select two messages (self.grbs.select is expensive since it scans the whole grib file).
        Each message is selected once and the station location data saved in an array,
        then all the domain_stations are iterated (in memory operation)
        to make the ceiling calculations for each station location.
        """
        # This is the original 'C' algorithm for grib
        # if(ceil_msl < -1000 ||
        #    ceil_msl > 1e10) {
        #   /* printf("setting ceil_agl for x/y %d/%d from %0f to 6000\n",xi,yj,ceil_msl); */
        #   ceil_agl = 6000;
        #   n_forced_clear++;
        # } else if(ceil_msl < 0) {
        #   /* weird '-1's in the grib files */
        #   printf("strange ceiling: %f. setting to 0.\n",ceil_msl);
        #   ceil_agl = 0;
        #   n_zero_ceils++;
        #  } else {
        #     ceil_agl = (ceil_msl - sfc_hgt)*0.32808; /* m -> tens of ft */
        #   }
        #   n_good_ceils++;
        #   if(ceil_agl < 0) {
        #     if(DEBUG == 1) {
        #       printf("negative AGL ceiling for %d: ceil (MSL?): %.0f sfc: %.0f (ft)\n",
        #              sp->sta_id,ceil_msl*3.2808,sfc_hgt*3.2808);
        #     }
        #     ceil_agl = 0;
        #     n_zero_ceils++;
        #   }
        # }
        try:
            message = self.grbs.select(name="Orography")[0]
            values = message["values"]
            surface_values = []
            fcst_valid_epoch = round(message.validDate.timestamp())
            for station in self.domain_stations:
                geo_index = get_geo_index(fcst_valid_epoch, station["geo"])
                x_gridpoint = round(station["geo"][geo_index]["x_gridpoint"])
                y_gridpoint = round(station["geo"][geo_index]["y_gridpoint"])
                surface_values.append(values[y_gridpoint, x_gridpoint])

            message = self.grbs.select(
                name="Geopotential Height", typeOfFirstFixedSurface="215"
            )[0]
            values = message["values"]
            fcst_valid_epoch = round(message.validDate.timestamp())
            ceil_msl_values = []
            for station in self.domain_stations:
                geo_index = get_geo_index(fcst_valid_epoch, station["geo"])
                x_gridpoint = round(station["geo"][geo_index]["x_gridpoint"])
                y_gridpoint = round(station["geo"][geo_index]["y_gridpoint"])
                # what do we do with a masked ceiling value?
                if not numpy.ma.is_masked(values[y_gridpoint, x_gridpoint]):
                    ceil_msl_values.append(values[y_gridpoint, x_gridpoint])
                else:
                    # masked values should be treated as all clear i.e. 60000
                    ceil_msl_values.append(60000)
            ceil_agl = []
            i = 0
            for station in self.domain_stations:
                if ceil_msl_values[i] == 60000:
                    ceil_agl.append(60000)
                else:
                    if ceil_msl_values[i] is None or surface_values[i] is None:
                        ceil_agl.append(None)
                    else:
                        if ceil_msl_values[i] < -1000 or ceil_msl_values[i] > 1e10:
                            ceil_agl.append(60000)
                        else:
                            if ceil_msl_values[i] < 0:
                                # weird '-1's in the grib files??? (from legacy code)
                                ceil_agl.append(0)
                            else:
                                tmp_ceil = (
                                    ceil_msl_values[i] - surface_values[i]
                                ) * 3.281
                                if tmp_ceil < 0:
                                    ceil_agl.append(0)
                                else:
                                    ceil_agl.append(tmp_ceil)
                i = i + 1
            return ceil_agl
        except Exception as _e:  # pylint:disable=broad-except
            logging.error(
                "%s handle_ceiling: Exception  error: %s",
                self.__class__.__name__,
                str(_e),
            )
            return None

        # SURFACE PRESSURE

    def handle_surface_pressure(self, params_dict):
        """
        translate all the pressures(one per station location) to milibars
        """
        pressures = []
        for _v, v_intrp_pressure in list(params_dict.values())[
            0
        ]:  # pylint:disable=unused-variable
            # Convert from pascals to milibars
            pressures.append(float(v_intrp_pressure) / 100)
        return pressures

        # Visibility - convert to float

    def handle_visibility(self, params_dict):
        """translate visibility variable
        Args:
            params_dict (dict): named function parameters
        Returns:
            [float]: translated value
        """
        # convert all the values to a float
        vis_values = []
        for _v, v_intrp_ignore in list(
            params_dict.values()
        )[  # pylint: disable=unused-variable
            0
        ]:  # pylint:disable=unused-variable
            vis_values.append(float(_v) / 1609.344 if _v is not None else None)
        return vis_values

        # relative humidity - convert to float

    def handle_RH(self, params_dict):  # pylint:disable=invalid-name
        """translate relative humidity variable
        Args:
            params_dict (dict): named function parameters
        Returns:
            [float]: translated value
        """
        # convert all the values to a float
        rh_interpolated_values = []
        for _v, v_intrp_pressure in list(params_dict.values())[
            0
        ]:  # pylint:disable=unused-variable
            rh_interpolated_values.append(
                float(v_intrp_pressure) if v_intrp_pressure is not None else None
            )
        return rh_interpolated_values

    def kelvin_to_farenheight(self, params_dict):
        """
        param:params_dict expects {'station':{},'*variable name':variable_value}
        Used for temperature and dewpoint
        """
        # Convert each station value from Kelvin to Farenheit
        tempf_values = []
        for _v, v_intrp_tempf in list(params_dict.values())[
            0
        ]:  # pylint:disable=unused-variable
            tempf_values.append(
                ((float(v_intrp_tempf) - 273.15) * 9) / 5 + 32
                if v_intrp_tempf is not None
                else None
            )
        return tempf_values

        # WIND SPEED

    def handle_wind_speed(self, params_dict):  # pylint:disable=unused-argument
        """The params_dict aren't used here since we need to
        select two messages (self.grbs.select is expensive since it scans the whole grib file).
        Each message is selected once and the station location data saved in an array,
        then all the domain_stations are iterated (in memory operation)
        to make the wind speed calculations for each station location.
        Args:
            params_dict unused
        Returns:
            [int]: translated wind speed
        """
        # interpolated value cannot use rounded gridpoints
        message = self.grbs.select(name="10 metre U wind component")[0]
        values = message["values"]
        fcst_valid_epoch = round(message.validDate.timestamp())
        uwind_ms_values = []
        for station in self.domain_stations:
            geo_index = get_geo_index(fcst_valid_epoch, station["geo"])
            x_gridpoint = station["geo"][geo_index]["x_gridpoint"]
            y_gridpoint = station["geo"][geo_index]["y_gridpoint"]
            uwind_ms_values.append(gg.interpGridBox(values, y_gridpoint, x_gridpoint))

        message = self.grbs.select(name="10 metre V wind component")[0]
        values = message["values"]
        fcst_valid_epoch = round(message.validDate.timestamp())
        vwind_ms_values = []
        for station in self.domain_stations:
            geo_index = get_geo_index(fcst_valid_epoch, station["geo"])
            x_gridpoint = station["geo"][geo_index]["x_gridpoint"]
            y_gridpoint = station["geo"][geo_index]["y_gridpoint"]
            vwind_ms_values.append(gg.interpGridBox(values, y_gridpoint, x_gridpoint))
        # Convert from U-V components to speed and direction (requires rotation if grid is not earth relative)
        # wind speed then convert to mph
        ws_mph = []
        for _i, uwind_ms in enumerate(uwind_ms_values):
            vwind_ms = vwind_ms_values[_i]
            ws_ms = math.sqrt(  # pylint:disable=c-extension-no-member
                (uwind_ms * uwind_ms) + (vwind_ms * vwind_ms)
            )  # pylint:disable=c-extension-no-member
            ws_mph.append((ws_ms / 0.447) + 0.5)
        return ws_mph

        # wind direction

    def handle_wind_direction(
        self, params_dict
    ):  # pylint:disable=unused-argument, disable=too-many-locals
        """The params_dict aren't used here since we need to
        select two messages (self.grbs.select is expensive since it scans the whole grib file).
        Each message is selected once and the station location data saved in an array,
        then all the domain_stations are iterated (in memory operation)
        to make the wind direction calculations for each station location.
        Each individual station longitude is used to rotate the wind direction.
        Args:
            params_dict unused
        Returns:
            [int]: wind direction
        """
        uwind_message = self.grbs.select(name="10 metre U wind component")[0]
        u_values = uwind_message["values"]
        fcst_valid_epoch = round(uwind_message.validDate.timestamp())
        uwind_ms = []
        for station in self.domain_stations:
            geo_index = get_geo_index(fcst_valid_epoch, station["geo"])
            x_gridpoint = station["geo"][geo_index]["x_gridpoint"]
            y_gridpoint = station["geo"][geo_index]["y_gridpoint"]
            # interpolated value cannot use rounded gridpoints
            uwind_ms.append(gg.interpGridBox(u_values, y_gridpoint, x_gridpoint))

        vwind_message = self.grbs.select(name="10 metre V wind component")[0]
        v_values = vwind_message["values"]
        fcst_valid_epoch = round(vwind_message.validDate.timestamp())
        vwind_ms = []
        for station in self.domain_stations:
            geo_index = get_geo_index(fcst_valid_epoch, station["geo"])
            x_gridpoint = station["geo"][geo_index]["x_gridpoint"]
            y_gridpoint = station["geo"][geo_index]["y_gridpoint"]
            vwind_ms.append(gg.interpGridBox(v_values, y_gridpoint, x_gridpoint))

        _wd = []
        for i, u_val in enumerate(uwind_ms):
            # theta = gg.getWindTheta(vwind_message, station['lon'])
            # radians = math.atan2(uwind_ms, vwind_ms)
            # wd = (radians*57.2958) + theta + 180
            v_val = vwind_ms[i]
            geo_index = get_geo_index(fcst_valid_epoch, station["geo"])
            longitude = self.domain_stations[i]["geo"][geo_index]["lon"]
            theta = gg.getWindTheta(vwind_message, longitude)
            radians = math.atan2(  # pylint:disable=c-extension-no-member
                u_val, v_val
            )  # pylint:disable=c-extension-no-member
            wd_value = (radians * 57.2958) + theta + 180
            # adjust for outliers
            if wd_value < 0:
                wd_value = wd_value + 360
            if wd_value > 360:
                wd_value = wd_value - 360
            _wd.append(wd_value)
        return _wd

    def getName(
        self, params_dict
    ):  # pylint:disable=unused-argument,disable=invalid-name
        """translate the station name
        Args:
            params_dict (object): named function parameters
        Returns:
            list: station names
        """
        station_names = []
        for station in self.domain_stations:
            station_names.append(station["name"])
        return station_names

    def handle_time(self, params_dict):  # pylint: disable=unused-argument
        """return the time variable as an epoch
        Args:
            params_dict (object): named function parameters
        Returns:
            int: epoch
        """
        # validTime = grbs[1].validate -> 2021-07-12 15:00:00
        return round(self.grbm.validDate.timestamp())

    def handle_iso_time(self, params_dict):  # pylint: disable=unused-argument
        """return the time variable as an iso
        Args:
            params_dict (object): named function parameters
        Returns:
            string: iso time
        """
        valid_time = valid_time = self.grbm.validDate
        return valid_time.isoformat()

    def handle_fcst_len(self, params_dict):  # pylint: disable=unused-argument
        """return the fcst length variable as an int
        Args:
            params_dict (object): named function parameters
        Returns:
            int: forecast length
        """
        fcst_len = self.grbm.forecastTime
        return fcst_len
