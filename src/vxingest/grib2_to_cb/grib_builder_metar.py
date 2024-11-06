"""
Program Name: Class grib_builder.py
Contact(s): Randy Pierce
History Log:  Initial version
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSL
"""

import copy
import cProfile
import datetime as dt
import logging
import math
import sys
from pathlib import Path
from pstats import Stats

import cfgrib
import numpy as np
import pyproj
import xarray as xr

from vxingest.builder_common.builder_utilities import get_geo_index
from vxingest.grib2_to_cb.grib_builder_parent import GribBuilder

# Get a logger with this module's name to help with debugging
logger = logging.getLogger(__name__)


# Concrete builders
class GribModelMETARBuilderV01(GribBuilder):
    """
    This is the builder for model data that is ingested from grib2 files. It is a concrete builder specifically
    for the model data.
    """

    def __init__(
        self,
        load_spec,
        ingest_document,
        number_stations=sys.maxsize,
    ):
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
            number_stations=sys.maxsize,
        )
        self.number_stations = number_stations
        self.same_time_rows = []
        self.time = 0
        self.interpolated_time = 0
        self.delta = ingest_document["validTimeDelta"]
        self.cadence = ingest_document["validTimeInterval"]
        self.template = ingest_document["template"]
        self.subset = self.template["subset"]
        self.land_use_types = None
        # self.do_profiling = True  # set to True to enable build_document profiling
        self.do_profiling = False  # set to True to enable build_document profiling

    def build_document(self, queue_element):
        """
        This is the entry point for the gribBuilders from the ingestManager.
        The ingest manager is giving us a grib file to process from the queue.
        These documents are id'd by valid time and fcstLen. The data section is a dictionary
        indexed by station name each element of which contains variable data and a station name.
        To process this file we need to iterate the domain_stations list and process the
        station name along with all the required variables.
        1) get the first epoch - if none was specified get the latest one from the db
        2) transform the projection from the grib file
        3) determine the stations for this domain, adding grid points to each station - build a station list
        4) enable profiling if requested
        5) handle_document - iterate the template and process all the keys and values
        6) build a datafile document to record that this file has been processed
        7) cfgrib leaves .idx files in the directory - delete the .idx file

        NOTE: For cfgrib variables are contained in datasets. Some variables are continuous,
        like temperature, and some are non-continuous, like ceiling and visibility.
        The continuous variables must have their coordinates interpolated, but not the non-continuous
        variables.
        For cfgrib the variables defined in the templates are to be defined by their long_name attribute.
        for example there is a ds_height_above_ground_2m dataset and also a ds_height_above_ground_10m dataset.
        Each of those datasets can have multiple variables, but only one variable with a given long_name.

        A given dataset may have multiple variables with different long_names. For example "2 metre temperature"
        and "2 metre dewpoint temperature" are both in the ds_height_above_ground_2m dataset.
        """

        try:
            # get the bucket, scope, and collection from the load_spec
            bucket = self.load_spec["cb_connection"]["bucket"]
            scope = self.load_spec["cb_connection"]["scope"]
            collection = self.load_spec["cb_connection"]["collection"]

            # translate the projection from the grib file
            # The projection is the same for all the variables in the grib file,
            # so we only need to get it once and from one variable - we'll use heightAboveGround
            # for 2 meters.

            # heightAboveGround variables
            ds_height_above_ground_2m = xr.open_dataset(
                queue_element,
                engine="cfgrib",
                backend_kwargs={
                    "filter_by_keys": {
                        "typeOfLevel": "heightAboveGround",
                        "stepType": "instant",
                        "level": 2,
                    },
                    "read_keys": ["projString"],
                    "indexpath": "",
                },
            )
            ds_height_above_ground_10m = xr.open_dataset(
                queue_element,
                engine="cfgrib",
                backend_kwargs={
                    "filter_by_keys": {
                        "typeOfLevel": "heightAboveGround",
                        "stepType": "instant",
                        "level": 10,
                    },
                    "indexpath": "",
                },
            )
            in_proj = pyproj.Proj(proj="latlon")
            proj_string = ds_height_above_ground_2m.r2.attrs["GRIB_projString"]
            max_x = ds_height_above_ground_2m.r2.attrs["GRIB_Nx"]
            max_y = ds_height_above_ground_2m.r2.attrs["GRIB_Ny"]
            spacing = ds_height_above_ground_2m.r2.attrs["GRIB_DxInMetres"]
            latitude_of_first_grid_point_in_degrees = (
                ds_height_above_ground_2m.r2.attrs[
                    "GRIB_latitudeOfFirstGridPointInDegrees"
                ]
            )
            longitude_of_first_grid_point_in_degrees = (
                ds_height_above_ground_2m.r2.attrs[
                    "GRIB_longitudeOfFirstGridPointInDegrees"
                ]
            )
            proj_params_dict = self.get_proj_params_from_string(proj_string)
            in_proj = pyproj.Proj(proj="latlon")
            out_proj = self.get_grid(
                proj_params_dict,
                latitude_of_first_grid_point_in_degrees,
                longitude_of_first_grid_point_in_degrees,
            )
            transformer = pyproj.Transformer.from_proj(
                proj_from=in_proj, proj_to=out_proj
            )
            # use these if necessary to comare projections for debugging
            # print()
            # print ('in_proj', in_proj, 'out_proj', out_proj, 'max_x', max_x, 'max_y', max_y, 'spacing', spacing)

            # we get the fcst_valid_epoch and fcst_len once for the entire file, from the heightAboveGround
            ds_fcst_valid_epoch = (
                ds_height_above_ground_2m.valid_time.values.astype("uint64") / 10**9
            ).astype("uint32")
            ds_fcst_len = (int)((ds_height_above_ground_2m.step.values) / 1e9 / 3600)

            # height_above_ground variables
            ds_hgt_2_metre_temperature = ds_height_above_ground_2m.filter_by_attrs(
                long_name="2 metre temperature"
            )
            # to get the values you can use the following...
            # ds_hgt_2_metre_temperature.variables[list(ds_2_metre_temperature.data_vars.keys())[0]].values
            ds_hgt_2_metre_dewpoint_temperature = (
                ds_height_above_ground_2m.filter_by_attrs(
                    long_name="2 metre dewpoint temperature"
                )
            )
            # to get the values you can use the following...
            # ds_hgt_2_metre_dewpoint_temperature.variables[list(ds_2_metre_dewpoint_temperature.data_vars.keys())[0]].values
            ds_hgt_2_metre_relative_humidity = (
                ds_height_above_ground_2m.filter_by_attrs(
                    long_name="2 metre relative humidity"
                )
            )
            # to get the values you can use the following...
            # ds_hgt_2_metre_relative_humidity.variables[list(ds_2_metre_relative_humidity.data_vars.keys())[0]].values
            ds_hgt_2_metre_specific_humidity = (
                ds_height_above_ground_2m.filter_by_attrs(
                    long_name="2 metre specific humidity"
                )
            )
            # to get the values you can use the following...
            # ds_hgt_2_metre_specific_humidity.variables[list(ds_hgt_2_metre_specific_humidity.data_vars.keys())[0]].values

            ds_hgt_10_metre_u_component_of_wind = (
                ds_height_above_ground_10m.filter_by_attrs(
                    long_name="10 metre U wind component"
                )
            )
            # to get the values you can use the following...
            # ds_10_metre_u_component_of_wind.variables[list(ds_10_metre_u_component_of_wind.data_vars.keys())[0]].values
            ds_hgt_10_metre_v_component_of_wind = (
                ds_height_above_ground_10m.filter_by_attrs(
                    long_name="10 metre V wind component"
                )
            )
            # to get the values you can use the following...
            # ds_hgt_10_metre_v_component_of_wind.variables[list(ds_hgt_10_metre_v_component_of_wind.data_vars.keys())[0]].values

            # ceiling variables - this one is different because it only has one variable
            ds_cloud_ceiling = xr.open_dataset(
                queue_element,
                engine="cfgrib",
                backend_kwargs={
                    "filter_by_keys": {
                        "typeOfLevel": "cloudCeiling",
                        "stepType": "instant",
                    },
                    "indexpath": "",
                },
            )
            # to get the values you can use the following...
            # ds_cloud_ceiling.variables[list(ds_cloud_ceiling.data_vars.keys())[0]].values

            # surface variables
            ds_surface = xr.open_dataset(
                queue_element,
                engine="cfgrib",
                backend_kwargs={
                    "filter_by_keys": {"typeOfLevel": "surface", "stepType": "instant"},
                    "read_keys": ["projString"],
                    "indexpath": "",
                },
            )
            ds_surface_pressure = ds_surface.filter_by_attrs(
                long_name="Surface pressure"
            )
            # to get the values you can use the following...
            # ds_surface_pressure.variables[list(ds_surface_pressure.data_vars.keys())[0]].values
            ds_surface_orog = ds_surface.filter_by_attrs(long_name="Orography")
            # to get the values you can use the following...
            # ds_surface_orog.variables[list(ds_surface_orog.data_vars.keys())[0]].values
            ds_surface_visibility = ds_surface.filter_by_attrs(long_name="Visibility")
            # to get the values you can use the following...
            # ds_surface_visibility.variables[list(ds_surface_visibility.data_vars.keys())[0]].values
            ds_surface_vegetation_type = ds_surface.filter_by_attrs(
                long_name="Vegetation Type"
            )
            # to get the values you can use the following...
            # ds_surface_vegetation_type.variables[list(ds_surface_vegetation_type.data_vars.keys())[0]].values

            # set up the variables map for the translate_template_item method. this way only the
            # translation map needs to be a class variable. Better data hiding.
            # It seems that cfgrib is graceful about missing variables, so we don't need to check
            # for them here. But when we try to get the indexed values we will get an exception
            # if the variable is missing. We will catch that exception and return an empty document
            try:
                self.ds_translate_item_variables_map = {
                    "2 metre temperature": ds_hgt_2_metre_temperature.variables[
                        list(ds_hgt_2_metre_temperature.data_vars.keys())[0]
                    ]
                    if len(list(ds_hgt_2_metre_temperature.data_vars.keys())) > 0
                    else None,
                    "2 metre dewpoint temperature": ds_hgt_2_metre_dewpoint_temperature.variables[
                        list(ds_hgt_2_metre_dewpoint_temperature.data_vars.keys())[0]
                    ]
                    if len(list(ds_hgt_2_metre_dewpoint_temperature.data_vars.keys()))
                    > 0
                    else None,
                    "2 metre relative humidity": ds_hgt_2_metre_relative_humidity.variables[
                        list(ds_hgt_2_metre_relative_humidity.data_vars.keys())[0]
                    ]
                    if len(list(ds_hgt_2_metre_relative_humidity.data_vars.keys())) > 0
                    else None,
                    "2 metre specific humidity": ds_hgt_2_metre_specific_humidity.variables[
                        list(ds_hgt_2_metre_specific_humidity.data_vars.keys())[0]
                    ]
                    if len(list(ds_hgt_2_metre_specific_humidity.data_vars.keys())) > 0
                    else None,
                    "10 metre U wind component": ds_hgt_10_metre_u_component_of_wind.variables[
                        list(ds_hgt_10_metre_u_component_of_wind.data_vars.keys())[0]
                    ]
                    if len(list(ds_hgt_10_metre_u_component_of_wind.data_vars.keys()))
                    > 0
                    else None,
                    "10 metre V wind component": ds_hgt_10_metre_v_component_of_wind.variables[
                        list(ds_hgt_10_metre_v_component_of_wind.data_vars.keys())[0]
                    ]
                    if len(list(ds_hgt_10_metre_v_component_of_wind.data_vars.keys()))
                    > 0
                    else None,
                    "Surface pressure": ds_surface_pressure.variables[
                        list(ds_surface_pressure.data_vars.keys())[0]
                    ]
                    if len(list(ds_surface_pressure.data_vars.keys())) > 0
                    else None,
                    "Visibility": ds_surface_visibility.variables[
                        list(ds_surface_visibility.data_vars.keys())[0]
                    ]
                    if len(list(ds_surface_visibility.data_vars.keys())) > 0
                    else None,
                    "Orography": ds_surface_orog.variables[
                        list(ds_surface_orog.data_vars.keys())[0]
                    ]
                    if len(list(ds_surface_orog.data_vars.keys())) > 0
                    else None,
                    "Cloud ceiling": ds_cloud_ceiling.variables[
                        list(ds_cloud_ceiling.data_vars.keys())[0]
                    ]
                    if len(list(ds_cloud_ceiling.data_vars.keys())) > 0
                    else None,
                    "Vegetation Type": ds_surface_vegetation_type.variables[
                        list(ds_surface_vegetation_type.data_vars.keys())[0]
                    ]
                    if len(list(ds_surface_vegetation_type.data_vars.keys())) > 0
                    else None,
                    "fcst_valid_epoch": ds_fcst_valid_epoch,
                    "fcst_len": ds_fcst_len,
                    "proj_params": proj_params_dict,
                }
            except IndexError as _e:
                logger.exception(
                    "%s: Exception with builder build_document retrieving grib variables: error: %s",
                    self.__class__.__name__,
                    _e,
                )
                # remove any idx file that may have been created
                self.delete_idx_file(queue_element)
                # return an empty document_map
                return {}
            # reset the builders document_map for a new file
            self.initialize_document_map()
            # get stations from couchbase and filter them so
            # that we retain only the ones for this models domain which is derived from the projection
            # also fill in the gridpoints for each station and for each geo within each station
            # NOTE: this is not about regions, this is about models
            self.domain_stations = []
            limit_clause = ";"
            if self.number_stations != sys.maxsize:
                limit_clause = f" limit {self.number_stations};"
            result = self.load_spec["cluster"].query(
                f"""SELECT geo, name
                    from `{bucket}`.{scope}.{collection}
                    where type='MD'
                    and docType='station'
                    and subset='{self.subset}'
                    and version='V01'
                    {limit_clause}"""
            )
            for row in result:
                station = copy.deepcopy(row)
                for geo_index in range(len(row["geo"])):
                    lat = row["geo"][geo_index]["lat"]
                    lon = row["geo"][geo_index]["lon"]
                    if lat == -90 and lon == 180:
                        continue  # don't know how to transform that station
                    (
                        _x,
                        _y,
                    ) = transformer.transform(lon, lat, radians=False)
                    x_gridpoint = _x / spacing
                    y_gridpoint = _y / spacing
                    # use for debugging if you must
                    # print (f"transform - lat: {lat}, lon: {lon}, x_gridpoint: {x_gridpoint}, y_gridpoint: {y_gridpoint}")
                    try:
                        if (
                            math.floor(x_gridpoint) < 0
                            or math.ceil(x_gridpoint) >= max_x
                            or math.floor(y_gridpoint) < 0
                            or math.ceil(y_gridpoint) >= max_y
                        ):
                            continue
                    except Exception as _e:
                        logger.error(
                            "%s: Exception with builder build_document processing station: error: %s",
                            self.__class__.__name__,
                            str(_e),
                        )
                        continue
                    # set the gridpoint for the station
                    station["geo"][geo_index]["x_gridpoint"] = x_gridpoint
                    station["geo"][geo_index]["y_gridpoint"] = y_gridpoint
                # if we have gridpoints for all the geos in the station, add it to the list
                has_gridpoints = True
                for elem in station["geo"]:
                    if "x_gridpoint" not in elem or "y_gridpoint" not in elem:
                        has_gridpoints = False
                if has_gridpoints:
                    self.domain_stations.append(station)
            # if we have asked for profiling go ahead and do it
            if self.do_profiling:
                with cProfile.Profile() as _pr:
                    self.handle_document()
                    with Path(self.profile_output_path / "profiling_stats.txt").open(
                        "w", encoding="utf-8"
                    ) as stream:
                        stats = Stats(_pr, stream=stream)
                        stats.strip_dirs()
                        stats.sort_stats("time")
                        stats.dump_stats(
                            self.profile_output_path / "profiling_stats.prof"
                        )
                        stats.print_stats()
            else:
                self.handle_document()

            document_map = self.get_document_map()
            data_file_id = self.create_data_file_id(
                self.subset, "grib2", self.template["model"], queue_element
            )
            if data_file_id is None:
                logger.error(
                    "%s: Failed to create DataFile ID:", self.__class__.__name__
                )
            data_file_doc = self.build_datafile_doc(
                file_name=queue_element,
                data_file_id=data_file_id,
                origin_type=self.template["model"],
            )
            document_map[data_file_doc["id"]] = data_file_doc
            self.delete_idx_file(queue_element)
            return document_map
        except (FileNotFoundError, cfgrib.IOProblemError):
            logger.error(
                "%s: Exception with builder build_document: file_name: %s, error: file not found or problem reading file - skipping this file",
                self.__class__.__name__,
                queue_element,
            )
            # remove any idx file that may have been created
            self.delete_idx_file(queue_element)
            return {}
        except Exception as _e:
            logger.exception(
                "%s: Exception with builder build_document: file_name: %s, exception %s",
                self.__class__.__name__,
                queue_element,
                _e,
            )
            # remove any idx file that may have been created
            self.delete_idx_file(queue_element)
            return {}

    def build_datafile_doc(self, file_name, data_file_id, origin_type):
        """
        This method will build a 'dataFile document' for GribBuilder. The dataFile
        document will represent the file that is ingested by the GribBuilder for audit purposes.
        This is not a Data Document. The document is intended to be added to the output folder
        and imported with the other data documents. The VxIngest will query the existing
        dataFile documents to determine if a specific file has already been ingested.
        """
        mtime = Path(file_name).stat().st_mtime
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
        Retrieve the in-memory document map.
        In case there are leftovers we have to process them first using handle_document.
        Returns:
            map(dict): the document_map
        """
        if len(self.same_time_rows) != 0:
            self.handle_document()
        return self.document_map

    def load_data(self, doc, key, element):
        """This method builds the data dictionary. It gets the data key ('data') and the data element
        which in this case is a map indexed by station name.
        Args:
            doc (Object): The document being created
            key (string): Not used
            element (Object): the observation data

        Returns:
            doc (Object): The document being created
        """
        if "data" not in doc or doc["data"] is None:
            keys = list(element.keys())
            doc["data"] = {}
            for i in range(len(self.domain_stations)):
                elem = {}
                for key in keys:
                    if element[key] is not None:
                        elem[key] = element[key][i]
                    else:
                        elem[key] = None
                doc["data"][elem["name"]] = elem
        return doc

    # named functions
    def handle_ceiling(self, params_dict):
        """
        returns the ceiling values for all the stations in a list
        the dict_params aren't used here since the calculations are all done here
        """
        # This is the original 'C' algorithm for calculating ceiling from grib (trying to remain faithful to the original algorithm)
        # Notice that the result values are divided from meters by tens of meters i.e. 60000 is 6000 feet
        # in the original algorithm but the code here does no such thing.
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
            # surface values
            surface_var_values = self.ds_translate_item_variables_map[
                "Orography"
            ].values
            surface_values = []
            if self.ds_translate_item_variables_map["Cloud ceiling"] is None:
                return None
            ceil_var_values = self.ds_translate_item_variables_map[
                "Cloud ceiling"
            ].values
            ceil_msl_values = []

            # get the initial surface values and ceil_msl values for each station
            for station in self.domain_stations:
                geo_index = get_geo_index(
                    self.ds_translate_item_variables_map["fcst_valid_epoch"],
                    station["geo"],
                )
                x_gridpoint = round(station["geo"][geo_index]["x_gridpoint"])
                y_gridpoint = round(station["geo"][geo_index]["y_gridpoint"])
                surface_values.append(surface_var_values[y_gridpoint, x_gridpoint])
                if (
                    np.isnan(ceil_var_values[int(y_gridpoint)][int(x_gridpoint)])
                    is np.True_
                ):
                    ceil_msl_values.append(60000)
                else:
                    # convert to np.float64 because np.float32 is not json serializable
                    ceil_msl_values.append(
                        np.float64(ceil_var_values[y_gridpoint, x_gridpoint])
                    )
            ceil_agl = []
            # determine the ceil_agl values for each station
            for i, _station in enumerate(self.domain_stations):
                ceil_msl = ceil_msl_values[i]
                surface = surface_values[i]

                if ceil_msl == 60000 or ceil_msl < -1000 or ceil_msl > 1e10:
                    ceil_agl.append(60000)
                elif ceil_msl is None or surface is None:
                    ceil_agl.append(None)
                # handle weird '-1's in the grib files??? (from legacy code)
                elif ceil_msl < 0:
                    ceil_agl.append(0)
                else:
                    tmp_ceil = (ceil_msl - surface) * 3.281  # m -> ft
                    ceil_agl.append(0 if tmp_ceil < 0 else tmp_ceil)
            return ceil_agl
        except Exception as _e:
            logger.error(
                "%s handle_ceiling: Exception  error: %s",
                self.__class__.__name__,
                str(_e),
            )
            return None

    def handle_surface_pressure(self, params_dict):
        """
        translate all the pressures(one per station location) to millibars
        """
        pressures = []
        for _v, v_intrp_pressure in list(params_dict.values())[0]:
            # Convert from pascals to millibars
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
        for _v, _v_intrp_ignore in list(params_dict.values())[0]:
            vis_values.append(float(_v) / 1609.344 if _v is not None else None)
        return vis_values

        # relative humidity - convert to float

    def handle_RH(self, params_dict):
        """translate relative humidity variable
        Args:
            params_dict (dict): named function parameters
        Returns:
            [float]: translated value
        """
        # convert all the values to a float
        rh_interpolated_values = []
        for _v, v_intrp_pressure in list(params_dict.values())[0]:
            rh_interpolated_values.append(
                float(v_intrp_pressure) if v_intrp_pressure is not None else None
            )
        return rh_interpolated_values

    def kelvin_to_fahrenheit(self, params_dict):
        """
        param:params_dict expects {'station':{},'*variable name':variable_value}
        Used for temperature and dewpoint
        """
        # Convert each station value from Kelvin to Fahrenheit
        tempf_values = []
        for _v, v_intrp_tempf in list(params_dict.values())[0]:
            tempf_values.append(
                ((float(v_intrp_tempf) - 273.15) * 9) / 5 + 32
                if v_intrp_tempf is not None
                else None
            )
        return tempf_values

        # WIND SPEED

    def handle_wind_speed(self, params_dict):
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
        # interpolated value cannot use rounded grid points
        if self.ds_translate_item_variables_map["10 metre U wind component"] is None:
            return None

        values = self.ds_translate_item_variables_map[
            "10 metre U wind component"
        ].values
        uwind_ms_values = []
        for station in self.domain_stations:
            geo_index = get_geo_index(
                self.ds_translate_item_variables_map["fcst_valid_epoch"], station["geo"]
            )
            x_gridpoint = station["geo"][geo_index]["x_gridpoint"]
            y_gridpoint = station["geo"][geo_index]["y_gridpoint"]
            uwind_ms_values.append(
                (float)(self.interp_grid_box(values, y_gridpoint, x_gridpoint))
            )

        values = self.ds_translate_item_variables_map[
            "10 metre V wind component"
        ].values
        vwind_ms_values = []
        for station in self.domain_stations:
            geo_index = get_geo_index(
                self.ds_translate_item_variables_map["fcst_valid_epoch"], station["geo"]
            )
            x_gridpoint = station["geo"][geo_index]["x_gridpoint"]
            y_gridpoint = station["geo"][geo_index]["y_gridpoint"]
            vwind_ms_values.append(
                (float)(self.interp_grid_box(values, y_gridpoint, x_gridpoint))
            )
        # Convert from U-V components to speed and direction (requires rotation if grid is not earth relative)
        # wind speed then convert to mph
        ws_mph = []
        for _i, uwind_ms in enumerate(uwind_ms_values):
            vwind_ms = vwind_ms_values[_i]
            ws_ms = math.sqrt((uwind_ms * uwind_ms) + (vwind_ms * vwind_ms))
            ws_mph.append((float)((ws_ms / 0.447) + 0.5))
        return ws_mph

        # wind direction

    def handle_wind_direction(self, params_dict):
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
        if self.ds_translate_item_variables_map["10 metre U wind component"] is None:
            return None
        u_values = self.ds_translate_item_variables_map[
            "10 metre U wind component"
        ].values
        uwind_ms = []
        for station in self.domain_stations:
            geo_index = get_geo_index(
                self.ds_translate_item_variables_map["fcst_valid_epoch"], station["geo"]
            )
            x_gridpoint = station["geo"][geo_index]["x_gridpoint"]
            y_gridpoint = station["geo"][geo_index]["y_gridpoint"]
            # interpolated value cannot use rounded grid points
            uwind_ms.append(self.interp_grid_box(u_values, y_gridpoint, x_gridpoint))
        # vwind_message = self.grbs.select(name="10 metre V wind component")[0]
        v_values = self.ds_translate_item_variables_map[
            "10 metre V wind component"
        ].values
        vwind_ms = []
        for station in self.domain_stations:
            geo_index = get_geo_index(
                self.ds_translate_item_variables_map["fcst_valid_epoch"], station["geo"]
            )
            x_gridpoint = station["geo"][geo_index]["x_gridpoint"]
            y_gridpoint = station["geo"][geo_index]["y_gridpoint"]
            vwind_ms.append(self.interp_grid_box(v_values, y_gridpoint, x_gridpoint))
        _wd = []
        for i, u_val in enumerate(uwind_ms):
            # theta = gg.getWindTheta(vwind_message, station['lon'])
            # radians = math.atan2(uwind_ms, vwind_ms)
            # wd = (radians*57.2958) + theta + 180
            v_val = vwind_ms[i]
            geo_index = get_geo_index(
                self.ds_translate_item_variables_map["fcst_valid_epoch"], station["geo"]
            )
            longitude = self.domain_stations[i]["geo"][geo_index]["lon"]
            lad_in_degrees = self.ds_translate_item_variables_map[
                "10 metre V wind component"
            ].attrs["GRIB_LaDInDegrees"]
            lov_in_degrees = self.ds_translate_item_variables_map[
                "10 metre V wind component"
            ].attrs["GRIB_LoVInDegrees"]
            proj_params = self.ds_translate_item_variables_map["proj_params"]
            theta = self.get_wind_theta(
                proj_params, lad_in_degrees, lov_in_degrees, longitude
            )
            radians = math.atan2(u_val, v_val)
            wd_value = (radians * 57.2958) + theta + 180
            # adjust for outliers
            if wd_value < 0:
                wd_value = wd_value + 360
            if wd_value > 360:
                wd_value = wd_value - 360
            _wd.append((float)(wd_value))
        return _wd

    def handle_wind_dir_u(self, params_dict):
        """returns the wind direction U component for this document
        Args:
            params_dict (dict): contains named_function parameters but is unused here
        Returns:
            float: wind direction U component
        """
        if self.ds_translate_item_variables_map["10 metre U wind component"] is None:
            return None
        u_values = self.ds_translate_item_variables_map[
            "10 metre U wind component"
        ].values
        uwind_ms = []
        for station in self.domain_stations:
            geo_index = get_geo_index(
                self.ds_translate_item_variables_map["fcst_valid_epoch"], station["geo"]
            )
            x_gridpoint = station["geo"][geo_index]["x_gridpoint"]
            y_gridpoint = station["geo"][geo_index]["y_gridpoint"]
            # interpolated value cannot use rounded grid points
            uwind_ms.append(
                (float)(self.interp_grid_box(u_values, y_gridpoint, x_gridpoint))
            )
        return uwind_ms

    def handle_wind_dir_v(self, params_dict):
        """returns the wind direction V component for this document
        Args:
            params_dict (dict): contains named_function parameters but is unused here
        Returns:
            float: wind direction V component
        """
        if self.ds_translate_item_variables_map["10 metre V wind component"] is None:
            return None
        v_values = self.ds_translate_item_variables_map[
            "10 metre V wind component"
        ].values
        vwind_ms = []
        for station in self.domain_stations:
            geo_index = get_geo_index(
                self.ds_translate_item_variables_map["fcst_valid_epoch"], station["geo"]
            )
            x_gridpoint = station["geo"][geo_index]["x_gridpoint"]
            y_gridpoint = station["geo"][geo_index]["y_gridpoint"]
            vwind_ms.append(
                (float)(self.interp_grid_box(v_values, y_gridpoint, x_gridpoint))
            )
        return vwind_ms

    def handle_specific_humidity(self, params_dict):
        """returns the specific humidity for this document
        Specific humidity:kg kg**-1 (instant):lambert:heightAboveGround:level 2 m
        Args:
            params_dict (dict): contains named_function parameters but is unused here
        Returns:
            float: specific humidity
        """
        if self.ds_translate_item_variables_map["2 metre specific humidity"] is None:
            return None
        values = self.ds_translate_item_variables_map[
            "2 metre specific humidity"
        ].values
        specific_humidity = []
        for station in self.domain_stations:
            geo_index = get_geo_index(
                self.ds_translate_item_variables_map["fcst_valid_epoch"], station["geo"]
            )
            x_gridpoint = station["geo"][geo_index]["x_gridpoint"]
            y_gridpoint = station["geo"][geo_index]["y_gridpoint"]
            specific_humidity.append(
                float(self.interp_grid_box(values, y_gridpoint, x_gridpoint))
            )
        return specific_humidity

    def handle_vegetation_type(self, params_dict):
        """returns the vegetation type for this document
        Args:
            params_dict (dict): contains named_function parameters but is unused here
        Returns:
            string: vegetation_type
        """
        if self.ds_translate_item_variables_map["Vegetation Type"] is None:
            return None
        values = self.ds_translate_item_variables_map["Vegetation Type"].values
        vegetation_type = []
        # I don't know which land_use_type to use i.e. USGS,MODIFIED_IGBP_MODIS_NOAH,NLCD40,USGS-RUC, or MODI-RUC
        # or which land_use_type_index i.e. "0". Jeff said to use these values but we should better understand why.
        land_use_type = "USGS"
        land_use_type_index = "0"
        # using lazy initialization get the land use types from the metadata, if not there set them to {}
        if self.land_use_types is None:
            try:
                # get the land use types from the metadata
                land_use_metadata = (
                    self.load_spec["collection"]
                    .get("MD:LAND_USE_TYPES:COMMON:V01")
                    .content_as[dict]
                )
                self.land_use_types = land_use_metadata[land_use_type][
                    land_use_type_index
                ]
            except Exception as _e:
                logger.error(
                    "%s handle_vegetation_type: Exception  error: %s",
                    self.__class__.__name__,
                    str(_e),
                )
                self.land_use_types = {}
        for station in self.domain_stations:
            geo_index = get_geo_index(
                self.ds_translate_item_variables_map["fcst_valid_epoch"], station["geo"]
            )
            x_gridpoint = station["geo"][geo_index]["x_gridpoint"]
            y_gridpoint = station["geo"][geo_index]["y_gridpoint"]
            vegetation_type_USGS_index = str(
                round(self.interp_grid_box(values, y_gridpoint, x_gridpoint))
            )
            vegetation_type_str = self.land_use_types.get(
                vegetation_type_USGS_index, None
            )
            vegetation_type.append(vegetation_type_str)
        return vegetation_type

    def getName(self, params_dict):
        """translate the station name
        Args:
            params_dict (object): named function parameters - unused here
        Returns:
            list: station names
        """
        station_names = []
        for station in self.domain_stations:
            station_names.append(station["name"])
        return station_names

    def handle_time(self, params_dict):
        """return the time variable as an epoch
        Args:
            params_dict (object): named function parameters
        Returns:
            int: epoch
        """
        return (int)(self.ds_translate_item_variables_map["fcst_valid_epoch"])

    def handle_iso_time(self, params_dict):
        """return the time variable as an iso
        Args:
            params_dict (object): named function parameters
        Returns:
            string: iso time
        """
        return dt.datetime.utcfromtimestamp(
            (int)(self.ds_translate_item_variables_map["fcst_valid_epoch"])
        ).isoformat()

    def handle_fcst_len(self, params_dict):
        """return the fcst length variable as an int
        Args:
            params_dict (object): named function parameters
        Returns:
            int: forecast length
        """
        return (int)(self.ds_translate_item_variables_map["fcst_len"])


