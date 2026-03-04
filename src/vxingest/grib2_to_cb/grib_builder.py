"""
Program Name: Class grib_builder.py
Contact(s): Randy Pierce
History Log:  Initial version
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSL
"""

import datetime as dt
import logging
import math
import sys
from pathlib import Path

import numpy as np
from metpy.calc import virtual_temperature_from_dewpoint
from metpy.units import units

from vxingest.builder_common.builder_utilities import get_geo_index
from vxingest.grib2_to_cb.grib_builder_parent import GribBuilder

# Get a logger with this module's name to help with debugging
logger = logging.getLogger(__name__)


# Concrete builders
class GribModelBuilderV01(GribBuilder):
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

    def load_data(self, doc, element):
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
                        if isinstance(element[key], list):
                            elem[key] = element[key][i]
                        else:
                            elem[key] = element[key]
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
        Convert interpolated surface pressure values to millibars
        """
        pressures = []
        for _v, v_intrp_pressure in list(params_dict.values())[0]:
            # Convert from pascals to millibars
            pressures.append(float(v_intrp_pressure) / 100)
        return pressures

    def handle_mslp(self, params_dict):
        """
        Convert interpolated mean sea level pressure values to millibars
        """
        pressures = []
        for _v, v_intrp_pressure in list(params_dict.values())[0]:
            # Convert from pascals to millibars
            pressures.append(float(v_intrp_pressure) / 100)
        return pressures

    def handle_elevation(self, params_dict):
        """
        Retrieve elevation variable passed as param in ingest spec and return interpolated elevation
        values at station locations.

        Args:
            params_dict (dict): A dict with keys of variable names and corresponding values
                that are each a list of tuples (nearest_neighbor_value, interpolated_value)
                corresponding to station locations.
                Expecting a single variable, i.e. 1 key in dict.
        Returns:
            List of interpolated elevation values (float) corresponding to stations list
        """
        # add check for len of params_dict.keys()... warn/error if >1
        elev_list = [
            float(interp_elev) for _, interp_elev in list(params_dict.values())[0]
        ]
        return elev_list

    def handle_normalized_surface_pressure(self, params_dict):
        """
        Compute surface (2 m) pressure at station locations, adjusted for station elevation by using hypsometric equation to
        extrapolate from interpolated pressure and elevation to documented station elevation

        Inputs needed:
        - Model elevation, z0 (m) -- interpolated
        - Station elevation, z (m) -- from station metadata
        - Model surface pressure, P0 (Pa) -- interpolated
        - Model 2m temperature, T (K) -- interpolated
        - Model 2m dewpoint, Td (K) -- interpolated

        Hypsometric Equation Transformed to find Pressure
        P=P0*EXP(-g(z-z0)/R*T)
        Vapor Pressure (Ambaum, 2020)
        Virtual Temperature (Hobbs, 2006)

        Use metpy to get virtual temperature

        Args:
            params_dict: not used

        Returns:
            List of pressure values in mb corresponding to stations list
        """
        norm_pressure_list = []

        # For all the input vars, they could be pulled from multiple sources
        #  1. grab from what's already been added to the doc/template (this will already be inpterpolated,
        #       but units may want to be reverted) --- not sure how to retrieve this (and if currently possible
        #       or would need some other code revisions)
        #  2. pass as params in the doc, which will use self.translate_template_item() to interpolate and pass
        #       as params to this method
        #  3. call self.ds_translate_item_variables_map[var] and interpolate directly in this method --- may
        #       need to add more vars to the map
        #
        # For now, I think #3 will be the simplest to implement, but should consider other options for future to
        #   be cleanest and most efficient. (#2 and #3 will be doing some redundant work)

        P0_var = "Surface pressure"  # in Pa in grib
        T_var = "2 metre temperature"  # in K in grib
        Td_var = "2 metre dewpoint temperature"  # in K in grib
        z0_var = "Orography"  # in gpm in grib (gepotential height)

        vars = [P0_var, T_var, Td_var, z0_var]
        values = {}
        interp_values = {}
        station_elev_list = []

        for var in vars:
            values[var] = self.ds_translate_item_variables_map[var].values
            # just creating an empty list here for each var, will populate in next loop
            interp_values[var] = []

        for station in self.domain_stations:
            geo_index = get_geo_index(
                self.ds_translate_item_variables_map["fcst_valid_epoch"], station["geo"]
            )
            x_gridpoint = station["geo"][geo_index]["x_gridpoint"]
            y_gridpoint = station["geo"][geo_index]["y_gridpoint"]
            for var in vars:
                interp_values[var].append(
                    (float)(self.interp_grid_box(values[var], y_gridpoint, x_gridpoint))
                )
            # get station elev
            station_elev_list.append(station["geo"][geo_index]["elev"])

        # NOTE: z0 from model is geopotential height at surface, and z from station metadata is
        #   (presumably) geometric height. Converting model surface elevation from geopotential to
        #   geometric height is insignificant for the pressure calculations and will be left as
        #   geopotential height here.

        R = 287  # gas constant for dry air, J/(kg*K)
        g = 9.81  # gravity constant, m/s
        inst_ht = 2  # height of instrument AGL at stations (m)
        gamma = 0.0065  # standard lapse rate (K/m)

        for P0, T, Td, z, z0 in zip(
            interp_values[P0_var],
            interp_values[T_var],
            interp_values[Td_var],
            station_elev_list,
            interp_values[z0_var],
            strict=True,
        ):
            # don't compute if station elevation obviously bad
            if z < -200 or z > 7000:
                P_mb = None
            else:
                # get model 2m virtual temperature
                Tv_z0 = virtual_temperature_from_dewpoint(
                    pressure=P0 * units.Pa,
                    temperature=T * units.degK,
                    dewpoint=Td * units.degK,
                ).magnitude
                # approximate model virtual temperature for station elevation (hydrostatic)
                Tv_z = Tv_z0 - ((z - z0) * gamma)
                # approximate average virtual temperature of layer
                Tv_layer = (Tv_z0 + Tv_z) / 2
                P_Pa = P0 * math.exp(-(g * (z + inst_ht - z0)) / (R * Tv_layer))
                P_mb = P_Pa / 100
            norm_pressure_list.append(P_mb)

        return norm_pressure_list

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

    def handle_RH(self, params_dict):
        """translate relative humidity variable: convert to float
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
        spfh = []
        for station in self.domain_stations:
            geo_index = get_geo_index(
                self.ds_translate_item_variables_map["fcst_valid_epoch"], station["geo"]
            )
            x_gridpoint = station["geo"][geo_index]["x_gridpoint"]
            y_gridpoint = station["geo"][geo_index]["y_gridpoint"]
            spfh.append((float)(self.interp_grid_box(values, y_gridpoint, x_gridpoint)))
        return spfh

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
        return dt.datetime.fromtimestamp(
            int(self.ds_translate_item_variables_map["fcst_valid_epoch"]),
            tz=dt.UTC,
        ).isoformat()

    def handle_fcst_len(self, params_dict):
        """return the fcst length variable as an int
        Args:
            params_dict (object): named function parameters
        Returns:
            int: forecast length
        """
        return (int)(self.ds_translate_item_variables_map["fcst_len"])
