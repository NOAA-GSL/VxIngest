# pylint: disable="too-many-lines"
"""
Program Name: Class grib_builder.py
Contact(s): Randy Pierce
History Log:  Initial version
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSL
"""

import os
import sys
import datetime as dt
import logging
import math
import numpy as np

from grib2_to_cb.grib_builder_parent import GribBuilder
from builder_common.builder_utilities import get_geo_index

# Get a logger with this module's name to help with debugging
logger = logging.getLogger(__name__)


# Concrete builders
class GribModelBuilderV01(GribBuilder):  # pylint:disable=too-many-instance-attributes
    """
    This is the builder for model data that is ingested from grib2 files. It is a concrete builder specifically
    for the model data.
    """

    def __init__(
        self,
        load_spec,
        ingest_document,
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
        Retrive the in-memory document map.
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
        if "data" not in doc.keys() or doc["data"] is None:
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
    # pylint: disable=no-self-use
    def handle_ceiling(
        self, params_dict
    ):  # pylint: disable=unused-argument, disable=too-many-branches
        """
        returns the ceiling values for all the stations in a list
        the dict_params aren't used here since the calculations are all done here
        """
        # This is the original 'C' algorithm for calculating ceiling from grib (trying to remain faithful to the original algorithm)
        # Notice that the result values are divided from meters by tens of meters i.e. 60000 is 6000 feet
        # in the original algorythm but the code here does no such thing.
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
            ceil_var_values = self.ds_translate_item_variables_map[
                "Cloud ceiling"
            ].values
            ceil_msl_values = []
            # print('fcst_valid_epoch',self.ds_translate_item_variables_map["fcst_valid_epoch"])
            for station in self.domain_stations:   # get the initial surface values and ceil_msl values for each station
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
                    ceil_msl_values.append(ceil_var_values[y_gridpoint, x_gridpoint])
            ceil_agl = []
            i = 0
            for station in self.domain_stations:  # determine the ceil_agl values for each station
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
                # print (station["geo"][0]['x_gridpoint'],station["geo"][0]['y_gridpoint'],round(ceil_msl_values[i],3), round(surface_values[i],3), round(ceil_agl[i],3))
                i = i + 1
            return ceil_agl
        except Exception as _e:  # pylint:disable=broad-except
            logger.error(
                "%s handle_ceiling: Exception  error: %s",
                self.__class__.__name__,
                str(_e),
            )
            return None

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
        for _v, v_intrp_ignore in list(  # pylint: disable=unused-variable
            params_dict.values()
        )[  # pylint: disable=unused-variable
            0
            # convert to miles (float)
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
            ws_ms = math.sqrt(  # pylint:disable=c-extension-no-member
                (uwind_ms * uwind_ms) + (vwind_ms * vwind_ms)
            )  # pylint:disable=c-extension-no-member
            ws_mph.append((float)((ws_ms / 0.447) + 0.5))
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
            # interpolated value cannot use rounded gridpoints
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
            radians = math.atan2(  # pylint:disable=c-extension-no-member
                u_val, v_val
            )  # pylint:disable=c-extension-no-member
            wd_value = (radians * 57.2958) + theta + 180
            # adjust for outliers
            if wd_value < 0:
                wd_value = wd_value + 360
            if wd_value > 360:
                wd_value = wd_value - 360
            _wd.append((float)(wd_value))
        return _wd

    def handle_wind_dir_u(self, params_dict):  # pylint: disable=unused-argument
        """returns the wind direction U component for this document
        Args:
            params_dict (dict): contains named_function parameters but is unused here
        Returns:
            float: wind direction U component
        """
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
            # interpolated value cannot use rounded gridpoints
            uwind_ms.append(
                (float)(self.interp_grid_box(u_values, y_gridpoint, x_gridpoint))
            )
        return uwind_ms

    def handle_wind_dir_v(self, params_dict):  # pylint: disable=unused-argument
        """returns the wind direction V component for this document
        Args:
            params_dict (dict): contains named_function parameters but is unused here
        Returns:
            float: wind direction V component
        """
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

    def handle_specific_humidity(self, params_dict):  # pylint: disable=unused-argument
        """returns the specific humidity for this document
        Specific humidity:kg kg**-1 (instant):lambert:heightAboveGround:level 2 m
        Args:
            params_dict (dict): contains named_function parameters but is unused here
        Returns:
            float: specific humidity
        """
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

    def handle_vegetation_type(self, params_dict):  # pylint:disable=unused-argument
        """returns the vegetation type for this document
        Args:
            params_dict (dict): contains named_function parameters but is unused here
        Returns:
            string: vegetation_type
        """
        values = self.ds_translate_item_variables_map["Vegetation Type"].values
        vegetation_type = []
        for station in self.domain_stations:
            geo_index = get_geo_index(
                self.ds_translate_item_variables_map["fcst_valid_epoch"], station["geo"]
            )
            x_gridpoint = station["geo"][geo_index]["x_gridpoint"]
            y_gridpoint = station["geo"][geo_index]["y_gridpoint"]
            vegetation_type.append(
                self.interp_grid_box(values, y_gridpoint, x_gridpoint)
            )
        return vegetation_type

    def getName(
        self, params_dict
    ):  # pylint:disable=unused-argument,disable=invalid-name
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

    def handle_time(self, params_dict):  # pylint: disable=unused-argument
        """return the time variable as an epoch
        Args:
            params_dict (object): named function parameters
        Returns:
            int: epoch
        """
        return (int)(self.ds_translate_item_variables_map["fcst_valid_epoch"])

    def handle_iso_time(self, params_dict):  # pylint: disable=unused-argument
        """return the time variable as an iso
        Args:
            params_dict (object): named function parameters
        Returns:
            string: iso time
        """
        return dt.datetime.utcfromtimestamp(
            (int)(self.ds_translate_item_variables_map["fcst_valid_epoch"])
        ).isoformat()

    def handle_fcst_len(self, params_dict):  # pylint: disable=unused-argument
        """return the fcst length variable as an int
        Args:
            params_dict (object): named function parameters
        Returns:
            int: forecast length
        """
        return (int)(self.ds_translate_item_variables_map["fcst_len"])
