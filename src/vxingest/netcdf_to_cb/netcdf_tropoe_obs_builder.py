"""
Program Name: Class netcdf_builder.py
Contact(s): Randy Pierce
History Log:  Initial version
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSL
"""

import datetime as dt
import logging

import netCDF4 as nc

from vxingest.netcdf_to_cb.netcdf_builder_parent import NetcdfBuilder

# Get a logger with this module's name to help with debugging
logger = logging.getLogger(__name__)


class NetcdfTropoeObsBuilderV01(NetcdfBuilder):
    """
    This is the builder for observation data that is ingested from netcdf (tropoe) files.
    Since tropoe data is associated with one specific station, the builder will use a recnum of 0.
    The data section is a map that is indexed by height(interpolated to standard levels) above ground AGL.
    Each file contains the data for a single mobile station.
    """

    def build_document(self, queue_element: str) -> dict:
        """This is the entry point for the NetcfBuilders from the ingestManager.
        These documents are id'd by fcstValidEpoch. The data section is an array
        each element of which contains variable data and a station name. To process this
        file we need to iterate the document by base_var_index and process the station name along
        with all the other variables in the template.
        Args:
            file_name (str): the name of the file being processed
        Returns:
            [dict]: document
        """
        # type checks
        if not isinstance(queue_element, str):
            logger.error(
                "%s: Exception with builder build_document: file_name: %s, error: file_name is not a string",
                self.__class__.__name__,
                queue_element,
            )
            return {}
        try:
            self.same_time_rows = {}
            # bucket, scope, collection = self.get_database_connection_details(queue_element)
            self.ncdf_data_set = nc.Dataset(queue_element)
            document_map = self.build_3d_document_map(queue_element, "time", "tropoe")
            return document_map
        except FileNotFoundError:
            logger.error(
                "%s: Exception with builder build_document: file_name: %s, error: file not found - skipping this file",
                self.__class__.__name__,
                queue_element,
            )
            return {}
        except Exception as _e:
            logger.exception(
                "%s: Exception with builder build_document: file_name: %s",
                self.__class__.__name__,
                queue_element,
            )
            return {}

    # specific handlers
    def get_tropoe_valid_time(self, params_dict):
        """
        This function is used to get the valid time for the tropoe data.
        Args:
            **kwargs: keyword arguments
        Returns:
            [dict]: document
        """
        try:
            # This is the base_time + time_offset[base_var_index]
            base_time = params_dict["base_time"]
            time_offset = params_dict["time_offset"]
            epoch = int(base_time + time_offset)
            return int(epoch)
        except Exception as _e:
            logger.error(
                "%s : Exception in named function get_tropoe_valid_time:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
        return None

    def get_tropoe_valid_time_ISO(self, params_dict):
        """
        This function is used to get the valid time for the tropoe data.
        Args:
            **kwargs: keyword arguments
        Returns:
            [dict]: document
        """
        try:
            # This is the base_time + time_offset[base_var_index]
            epoch = self.get_tropoe_valid_time(params_dict)
            if epoch is None:
                return None
            # convert the epoch to an ISO time
            _iso_time = dt.datetime.utcfromtimestamp(epoch).isoformat()
            return _iso_time
        except Exception as _e:
            logger.error(
                "%s : Exception in named function get_tropoe_valid_time_ISO:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
        return None

        # # add the height and load the raw data into the document - convert km to m
        # data_elem["height"] = [
        #     i * 1000 for i in self.ncdf_data_set["height"][:].tolist()
        # ]
        # doc["raw_data"] = data_elem
        # # interpolate the data
        # interpolated_data = self.interpolate_3d_data(data_elem)
        # flat_interpolated_data = {}
        # flat_interpolated_data["levels"] = list(interpolated_data[key].keys())
        # for key in interpolated_data:
        #     flat_interpolated_data[key] = list(interpolated_data[key].values())
        # doc["data"] = flat_interpolated_data

    def get_raw_data(self, params_dict):
        raw_data = {}
        base_var_index = params_dict["base_var_index"]
        variables = []
        for k in params_dict:
            if k == "base_var_index":
                continue
            variables.append(k)
        try:
            for variable in variables:
                raw_data[variable] = self.ncdf_data_set[variable][
                    base_var_index
                ].tolist()
            # add the height
            raw_data["height"] = [
                v * 1000 for v in self.ncdf_data_set["height"][:].tolist()
            ]
        except Exception as _e:
            logger.error(f"*** get_raw_data: Exception: {str(_e)}")
            raise _e
        return raw_data

    def get_interpolated_data(self, params_dict):
        interpolated_data = {}
        lower = 0
        lower_variable = "lower"
        upper = 500
        upper_variable = "upper"
        try:
            for variable in params_dict:
                if variable.startswith("lower"):
                    lower = int(params_dict[variable].replace("lower:", ""))
                    lower_variable = variable
                if variable.startswith("upper"):
                    upper = int(params_dict[variable].replace("upper:", ""))
                    upper_variable = variable
            del params_dict[lower_variable]
            del params_dict[upper_variable]
            _raw_data = self.get_raw_data(params_dict)
            interpolated_data = self.interpolate_3d_data(_raw_data)
            # Flatten the interpolated data
            flat_interpolated_data = {}
            flat_interpolated_data["levels"] = list(
                interpolated_data[list(interpolated_data.keys())[0]].keys()
            )
            for key in interpolated_data:
                flat_interpolated_data[key] = list(interpolated_data[key].values())[
                    lower:upper
                ]
        except Exception as _e:
            logger.error(f"*** get_interpolated_data: Exception: {str(_e)}")
            raise _e
        return flat_interpolated_data
