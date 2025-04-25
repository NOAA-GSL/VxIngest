"""
Program Name: Class netcdf_builder.py
Contact(s): Randy Pierce
History Log:  Initial version
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSL
"""

import logging
import math
import re
import time
import traceback

import netCDF4 as nc
import numpy.ma as ma

from vxingest.builder_common.builder_utilities import truncate_round
from vxingest.netcdf_to_cb.netcdf_builder_parent import NetcdfBuilder

# Get a logger with this module's name to help with debugging
logger = logging.getLogger(__name__)


# Concrete builders
class NetcdfMetarObsBuilderV01(NetcdfBuilder):
    """
    This is the builder for observation data that is ingested from netcdf (madis) files
    """

    def __init__(self, load_spec, ingest_document):
        """
        This builder creates a set of V01 obs documents using the V01 station documents.
        This builder loads V01 station data into memory, and uses them to associate a station with an observation
        lat, lon point.
        In each document the observation data is an array of objects each of which is the obs data
        for a specific station.
        If a station from a metar file does not exist in the couchbase database
        a station document will be created from the metar record data and
        the station document will be added to the document map. If a station location has changed
        the geo element will be updated to have an additional geo element that has the new location
        and time bracket for the location.
        :param ingest_document: the document from the ingest document
        :param cluster: - a Couchbase cluster object, used for N1QL queries (QueryService)
        :param collection: - essentially a couchbase connection object, used to get documents by id (DataService)
        """
        NetcdfBuilder.__init__(self, load_spec, ingest_document)
        self.same_time_rows = []
        self.time = 0
        self.interpolated_time = 0
        self.delta = ingest_document["validTimeDelta"]
        self.cadence = ingest_document["validTimeInterval"]
        self.template = ingest_document["template"]
        self.subset = self.template["subset"]
        # self.do_profiling = True  # set to True to enable build_document profiling
        self.do_profiling = False  # set to True to enable build_document profiling

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

        try:
            bucket, scope, collection = self.get_database_connection_details(
                queue_element
            )

            self.ncdf_data_set = nc.Dataset(queue_element)
            if len(self.stations) == 0:
                stmnt = f"""SELECT {self.subset}.*
                    FROM `{bucket}`.{scope}.{collection}
                    WHERE type = 'MD'
                    AND docType = 'station'
                    AND subset = '{self.subset}'
                    AND version = 'V01';"""
                result = self.load_spec["cluster"].query(stmnt)
                self.stations = list(result)
            # handle stations here?
            rec_num_var_data_size = self.ncdf_data_set.dimensions["recNum"].size
            if rec_num_var_data_size == 0:
                return
            for _rec_num in range(rec_num_var_data_size):
                _station_name = str(
                    nc.chartostring(self.ncdf_data_set["stationName"][_rec_num])
                )
                self.handle_station(
                    {"base_var_index": _rec_num, "stationName": _station_name}
                )
            document_map = self.build_document_map(queue_element, "recNum", "madis")
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

    def ceiling_transform(self, params_dict):
        """retrieves skyCover and skyLayerBase data and transforms it into a Ceiling value
        Args:
            params_dict (dict): named function parameters
        Returns:
            [type]: [description]
        """
        try:
            skyCover = params_dict["skyCover"]
            skyLayerBase = params_dict["skyLayerBase"]
            # code clear as 60,000 ft
            mCLR = re.compile(".*CLR.*")
            mSKC = re.compile(".*SKC.*")
            mNSC = re.compile(".*NSC.*")
            mFEW = re.compile(".*FEW.*")
            mSCT = re.compile(".*SCT.*")
            mBKN = re.compile(".*BKN.*")  # Broken
            mOVC = re.compile(".*OVC.*")  # Overcast
            mVV = re.compile(".*VV.*")  # Vertical Visibility
            # by the time we get here the skyLayerBase and skyCover arrays have been processed
            # to remove the masked values (replaced with fill values from translate_template_item)
            # but I need the mask here so I need to retrieve it from the netcdf again
            mask_array = self.ncdf_data_set["skyLayerBase"][
                params_dict["base_var_index"]
            ].mask.tolist()
            # mask_array = ma.getmaskarray(skyLayerBase)
            skyCover_array = skyCover[1:-1].replace("'", "").split(" ")
            # check for unmasked ceiling values - broken, overcast, vertical visibility - return associated skyLayerBase
            # name = str(nc.chartostring(self.ncdf_data_set['stationName'][params_dict['base_var_index']]))
            for index, sca_val in enumerate(skyCover_array):
                # also convert meters to feet (* 3.281)
                if (not mask_array[index]) and (
                    mBKN.match(sca_val) or mOVC.match(sca_val) or mVV.match(sca_val)
                ):
                    return math.floor(skyLayerBase[index] * 3.281)
            # check for unmasked ceiling values - all the others - CLR, SKC, NSC, FEW, SCT - return 60000
            for index, sca_val in enumerate(skyCover_array):
                # 60000 is aldready feet
                if (not mask_array[index]) and (
                    mCLR.match(sca_val)
                    or mSKC.match(sca_val)
                    or mNSC.match(sca_val)
                    or mFEW.match(sca_val)
                    or mSCT.match(sca_val)
                ):
                    return 60000
            # nothing was unmasked - return 60000 if there is a ceiling value in skycover array
            for sca_val in skyCover_array:
                if (
                    mCLR.match(sca_val)
                    or mSKC.match(sca_val)
                    or mNSC.match(sca_val)
                    or mFEW.match(sca_val)
                    or mSCT.match(sca_val)
                ):
                    return 60000
            #  masked and no ceiling value in skyCover_array
            return None
        except Exception as _e:
            logger.error(
                "%s ceiling_transform: Exception in named function ceiling_transform:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
            logger.error(
                "ceiling_transform skyCover_array: %s skyLayerBase %s",
                str(skyCover_array),
                str(skyLayerBase),
            )
            logger.error("ceiling_transform stacktrace %s", str(traceback.format_exc()))
            return None

    def handle_visibility(self, params_dict):
        """Retrieves a visibility value and performs data transformations
        Args:
            params_dict (dict): named function parameters
        Returns:
            float: the visibility in miles
        """
        # vis_sm = vis_m / 1609.344
        try:
            value = self.retrieve_from_netcdf(params_dict)
            if value is not None:
                value = float(value) / 1609.344
            return value
        except Exception as _e:
            logger.error(
                "%s handle_visibility: Exception in named function:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
            return None

    def fill_from_netcdf(self, base_var_index, netcdf):
        """
        Used by handle_stations to get the records from netcdf for comparing with the
        records from the database.
        """
        netcdf = {}
        if not ma.getmask(self.ncdf_data_set["latitude"][base_var_index]):
            netcdf["latitude"] = ma.compressed(
                self.ncdf_data_set["latitude"][base_var_index]
            )[0]
        else:
            netcdf["latitude"] = None
        if not ma.getmask(self.ncdf_data_set["longitude"][base_var_index]):
            netcdf["longitude"] = ma.compressed(
                self.ncdf_data_set["longitude"][base_var_index]
            )[0]
        else:
            netcdf["longitude"] = None
        if not ma.getmask(self.ncdf_data_set["elevation"][base_var_index]):
            netcdf["elevation"] = ma.compressed(
                self.ncdf_data_set["elevation"][base_var_index]
            )[0]
        else:
            netcdf["elevation"] = None

        netcdf["description"] = str(
            nc.chartostring(self.ncdf_data_set["locationName"][base_var_index])
        )
        netcdf["name"] = str(
            nc.chartostring(self.ncdf_data_set["stationName"][base_var_index])
        )
        return netcdf

    def handle_station(self, params_dict):
        """
        This method is called from the NetcdfMetarObsBuilderV01.build_document method to reconcile a station from the netcdf file
        with the station list from the database.
        This method uses the station name in the params_dict
        to find a station with that name from self.stations (which are all the station documents
        from couchbase).
        If the station does not exist it will be created with data from the
        netcdf file. If the station exists the lat, lon, and elev from the netcdf file
        will be compared to that in the existing station and if an update of the geo list is required it will be updated.
        Any modified or newly created stations get added to the document_map and automatically upserted.
        :param params_dict: {station_name:a_station_name}
        :return:
        """
        base_var_index = params_dict["base_var_index"]
        station_name = params_dict["stationName"]
        an_id = None
        netcdf = {}
        fcst_valid_epoch = self.derive_valid_time_epoch(
            {"file_name_pattern": self.load_spec["fmask"]}
        )

        try:
            # get the netcdf fields for comparing or adding new
            netcdf = self.fill_from_netcdf(base_var_index, netcdf)
            elev = truncate_round(float(netcdf["elevation"]), 5)
            lat = truncate_round(float(netcdf["latitude"]), 5)
            lon = truncate_round(float(netcdf["longitude"]), 5)
            station = None
            station_index = None
            for idx, a_station in enumerate(self.stations):
                if a_station["name"] == station_name:
                    station = a_station
                    station_index = idx
                    break

            if station is None:
                # get the netcdf fields for comparing or adding new
                an_id = "MD:V01:METAR:station:" + netcdf["name"]
                new_station = self.get_new_station(
                    an_id, netcdf, fcst_valid_epoch, elev, lat, lon
                )
                # add the new station to the document map with the new id
                if an_id not in self.document_map:
                    self.document_map[an_id] = new_station
                self.stations.append(new_station)
            else:
                # station does exist but is there a matching geo?
                # if there is not a matching geo create a new geo
                # if there is a matching geo then update the matching geo time range
                matching_location = False
                requires_new_geo = False
                for geo_index in range(len(self.stations[station_index]["geo"])):
                    geo = self.stations[station_index]["geo"][geo_index]
                    if geo["lat"] == lat and geo["lon"] == lon and geo["elev"] == elev:
                        matching_location = True
                        break
                if matching_location:
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
                    # This station requires a new geo because there are no matching locations i.e. the location has changed
                    requires_new_geo = True
                if requires_new_geo:
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
                self.stations[station_index]["updateTime"] = int(time.time())
                an_id = self.stations[station_index]["id"]
                self.document_map[an_id] = self.stations[station_index]
            return params_dict["stationName"]
        except Exception as _e:
            logger.exception(
                "%s netcdfObsBuilderV01.handle_station: Exception finding or creating station to match station_name: params: %s",
                self.__class__.__name__,
                str(params_dict),
            )
            return ""

    def get_new_station(
        self,
        an_id: str,
        netcdf: dict,
        fcst_valid_epoch: int,
        elev: float,
        lat: float,
        lon: float,
    ) -> dict:
        """
        Creates a new station dictionary with the provided parameters.
        Args:
            an_id (str): The unique identifier for the station.
            netcdf (dict): A dictionary containing metadata about the station,
                           including "description" and "name" keys.
            fcst_valid_epoch (int): The forecast valid epoch time (in seconds since the epoch).
            elev (float): The elevation of the station.
            lat (float): The latitude of the station.
            lon (float): The longitude of the station.
        Returns:
            dict: A dictionary representing the new station with the provided details.
        """
        # typechecks
        if not isinstance(an_id, str):
            raise TypeError("an_id must be a string")
        if not isinstance(netcdf, dict):
            raise TypeError("netcdf must be a dictionary")
        if not isinstance(fcst_valid_epoch, int):
            raise TypeError("fcst_valid_epoch must be an integer")
        if not isinstance(elev, float):
            raise TypeError("elev must be a float")
        if not isinstance(lat, float):
            raise TypeError("lat must be a float")
        if not isinstance(lon, float):
            raise TypeError("lon must be a float")
        # create the new station
        if (
            an_id is not None
            and netcdf["description"] is not None
            and netcdf["name"] is not None
        ):
            # create the new station
            new_station = {
                "id": an_id,
                "description": netcdf["description"],
                "docType": "station",
                "geo": [
                    {
                        "firstTime": fcst_valid_epoch,
                        "elev": elev,
                        "lat": lat,
                        "lon": lon,
                        "lastTime": fcst_valid_epoch,
                    }
                ],
                "name": netcdf["name"],
                "subset": "METAR",
                "type": "MD",
                "updateTime": int(time.time()),
                "version": "V01",
            }

        return new_station
