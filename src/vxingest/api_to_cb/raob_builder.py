"""
Program Name: raob_uilder.py
Contact(s): Randy Pierce
History Log:  Initial version
Copyright 2019 UCAR/NCAR/RAL, CSU/CIRES, Regents of the University of
Colorado, NOAA/OAR/ESRL/GSL
"""

import abc
import calendar
import copy
import cProfile
import datetime as dt
import logging
import os
import time
from pstats import Stats

import requests
from api_to_cb.api_builder import ApiBuilder
from numpy import ma

from builder_common.builder import Builder
from builder_common.builder_utilities import convert_to_iso, truncate_round


class RaobObsBuilder(ApiBuilder):  # pylint: disable=too-many-instance-attributes
    """
    This is the parent class of builders for observation data that is ingested from
    RAOBs API's. One subclass is RaobsGslObsBuilder which gets data from the GSL
    RAOBs database (affectionately known to GSL as Mark Govett's database). Another builder
    gets data from PrepBufr.
    This is the class heirarchy. The ApiBuilder is a parent of RaobsBuilder and defines the
    interface for API based builders as well as defines some common code.
    Builder ApiBuilder← RaobObsBuilder ← RaobsGslObsBuilder
    Builder ApiBuilder← RaobObsBuilder ← RaobsPrepBufrObsBuilder

    These builders will retrieve data values from the API. The pressure levels in the API do
    not correspond directly to the levels in the model output. In the API they are organized
    on pressure levels (mb) and we want them organized on standardized model levels.
    There are many more levels in the RAOB observation data than in the model output and
    in the native model output they are organized by geopotential height along
    (with associated pressure level). The intent is to have a standardized list of pressure
    levels and interpolate the outputs (model and obs) to the standardized list.
    The standardized list of levels needs to be in metadata. The list should be
    1000mb to 100mb by 10 mb increments.
    """

    def __init__(self, load_spec, ingest_document):
        """
        This builder creates a set of V01 obs documents using the V01 station documents.
        This builder loads V01 station data into memory, and uses them to associate a
        station with an observation lat, lon point.
        In each document the observation data is a map of objects keyed by station identifier
        each of which is the obs data for a specific station.
        If a station does not exist in the couchbase database a station document will be created
        from the metar record data and the station document will be added to the document map.
        If a station location has changed the geo element will be updated to have an additional
        geo element that has the new location and time bracket for the location.
        :param ingest_document: the document from the ingest document
        :param cluster: - a Couchbase cluster object, used for N1QL queries (QueryService)
        :param collection: - essentially a couchbase connection object, used to get documents by id (DataService)
        """
        ApiBuilder.__init__(self, load_spec, ingest_document)
        self.same_time_rows = []
        self.time = 0
        self.interpolated_time = 0
        self.delta = ingest_document["validTimeDelta"]
        self.cadence = ingest_document["validTimeInterval"]
        self.template = ingest_document["template"]
        self.subset = self.template["subset"]
        # self.do_profiling = True  # set to True to enable build_document profiling
        self.do_profiling = False  # set to True to enable build_document profiling
        self.load_spec = load_spec
        self.ingest_document = None
        self.template = None
        self.subset = None

    @abc.abstractmethod
    def read_data_from_api(self):
        """read data from the api and load it into a dictionary structure"""
        return

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
        except Exception as _e:  # pylint:disable=broad-except
            logging.exception(
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
        :param doc: The document being created
        :param key: Not used
        :param element: the observation data
        :return: the document being created
        """
        if "data" not in doc.keys() or doc["data"] is None:
            doc["data"] = {}
        if element["name"] not in doc["data"].keys():
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
        except Exception as _e:  # pylint:disable=broad-except
            logging.error(
                "%s handle_data: Exception in named function meterspersecond_to_milesperhour:  error: %s",
                self.__class__.__name__,
                str(_e),
            )
            return None

    def kelvin_to_farenheight(self, params_dict):
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
        except Exception as _e:  # pylint:disable=broad-except
            logging.error(
                "%s handle_data: Exception in named function kelvin_to_farenheight:  error: %s",
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
            rec_num = params_dict["recNum"]
            for key in params_dict.keys():
                if key != "recNum":
                    break
            nc_value = self.ncdf_data_set[key][rec_num]
            if not ma.getmask(nc_value):
                value = ma.compressed(nc_value)[0]
                return float(value)
            else:
                return None
        except Exception as _e:  # pylint:disable=broad-except
            logging.error(
                "%s umask_value_transform: Exception in named function umask_value_transform for key %s:  error: %s",
                self.__class__.__name__,
                key,
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
        except Exception as _e:  # pylint:disable=broad-except
            logging.error(
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
            for key in params_dict.keys():
                if key != "recNum":
                    break
            _file_utc_time = dt.datetime.strptime(self.file_name, params_dict[key])
            epoch = (_file_utc_time - dt.datetime(1970, 1, 1)).total_seconds()
            iso = convert_to_iso(epoch)
            return iso
        except Exception as _e:  # pylint:disable=broad-except
            logging.error(
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
            for key in params_dict.keys():
                if key != "recNum":
                    break
            _file_utc_time = dt.datetime.strptime(self.file_name, params_dict[key])
            epoch = (_file_utc_time - dt.datetime(1970, 1, 1)).total_seconds()
            return int(epoch)
        except Exception as _e:  # pylint:disable=broad-except
            logging.error(
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

        except Exception as _e:  # pylint:disable=broad-except
            logging.error(
                "%s handle_data: Exception in named function interpolate_time:  error: %s",
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
        except Exception as _e:  # pylint:disable=broad-except
            logging.error(
                "%s handle_data: Exception in named function interpolate_time_iso:  error: %s",
                self.__class__.__name__,
                str(_e),
            )

    def handle_station(self, params_dict):
        """
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
        rec_num = params_dict["recNum"]
        station_name = params_dict["stationName"]
        an_id = None
        netcdf = {}
        fcst_valid_epoch = self.derive_valid_time_epoch(
            {"file_name_pattern": self.load_spec["fmask"]}
        )
        # noinspection PyBroadException
        try:
            # get the netcdf fields for comparing or adding new
            netcdf = self.fill_from_netcdf(rec_num, netcdf)
            elev = truncate_round(float(netcdf["elevation"]), 5)
            lat = truncate_round(float(netcdf["latitude"]), 5)
            lon = truncate_round(float(netcdf["longitude"]), 5)
            station = None
            station_index = None
            for station_index, a_station in enumerate(self.stations):
                if a_station["name"] == station_name:
                    station = a_station
                    break
            if station is None:
                # get the netcdf fields for comparing or adding new
                an_id = "MD:V01:METAR:station:" + netcdf["name"]
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
                # add the new station to the document map with the new id
                if not an_id in self.document_map.keys():
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
        except Exception as _e:  # pylint:disable=broad-except
            logging.exception(
                "%s netcdfObsBuilderV01.handle_station: Exception finding or creating station to match station_name: params: %s",
                self.__class__.__name__,
                str(params_dict),
            )
            return ""


class RaobsGslObsBuilder(RaobObsBuilder):
    """
            This builder creates a set of V01 obs documents using the API interface
            to the GSL RAOBS.
            The GSL API is called for all stations and all levels at a specified valid time.
            The specified valid Time is passed in to the builder in the load_spec.
            The returned data is processed and added to a raob_data dictionary. The raob_data
            is keyed by level since the valid time is already known. The level must be interolated
            to the standard levels used by GSL. The format of the data returned from the API is
            described here https://ruc.noaa.gov/raobs/fsl_format-new.html. This data structure looks
            like.....     1          2          3          4          5          6           7
     LINTYP
                                    header lines
        254        HOUR        DAY      MONTH       YEAR    (blank)     (blank)
          1       WBAN#       WMO#        LAT D      LON D     ELEV       RTIME
          2       HYDRO       MXWD      TROPL      LINES     TINDEX      SOURCE
          3     (blank)      STAID    (blank)    (blank)      SONDE     WSUNITS
                                    data lines
          4..8
          9    PRESSURE     HEIGHT       TEMP      DEWPT   WIND DIR    WIND SPD
    LINTYP: type of identification line
            254 = indicates a new sounding in the output file
              1 = station identification line
              2 = sounding checks line
              3 = station identifier and other indicators line
              4 = mandatory level
              5 = significant level
              6 = wind level (PPBB) (GTS or merged data)
              7 = tropopause level (GTS or merged data)
              8 = maximum wind level (GTS or merged data)
              9 = surface level
    There is not a mandatory level (type 4) for all of the standard GSL levels desired,
    therefore the non mandatory levels must be interpolated into the GSL standard levels.
    The interpolation is simple, first the standard level is bracketed and the closest data
    level above or below is taken to be the standard level.

            :param ingest_document: the document from the ingest document
            :param cluster: - a Couchbase cluster object, used for N1QL queries (QueryService)
            :param collection: - essentially a couchbase connection object, used to get documents by id (DataService)
    """

    def __init__(self, load_spec, ingest_document):
        RaobObsBuilder.__init__(self, load_spec, ingest_document)


class RaobsPrepBufrObsBuilder(RaobObsBuilder):
    """
    This builder creates a set of V01 obs documents using the API interface to PrepBufr.
    :param ingest_document: the document from the ingest document
    :param cluster: - a Couchbase cluster object, used for N1QL queries (QueryService)
    :param collection: - essentially a couchbase connection object, used to get documents by id (DataService)
    """

    def __init__(self, load_spec, ingest_document):
        RaobObsBuilder.__init__(self, load_spec, ingest_document)

    def read_data_from_api(self):
        """
        read data from the GSL raobs api and load the self.obs_data dictionary.
        The data is not interpolated onto GSL specific levels do this routine must do that.
        GSL specific levels are ...

        NOTE: Need to figure out how to interpolate the levels and create documents that are keyed including level.
        i.e. Each of these requests will result in up to 20 documents 50 to 1000 mb by 50's.
        """
        hour = 0
        begin_date = "2022122612"
        end_date = "2022122612"
        req_url = f"""https://ruc.noaa.gov/raobs/intl/GetRaobs.cgi?shour={hour}z+ONLY&
                    ltype=All+Levels&wunits=Tenths+of+Meters%2FSecond&bdate={begin_date}&edate={end_date}&
                    access=All+Sites&view=NO&osort=Station+Series+Sort&oformat=FSL+format+%28ASCII+text%29"""
        _r = requests.get(req_url)
        for _l in _r.iter_lines():
            parts = _l.decode("utf-8").split()
            line_type = parts[0]

            # 254 = indicates a new sounding in the output file
            # 1 = station identification line
            # 2 = sounding checks line
            # 3 = station identifier and other indicators line
            # 4 = mandatory level
            # 5 = significant level
            # 6 = wind level (PPBB) (GTS or merged data)
            # 7 = tropopause level (GTS or merged data)
            # 8 = maximum wind level (GTS or merged data)
            # 9 = surface level
            # 254   HOUR        DAY      MONTH       YEAR    (blank)     (blank)
            # 1     WBAN#       WMO#        LAT D      LON D     ELEV       RTIME
            # 2     HYDRO       MXWD      TROPL      LINES     TINDEX      SOURCE
            # 3     (blank)      STAID    (blank)    (blank)      SONDE     WSUNITS
            #                             data lines
            #         PRESSURE     HEIGHT       TEMP      DEWPT   WIND DIR    WIND SPD
            # 4,5,6,7,8,9

            if line_type == "254":
                new_record = {
                    "STATION_ID": {
                        "hour": parts[1],
                        "day": parts[2],
                        "month": parts[3],
                        "year": parts[4],
                    }
                }
            elif line_type == "1":
                new_record["SOUNDING_CHECKS"] = {
                    "WBAN": parts[1],
                    "WMOID": parts[2],
                    "LAT": parts[3],
                    "LON": parts[4],
                    "ELEVATION": parts[5],
                    "RTIME": parts[6],
                }
            elif line_type == "2":
                new_record["STATION_IDENTIFICATION"] = {
                    "HYDRO": parts[1],
                    "MXWD": parts[2],
                    "TROPL": parts[3],
                    "LINES": parts[4],
                    "TINDEX": parts[5],
                    "SOURCE": parts[6],
                }
            elif line_type == "3":
                new_record["STATION_IDENTIFIER"] = {
                    "STAID": parts[2],
                    "SONDE": parts[5],
                    "WSUNITS": parts[6],
                }
            elif line_type in ["4", "5", "6", "7", "8", "9"]:
                if line_type == "4":
                    value = "MANDATORY_LEVEL"
                elif line_type == "5":
                    value = "SIGNIFICANT_LEVEL"
                elif line_type == "6":
                    value = "WIND_LEVEL"
                elif line_type == "7":
                    value = "TROPOPAUSE_LEVEL"
                elif line_type == "8":
                    value = "MAXIMUM_WIND_LEVEL"
                elif line_type == "9":
                    value = "SURFACE_LEVEL"

                new_record[value] = {
                    "PRESSURE": parts[1],
                    "HEIGHT": parts[2],
                    "TEMP": parts[3],
                    "DEWPT": parts[4],
                    "WIND_DIR": parts[5],
                    "WIND_SPD": parts[6],
                }
            else:
                raise Exception(
                    f"RAOB_GSL_BUILDER: invalid line type:{line_type} for line:{parts}"
                )
        # match data to stations
        # load data to obs_datazA
        return self.obs_data
