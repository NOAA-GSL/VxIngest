"""
Program Name: Class grib_raob_builder.py
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
import numbers
import sys
from pathlib import Path
from pstats import Stats

import cfgrib
import pyproj
import xarray as xr

from vxingest.grib2_to_cb.grib_builder_parent import GribBuilder

# Get a logger with this module's name to help with debugging
logger = logging.getLogger(__name__)


class GribModelRaobBuilderV01(GribBuilder):
    """
    This is the builder for model data that is ingested from grib2 files for Raob data. It is a concrete builder specifically
    for the model data. The RAOB data are upper-air observations typically obtained from radiosondes.
    """

    def __init__(
        self,
        load_spec,
        ingest_document,
        number_stations=sys.maxsize,
    ):
        """This builder creates a set of V01 model documents using the stations in the station list.
        This builder loads domain qualified station data into memory, and uses the domain_station
        list to associate a station with a grid value at an x_lat, x_lon point, and a standard height
        In each document the data is an array of objects each of which is the model variable data
        for specific variables at a point associated with a specific station at the time and fcstLen of
        the document. All of the mandatory levels are iterated an a document is created for each level.
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
        self.raob_units = {}
        # self.do_profiling = True  # set to True to enable build_document profiling
        self.do_profiling = False  # set to True to enable build_document profiling

    def get_mandatory_levels(self):
        """These are all the POSSIBLE mandatory levels for the RAOB grib files.
        but many grib files won't have ALL of these levels. The ones that
        actually exist in the file are filtered in the builder.
        """
        return list(range(1010, 10, -10))

    def build_document(self, queue_element):
        """
        This is the entry point for this Raob gribBuilder from the ingestManager.
        The ingest manager is giving us a grib file to process from the queue.
        These documents are id'd by valid time and fcstLen, as well as the standard level. The data section is a dictionary
        indexed by station name each element of which contains variable data and a station name.
        To process this file we need to iterate the domain_stations list and process the
        station name along with all the required variables. The data for each standard level is processed separately
        into a single document in the document map and the id of each document specifies the level
        of the standard level for the data. There will be one document in the map per standard level per grib file.
        For reference consider the following snippet...
            import cfgrib
            mandatory_levels = list(range(1010, 10, -10))
            datasets = cfgrib.open_datasets(queue_element,
                                                backend_kwargs={
                        "filter_by_keys": {
                            "typeOfLevel": "isobaricInhPa",
                            #'shortName': 't',
                            "stepType": "instant",
                        },
                        "read_keys": ["projString"],
                        "indexpath": "",
                    },
                )
            for ds in datasets:
                for v in ds.data_vars:
                    print(f"{v}")
                    for level in mandatory_levels:
                        if "isobaricInhPa" not in ds[v].coords:
                            continue
                        try:
                            selected = ds[v].sel(
                                isobaricInhPa=level,
                                method="nearest",
                                tolerance=0.1,
                            )
                        except KeyError:
                            continue
                        print(f"Selected data for {v} at {level} hPa: with long name '{ds[v].long_name}'")
                        print(
                            f"{v} at {level} hPa: "
                            f"dims={selected.dims}, shape={selected.shape}, "
                            f"min={selected.min().item():.2f}, "
                            f"max={selected.max().item():.2f}, "
                            f"mean={selected.mean().item():.2f}, "
                            f"units={selected.attrs.get('units')}"
                        )
        This code would output data like...
        t
        Selected data for t at 1000 hPa: with long name 'Temperature'
        t at 1000 hPa: dims=('y', 'x'), shape=(1059, 1799), min=282.97, max=313.84, mean=297.83, units=K
        Selected data for t at 950 hPa: with long name 'Temperature'
        t at 950 hPa: dims=('y', 'x'), shape=(1059, 1799), min=280.70, max=310.82, mean=294.66, units=K
        .....
        Selected data for dpt at 100 hPa: with long name 'Dew point temperature'
        dpt at 100 hPa: dims=('y', 'x'), shape=(1059, 1799), min=192.12, max=200.69, mean=192.13, units=K
        Selected data for dpt at 50 hPa: with long name 'Dew point temperature'
        dpt at 50 hPa: dims=('y', 'x'), shape=(1059, 1799), min=192.12, max=192.12, mean=192.12, units=K

        Capturing data goes along these lines...
        1) get the first epoch - if none was specified get the latest one from the db
        2) transform the projection from the grib file and read the data from the grib file.
        3) determine the stations for this domain, adding gridpoints to each station - build a station list
        4) enable profiling if requested
        5) handle_document - iterate the template and process all the keys and values
        6) build datafile documents to record that this file has been processed
        7) cfgrib leaves .idx files in the directory - delete any .idx files

        These are the variables to be processed "short_name: long_name"
        Notice that these may depend on the file itself...
        't':'Temperature'
        'u': 'U component of wind'
        'v': 'V component of wind'
        'q': 'Specific humidity'
        'w': 'Vertical velocity'
        'gh': 'Geopotential height'
        'r': 'Relative humidity'
        'dpt': 'Dew point temperature'
        'absv': 'Absolute vorticity'
        'clwmr': 'Cloud mixing ratio'
        'rwmr': 'Rain mixing ratio'
        'snmr': 'Snow mixing ratio'
        'grle': 'Graupel (snow pellets)'}


        NOTE: The cfgrib variables are contained in datasets.
        For cfgrib, the variables defined in the templates are to be defined by their long_name attribute.
        for example data for t at 1000 hPa: with long name 'Temperature' is the Temperature data for the 1000mb level and it will
        be an array of values corresponding to the grid points for that variable .
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

            # heightAboveGround variable - used to get the projection information from the grib file
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
            # use these if necessary to compare projections for debugging
            # print()
            # print ('in_proj', in_proj, 'out_proj', out_proj, 'max_x', max_x, 'max_y', max_y, 'spacing', spacing)

            # we get the fcst_valid_epoch and fcst_len once for the entire file, from the heightAboveGround
            ds_fcst_valid_epoch = (
                ds_height_above_ground_2m.valid_time.values.astype("uint64") / 10**9
            ).astype("uint32")
            ds_fcst_len = (int)((ds_height_above_ground_2m.step.values) / 1e9 / 3600)

            # set up the variables map for the translate_template_item method. this way only the
            # translation map needs to be a class variable. Better data hiding.
            # It seems that cfgrib is graceful about missing variables, so we don't need to check
            # for them here. But when we try to get the indexed values we will get an exception
            # if the variable is missing. We will catch that exception and return an empty document
            # NOTE: In this code, each element of datasets is a separate xarray Dataset
            # that cfgrib had to split apart because the GRIB messages could not all be
            # represented cleanly in one dataset. The individual datasets need to be processed separately
            # and they do not necessarily represent indiviual levels or variables.
            try:
                self.ds_translate_item_variables_map = {
                    "fcst_valid_epoch": ds_fcst_valid_epoch,
                    "fcst_len": ds_fcst_len,
                    "proj_params": proj_params_dict,
                }
                mandatory_levels = self.get_mandatory_levels()
                datasets = cfgrib.open_datasets(
                    queue_element,
                    backend_kwargs={
                        "filter_by_keys": {
                            "typeOfLevel": "isobaricInhPa",
                            #'shortName': 't',
                            "stepType": "instant",
                        },
                        "read_keys": ["projString"],
                        "indexpath": "",
                    },
                )

                # Variables short_name: long_name - hrrr_ops example
                # 't': 'Temperature',
                # 'u': 'U component of wind',
                # 'v': 'V component of wind',
                # 'q': 'Specific humidity',
                # 'w': 'Vertical velocity',
                # 'gh': 'Geopotential height',
                # 'r': 'Relative humidity',
                # 'dpt': 'Dew point temperature',
                # 'absv': 'Absolute vorticity',
                # 'clwmr': 'Cloud mixing ratio',
                # 'rwmr': 'Rain mixing ratio',
                # 'snmr': 'Snow mixing ratio',
                # 'grle': 'Graupel (snow pellets)'

                vars_units = {}
                for level in mandatory_levels:
                    level_map = self.ds_translate_item_variables_map.setdefault(
                        level, {}
                    )
                    for ds in datasets:
                        for v in ds.data_vars:
                            if v == "unknown":
                                continue
                            variable = ds[v]
                            if variable.long_name not in vars_units:
                                vars_units[variable.long_name] = variable.attrs.get(
                                    "units"
                                )
                            logger.debug("Processing GRIB variable %s", v)
                            if "isobaricInhPa" not in variable.coords:
                                logger.debug(
                                    "GRIB variable %s has no isobaricInhPa coordinate",
                                    v,
                                )
                                continue
                            try:
                                selected = variable.sel(
                                    isobaricInhPa=level,
                                    method="nearest",
                                    tolerance=0.1,
                                )
                            except KeyError as ke:
                                # This is expected when a level is absent from a variable.
                                logger.debug(
                                    "No %s data found within 0.1 hPa of level %s - message: %s",
                                    v,
                                    level,
                                    ke,
                                )
                                continue
                            logger.debug(
                                "Selected data for %s at %s hPa: with long name %r",
                                v,
                                level,
                                variable.long_name,
                            )
                            logger.debug(
                                "%s at %s hPa: dims=%s, shape=%s, min=%.2f, "
                                "max=%.2f, mean=%.2f, units=%s",
                                v,
                                level,
                                selected.dims,
                                selected.shape,
                                selected.min().item(),
                                selected.max().item(),
                                selected.mean().item(),
                                selected.attrs.get("units"),
                            )
                            level_map[v] = selected
                    # If no dataset added anything to level_map, clean it up
                    if not level_map:
                        self.ds_translate_item_variables_map.pop(level, None)
                # Keep units metadata separate from the gridded variable map.
                self.raob_units = vars_units
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
            stmnt = f"""SELECT geo, wmoid
                    from `{bucket}`.{scope}.{collection}
                    where type='MD'
                    and docType='station'
                    and subset='{self.subset}'
                    and version='V01'
                    {limit_clause}"""
            result = self.load_spec["cluster"].query(stmnt)
            for row in result:
                station = copy.deepcopy(row)
                for geo_index in range(len(row["geo"])):
                    lat = row["geo"][geo_index]["lat"]
                    lon = row["geo"][geo_index]["lon"]
                    if lat == -90 and lon == 180 or lat == 0 or lon == 0:
                        # skip stations with bad lat/lon
                        # these are probably buoys or ships or mistakes.
                        logger.info(
                            "%s: builder build_document skipping station with bad lat/lon: name: %s, lat: %s, lon: %s",
                            self.__class__.__name__,
                            row["wmoid"],
                            str(lat),
                            str(lon),
                        )
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
        except FileNotFoundError as _e:
            logger.error(
                "%s: Exception with builder build_document: file_name: %s, error: file not found or problem reading file - skipping this file: %s",
                self.__class__.__name__,
                queue_element,
                _e,
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

    def handle_document(self, level=None):
        """Build one RAOB document for each mandatory pressure level.

        Unlike the single-level GribModelMetarBuilderV01, which relies on the
        parent's handle_document(level=None) to build a single document, RAOB
        data has multiple pressure levels per file, so build_document() always
        calls this overridden method with level=None to trigger the loop below. The level-is-not-None
        branch only exists to let a caller target one specific level directly (for example for testing or debugging).
        """
        if level is not None:
            return super().handle_document(level=level)
        for pressure_level in self.get_mandatory_levels():
            super().handle_document(level=pressure_level)

    def handle_key(self, doc, key, level=None):
        if key == "units":
            doc[key] = self.raob_units
            return doc
        return super().handle_key(doc, key, level=level)

    def handle_raob_variable(self, params_dict, short_name=None):
        """translate RAOB variable: convert to float
        Args:
            params_dict (dict): named function parameters
        Returns:
            [float]: translated value
        """
        try:
            if not params_dict:
                return None
            if short_name is None:
                short_name = next(
                    (key for key in params_dict if key != "level"),
                    None,
                )
            if short_name is None:
                return None
            variable_values = params_dict.get(short_name)
            if variable_values is None:
                return None
            v_interpolated_values = []
            for _v, v_intrp_v in variable_values:
                v_interpolated_values.append(
                    float(v_intrp_v)
                    if isinstance(v_intrp_v, numbers.Number)
                    else v_intrp_v
                )
            return v_interpolated_values
        except (TypeError, ValueError) as error:
            logger.exception(
                "%s.handle_raob_variable failed for %s: %s",
                self.__class__.__name__,
                short_name,
                error,
            )
            return None

    def handle_wind_speed(self, params_dict):
        # wind speed = square root(u**2 + v**2)
        try:
            u_values = self.handle_raob_variable(params_dict, "u")
            v_values = self.handle_raob_variable(params_dict, "v")
            if u_values is None or v_values is None:
                return None
            return [
                (
                    (math.sqrt(float(u_value) ** 2 + float(v_value) ** 2) / 0.447) + 0.5
                    if u_value is not None and v_value is not None
                    else None
                )
                for u_value, v_value in zip(u_values, v_values, strict=True)
            ]
        except (TypeError, ValueError) as error:
            logger.exception(
                "%s.handle_wind_speed failed: %s",
                self.__class__.__name__,
                error,
            )
            return None

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
                doc["data"][elem["wmoid"]] = elem
        return doc

    # named functions

    def kelvin_to_fahrenheit(self, params_dict):
        """
        param:params_dict expects {'station':{},'*variable name':variable_value}
        Used for temperature and dewpoint
        """
        try:
            # Convert each station value from Kelvin to Fahrenheit
            tempf_values = []
            for _v, v_intrp_tempf in list(params_dict.values())[0]:
                tempf_values.append(
                    ((float(v_intrp_tempf) - 273.15) * 9) / 5 + 32
                    if v_intrp_tempf is not None
                    else None
                )
            return tempf_values
        except (IndexError, TypeError, ValueError) as error:
            logger.exception(
                "%s.kelvin_to_fahrenheit failed: %s", self.__class__.__name__, error
            )
            return None

    def handle_wmoid(self, params_dict):  # @UnusedVariable
        """translate the station wmoid
        Args:
            params_dict (object): named function parameters - unused here
        Returns:
            list: station wmoids
        """
        try:
            station_wmoids = []
            for station in self.domain_stations:
                station_wmoids.append(station["wmoid"])
            return station_wmoids
        except (KeyError, TypeError) as error:
            logger.exception("%s.get_wmoid failed: %s", self.__class__.__name__, error)
            return None

    def handle_time(self, params_dict):  # @UnusedVariable
        """return the time variable as an epoch
        Args:
            params_dict (object): named function parameters
        Returns:
            int: epoch
        """
        try:
            return int(self.ds_translate_item_variables_map["fcst_valid_epoch"])
        except (KeyError, TypeError, ValueError) as error:
            logger.exception(
                "%s.handle_time failed: %s", self.__class__.__name__, error
            )
            return None

    def handle_iso_time(self, params_dict):  # @UnusedVariable
        """return the time variable as an iso
        Args:
            params_dict (object): named function parameters
        Returns:
            string: iso time
        """
        try:
            return dt.datetime.fromtimestamp(
                int(self.ds_translate_item_variables_map["fcst_valid_epoch"]),
                tz=dt.UTC,
            ).isoformat()
        except (KeyError, TypeError, ValueError, OverflowError) as error:
            logger.exception(
                "%s.handle_iso_time failed: %s", self.__class__.__name__, error
            )
            return None

    def handle_fcst_len(self, params_dict):  # @UnusedVariable
        """return the fcst length variable as an int
        Args:
            params_dict (object): named function parameters
        Returns:
            int: forecast length
        """
        try:
            return int(self.ds_translate_item_variables_map["fcst_len"])
        except (KeyError, TypeError, ValueError) as error:
            logger.exception(
                "%s.handle_fcst_len failed: %s", self.__class__.__name__, error
            )
            return None

    def handle_level(self, params_dict):  # @UnusedVariable
        """return the level variable as an int
        Args:
            params_dict (object): named function parameters
        Returns:
            int: level
        """
        try:
            return int(params_dict["level"])
        except (KeyError, TypeError, ValueError) as error:
            logger.exception(
                "%s.handle_level failed: %s", self.__class__.__name__, error
            )
            return None
