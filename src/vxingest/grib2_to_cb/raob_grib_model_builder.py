import copy
import cProfile
import logging
import math
import sys
from pathlib import Path
from pstats import Stats

import pyproj
import xarray as xr

from vxingest.grib2_to_cb.grib_builder import GribModelBuilderV01

# Get a logger with this module's name to help with debugging
logger = logging.getLogger(__name__)


class RaobGribModelBuilder(GribModelBuilderV01):
    """
    Builder for RAOB (upper air sounding) model data ingested from GRIB2 files.
    Inherits all functionality from GribModelBuilderV01.
    Extend this class with RAOB-specific handlers or overrides as needed.
    """

    def __init__(self, load_spec, ingest_document, number_stations=None):
        super().__init__(
            load_spec,
            ingest_document,
            number_stations=number_stations
            if number_stations is not None
            else sys.maxsize,
        )
        self.fcst_valid_epoch = None
        self.fcst_len = None

    def get_raw_data(self, dataset_map, raw_data_template):
        """
        Extracts raw data from the dataset map.
        This method is overridden to handle RAOB-specific data extraction.
        Args:
            dataset_map (dict): A dictionary containing datasets keyed by their type of level.
        Returns:
            dict: A dictionary containing raw data extracted from the datasets.
        """
        pass

    def get_interpolated_data(self, dataset_map, raw_data):
        """
        Interpolates data from the dataset map.
        This method is overridden to handle RAOB-specific data interpolation.
        Args:
            dataset_map (dict): A dictionary containing datasets keyed by their type of level.
            raw_data (dict): A dictionary containing raw data for each station.
        Returns:
            dict: A dictionary containing interpolated data for each station.
        """
        # This method should return a dictionary keyed by station name
        # and containing the interpolated data for that station.
        pass

    def interpolate_variable_for_level(
        self,
        variable,
        nearest_higher_pressure_index,
        nearest_lower_pressure_index,
        obs_data,
        wanted_pressure_level_mb,
    ):
        """
        This method interpolates the data for a given variable to the wanted level
        using the nearest higher and lower pressure indexes. This method assumes that the variables
        are masked arrays with the same shape. The wanted pressure is a mandatory level provided in mb
        and the pressure masked array is also assumed to be in mb.
        :param variable: the variable to interpolate
        :param nearest_higher_pressure_index: the nearest higher pressure index
        :param nearest_lower_pressure_index: the nearest lower pressure index
        :param wanted_pressure_level_mb: the wanted pressure level in mb
        :return: the interpolated value for the variable at the wanted pressure level
        """
        try:
            value = None
            nearest_higher_pressure = obs_data["pressure"][
                nearest_higher_pressure_index
            ]
            nearest_lower_pressure = obs_data["pressure"][nearest_lower_pressure_index]
            try:
                weight = (float)(
                    (
                        math.log(nearest_higher_pressure)
                        - math.log(wanted_pressure_level_mb)
                    )
                    / (
                        math.log(nearest_higher_pressure)
                        - math.log(nearest_lower_pressure)
                    )
                )
            except Exception as _e1:
                if not isinstance(_e1, ZeroDivisionError):
                    # don't log divide by zero (two adjacent levels with same)
                    logger.error(
                        "PrepBufrBuilder.interpolate_variable_for_level: Exception  error: %s",
                        str(_e1),
                    )
                return None
            if variable == "wind_direction":  # if it is a wind_direction do this
                # interpolates wind directions in the range 0 - 359 degrees
                if not self.is_a_number(nearest_lower_pressure) or not self.is_a_number(
                    nearest_higher_pressure
                ):
                    return None

                next_higher_pressure_direction = obs_data["wind_direction"][
                    nearest_higher_pressure_index
                ]
                next_lower_pressure_direction = obs_data["wind_direction"][
                    nearest_lower_pressure_index
                ]
                if not self.is_a_number(
                    next_lower_pressure_direction
                ) or not self.is_a_number(next_higher_pressure_direction):
                    return None

                dir_dif = next_lower_pressure_direction - next_higher_pressure_direction
                if dir_dif > 180:
                    dir_dif -= 360
                else:
                    if dir_dif < -180:
                        dir_dif += 360
                # round the possible floating point number to the nearest integer
                value = round(next_higher_pressure_direction + weight * (dir_dif))
                if value < 0:
                    value += 360
                else:
                    if value >= 360:
                        value -= 360
                return value
            else:  # if it isn't a wind_direction do this
                next_higher_pressure_variable_value = obs_data[variable][
                    nearest_higher_pressure_index
                ]
                next_lower_pressure_variable_value = obs_data[variable][
                    nearest_lower_pressure_index
                ]
                if not self.is_a_number(
                    next_higher_pressure_variable_value
                ) or not self.is_a_number(next_lower_pressure_variable_value):
                    return None
                else:
                    value = next_higher_pressure_variable_value + weight * (
                        next_lower_pressure_variable_value
                        - next_higher_pressure_variable_value
                    )
                    return value
        except Exception as _e:
            logger.error(
                "PrepBufrBuilder.interpolate_level: Exception  error: %s", str(_e)
            )
            return None

    def interpolate_data(self, raw_obs_data):
        """fill in the mandatory levels with interpolated data using the log difference interpolation method.
        For each pressure level in the mandatory levels, find the nearest higher and lower pressure levels
        and interpolate the data for each variable at the mandatory level. Set the pressure level to the mandatory level.
        Args:
            raw_data (): this is the raw data from the prepbufr file with missing heights having been interpolated
            using the hypsometric equation for thickness.
        Returns: the interpolated_data
        """
        try:
            interpolated_data = {}
            mandatory_levels = self.get_mandatory_levels()
            for station in raw_obs_data:
                if station not in interpolated_data:
                    interpolated_data[station] = {}
                for report in raw_obs_data[station]:
                    if report not in interpolated_data[station]:
                        interpolated_data[station][report] = {}
                        if "data" not in interpolated_data[station][report]:
                            interpolated_data[station][report]["data"] = {}
                    if raw_obs_data[station][report][
                        "obs_data"
                    ] is None or not isinstance(
                        raw_obs_data[station][report]["obs_data"]["pressure"],
                        list,
                    ):
                        # I cannot process this station - there is no array of pressure data
                        del interpolated_data[station]
                        break
                    for variable in raw_obs_data[station][report]["obs_data"]:
                        # create masked array for the variable with ALL the mandatory levels
                        # though the levels below the bottom level and above the top level will be masked
                        if report == 120 and "wind" in variable.lower():
                            # skip this one - it is handled in the 220 report
                            continue
                        if (
                            report == 220
                            and variable.lower() != "pressure"
                            and "wind" not in variable.lower()
                        ):
                            # skip this one - it is handled in the 120 report - except for pressure
                            continue
                        if variable not in interpolated_data[station][report]["data"]:
                            interpolated_data[station][report]["data"][variable] = {}
                        if (
                            raw_obs_data[station][report]["obs_data"][variable] is None
                            or len(raw_obs_data[station][report]["obs_data"][variable])
                            == 0
                        ):
                            # can't do this, there is no raw data for this variable
                            interpolated_data[station][report]["data"][variable] = None
                            continue
                        # now we can interpolate the levels for each variable
                        for level in mandatory_levels:
                            # find the nearest higher and lower pressure to this level
                            p_arr = np.asarray(
                                raw_obs_data[station][report]["obs_data"]["pressure"]
                            )
                            if (
                                np.isnan(p_arr).all()
                                or level > p_arr.max()
                                or level < p_arr.min()
                            ):
                                # this level is outside the range of the data - have to skip it
                                interpolated_data[station][report]["data"][variable][
                                    level
                                ] = None
                                continue
                            p_no_nan_arr = p_arr[~np.isnan(p_arr)]
                            if level > p_no_nan_arr.max() or level < p_no_nan_arr.min():
                                # this level is outside the range of the data - have to skip it
                                interpolated_data[station][report]["data"][variable][
                                    level
                                ] = None
                                continue
                            nearest_higher_pressure = p_no_nan_arr[
                                p_no_nan_arr >= level
                            ].min()
                            nearest_higher_i = raw_obs_data[station][report][
                                "obs_data"
                            ]["pressure"].index(nearest_higher_pressure)
                            nearest_lower_pressure = p_no_nan_arr[
                                p_no_nan_arr <= level
                            ].max()
                            nearest_lower_i = raw_obs_data[station][report]["obs_data"][
                                "pressure"
                            ].index(nearest_lower_pressure)

                            if (
                                nearest_higher_i == nearest_lower_i
                                and nearest_higher_pressure == level
                            ):
                                # this is the level we want - it matches the mandatory level
                                interpolated_data[station][report]["data"][variable][
                                    level
                                ] = (
                                    raw_obs_data[station][report]["obs_data"][variable][
                                        nearest_lower_i
                                    ]
                                    if self.is_a_number(
                                        raw_obs_data[station][report]["obs_data"][
                                            variable
                                        ][nearest_lower_i]
                                    )
                                    else None
                                )
                                continue
                            # have to interpolate the data for this variable and level
                            try:
                                interpolated_data[station][report]["data"][variable][
                                    level
                                ] = self.interpolate_variable_for_level(
                                    variable,
                                    nearest_higher_i,
                                    nearest_lower_i,
                                    raw_obs_data[station][report]["obs_data"],
                                    level,
                                )
                            except Exception as _e:
                                logger.error(
                                    "PrepBufrBuilder.interpolate_data: Exception  error: %s",
                                    str(_e),
                                )
                                interpolated_data[station][report]["data"][variable][
                                    level
                                ] = None
        except Exception as _e:
            logger.error(
                "PrepBufrBuilder.interpolate_data: Exception  error: %s", str(_e)
            )
        # set the pressure levels to the mandatory levels now that the data has all been interpolated to mandatory levels
        for station in raw_obs_data:
            for report in raw_obs_data[station]:
                for _l in mandatory_levels:
                    if station not in interpolated_data:
                        continue
                    interpolated_data[station][report]["data"]["pressure"][_l] = _l
        return interpolated_data

    def get_mandatory_levels(self):
        """
        This method gets the mandatory levels for the raw data set.
        :param report: the bufr report i.e. the subset.report_type
        :return: the mandatory levels
        """
        if not self.mandatory_levels:
            self.mandatory_levels = list(range(1010, 10, -10))
        return self.mandatory_levels

    def handle_document(self):
        """
        This routine processes the complete document (essentially a complete bufr file)
        which includes a new document for each mandatory level. The data section of each document
        is a dictionary keyed by the station name. The handle_data method is called and it will
        process each station in the interpolated_data and reconcile station locations with the
        couchbase station documents. If a station is not found in the couchbase database
        a new station document will be created and added to the document map.
        :return: The modified document_map
        The document map should be a dictionary keyed by the document id
        and each document id should look like DD:V01:RAOB:obs:prepbufr:500:1625097600

        where the type is "DD", the version is "V01", the subset is "RAOB", the docType is "obs",
        the docSubType is "prepbufr", the level is "500 (in mb)", and the valid time epoch is "1625097600".

        Each Document shall have a data dictionary that is keyed by the station name. The data section is defined by
        the template in the ingest document.

        The existence of the level key in the template indicates that the template is a multilevel template.
        """
        # noinspection PyBroadException
        try:
            for level in self.get_mandatory_levels():
                new_document = copy.deepcopy(self.template)
                # make a copy of the template, which will become the new document
                # once all the translations have occurred
                # set the level right away (it is needed for the handle_data)
                # clean out the data template from the data portion of the newDocument
                new_document["data"] = {}
                for key in self.template:
                    if key == "level":
                        new_document["level"] = level
                        continue
                    if key == "data":
                        self.handle_data(level, doc=new_document)
                        continue
                    # handle the key for this level that isn't data and isn't level
                    # level is the same for all the variables and all the stations
                    # variables will be handled in the data section for every station
                    new_document = self.handle_key(new_document, level, key)
                    # put new document into document map
                    if new_document["id"]:
                        logger.info(
                            "PrepbufrBuilder.handle_document - adding document %s",
                            new_document["id"],
                        )
                        self.document_map[new_document["id"]] = new_document
                    else:
                        logger.info(
                            "PrepbufrBuilder.handle_document - cannot add document with key %s",
                            str(new_document["id"]),
                        )
                        self.document_map[new_document["id"]] = new_document
        except Exception as _e:
            logger.error(
                "PrepbufrBuilder.handle_document: Exception instantiating builder: %s error: %s",
                self.__class__.__name__,
                str(_e),
            )
            raise _e

    def handle_data(self, level, **kwargs):
        """This method must handle each station. For each station this method iterates
        the template entries, deciding for each entry to either
        handle_named_function (if the entry starts with a '&') or to translate_template_item
        if it starts with an '*'. It handles both keys and values for each template entry. The level
        and the station are included in the params for the named function or the template item.
        The inclusion of level and station is what allows the proper access to the interpolated data for the station.
        Args:
            doc (Object): this is the data document that is being built
            level (int): the level for which the data is being built
        Returns:
            (Object): this is the data document that is being built
        Raises: AllMaskedException if all the data is masked for a given level
                ValueError if the data is not valid.
                Either exception will cause the document to be skipped.
        """
        try:
            doc = kwargs["doc"]
            for _station_name in self.interpolated_data:
                data_elem = {}
                data_template = self.template["data"]["*stationName"]
            for _data_key in data_template:
                try:
                    value = data_template[_data_key]
                    # values can be null...
                    if (
                        value
                        and not isinstance(value, dict)
                        and value.startswith("&")
                    ):
                        replace_value = self.handle_named_function(
                            _station_name, level, value
                        )
                    else:
                        replace_value = self.translate_template_item(
                            _station_name, level, value
                        )
                    data_elem[_data_key] = replace_value
                except Exception as _e:
                    replace_value = None
                    logger.warning(
                        "%s Builder.handle_data - value is None",
                        self.__class__.__name__,
                    )
                    raise _e  # probably cannot use this document - throw it away
                except ValueError as _ve:
                    continue  # do not use this one - we didn't have enough data to create a new station document
                doc["data"][_station_name] = data_elem
            return doc
        except Exception as _e:
            logger.exception(
                "%s handle_data: Exception instantiating builder",
                self.__class__.__name__,
            )
        return doc

    def build_document(self, queue_element):
        """
         This is the entry point for the RAOB gribBuilders from the ingestManager.
         The ingest manager is giving us a grib file to process from the queue.
         These documents are id'd by valid time and fcstLen. The data section is a dictionary
         indexed by RAOB station name each element of which contains variable data and a station name.
         To process this file we need to iterate the stations list and process the
         station name along with all the required variables.
         1) get the first epoch - if none was specified get the latest one from the db
         2) transform the projection from the grib file
         3) determine the stations, adding gridpoints to each station - build a station list
         4) enable profiling if requested
         5) handle_document - iterate the template and process all the keys and values
         6) build a datafile document to record that this file has been processed
         7) cfgrib leaves .idx files in the directory - delete the .idx file

        NOTE: For cfgrib, variables are contained in datasets.
        For RAOB data based on pressure levels the variables are contained in the
        isobaricInhPa dataset. Within this dataset, the variables are indexed by the
        isobaricInhPa as well as time, step, lat, and lon. Each file contains the variables
        for all the isobaricInhPa levels for a given valid time, i.e. ds["time"].data. For this example,
        the dataset represents the variables indexed by the isobaric level, lat, lon for the time '2024-07-31T00:00:00.000000000'
        which can be found by using the following code:
        ```python
        import xarray as xr
        import cfgrib
        dataset = xr.open_dataset("/opt/data/grib2_to_cb/hrrr_ops/input_files/conus/wrfprs/grib2",engine="cfgrib",filter_by_keys={'typeOfLevel': 'isobaricInhPa'})
        validTime = dataset["time"].data
        ```

        For RAOB data based on native levels, the variables are contained in the
        hybrid dataset. Within this dataset, the variables are indexed by the native level,
        lat, lon, and time. Each file contains the variables for all the native levels for

        Other variables like temperature are indexed by their GRIB_shortName, i.e. "t" for temperature.
        The template has a field "templateTermToGribShortName" that maps the template terms to the GRIB short names.
        This is used to translate the template terms to the GRIB short names so that the data documents are easier to read.
        """

        try:
            # get the bucket, scope, and collection from the load_spec
            bucket = self.load_spec["cb_connection"]["bucket"]
            scope = self.load_spec["cb_connection"]["scope"]
            collection = self.load_spec["cb_connection"]["collection"]
            # change collection to RAOB
            collection = "RAOB"

            datasetMap = {}
            # The raw_data keys are the dataset names
            # The dataset names are the typeOfLevel values in the grib file
            # For example, for a grib file with isobaricInhPa levels,
            # the raw_data_template keys of interest will only be ['isobaricInhPa']
            raw_data_template = self.ingest_document.get("raw_data_template")
            for key in raw_data_template:
                datasetMap[key] = xr.open_dataset(
                    queue_element,
                    engine="cfgrib",
                    backend_kwargs={
                        "filter_by_keys": {
                            "typeOfLevel": key,
                            "stepType": "instant",
                        },
                        "read_keys": ["projString"],
                        "indexpath": "",
                    },
                    decode_timedelta=True,
                )
            # translate the projection from the grib file
            # The projection is the same for all the variables in the grib file,
            first_dataset = datasetMap.get(next(iter(datasetMap)))
            in_proj = pyproj.Proj(proj="latlon")
            proj_string = first_dataset.r.attrs.get("GRIB_projString")
            max_x = first_dataset.r.attrs.get("GRIB_Nx")
            max_y = first_dataset.r.attrs.get("GRIB_Ny")
            spacing = first_dataset.r.attrs.get("GRIB_DxInMetres")
            latitude_of_first_grid_point_in_degrees = first_dataset.r.attrs.get(
                "GRIB_latitudeOfFirstGridPointInDegrees")
            longitude_of_first_grid_point_in_degrees = first_dataset.r.attrs.get(
                "GRIB_longitudeOfFirstGridPointInDegrees"
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

            # we get the fcst_valid_epoch and fcst_len once for the entire file
            if self.fcst_valid_epoch is None:
                self.fcst_valid_epoch = (
                    first_dataset.step.valid_time.values.astype("uint64") / 10**9
                ).astype("uint32")
            if self.fcst_len is None:
                self.fcst_len = (int)(first_dataset.step.step.values / 1e9 / 3600)
            # reset the builders document_map for a new file
            self.initialize_document_map()
            # get stations from couchbase and filter them so
            # that we retain only the ones for this models domain which is derived from the projection
            # also fill in the gridpoints for each station and for each geo within each station
            # NOTE: this is not about regions, this is about models
            self.stations = []
            limit_clause = ";"
            if self.number_stations != sys.maxsize:
                limit_clause = f" limit {self.number_stations};"
            result = self.load_spec["cluster"].query(
                f"""SELECT geo, wmoid
                        from `{bucket}`.{scope}.{collection}
                        where type='MD'
                        and docType='station'
                        and subset='{self.subset}'
                        and version='V01'
                        {limit_clause}"""
            )
            for row in result:
                station = copy.deepcopy(row)
                station["wmoid"] = row.get("wmoid")
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
                    self.stations.append(station)

            # get the raw and the interpolated data
            self.raw_data_template = self.get_raw_data(datasetMap, raw_data_template)
            self.interpolated_data = self.get_interpolated_data(
                datasetMap, self.raw_data_template
            )

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
