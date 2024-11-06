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
class GribModelRaobNativeBuilderV01(GribBuilder):
    """This is the builder for model data that is ingested from grib2 NATIVE levels files.
    It is a concrete builder specifically for the model raob data that are organized based
    on the models preset vertical levels. This varies quite a bit from model to model
    and is dependent on the configuration set up before the model runs.
    This builder is a subclass of the GribModelBuilderV01 class.
    The primary differences in these two classes are the handlers that derive the pressure level.
    The pressure level needs to be interpolated according to a specific algorithm.

    Args:
        load_spec (Object): The load spec used to init the parent
        ingest_document (Object): the ingest document
        number_stations (int, optional): the maximum number of stations to process (for debugging). Defaults to sys.maxsize.
    """

    def __init__(
        self,
        load_spec,
        ingest_document,
        number_stations=sys.maxsize,
    ):
        GribBuilder.__init__(
            self,
            load_spec,
            ingest_document,
            number_stations=sys.maxsize,
        )


class GribModelRaobPressureBuilderV01(GribBuilder):
    """This is the builder for model data that is ingested from grib2 PRESSURE files.
    In the NODD these can be found in the model_raob_pressure directory. For example:
    https://noaa-hrrr-bdp-pds.s3.amazonaws.com/index.html#hrrr.20240731/conus/hrrr.t00z.wrfprsf00.grib2
    is the pressure file for the HRRR operational model at 00Z on 2014-07-31, which is the one used for the
    integration tests that are associated with this builder.
    It is a concrete builder specifically for the model raob data that are organized
    by isobaric level (pressure). While they can differ from model to model, these levels
    are mainly standardized below 100 mb to every 25 mb.
    This builder is a subclass of the GribBuilder class.
    Args:
        load_spec (Object): The load spec used to init the parent
        ingest_document (Object): the ingest document
        number_stations (int, optional): the maximum number of stations to process (for debugging). Defaults to sys.maxsize.
    """

    def __init__(
        self,
        load_spec,
        ingest_document,
        number_stations=sys.maxsize,
    ):
        GribBuilder.__init__(
            self,
            load_spec,
            ingest_document,
            number_stations=sys.maxsize,
        )

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

            ds_isobaricInhPa = xr.open_dataset(
                queue_element,
                engine="cfgrib",
                backend_kwargs={"filter_by_keys": {"typeOfLevel": "isobaricInhPa"},
                    "read_keys": ["projString"],
                    "indexpath": "",
                },
            )
            in_proj = pyproj.Proj(proj="latlon")
            proj_string = ds_isobaricInhPa.r2.attrs["GRIB_projString"]
            max_x = ds_isobaricInhPa.r2.attrs["GRIB_Nx"]
            max_y = ds_isobaricInhPa.r2.attrs["GRIB_Ny"]
            spacing = ds_isobaricInhPa.r2.attrs["GRIB_DxInMetres"]
            latitude_of_first_grid_point_in_degrees = ds_isobaricInhPa.r2.attrs[
                "GRIB_latitudeOfFirstGridPointInDegrees"
            ]
            longitude_of_first_grid_point_in_degrees = ds_isobaricInhPa.r2.attrs[
                "GRIB_longitudeOfFirstGridPointInDegrees"
            ]
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
                ds_isobaricInhPa.valid_time.values.astype("uint64") / 10**9
            ).astype("uint32")
            ds_fcst_len = (int)((ds_isobaricInhPa.step.values) / 1e9 / 3600)

            ds_pressure = ds_isobaricInhPa.coords["isobaricInhPa"].values
            ds_height = ds_isobaricInhPa.gh
            ds_temperature = ds_isobaricInhPa.t
            ds_relative_humidity = ds_isobaricInhPa.r
            ds_dewpoint = ds_isobaricInhPa.dpt
            ds_specific_humidity = ds_isobaricInhPa.q
            ds_u_wind = ds_isobaricInhPa.u
            ds_v_wind = ds_isobaricInhPa.v

            # gh   Geopotential height
            # t   Temperature
            # r   Relative humidity
            # dpt   Dew point temperature
            # q   Specific humidity
            # u   U component of wind
            # v   V component of wind

            # to get the values you can use something like the following...
            # ds.t.values  - which gets the entire grid of values
            # or you can get a specific value by using the grid coordinates
            # ds.t.values[9, 100, 200]  - which gets the value at the
            # ninth pressure level , the 100th x coordinate and the 200th y coordinate
            # >>> ds.t.values[9,100, 200]
            # np.float32(291.29755)  - temp in kelvin
            # example:
            # ...     ds[v].values[9,100,200]
            # example:
            # print (v, ": ", ds_temperature[9,100,200])

            # set up the variables map for the translate_template_item method. this way only the
            # translation map needs to be a class variable. Better data hiding.
            # It seems that cfgrib is graceful about missing variables, so we don't need to check
            # for them here. But when we try to get the indexed values we will get an exception
            # if the variable is missing. We will catch that exception and return an empty document
            try:
                self.ds_translate_item_variables_map = {
                    "temperature": ds_temperature,
                    "dewpoint": ds_dewpoint,
                    "specific_humidity": ds_specific_humidity,
                    "u_wind": ds_u_wind,
                    "v_wind": ds_v_wind,
                    "height": ds_height,
                    "pressure": ds_pressure,
                    "relative_humidity": ds_relative_humidity,
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
