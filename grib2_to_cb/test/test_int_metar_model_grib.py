# pylint: disable=too-many-lines
"""
    integration tests for grib builder
    This test expects to find a valid grib file in the local directory /opt/public/data/grids/hrrr/conus/wrfprs/grib2.
This test expects to write to the local output directory /opt/data/grib_to_cb/output so that directory should exist.
21 196 14 000018 %y %j %H %f  treating the last 6 decimals as microseconds even though they are not.
these files are two digit year, day of year, hour, and forecast lead time (6 digit ??)
"""
import copy
import datetime as DT
from datetime import timedelta
import glob
import json
import math
import os
from glob import glob

import numpy as np
import pygrib
import pyproj
import pytest
import yaml
from couchbase.cluster import Cluster, ClusterOptions, ClusterTimeoutOptions
from couchbase.auth import PasswordAuthenticator
from grib2_to_cb.run_ingest_threads import VXIngest
from grib2_to_cb import get_grid as gg


def get_geo_index(fcst_valid_epoch, geo):
    """return the index of the geo list that corresponds to the given fcst_valid_epoch
    - the location of a station might change over time and this list contains the
    lat/lon of the station at different times.  The fcst_valid_epoch is used to
    determine which lat/lon to use.
    Args:
        fcst_valid_epoch (int): an epoch
        geo (list): a list of geo objects

    Returns:
        int : the index
    """
    latest_time = 0
    latest_index = 0
    try:
        for geo_index in range(len(geo)):  # pylint: disable=consider-using-enumerate
            if geo[geo_index]["lastTime"] > latest_time:
                latest_time = geo[geo_index]["lastTime"]
                latest_index = geo_index
            found = False
            if (
                geo[geo_index]["firstTime"] >= fcst_valid_epoch
                and fcst_valid_epoch <= geo[geo_index]["lastTime"]
            ):
                found = True
                break
        if found:
            return geo_index
        else:
            return latest_index
    except Exception as _e:  # pylint: disable=broad-except
        assert False, f"GribBuilder.get_geo_index: Exception  error: {_e}"



def test_grib_builder_one_thread_file_pattern_hrrr_ops_conus():
    """test gribBuilder with one thread"""
    try:
        # 1632412800 fcst_len 1 -> 1632412800 - 1 * 3600 -> 1632409200 September 23, 2021 15:00:00 -> 2126615000001
        # 1632412800 fcst_len 3 -> 1632412800 - 3 * 3600 -> 1632402000 September 23, 2021 13:00:00 -> 2126613000003
        # 1632412800 fcst_len 15 -> 1632412800 - 15 * 3600 -> 1632358800 September 22, 2021 19:00:00  ->  (missing)
        # 1632412800 fcst_len 18 -> 1632412800 - 18 * 3600 -> 1632348000 September 22, 2021 22:00:00 -> 2126522000018 (missing)
        # 1632420000 September 23, 2021 18:00:00  2126616000018
        # 1632423600  September 23, 2021 19:00:00 2126617000001
        # first_epoch = 1634252400 - 10
        # last_epoch = 1634252400 + 10
        credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
        # remove output files
        for _f in glob("/opt/data/grib2_to_cb/output/test1/*.json"):
            os.remove(_f)
        vx_ingest = VXIngest()
        vx_ingest.runit(
            {
                "job_id": "JOB-TEST:V01:METAR:GRIB2:MODEL:HRRR",
                "credentials_file": credentials_file,
                "path": "/opt/public/data/grids/hrrr/conus/wrfprs/grib2",
                "file_name_mask": "%y%j%H%f",
                "output_dir": "/opt/data/grib2_to_cb/output/test1",
                "threads": 1,
                "file_pattern": "21287230000[0123456789]?",
            }
        )

    except Exception as _e:  # pylint: disable=broad-except
        assert (
            False
        ), f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus Exception failure: {_e}"
    finally:
        # remove output files
        for _f in glob("/opt/data/grib2_to_cb/output/test1/*.json"):
            os.remove(_f)


def test_grib_builder_two_threads_file_pattern_hrrr_ops_conus():
    """test gribBuilder multi-threaded"""
    try:
        # 1632412800 fcst_len 1 -> 1632412800 - 1 * 3600 -> 1632409200 September 23, 2021 15:00:00 -> 2126615000001
        # 1632412800 fcst_len 3 -> 1632412800 - 3 * 3600 -> 1632402000 September 23, 2021 13:00:00 -> 2126613000003
        # 1632412800 fcst_len 15 -> 1632412800 - 15 * 3600 -> 1632358800 September 22, 2021 19:00:00  ->  (missing)
        # 1632412800 fcst_len 18 -> 1632412800 - 18 * 3600 -> 1632348000 September 22, 2021 22:00:00 -> 2126522000018 (missing)
        # 1632420000 September 23, 2021 18:00:00  2126616000018
        # 1632423600  September 23, 2021 19:00:00 2126617000001
        # first_epoch = 1634252400 - 10
        # last_epoch = 1634252400 + 10
        credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
        # remove output files
        for _f in glob("/opt/data/grib2_to_cb/output/test2/*.json"):
            os.remove(_f)
        vx_ingest = VXIngest()
        vx_ingest.runit(
            {
                "job_id": "JOB-TEST:V01:METAR:GRIB2:MODEL:HRRR",
                "credentials_file": credentials_file,
                "path": "/opt/public/data/grids/hrrr/conus/wrfprs/grib2",
                "file_name_mask": "%y%j%H%f",
                "output_dir": "/opt/data/grib2_to_cb/output/test2",
                "threads": 2,
                "file_pattern": "21287230000[0123456789]?",
            }
        )

    except Exception as _e:  # pylint: disable=broad-except
        assert (
            False
        ), f"TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus Exception failure: {_e} "
    finally:
        # remove output files
        for _f in glob("/opt/data/grib2_to_cb/output/test2/*.json"):
            os.remove(_f)


def test_grib_builder_verses_script():  # pylint: disable=too-many-locals, too-many-branches, too-many-statements
    """test gribBuilder matches what the test script returns"""
    # noinspection PyBroadException
    try:
        # remove output files
        for _f in glob("/opt/data/grib2_to_cb/output/test3/*.json"):
            os.remove(_f)
        # the input_data_path is specified in the job spec
        # for this test it should be
        # /opt/data/grib2_to_cb/input_files
        # we need to touch these file to ensure that the mtime is more recent than the
        # last time the test was run (because the test script will only process files
        # that are newer than the last time it was last run)

        credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
        vx_ingest = VXIngest()
        # using a test JOB SPEC because the input path is local
        vx_ingest.runit(
            {
                "job_id": "JOB-TEST:V01:METAR:GRIB2:MODEL:HRRR",
                "credentials_file": credentials_file,
                "file_name_mask": "%y%j%H%f",
                "output_dir": "/opt/data/grib2_to_cb/output/test3",
                "threads": 1,
                "file_pattern": "212872300000[012]",
            }
        )
        #'file_pattern': '21287230000[0123456789]?'

        list_of_output_files = glob(
            "/opt/data/grib2_to_cb/output/test3/[0123456789]????????????.json"
        )
        latest_output_file = max(list_of_output_files, key=os.path.getctime)
        # Opening JSON file
        _f = open(latest_output_file, encoding='utf-8')
        # returns JSON object as
        # a dictionary
        vx_ingest_output_data = json.load(_f)
        # Closing file
        _f.close()
        expected_station_data = {}
        _f = open (credentials_file, encoding='utf-8')
        _yaml_data = yaml.load(_f, yaml.SafeLoader)
        _host = _yaml_data["cb_host"]
        _user = _yaml_data["cb_user"]
        _password = _yaml_data["cb_password"]
        _bucket = _yaml_data["cb_bucket"]
        _collection = _yaml_data["cb_collection"]
        _scope = _yaml_data["cb_scope"]
        _f.close()

        timeout_options=ClusterTimeoutOptions(kv_timeout=timedelta(seconds=25), query_timeout=timedelta(seconds=120))
        options=ClusterOptions(PasswordAuthenticator(_user, _password), timeout_options=timeout_options)
        cluster = Cluster("couchbase://" + _host, options)

        # Grab the projection information from the test file
        latest_input_file = (
            "/opt/public/data/grids/hrrr/conus/wrfprs/grib2/"
            + os.path.basename(
                "/opt/data/grib2_to_cb/output/test3/2128723000002.json"
            ).split(".")[0]
        )
        projection = gg.getGrid(latest_input_file)
        grbs = pygrib.open(latest_input_file)  # pylint: disable=no-member
        grbm = grbs.message(1)
        fcst_valid_epoch = round(grbm.validDate.timestamp())
        spacing, max_x, max_y = gg.getAttributes(latest_input_file)

        assert (
            projection.description == "PROJ-based coordinate operation"
        ), "projection description: is Not corrrect"
        # Set the two projections to be used during the transformation (nearest neighbor method, what we use for everything with METARS)
        in_proj = pyproj.Proj(proj="latlon")
        out_proj = projection
        transformer = pyproj.Transformer.from_proj(proj_from=in_proj, proj_to=out_proj)
        transformer_reverse = (  # pylint: disable=unused-variable
            pyproj.Transformer.from_proj(proj_from=out_proj, proj_to=in_proj)
        )
        domain_stations = []
        for i in vx_ingest_output_data[0]["data"].keys():
            station_name = i
            result = cluster.query(
                f"SELECT {_collection}.geo from `{_bucket}`.{_scope}.{_collection} where type='MD' and docType='station' and subset='{_collection}' and version='V01' and {_collection}.name = '{station_name}'"
            )
            if not list(result):
                continue
            rows = list(result.rows())
            row = rows[0]
            geo_index = get_geo_index(fcst_valid_epoch, row["geo"])
            #i["lat"] = row["geo"][geo_index]["lat"]
            #i["lon"] = row["geo"][geo_index]["lon"]
            # pyproj.transformer.transform returns a tuple - function signatures are overloaded with @overload
            _x, _y = transformer.transform(   # pylint: disable=unpacking-non-sequence
                row["geo"][geo_index]["lon"],
                row["geo"][geo_index]["lat"],
                radians=False,
            )
            x_gridpoint, y_gridpoint = _x / spacing, _y / spacing
            if (
                x_gridpoint < 0
                or x_gridpoint > max_x
                or y_gridpoint < 0
                or y_gridpoint > max_y
            ):
                continue
            station = copy.deepcopy(row)
            station["geo"][geo_index]["x_gridpoint"] = x_gridpoint
            station["geo"][geo_index]["y_gridpoint"] = y_gridpoint
            station["name"] = station_name
            domain_stations.append(station)

        expected_station_data["fcstValidEpoch"] = fcst_valid_epoch
        assert (
            expected_station_data["fcstValidEpoch"]
            == vx_ingest_output_data[0]["fcstValidEpoch"]
        ), "expected fcstValidEpoch and derived fcstValidEpoch are not the same"
        expected_station_data["fcstValidISO"] = DT.datetime.fromtimestamp(
            fcst_valid_epoch
        ).isoformat()
        assert (
            expected_station_data["fcstValidISO"]
            == vx_ingest_output_data[0]["fcstValidISO"]
        ), "expected fcstValidISO and derived fcstValidISO are not the same"
        expected_station_data["id"] = (
            "DD:V01:METAR:HRRR_OPS:"
            + str(expected_station_data["fcstValidEpoch"])
            + ":"
            + str(grbm.forecastTime)
        )
        # NOTE: This test was originally supposed to use a test job-spec.
        # which would retrieve "DD-TEST:V01:METAR:HRRR_OPS:"
        assert (
            expected_station_data["id"] == vx_ingest_output_data[0]["id"]
        ), "expected id and derived id are not the same"
        # Ceiling
        message = grbs.select(name="Orography")[0]
        surface_hgt_values = message["values"]

        message = grbs.select(
            name="Geopotential Height", typeOfFirstFixedSurface="215"
        )[0]
        ceil_values = message["values"]

        # iterate the stations and get the values from the grib file
        # Each station has a geo list that contains the lat/lon of the station
        # and the lat/lon of the gridpoint that is closest to the station
        # The gridpoint lat/lon is used to get the values from the grib file
        # for ceil_msl and surface level which are used to get the ceiling AGL
        # for each station ceil_agl = (ceil_msl - surface) * 3.281 (convert to feet)

        for i in range(len(domain_stations)):  # pylint: disable=consider-using-enumerate
            station = domain_stations[i]
            # get the correct lat / lon for this station and fcst_valid_epoch (the station lat/lon might change over time)
            geo_index = get_geo_index(fcst_valid_epoch, station["geo"])
            surface = surface_hgt_values[
                round(station["geo"][geo_index]["y_gridpoint"]),
                round(station["geo"][geo_index]["x_gridpoint"]),
            ]
            ceil_msl = ceil_values[
                round(station["geo"][geo_index]["y_gridpoint"]),
                round(station["geo"][geo_index]["x_gridpoint"]),
            ]
            # Convert to ceiling AGL and from meters to tens of feet (what is currently inside SQL, we'll leave it as just feet in CB)
            ceil_agl = (ceil_msl - surface) * 3.281

            # lazy initialization of _expected_station_data
            if "data" not in expected_station_data.keys():
                expected_station_data["data"] = []
            if len(expected_station_data["data"]) <= i:
                expected_station_data["data"].append({})

            expected_station_data["data"][i]["Ceiling"] = (
                ceil_agl if not np.ma.is_masked(ceil_agl) else None
            )

        # Surface Pressure
        message = grbs.select(name="Surface pressure")[0]
        values = message["values"]
        for i, station in enumerate(domain_stations):
            geo_index = get_geo_index(fcst_valid_epoch, station["geo"])
            value = values[
                round(station["geo"][geo_index]["y_gridpoint"]),
                round(station["geo"][geo_index]["x_gridpoint"]),
            ]
            # interpolated gridpoints cannot be rounded
            interpolated_value = gg.interpGridBox(
                values,
                station["geo"][geo_index]["y_gridpoint"],
                station["geo"][geo_index]["x_gridpoint"],
            )
            pres_mb = interpolated_value / 100
            expected_station_data["data"][i]["Surface Pressure"] = (
                pres_mb if not np.ma.is_masked(pres_mb) else None
            )

        # Temperature
        message = grbs.select(name="2 metre temperature")[0]
        values = message["values"]
        for i, station in enumerate(domain_stations):
            geo_index = get_geo_index(fcst_valid_epoch, station["geo"])
            tempk = gg.interpGridBox(
                values,
                station["geo"][geo_index]["y_gridpoint"],
                station["geo"][geo_index]["x_gridpoint"],
            )
            tempf = ((tempk - 273.15) * 9) / 5 + 32
            expected_station_data["data"][i]["Temperature"] = (
                tempf if not np.ma.is_masked(tempf) else None
            )

        # Dewpoint
        message = grbs.select(name="2 metre dewpoint temperature")[0]
        values = message["values"]
        for i, station in enumerate(domain_stations):
            geo_index = get_geo_index(fcst_valid_epoch, station["geo"])
            dpk = gg.interpGridBox(
                values,
                station["geo"][geo_index]["y_gridpoint"],
                station["geo"][geo_index]["x_gridpoint"],
            )
            dpf = ((dpk - 273.15) * 9) / 5 + 32
            expected_station_data["data"][i]["DewPoint"] = (
                dpf if not np.ma.is_masked(dpf) else None
            )

        # Relative Humidity
        message = grbs.select(name="2 metre relative humidity")[0]
        values = message["values"]
        for i, station in enumerate(domain_stations):
            geo_index = get_geo_index(fcst_valid_epoch, station["geo"])
            _rh = gg.interpGridBox(
                values,
                station["geo"][geo_index]["y_gridpoint"],
                station["geo"][geo_index]["x_gridpoint"],
            )
            expected_station_data["data"][i]["RH"] = (
                _rh if not np.ma.is_masked(_rh) else None
            )

        # Wind Speed
        message = grbs.select(name="10 metre U wind component")[0]
        uwind_values = message["values"]

        vwind_message = grbs.select(name="10 metre V wind component")[0]
        vwind_values = vwind_message["values"]

        for i, station in enumerate(domain_stations):
            geo_index = get_geo_index(fcst_valid_epoch, station["geo"])
            uwind_ms = gg.interpGridBox(
                uwind_values,
                station["geo"][geo_index]["y_gridpoint"],
                station["geo"][geo_index]["x_gridpoint"],
            )
            vwind_ms = gg.interpGridBox(
                vwind_values,
                station["geo"][geo_index]["y_gridpoint"],
                station["geo"][geo_index]["x_gridpoint"],
            )
            # Convert from U-V components to speed and direction (requires rotation if grid is not earth relative)
            # wind speed then convert to mph
            ws_ms = math.sqrt(
                (uwind_ms * uwind_ms) + (vwind_ms * vwind_ms)
            )  # pylint: disable=c-extension-no-member
            ws_mph = (ws_ms / 0.447) + 0.5
            expected_station_data["data"][i]["WS"] = (
                ws_mph if not np.ma.is_masked(ws_mph) else None
            )

            # wind direction   - lon is the lon of the station
            station = domain_stations[i]
            theta = gg.getWindTheta(vwind_message, station["geo"][geo_index]["lon"])
            radians = math.atan2(
                uwind_ms, vwind_ms
            )  # pylint: disable=c-extension-no-member
            _wd = (radians * 57.2958) + theta + 180
            # adjust for outliers
            if _wd < 0:
                _wd = _wd + 360
            if _wd > 360:
                _wd = _wd - 360

            expected_station_data["data"][i]["WD"] = (
                _wd if not np.ma.is_masked(_wd) else None
            )

        # Visibility
        message = grbs.select(name="Visibility")[0]
        values = message["values"]
        for i, station in enumerate(domain_stations):
            geo_index = get_geo_index(fcst_valid_epoch, station["geo"])
            value = values[
                round(station["geo"][geo_index]["y_gridpoint"]),
                round(station["geo"][geo_index]["x_gridpoint"]),
            ]
            expected_station_data["data"][i]["Visibility"] = (
                value / 1609.344 if not np.ma.is_masked(value) else None
            )
        grbs.close()

        for i, station in enumerate(domain_stations):
            station_name = station['name']
            if expected_station_data["data"][i]["Ceiling"] is not None:
                assert expected_station_data["data"][i]["Ceiling"] == pytest.approx(
                    vx_ingest_output_data[0]["data"][station_name]["Ceiling"]
                ), "Expected Ceiling and derived Ceiling are not equal"
            if expected_station_data["data"][i]["Surface Pressure"] is not None:
                assert expected_station_data["data"][i][
                    "Surface Pressure"
                ] == pytest.approx(
                    vx_ingest_output_data[0]["data"][station_name]["Surface Pressure"]
                ), "Expected Surface Pressure and derived Surface Pressure are not equal"
            if expected_station_data["data"][i]["Temperature"] is not None:
                assert expected_station_data["data"][i]["Temperature"] == pytest.approx(
                    vx_ingest_output_data[0]["data"][station_name]["Temperature"]
                ), "Expected Temperature and derived Temperature are not equal"
            if expected_station_data["data"][i]["DewPoint"] is not None:
                assert expected_station_data["data"][i]["DewPoint"] == pytest.approx(
                    vx_ingest_output_data[0]["data"][station_name]["DewPoint"]
                ), "Expected DewPoint and derived DewPoint are not equal"
            if expected_station_data["data"][i]["RH"] is not None:
                assert expected_station_data["data"][i]["RH"] == pytest.approx(
                    vx_ingest_output_data[0]["data"][station_name]["RH"]
                ), "Expected RH and derived RH are not equal"
            if expected_station_data["data"][i]["WS"] is not None:
                assert expected_station_data["data"][i]["WS"] == pytest.approx(
                    vx_ingest_output_data[0]["data"][station_name]["WS"]
                ), "Expected WS and derived WS are not equal"
            if expected_station_data["data"][i]["WD"] is not None:
                assert expected_station_data["data"][i]["WD"] == pytest.approx(
                    vx_ingest_output_data[0]["data"][station_name]["WD"]
                ), "Expected WD and derived WD are not equal"
            if expected_station_data["data"][i]["Visibility"] is not None:
                assert expected_station_data["data"][i]["Visibility"] == pytest.approx(
                    vx_ingest_output_data[0]["data"][station_name]["Visibility"]
                ), "Expected Visibility and derived Visibility are not equal"
    except Exception as _e:  # pylint: disable=broad-except
        assert False, f"TestGribBuilderV01 Exception failure: {_e}"
