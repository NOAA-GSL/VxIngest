"""
test station utils
"""
import os
import time

from pathlib import Path

import pygrib
import pyproj
import pytest
import yaml
from couchbase.cluster import Cluster, ClusterOptions

# from couchbase.search import GeoBoundingBoxQuery
from couchbase_core.cluster import PasswordAuthenticator

import grib2_to_cb.get_grid as gg


def test_utility_script():  # pylint: disable=too-many-locals
    # noinspection PyBroadException
    """
    # from test_grid.py
    projection = gg.getGrid(grib2_file)
    spacing, max_x, max_y = gg.getAttributes(grib2_file)
    # Set the two projections to be used during the transformation (nearest neighbor method, what we use for everything with METARS)
    in_proj = pyproj.Proj(proj='latlon')
    out_proj = projection
    """
    try:
        credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
        assert Path(credentials_file).is_file(), "credentials_file Does not exist"

        _f = open(credentials_file)
        yaml_data = yaml.load(_f, yaml.SafeLoader)
        host = yaml_data["cb_host"]
        user = yaml_data["cb_user"]
        password = yaml_data["cb_password"]
        options = ClusterOptions(PasswordAuthenticator(user, password))
        cluster = Cluster("couchbase://" + host, options)
        collection = cluster.bucket(
            "mdata"
        ).default_collection()  # pylint: disable=unused-variable
        result = cluster.query("SELECT RAW CLOCK_MILLIS()")
        current_clock = result.rows()[0] / 1000
        current_time = time.time()

        assert current_clock == pytest.approx(
            current_time
        ), "SELECT RAW CLOCK_MILLIS() did not return current time"

        # Grab the projection information from the test file
        grib2_file = "/opt/public/data/grids/hrrr/conus/wrfprs/grib2/2119614000018"
        assert Path(
            grib2_file
        ).is_file(), "/opt/public/data/grids/hrrr/conus/wrfprs/grib2/2119614000018 Does not exist"

        projection = gg.getGrid(grib2_file)
        grbs = pygrib.open(grib2_file)  # pylint: disable=no-member
        grbm = grbs.message(1)
        spacing, max_x, max_y = gg.getAttributes(grib2_file)

        assert (
            projection.description == "PROJ-based coordinate operation"
        ), "projection description: is Not corrrect"
        # Set the two projections to be used during the transformation (nearest neighbor method, what we use for everything with METARS)
        in_proj = pyproj.Proj(proj="latlon")
        out_proj = projection
        transformer = pyproj.Transformer.from_proj(proj_from=in_proj, proj_to=out_proj)
        transformer_reverse = (
            pyproj.Transformer.from_proj(  # pylint: disable=unused-variable
                proj_from=out_proj, proj_to=in_proj
            )
        )
        # get stations from couchbase
        fcst_valid_epoch = round(grbm.validDate.timestamp())
        domain_stations = []
        result = cluster.query(
            "SELECT mdata.geo, name from mdata where type='MD' and docType='station' and subset='METAR' and version='V01'"
        )
        for row in result:
            # choose the geo whose timeframe includes this grib file init time
            geo_index = get_geo_index(fcst_valid_epoch, row["geo"])
            x, y = transformer.transform(
                row["geo"][geo_index]["lon"],
                row["geo"][geo_index]["lat"],
                radians=False,
            )
            x_stat, y_stat = x / spacing, y / spacing
            if x_stat < 0 or x_stat > max_x or y_stat < 0 or y_stat > max_y:
                continue
            domain_stations.append(row)
        assert len(domain_stations) != len(
            result.buffered_rows
        ), "station query result and domain_station length are the same - no filtering?"
    except Exception as _e:  # pylint: disable=broad-except
        assert False, f"TestGsdIngestManager Exception failure: {_e}"


def get_geo_index(fcst_valid_epoch, geo):
    latest_time = 0
    latest_index = 0
    for geo_index in range(len(geo)):
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
