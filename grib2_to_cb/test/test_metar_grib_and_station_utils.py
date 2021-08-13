import os
import time
import unittest
from pathlib import Path

import grib2_to_cb.get_grid as gg
import pygrib
import pyproj
import yaml
from couchbase.cluster import Cluster, ClusterOptions
from couchbase.search import GeoBoundingBoxQuery
from couchbase_core.cluster import PasswordAuthenticator


class TestGribStationUtils(unittest.TestCase):

    def test_utility_script(self):
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
            cwd = os.getcwd()
            self.credentials_file = os.environ['HOME'] + '/adb-cb1-credentials'
            self.assertTrue(Path(self.credentials_file).is_file(),
                            "credentials_file Does not exist")

            f = open(self.credentials_file)
            yaml_data = yaml.load(f, yaml.SafeLoader)
            host = yaml_data['cb_host']
            user = yaml_data['cb_user']
            password = yaml_data['cb_password']
            options = ClusterOptions(PasswordAuthenticator(user, password))
            self.cluster = Cluster('couchbase://' + host, options)
            self.collection = self.cluster.bucket("mdata").default_collection()
            result = self.cluster.query("SELECT RAW CLOCK_MILLIS()")
            current_clock = result.rows()[0] / 1000
            current_time = time.time()

            self.assertAlmostEqual(current_clock, current_time, places=None, delta=100,
                                   msg="SELECT RAW CLOCK_MILLIS() did not return current time")

            # Grab the projection information from the test file
            grib2_file = '/opt/public/data/grids/hrrr/conus/wrfprs/grib2/2119614000018'

            self.assertTrue(Path(grib2_file).is_file(
            ), "/opt/public/data/grids/hrrr/conus/wrfprs/grib2/2119614000018 Does not exist")

            self.projection = gg.getGrid(grib2_file)
            self.grbs = pygrib.open(grib2_file)
            self.grbm = self.grbs.message(1)
            self.spacing, max_x, max_y = gg.getAttributes(grib2_file)

            self.assertEqual(self.projection.description, 'PROJ-based coordinate operation',
                             "projection description: is Not corrrect")
            # Set the two projections to be used during the transformation (nearest neighbor method, what we use for everything with METARS)
            self.in_proj = pyproj.Proj(proj='latlon')
            self.out_proj = self.projection
            self.transformer = pyproj.Transformer.from_proj(
                proj_from=self.in_proj, proj_to=self.out_proj)
            self.transformer_reverse = pyproj.Transformer.from_proj(
                proj_from=self.out_proj, proj_to=self.in_proj)
            # get stations from couchbase
            self.domain_stations = []
            result = self.cluster.query(
                "SELECT mdata.geo.lat, mdata.geo.lon, name from mdata where type='MD' and docType='station' and subset='METAR' and version='V01'")
            for row in result:
                x, y = self.transformer.transform(
                    row['lon'], row['lat'], radians=False)
                x_stat, y_stat = x/self.spacing, y/self.spacing
                if x_stat < 0 or x_stat > max_x or y_stat < 0 or y_stat > max_y:
                    continue
                self.domain_stations.append(row)
            self.assertNotEqual(len(self.domain_stations), len(
                result.buffered_rows), "station query result and domain_station length are the same - no filtering?")
        except Exception as e:
            self.fail("TestGsdIngestManager Exception failure: " + str(e))
