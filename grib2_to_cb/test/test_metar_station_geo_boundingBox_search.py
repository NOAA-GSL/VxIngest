import sys
import os
import unittest
import yaml
import time
from pathlib import Path
from couchbase.cluster import Cluster, ClusterOptions
from couchbase_core.cluster import PasswordAuthenticator
from couchbase.search import GeoBoundingBoxQuery
import pyproj
import pygrib
import grib2_to_cb.get_grid as gg


class TestStationBoundingBoxQuery(unittest.TestCase):

    def test_main(self):
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
            self.assertTrue (Path(self.credentials_file).is_file(),"credentials_file Does not exist")
            
            _f = open(self.credentials_file)
            _yaml_data = yaml.load(_f, yaml.SafeLoader)
            host = _yaml_data['cb_host']
            user = _yaml_data['cb_user']
            password = _yaml_data['cb_password']
            options = ClusterOptions(PasswordAuthenticator(user, password))
            self.cluster = Cluster('couchbase://' + host, options)
            self.collection = self.cluster.bucket("mdata").default_collection()
            result = self.cluster.query("SELECT RAW CLOCK_MILLIS()")
            current_clock = int(result.rows()[0] / 1000)
            current_time = int(time.time())

            self.assertTrue(current_clock == current_time, "SELECT RAW CLOCK_MILLIS() did not return current time")
            
            # Grab the projection information from the test file
            grib2_file = '/opt/public/data/grids/hrrr/conus/wrfprs/grib2/2119614000018'

            self.assertTrue (Path(grib2_file).is_file(), "/opt/public/data/grids/hrrr/conus/wrfprs/grib2/2119614000018 Does not exist" )

            projection = gg.getGrid(grib2_file)
            grbs = pygrib.open(grib2_file)
            grbm = grbs.message(1)
            lat,lon = grbm.latlons()
            lat_shape = lat.shape
            lat_min = lat.min()
            lat_max = lat.max()
            lon_shape = lon.shape
            lon_min = lon.min()
            lon_max = lon.max()
            spacing, max_x, max_y = gg.getAttributes(grib2_file)

            self.assertEqual(projection.description, 'PROJ-based coordinate operation', "projection description: is Not corrrect")
            # Set the two projections to be used during the transformation (nearest neighbor method, what we use for everything with METARS)
            out_proj = pyproj.Proj(proj='latlon')
            in_proj = projection

            transformer_reverse = pyproj.Transformer.from_proj(proj_from=in_proj,proj_to=out_proj)

            # Grab the lat-lon of the max grid points
            t_lon_min, t_lat_min = transformer_reverse.transform(0,0, radians=False)
            t_lon_max, t_lat_max = transformer_reverse.transform(max_x*spacing,max_y*spacing, radians=False)
            #Location objects are specified as a Tuple[SupportsFloat,SupportsFloat] of longitude and latitude respectively
            top_left = (lon_min,lat_max)
            bottom_right = (lon_max, lat_min)
            query = GeoBoundingBoxQuery(lat_max, lon_min, lat_min, lon_max )
            result = self.cluster.search_query("geo-station", query)
            self.assertEqual( result.metadata().metrics().total_rows(), 3000, "Reported total rows: is not 3000")
        except Exception as e:
            self.fail("TestGsdIngestManager Exception failure: " + str(e))
