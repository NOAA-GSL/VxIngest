import copy
import glob
import json
import math
import os
import sys
import datetime
import unittest

import grib2_to_cb.get_grid as gg
import pygrib
import pyproj
import yaml
import numpy
from couchbase.cluster import Cluster, ClusterOptions
from couchbase_core.cluster import PasswordAuthenticator
from grib2_to_cb.run_ingest_threads import VXIngest


class TestGribBuilderV01(unittest.TestCase):
    """
    This test expects to find a valid grib file in the local directory /opt/public/data/grids/hrrr/conus/wrfprs/grib2.
    This test expects to write to the local output directory /opt/data/grib_to_cb/output so that directory should exist.
    """
    # 21 196 14 000018 %y %j %H %f  treating the last 6 decimals as microseconds even though they are not.
    # these files are two digit year, day of year, hour, and forecast lead time (6 digit ??)

    def test_gribBuilder_one_epoch_hrrr_ops_conus(self):
        try:
            #1632412800 fcst_len 1 -> 1632412800 - 1 * 3600 -> 1632409200 September 23, 2021 15:00:00 -> 2126615000001
            #1632412800 fcst_len 3 -> 1632412800 - 3 * 3600 -> 1632402000 September 23, 2021 13:00:00 -> 2126613000003
            #1632412800 fcst_len 15 -> 1632412800 - 15 * 3600 -> 1632358800 September 22, 2021 19:00:00  ->  (missing)
            #1632412800 fcst_len 18 -> 1632412800 - 18 * 3600 -> 1632348000 September 22, 2021 22:00:00 -> 2126522000018 (missing)
            #1632420000 September 23, 2021 18:00:00  2126616000018
            #1632423600  September 23, 2021 19:00:00 2126617000001
            first_epoch = 1632409200 - 10
            last_epoch = 1632409200 + 10
            cwd = os.getcwd()
            self.credentials_file = os.environ['HOME'] + '/adb-cb1-credentials'
            self.spec_file = cwd + '/grib2_to_cb/test/test_load_spec_grib_metar_hrrr_ops_V01.yaml'
            vxIngest = VXIngest()
            vxIngest.runit({'spec_file': self.spec_file,
                            'credentials_file': self.credentials_file,
                            'path': '/opt/public/data/grids/hrrr/conus/wrfprs/grib2',
                            'file_name_mask': '%y%j%H%f',
                            'output_dir': '/opt/data/grib2_to_cb/output',
                            'threads': 1,
                            'first_epoch': first_epoch,
                            'last_epoch': last_epoch
                            })
        except:
            self.fail("TestGribBuilderV01.test_gribBuilder_one_epoch_hrrr_ops_conus Exception failure: " +
                      str(sys.exc_info()[0]))
        return

    def test_gribBuilder_verses_script(self):
        # noinspection PyBroadException
        try:
            outfilelist = glob.glob('/opt/data/grib2_to_cb/output/*')
            for f in outfilelist:
                os.remove(f)
            list_of_input_files = glob.glob(
                '/opt/public/data/grids/hrrr/conus/wrfprs/grib2/*')
            latest_input_file = max(list_of_input_files, key=os.path.getctime)
            file_utc_time = datetime.datetime.strptime(os.path.basename(latest_input_file), '%y%j%H%f')
            latest_input_file_time = int((file_utc_time - datetime.datetime(1970, 1, 1)).total_seconds())
            first_epoch = round(latest_input_file_time) - 100
            last_epoch = round(latest_input_file_time) + 100
            cwd = os.getcwd()
            self.credentials_file = os.environ['HOME'] + '/adb-cb1-credentials'
            self.spec_file = cwd + '/grib2_to_cb/test/test_load_spec_grib_metar_hrrr_ops_V01.yaml'
            vxIngest = VXIngest()
            vxIngest.runit({'spec_file': self.spec_file,
                            'credentials_file': self.credentials_file,
                            'path': '/opt/public/data/grids/hrrr/conus/wrfprs/grib2',
                            'file_name_mask': '%y%j%H%f',
                            'output_dir': '/opt/data/grib2_to_cb/output',
                            'threads': 1,
                            'number_stations': 3,  # only process 3 stations
                            'first_epoch': first_epoch,
                            'last_epoch': last_epoch
                            })
            list_of_output_files = glob.glob('/opt/data/grib2_to_cb/output/*')
            latest_output_file = max(
                list_of_output_files, key=os.path.getctime)
            # Opening JSON file
            f = open(latest_output_file)
            # returns JSON object as
            # a dictionary
            vxIngest_output_data = json.load(f)
            # Closing file
            f.close()
            output_station_data = {}
            expected_station_data = {}

            f = open(self.credentials_file)
            yaml_data = yaml.load(f, yaml.SafeLoader)
            host = yaml_data['cb_host']
            user = yaml_data['cb_user']
            password = yaml_data['cb_password']
            options = ClusterOptions(PasswordAuthenticator(user, password))
            self.cluster = Cluster('couchbase://' + host, options)
            self.collection = self.cluster.bucket("mdata").default_collection()

            # Grab the projection information from the test file

            self.projection = gg.getGrid(latest_input_file)
            self.grbs = pygrib.open(latest_input_file)
            self.grbm = self.grbs.message(1)
            self.spacing, max_x, max_y = gg.getAttributes(latest_input_file)

            self.assertEqual(self.projection.description, 'PROJ-based coordinate operation',
                             "projection description: is Not corrrect")
            # Set the two projections to be used during the transformation (nearest neighbor method, what we use for everything with METARS)
            self.in_proj = pyproj.Proj(proj='latlon')
            self.out_proj = self.projection
            self.transformer = pyproj.Transformer.from_proj(
                proj_from=self.in_proj, proj_to=self.out_proj)
            self.transformer_reverse = pyproj.Transformer.from_proj(
                proj_from=self.out_proj, proj_to=self.in_proj)
            self.domain_stations = []
            for i in vxIngest_output_data[0]['data']:
                station_name = i['name']
                result = self.cluster.query(
                    "SELECT mdata.geo.lat, mdata.geo.lon from mdata where type='MD' and docType='station' and subset='METAR' and version='V01' and mdata.name = $name",
                    name=station_name)
                row = result.get_single_result()
                i['lat'] = row['lat']
                i['lon'] = row['lon']
                x, y = self.transformer.transform(
                    row['lon'], row['lat'], radians=False)
                x_gridpoint, y_gridpoint = x/self.spacing, y/self.spacing
                if x_gridpoint < 0 or x_gridpoint > max_x or y_gridpoint < 0 or y_gridpoint > max_y:
                    continue
                station = copy.deepcopy(row)
                station['x_gridpoint'] = x_gridpoint
                station['y_gridpoint'] = y_gridpoint
                station['name'] = station_name
                self.domain_stations.append(station)

            expected_station_data['fcstValidEpoch'] = round(
                self.grbm.validDate.timestamp())
            self.assertEqual(expected_station_data['fcstValidEpoch'], vxIngest_output_data[0]['fcstValidEpoch'],
                             "expected fcstValidEpoch and derived fcstValidEpoch are not the same")
            expected_station_data['fcstValidBeg'] = self.grbm.validDate.isoformat(
            )
            self.assertEqual(expected_station_data['fcstValidBeg'], vxIngest_output_data[0]['fcstValidBeg'],
                             "expected fcstValidBeg and derived fcstValidBeg are not the same")
            expected_station_data['id'] = "DD-TEST:V01:METAR:HRRR_OPS:" + str(expected_station_data['fcstValidEpoch']) + ":" + str(
                self.grbm.forecastTime)
            self.assertEqual(expected_station_data['id'], vxIngest_output_data[0]['id'],
                             "expected id and derived id are not the same")

            # Ceiling
            message = self.grbs.select(name='Orography')[0]
            surface_hgt_values = message['values']

            message = self.grbs.select(
                name='Geopotential Height', typeOfFirstFixedSurface='215')[0]
            ceil_values = message['values']

            for i in range(len(self.domain_stations)):
                station = self.domain_stations[i]
                surface = surface_hgt_values[round(
                    station['y_gridpoint']), round(station['x_gridpoint'])]
                ceil_msl = ceil_values[round(
                    station['y_gridpoint']), round(station['x_gridpoint'])]
                # Convert to ceiling AGL and from meters to tens of feet (what is currently inside SQL, we'll leave it as just feet in CB)
                ceil_agl = (ceil_msl - surface) * 3.281

                # lazy initialization of _expected_station_data
                if 'data' not in expected_station_data.keys():
                    expected_station_data['data'] = []
                if len(expected_station_data['data']) <= i:
                    expected_station_data['data'].append({})

                expected_station_data['data'][i]['Ceiling'] = ceil_agl if not numpy.ma.is_masked(ceil_agl) else None

            # Surface Pressure
            message = self.grbs.select(name='Surface pressure')[0]
            values = message['values']
            for i in range(len(self.domain_stations)):
                station = self.domain_stations[i]
                value = values[round(station['y_gridpoint']), round(
                    station['x_gridpoint'])]
                # interpolated gridpoints cannot be rounded
                interpolated_value = gg.interpGridBox(
                    values, station['y_gridpoint'], station['x_gridpoint'])
                pres_mb = interpolated_value / 100
                expected_station_data['data'][i]['Surface Pressure'] = pres_mb if not numpy.ma.is_masked(pres_mb) else None

            # Temperature
            message = self.grbs.select(name='2 metre temperature')[0]
            values = message['values']
            for i in range(len(self.domain_stations)):
                station = self.domain_stations[i]
                tempk = gg.interpGridBox(
                    values, station['y_gridpoint'], station['x_gridpoint'])
                tempf = ((tempk-273.15)*9)/5 + 32
                expected_station_data['data'][i]['Temperature'] = tempf if not numpy.ma.is_masked(tempf) else None

            # Dewpoint
            message = self.grbs.select(name='2 metre dewpoint temperature')[0]
            values = message['values']
            for i in range(len(self.domain_stations)):
                station = self.domain_stations[i]
                dpk = gg.interpGridBox(
                    values, station['y_gridpoint'], station['x_gridpoint'])
                dpf = ((dpk-273.15)*9)/5 + 32
                expected_station_data['data'][i]['DewPoint'] = dpf if not numpy.ma.is_masked(dpf) else None

            # Relative Humidity
            message = self.grbs.select(name='2 metre relative humidity')[0]
            values = message['values']
            for i in range(len(self.domain_stations)):
                station = self.domain_stations[i]
                rh = gg.interpGridBox(
                    values, station['y_gridpoint'], station['x_gridpoint'])
                expected_station_data['data'][i]['RH'] = rh if not numpy.ma.is_masked(rh) else None

            # Wind Speed
            message = self.grbs.select(name='10 metre U wind component')[0]
            uwind_values = message['values']

            vwind_message = self.grbs.select(
                name='10 metre V wind component')[0]
            vwind_values = vwind_message['values']

            for i in range(len(self.domain_stations)):
                station = self.domain_stations[i]
                uwind_ms = gg.interpGridBox(
                    uwind_values, station['y_gridpoint'], station['x_gridpoint'])
                vwind_ms = gg.interpGridBox(
                    vwind_values, station['y_gridpoint'], station['x_gridpoint'])
                # Convert from U-V components to speed and direction (requires rotation if grid is not earth relative)
                # wind speed then convert to mph
                ws_ms = math.sqrt((uwind_ms*uwind_ms)+(vwind_ms*vwind_ms))
                ws_mph = (ws_ms/0.447) + 0.5
                expected_station_data['data'][i]['WS'] = ws_mph if not numpy.ma.is_masked(ws_mph) else None

                # wind direction   - lon is the lon of the station
                station = self.domain_stations[i]
                theta = gg.getWindTheta(vwind_message, station['lon'])
                radians = math.atan2(uwind_ms, vwind_ms)
                wd = (radians*57.2958) + theta + 180
                expected_station_data['data'][i]['WD'] = wd if not numpy.ma.is_masked(wd) else None

            # Visibility
            message = self.grbs.select(name='Visibility')[0]
            values = message['values']
            for i in range(len(self.domain_stations)):
                station = self.domain_stations[i]
                value = values[round(station['y_gridpoint']), round(
                    station['x_gridpoint'])]
                expected_station_data['data'][i]['Visibility'] = value / 1609.344 if not numpy.ma.is_masked(value) else None
            self.grbs.close()

            for i in range(len(self.domain_stations)):
                if expected_station_data['data'][i]['Ceiling'] is not None:
                    self.assertAlmostEqual(expected_station_data['data'][i]['Ceiling'],
                                       vxIngest_output_data[0]['data'][i]['Ceiling'], msg="Expected Ceiling and derived Ceiling are not equal")

                if expected_station_data['data'][i]['Surface Pressure'] is not None:
                    self.assertAlmostEqual(expected_station_data['data'][i]['Surface Pressure'],
                                       vxIngest_output_data[0]['data'][i]['Surface Pressure'], msg="Expected Surface Pressure and derived Surface Pressure are not equal")

                if expected_station_data['data'][i]['Temperature'] is not None:
                    self.assertAlmostEqual(expected_station_data['data'][i]['Temperature'],
                                       vxIngest_output_data[0]['data'][i]['Temperature'], msg="Expected Temperature and derived Temperature are not equal")

                if expected_station_data['data'][i]['DewPoint'] is not None:
                    self.assertAlmostEqual(expected_station_data['data'][i]['DewPoint'],
                                       vxIngest_output_data[0]['data'][i]['DewPoint'], msg="Expected DewPoint and derived DewPoint are not equal")

                if expected_station_data['data'][i]['RH'] is not None:
                    self.assertAlmostEqual(expected_station_data['data'][i]['RH'],
                                       vxIngest_output_data[0]['data'][i]['RH'], msg="Expected RH and derived RH are not equal")

                if expected_station_data['data'][i]['WS'] is not None:
                    self.assertAlmostEqual(expected_station_data['data'][i]['WS'],
                                       vxIngest_output_data[0]['data'][i]['WS'], msg="Expected WS and derived WS are not equal")

                if expected_station_data['data'][i]['WD'] is not None:
                    self.assertAlmostEqual(expected_station_data['data'][i]['WD'],
                                       vxIngest_output_data[0]['data'][i]['WD'], msg="Expected WD and derived WD are not equal")

                if expected_station_data['data'][i]['Visibility'] is not None:
                    self.assertAlmostEqual(expected_station_data['data'][i]['Visibility'],
                                       vxIngest_output_data[0]['data'][i]['Visibility'], msg="Expected Visibility and derived Visibility are not equal")

        except:
            self.fail("TestGribBuilderV01 Exception failure: " +
                      str(sys.exc_info()[0]))
        return
