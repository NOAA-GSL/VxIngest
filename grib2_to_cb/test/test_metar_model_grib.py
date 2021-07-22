import copy
import glob
import json
import math
import os
import sys
import unittest

import grib2_to_cb.get_grid as gg
import pygrib
import pyproj
import yaml
from couchbase.cluster import Cluster, ClusterOptions
from couchbase_core.cluster import PasswordAuthenticator
from grib2_to_cb.run_ingest_threads import VXIngest


class TestGribBuilderV01(unittest.TestCase):
    # 21 196 14 000018 %y %j %H %f  treating the last 6 decimals as microseconds even though they are not.
    # these files are two digit year, day of year, hour, and forecast lead time (6 digit ??)
    def test_main(self):
        # noinspection PyBroadException
        try:
            list_of_input_files = glob.glob(
                '/opt/public/data/grids/hrrr/conus/wrfprs/grib2/*')
            latest_input_file = max(list_of_input_files, key=os.path.getctime)
            latest_input_file_time = os.path.getmtime(latest_input_file)
            first_epoch = round(latest_input_file_time) - 60
            last_epoch = round(latest_input_file_time) + 60
            cwd = os.getcwd()
            self.credentials_file = os.environ['HOME'] + '/adb-cb1-credentials'
            self.spec_file = cwd + '/grib2_to_cb/test/test_load_spec_grib_metar_hrrr_ops_V01.yaml'
            vxIngest = VXIngest()
            vxIngest.runit({'spec_file': self.spec_file,
                            'credentials_file': self.credentials_file,
                            'path': '/opt/public/data/grids/hrrr/conus/wrfprs/grib2',
                            'file_name_mask': '%y%j%H%f',
                            'output_dir': '/opt/data/grib_to_cb/output',
                            'threads': 1,
                            'number_stations': 3,  # only process 3 stations
                            'first_epoch': first_epoch,
                            'last_epoch': last_epoch
                            })
            _list_of_output_files = glob.glob('/opt/data/grib_to_cb/output/*')
            _latest_output_file = max(
                _list_of_output_files, key=os.path.getctime)
            # Opening JSON file
            _f = open(_latest_output_file)
            # returns JSON object as
            # a dictionary
            vxIngest_output_data = json.load(_f)
            # Closing file
            _f.close()
            _output_station_data = {}
            _expected_station_data = {}

            _f = open(self.credentials_file)
            _yaml_data = yaml.load(_f, yaml.SafeLoader)
            host = _yaml_data['cb_host']
            user = _yaml_data['cb_user']
            password = _yaml_data['cb_password']
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
                _station_name = i['name']
                result = self.cluster.query(
                    "SELECT mdata.geo.lat, mdata.geo.lon from mdata where type='MD' and docType='station' and subset='METAR' and version='V01' and mdata.name = $name",
                    name=_station_name)
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
                station['name'] = _station_name
                self.domain_stations.append(station)

            _expected_station_data['fcstValidEpoch'] = round(
                self.grbm.analDate.timestamp())
            self.assertEqual(_expected_station_data['fcstValidEpoch'], vxIngest_output_data[0]['fcstValidEpoch'],
                             "expected fcstValidEpoch and derived fcstValidEpoch are not the same")
            _expected_station_data['fcstValidBeg'] = self.grbm.analDate.isoformat(
            )
            self.assertEqual(_expected_station_data['fcstValidBeg'], vxIngest_output_data[0]['fcstValidBeg'],
                             "expected fcstValidBeg and derived fcstValidBeg are not the same")
            _expected_station_data['id'] = "DD-TEST:V01:METAR:HRRR_OPS:1626379200:" + str(
                self.grbm.forecastTime)
            self.assertEqual(_expected_station_data['id'], vxIngest_output_data[0]['id'],
                             "expected id and derived id are not the same")

            # Ceiling
            _message = self.grbs.select(name='Orography')[0]
            surface_hgt_values = _message['values']

            _message = self.grbs.select(name='Geopotential Height', typeOfFirstFixedSurface='215')[0]
            ceil_values = _message['values']

            for i in range(len(self.domain_stations)):
                station = self.domain_stations[i]
                surface = surface_hgt_values[round(station['y_gridpoint']), round(station['x_gridpoint'])]
                ceil_msl = ceil_values[round(station['y_gridpoint']), round(station['x_gridpoint'])]
                # Convert to ceiling AGL and from meters to tens of feet (what is currently inside SQL, we'll leave it as just feet in CB)
                ceil_agl = (ceil_msl - surface) * 0.32808

                # lazy initialization of _expected_station_data 
                if 'data' not in _expected_station_data.keys():
                    _expected_station_data['data'] = []
                if len(_expected_station_data['data']) <= i:
                    _expected_station_data['data'].append({})

                _expected_station_data['data'][i]['Ceiling'] = ceil_agl

            # Surface Pressure
            _message = self.grbs.select(name='Surface pressure')[0]
            _values = _message['values']
            for i in range(len(self.domain_stations)):
                station = self.domain_stations[i]
                _value = _values[round(station['y_gridpoint']), round(station['x_gridpoint'])]
                # interpolated gridpoints cannot be rounded
                _interpolated_value = gg.interpGridBox(_values, station['y_gridpoint'], station['x_gridpoint'])
                pres_mb = _interpolated_value * 100
                _expected_station_data['data'][i]['Surface Pressure'] = pres_mb

            # Temperature
            _message = self.grbs.select(name='2 metre temperature')[0]
            _values = _message['values']
            for i in range(len(self.domain_stations)):
                station = self.domain_stations[i]
                tempk = gg.interpGridBox(_values, station['y_gridpoint'], station['x_gridpoint'])
                tempf = ((tempk-273.15)*9)/5 + 32
                _expected_station_data['data'][i]['Temperature'] = tempf

            # Dewpoint
            _message = self.grbs.select(name='2 metre dewpoint temperature')[0]
            _values = _message['values']
            for i in range(len(self.domain_stations)):
                station = self.domain_stations[i]
                dpk = gg.interpGridBox(_values, station['y_gridpoint'], station['x_gridpoint'])
                dpf = ((dpk-273.15)*9)/5 + 32
                _expected_station_data['data'][i]['DewPoint'] = dpf

            # Relative Humidity
            _message = self.grbs.select(name='2 metre relative humidity')[0]
            _values = _message['values']
            for i in range(len(self.domain_stations)):
                station = self.domain_stations[i]
                rh = gg.interpGridBox(_values, station['y_gridpoint'], station['x_gridpoint'])
                _expected_station_data['data'][i]['RH'] = rh

            # Wind Speed
            _message = self.grbs.select(name='10 metre U wind component')[0]
            _uwind_values = _message['values']

            _vwind_message = self.grbs.select(name='10 metre V wind component')[0]
            _vwind_values = _vwind_message['values']

            for i in range(len(self.domain_stations)):
                station = self.domain_stations[i]
                uwind_ms = gg.interpGridBox(_uwind_values, station['y_gridpoint'], station['x_gridpoint'])
                vwind_ms = gg.interpGridBox(_vwind_values, station['y_gridpoint'], station['x_gridpoint'])
                # Convert from U-V components to speed and direction (requires rotation if grid is not earth relative)
                # wind speed then convert to mph
                ws_ms = math.sqrt((uwind_ms*uwind_ms)+(vwind_ms*vwind_ms))
                ws_mph = (ws_ms/0.447) + 0.5
                _expected_station_data['data'][i]['WS'] = ws_mph

                # wind direction   - lon is the lon of the station
                station = self.domain_stations[i]
                theta = gg.getWindTheta(_vwind_message, station['lon'])
                radians = math.atan2(uwind_ms, vwind_ms)
                wd = (radians*57.2958) + theta + 180
                _expected_station_data['data'][i]['WD'] = wd

            # Visibility
            _message = self.grbs.select(name='Visibility')[0]
            _values = _message['values']
            for i in range(len(self.domain_stations)):
                station = self.domain_stations[i]
                _value = _values[round(station['y_gridpoint']), round(station['x_gridpoint'])]
                _expected_station_data['data'][i]['Visibility'] = _value
            self.grbs.close()

            for i in range(len(self.domain_stations)):
                self.assertAlmostEqual(_expected_station_data['data'][i]['Ceiling'],
                                       vxIngest_output_data[0]['data'][i]['Ceiling'], msg="Expected Ceiling and derived Ceiling are not equal")

                self.assertAlmostEqual(_expected_station_data['data'][i]['Surface Pressure'],
                                       vxIngest_output_data[0]['data'][i]['Surface Pressure'], msg="Expected Surface Pressure and derived Surface Pressure are not equal")

                self.assertAlmostEqual(_expected_station_data['data'][i]['Temperature'],
                                       vxIngest_output_data[0]['data'][i]['Temperature'], msg="Expected Temperature and derived Temperature are not equal")

                self.assertAlmostEqual(_expected_station_data['data'][i]['DewPoint'],
                                       vxIngest_output_data[0]['data'][i]['DewPoint'], msg="Expected DewPoint and derived DewPoint are not equal")

                self.assertAlmostEqual(_expected_station_data['data'][i]['RH'],
                                       vxIngest_output_data[0]['data'][i]['RH'], msg="Expected RH and derived RH are not equal")

                self.assertAlmostEqual(_expected_station_data['data'][i]['WS'],
                                       vxIngest_output_data[0]['data'][i]['WS'], msg="Expected WS and derived WS are not equal")

                self.assertAlmostEqual(_expected_station_data['data'][i]['WD'],
                                       vxIngest_output_data[0]['data'][i]['WD'], msg="Expected WD and derived WD are not equal")

                self.assertAlmostEqual(_expected_station_data['data'][i]['Visibility'],
                                       vxIngest_output_data[0]['data'][i]['Visibility'], msg="Expected Visibility and derived Visibility are not equal")

        except:
            self.fail("TestGribBuilderV01 Exception failure: " +
                      str(sys.exc_info()[0]))
