import sys
import os
import shutil
from glob import glob
import yaml
import pymysql
import numpy as np
import netCDF4 as nc
from pymysql.constants import CLIENT
from unittest import TestCase
from netcdf_to_cb.run_ingest_threads import VXIngest
from pathlib import Path
from couchbase.cluster import Cluster, ClusterOptions
from couchbase_core.cluster import PasswordAuthenticator
from netcdf_to_cb.load_spec_yaml import LoadYamlSpecFile
from netcdf_to_cb.netcdf_builder import NetcdfMetarObsBuilderV01
from datetime import datetime

class TestNetcdfObsBuilderV01Unit(TestCase):

    def setup_mysql_connection(self):
        _credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
        _f = open(_credentials_file)
        _yaml_data = yaml.load(_f, yaml.SafeLoader)
        _f.close()
        host = _yaml_data["mysql_host"]
        user = _yaml_data["mysql_user"]
        passwd = _yaml_data["mysql_password"]
        connection = pymysql.connect(
            host=host,
            user=user,
            passwd=passwd,
            local_infile=True,
            autocommit=True,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.SSDictCursor,
            client_flag=CLIENT.MULTI_STATEMENTS,
        )
        _cursor = connection.cursor(pymysql.cursors.SSDictCursor)
        return _cursor

    def setup_connection(self):
        """test setup
        """
        try:
            cwd = os.getcwd()
            _vx_ingest = VXIngest()
            _vx_ingest.spec_file = cwd + "/netcdf_to_cb/test/test_load_spec_netcdf_metar_obs_V01.yaml"
            _vx_ingest.credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
            _vx_ingest.cb_credentials = _vx_ingest.get_credentials(_vx_ingest.load_spec)
            _load_spec_file = LoadYamlSpecFile({"spec_file": _vx_ingest.spec_file})
            # read in the load_spec file
            _vx_ingest.load_spec = dict(_load_spec_file.read())
            _vx_ingest.connect_cb()
            return _vx_ingest
        except Exception as _e: #pylint:disable=broad-except
            self.fail("test_credentials_and_load_spec Exception failure: " + str(_e))

    def test_credentials_and_load_spec(self):
        """test the get_credentials and load_spec
        """
        try:
            vx_ingest = self.setup_connection()
            self.assertTrue(vx_ingest.load_spec['cb_connection']['user'], "cb_user")
        except Exception as _e: #pylint:disable=broad-except
            self.fail("test_credentials_and_load_spec Exception failure: " + str(_e))
        finally:
            vx_ingest.close_cb()

    def test_cb_connect_disconnect(self):
        """test the cb connect and close
        """
        try:
            vx_ingest = self.setup_connection()
            result = vx_ingest.cluster.query("SELECT raw CLOCK_LOCAL() as time")
            local_time = [list(result)[0]]
            self.assertTrue(local_time is not None)
            vx_ingest.close_cb()
        except Exception as _e: #pylint:disable=broad-except
            self.fail("test_cb_connect_disconnect Exception failure: " + str(_e))
        finally:
            vx_ingest.close_cb()

    def test_compare_stations_to_mysql(self):
        """test are couchbase stations the same as mysql
        """
        try:
            vx_ingest = self.setup_connection()
            cluster = vx_ingest.cluster
            result = cluster.query("""SELECT name, geo.lat lat, geo.lon lon
                FROM mdata
                WHERE
                    docType='station'
                    AND type='MD'
                    AND version='V01'
                    AND subset='METAR'""")
            cb_station_list = list(result)
            cb_stations = {x['name']: x for x in cb_station_list}
            cursor = self.setup_mysql_connection()
            cursor.execute("""select s.name, l.lat / 182 as lat, l.lon / 182 as lon
                from
                madis3.stations as s,
                madis3.locations as l,
                madis3.obs as o
                where
                1 = 1
                and o.time >= 1641168000 - 1800
                and o.time < 1641168000 + 1800
                and s.id = o.sta_id
                and o.loc_id = l.id
                and s.net = 'METAR'
                ORDER BY s.name;""")
            mysql_station_list = cursor.fetchall()
            mysql_stations = {x['name']: x for x in mysql_station_list}
            cb_station_names = cb_stations.keys()
            mysql_station_names = mysql_stations.keys()
            common_station_names = [value for value in cb_station_names if value in mysql_station_names]
            for station_name in common_station_names:
                self.assertAlmostEqual(cb_stations[station_name]['lat'],float(mysql_stations[station_name]['lat']),2,"mysql lat does not equal cb lat for station {s}".format(s=station_name))
                self.assertAlmostEqual(cb_stations[station_name]['lon'],float(mysql_stations[station_name]['lon']),2,"mysql lon does not equal cb lon for station {s}".format(s=station_name))
        except Exception as _e: #pylint:disable=broad-except
            self.fail("test_compare_stations_to_mysql Exception failure: " + str(_e))
        finally:
            vx_ingest.close_cb()

    def test_write_load_job_to_files(self):
        """test write the load job
        """
        try:
            vx_ingest = self.setup_connection()
            vx_ingest.load_job_id = "test_id"
            vx_ingest.output_dir = "/tmp"
            vx_ingest.load_spec["load_job_doc"] = {"test":"a line of text"}
            vx_ingest.write_load_job_to_files()
            os.remove("/tmp/test_id.json")
        except Exception as _e: #pylint:disable=broad-except
            self.fail("test_write_load_job_to_files Exception failure: " + str(_e))
        finally:
            vx_ingest.close_cb()

    def test_build_load_job_doc(self):
        """test the build load job
        """
        try:
            vx_ingest = self.setup_connection()
            vx_ingest.load_job_id = "test_id"
            vx_ingest.path = "/tmp"
            vx_ingest.load_spec["load_job_doc"] = {"test":"a line of text"}
            vx_ingest.spec_file = "/tmp/test_file"
            ljd = vx_ingest.build_load_job_doc()
            self.assertTrue(ljd['id'].startswith('LJ:netcdf_to_cb.run_ingest_threads:VXIngest'))
        except Exception as _e: #pylint:disable=broad-except
            self.fail("test_build_load_job_doc Exception failure: " + str(_e))
        finally:
            vx_ingest.close_cb()

    def test_umask_value_transform(self):
        """test the derive_valid_time_epoch
        requires self.file_name which should match the format for grib2 hrr_ops files
        i.e. "20210920_1700", and params_dict['file_name_mask'] = "%Y%m%d_%H%M"
        """
        try:
            # first we have to create a netcdf dataset and a temperature variable
            _nc = nc.Dataset('inmemory.nc', format="NETCDF3_CLASSIC", mode='w',memory=1028,fill_value=3.402823e+38) #pylint:disable=no-member
            _d = _nc.createDimension('recNum',None)
            """	float temperature(recNum) ;
        		temperature:long_name = "temperature" ;
                temperature:units = "kelvin" ;
                temperature:_FillValue = 3.402823e+38f ;
                temperature:standard_name = "air_temperature" ;
`            """
            _v = _nc.createVariable('temperature',np.float,("recNum"))
            _v.units = "kelvin"
            _v.standard_name = "air_temperature"
            _v[0] = 250.15

            vx_ingest = self.setup_connection()
            cluster = vx_ingest.cluster
            collection = vx_ingest.collection
            load_spec = vx_ingest.load_spec
            ingest_document_id = vx_ingest.load_spec["ingest_document_id"]
            ingest_document = collection.get(ingest_document_id).content
            builder = NetcdfMetarObsBuilderV01(load_spec, ingest_document, cluster, collection)
            builder.file_name = "20210920_1700"
            # assign our temporary in-memory dataset to the builder
            builder.ncdf_data_set = _nc
            #assign our handler parameters
            params_dict = {}
            params_dict["recNum"] = 0
            params_dict['temperature'] = "temperature"
            # call the handler
            temp = builder.umask_value_transform(params_dict)
            self.assertTrue(temp == 250.15)
        except Exception as _e: #pylint:disable=broad-except
            self.fail("test_build_load_job_doc Exception failure: " + str(_e))
        finally:
            vx_ingest.close_cb()
            _nc.close() # close returns memoryview

    def test_vxingest_get_file_list(self):
        """test the vxingest get_file_list
        """
        try:
            vx_ingest = self.setup_connection()
            vx_ingest.load_job_id = "test_id"
            if os.path.exists("/tmp/test"):
                shutil.rmtree("/tmp/test")
            os.mkdir("/tmp/test")
            # order is important to see if the files are getting returned sorted by mtime
            Path('/tmp/test/f_fred_01').touch()
            Path('/tmp/test/f_fred_02').touch()
            Path('/tmp/test/f_fred_04').touch()
            Path('/tmp/test/f_fred_05').touch()
            Path('/tmp/test/f_fred_03').touch()
            Path('/tmp/test/f_1_fred_01').touch()
            Path('/tmp/test/f_2_fred_01').touch()
            Path('/tmp/test/f_3_fred_01').touch()
            query = """ SELECT url, mtime
                FROM mdata
                WHERE
                subset='metar'
                AND type='DF'
                AND fileType='grib2'
                AND originType='model'
                AND model='HRRR_OPS' order by url;"""
            files = vx_ingest.get_file_list(query,"/tmp/test","f_fred_*")
            self.assertListEqual(files,['/tmp/test/f_fred_01','/tmp/test/f_fred_02','/tmp/test/f_fred_04','/tmp/test/f_fred_05','/tmp/test/f_fred_03'], "get_file_list wrong list")
        except Exception as _e: #pylint:disable=broad-except
            self.fail("test_build_load_job_doc Exception failure: " + str(_e))
        finally:
            shutil.rmtree("/tmp/test")
            vx_ingest.close_cb()

    def test_interpolate_time(self):
        """test the interpolate time routine in netcdf_builder
        """
        try:
            vx_ingest = self.setup_connection()
            _cluster = vx_ingest.cluster
            _collection = vx_ingest.collection
            _load_spec = vx_ingest.load_spec
            _ingest_document_id = vx_ingest.load_spec["ingest_document_id"]
            _ingest_document = _collection.get(_ingest_document_id).content
            _builder = NetcdfMetarObsBuilderV01(_load_spec, _ingest_document, _cluster, _collection)
            for delta in [0, -1, 1, -1799, 1799, -1800, 1800, -1801, 1801, -3599, 3599, -3600, 3600, -3601, 3601]:
                _t = np.array([1636390800 + delta])
                _t.view(np.ma.MaskedArray)
                t_interpolated = _builder.interpolate_time({"timeObs":_t})
                print ("for an offset: " + str(delta) + " results in interpolation: " + str(t_interpolated))
                if delta >= -1800 and delta <= 1799:
                    self.assertEqual(1636390800,t_interpolated,"{t} interpolated to {it} is not equal".format(t=1636390800 - delta, it=t_interpolated))
                if delta <= -1801:
                    self.assertEqual(1636390800 - 3600,t_interpolated,"{t} interpolated to {it} is not equal".format(t=1636390800 - delta, it=t_interpolated))
                if delta >= 1800:
                    self.assertEqual(1636390800 + 3600,t_interpolated,"{t} interpolated to {it} is not equal".format(t=1636390800 - delta, it=t_interpolated))
        except Exception as _e: #pylint:disable=broad-except
            self.fail("test_interpolate_time Exception failure: " + str(_e))

    def test_interpolate_time_iso(self):
        """test the interpolate time routine in netcdf_builder
        """
        try:
            vx_ingest = self.setup_connection()
            _cluster = vx_ingest.cluster
            _collection = vx_ingest.collection
            load_spec = vx_ingest.load_spec
            ingest_document_id = vx_ingest.load_spec["ingest_document_id"]
            ingest_document = _collection.get(ingest_document_id).content
            _builder = NetcdfMetarObsBuilderV01(load_spec, ingest_document, _cluster, _collection)
            for delta in [0, -1, 1, -1799, 1799, -1800, 1800, -1801, 1801, -3599, 3599, -3600, 3600, -3601, 3601]:
                _t = np.array([1636390800 + delta])
                _t.view(np.ma.MaskedArray)
                t_interpolated = _builder.interpolate_time_iso({"timeObs":_t})
                if delta >= -1800 and delta <= 1799:
                    self.assertEqual((datetime.utcfromtimestamp(1636390800).isoformat()),t_interpolated,"{t} interpolated to {it} is not equal".format(t=1636390800 - delta, it=t_interpolated))
                if delta <= -1801:
                    self.assertEqual((datetime.utcfromtimestamp(1636390800 - 3600).isoformat()),t_interpolated,"{t} interpolated to {it} is not equal".format(t=1636390800 - delta, it=t_interpolated))
                if delta >= 1800:
                    self.assertEqual((datetime.utcfromtimestamp(1636390800 + 3600).isoformat()),t_interpolated,"{t} interpolated to {it} is not equal".format(t=1636390800 - delta, it=t_interpolated))
        except Exception as _e: #pylint:disable=broad-except
            self.fail("test_interpolate_time_iso Exception failure: " + str(_e))

    def test_derive_valid_time_epoch(self):
        """test the derive_valid_time_epoch routine in netcdf_builder
        """
        try:
            vx_ingest = self.setup_connection()
            _cluster = vx_ingest.cluster
            _collection = vx_ingest.collection
            load_spec = vx_ingest.load_spec
            ingest_document_id = vx_ingest.load_spec["ingest_document_id"]
            ingest_document = _collection.get(ingest_document_id).content
            _builder = NetcdfMetarObsBuilderV01(load_spec, ingest_document, _cluster, _collection)
            _builder.file_name = "20211108_0000"
            _pattern = "%Y%m%d_%H%M"
            _file_utc_time = datetime.strptime(_builder.file_name, _pattern)
            expected_epoch = (_file_utc_time - datetime(1970, 1, 1)).total_seconds()
            derived_epoch = _builder.derive_valid_time_epoch({"file_name_pattern":_pattern})
            self.assertEqual(expected_epoch,derived_epoch,"derived epoch {de} is not equal to 1636329600".format(de=derived_epoch))
        except Exception as _e: #pylint:disable=broad-except
            self.fail("test_derive_valid_time_epoch Exception failure: " + str(_e))

    def test_derive_valid_time_iso(self):
        """test the derive_valid_time_iso routine in netcdf_builder
        """
        try:
            vx_ingest = self.setup_connection()
            _cluster = vx_ingest.cluster
            _collection = vx_ingest.collection
            load_spec = vx_ingest.load_spec
            ingest_document_id = vx_ingest.load_spec["ingest_document_id"]
            ingest_document = _collection.get(ingest_document_id).content
            _builder = NetcdfMetarObsBuilderV01(load_spec, ingest_document, _cluster, _collection)
            _builder.file_name = "20211108_0000"
            derived_epoch = _builder.derive_valid_time_iso({"file_name_pattern":"%Y%m%d_%H%M"})
            self.assertEqual("2021-11-08T00:00:00Z",derived_epoch,"derived epoch {de} is not equal to 1636390800".format(de=derived_epoch))
        except Exception as _e: #pylint:disable=broad-except
            self.fail("test_derive_valid_time_epoch Exception failure: " + str(_e))
