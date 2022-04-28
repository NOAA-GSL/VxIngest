from copy import deepcopy
import sys
import os
import shutil
from glob import glob
import yaml
import pymysql
import numpy as np
import netCDF4 as nc
import geopy.distance as geopyd
from pymysql.constants import CLIENT
from unittest import TestCase
from netcdf_to_cb.run_ingest_threads import VXIngest
from pathlib import Path
from couchbase.cluster import Cluster, ClusterOptions, QueryOptions, QueryScanConsistency
from couchbase.collection import RemoveOptions
from couchbase_core.durability import Durability
from couchbase.durability import ServerDurability
from couchbase_core.cluster import PasswordAuthenticator
from builder_common.load_spec_yaml import LoadYamlSpecFile
from netcdf_to_cb.netcdf_builder import NetcdfMetarObsBuilderV01
from datetime import datetime


class TestNetcdfObsBuilderV01Unit(TestCase):
    """various unit tests for the obs builder.
        to run one of these from the command line....
        python3 -m pytest -s -v  netcdf_to_cb/test/test_unit_metar_obs_netcdf.py::TestNetcdfObsBuilderV01Unit::test....

    Args:
        TestCase (): [description]
    """

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
        """test setup"""
        try:
            cwd = os.getcwd()
            _vx_ingest = VXIngest()
            _vx_ingest.spec_file = (
                cwd + "/netcdf_to_cb/test/test_load_spec_netcdf_metar_obs_V01.yaml"
            )
            _vx_ingest.credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
            _vx_ingest.cb_credentials = _vx_ingest.get_credentials(_vx_ingest.load_spec)
            _load_spec_file = LoadYamlSpecFile({"spec_file": _vx_ingest.spec_file})
            # read in the load_spec file
            _vx_ingest.load_spec = dict(_load_spec_file.read())
            _vx_ingest.connect_cb()
            return _vx_ingest
        except Exception as _e:  # pylint:disable=broad-except
            self.fail("test_credentials_and_load_spec Exception failure: " + str(_e))

    def test_credentials_and_load_spec(self):
        """test the get_credentials and load_spec"""
        try:
            vx_ingest = self.setup_connection()
            self.assertTrue(vx_ingest.load_spec["cb_connection"]["user"], "cb_user")
        except Exception as _e:  # pylint:disable=broad-except
            self.fail("test_credentials_and_load_spec Exception failure: " + str(_e))
        finally:
            vx_ingest.close_cb()

    def test_cb_connect_disconnect(self):
        """test the cb connect and close"""
        try:
            vx_ingest = self.setup_connection()
            result = vx_ingest.cluster.query("SELECT raw CLOCK_LOCAL() as time")
            local_time = [list(result)[0]]
            self.assertTrue(local_time is not None)
            vx_ingest.close_cb()
        except Exception as _e:  # pylint:disable=broad-except
            self.fail("test_cb_connect_disconnect Exception failure: " + str(_e))
        finally:
            vx_ingest.close_cb()

    def test_compare_stations_to_mysql(self):
        """test are couchbase stations the same as mysql.
        useful .... awk 'NF > 20 {print $(NF-5), $(NF-1), $(NF)}' ~/stations_comare.txt | sort | uniq
        """
        try:
            vx_ingest = self.setup_connection()
            cluster = vx_ingest.cluster
            result = cluster.query(
                """SELECT name, geo
                FROM mdata
                WHERE
                    docType='station'
                    AND type='MD'
                    AND version='V01'
                    AND subset='METAR'"""
            )
            cb_station_list = list(result)
            cb_stations = {}
            for s in cb_station_list:
                cb_stations[s["name"]] = {"name":s["name"],'lat':s['geo'][0]['lat'],'lon':s['geo'][0]['lon']}

            cursor = self.setup_mysql_connection()
            cursor.execute(
                """select s.name, l.lat / 182 as lat, l.lon / 182 as lon
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
                ORDER BY s.name;"""
            )
            mysql_station_list = cursor.fetchall()
            mysql_stations = {x["name"]: x for x in mysql_station_list}
            cb_station_names = cb_stations.keys()
            mysql_station_names = mysql_stations.keys()
            common_station_names = [
                value for value in cb_station_names if value in mysql_station_names
            ]
            for station_name in common_station_names:
                coords_1 = (
                    cb_stations[station_name]["lat"],
                    cb_stations[station_name]["lon"]
                )
                coords_2 = (
                    mysql_stations[station_name]["lat"],
                    mysql_stations[station_name]["lon"]
                )
                distance = geopyd.distance(coords_1, coords_2).km
                try:
                    self.assertAlmostEqual(
                        cb_stations[station_name]["lat"],
                        float(mysql_stations[station_name]["lat"]),
                        4,
                        "cb lat {c} does not equal mysql lat {m} for station {s} distance offset is {d} km".format(
                            c=str(cb_stations[station_name]["lat"]),
                            m=str(mysql_stations[station_name]["lat"]),
                            s=station_name,
                            d=distance,
                        ),
                    )
                except Exception as e1:
                    print("test_compare_stations_to_mysql lat failure: " + str(e1))
                try:
                    self.assertAlmostEqual(
                        cb_stations[station_name]["lon"],
                        float(mysql_stations[station_name]["lon"]),
                        4,
                        "cb lon {c} does not equal mysql lon {m} for station {s} distance offset is {d} km".format(
                            c=str(cb_stations[station_name]["lon"]),
                            m=str(mysql_stations[station_name]["lon"]),
                            s=station_name,
                            d=distance,
                        ),
                    )
                except Exception as e1:
                    print("test_compare_stations_to_mysql lon failure: " + str(e1))
        except Exception as _e:  # pylint:disable=broad-except
            self.fail("test_compare_stations_to_mysql Exception failure: " + str(_e))
        finally:
            vx_ingest.close_cb()

    def test_write_load_job_to_files(self):
        """test write the load job"""
        try:
            vx_ingest = self.setup_connection()
            vx_ingest.load_job_id = "test_id"
            vx_ingest.output_dir = "/tmp"
            vx_ingest.load_spec["load_job_doc"] = {"test": "a line of text"}
            vx_ingest.write_load_job_to_files()
            os.remove("/tmp/test_id.json")
        except Exception as _e:  # pylint:disable=broad-except
            self.fail("test_write_load_job_to_files Exception failure: " + str(_e))
        finally:
            vx_ingest.close_cb()

    def test_build_load_job_doc(self):
        """test the build load job"""
        try:
            vx_ingest = self.setup_connection()
            vx_ingest.load_job_id = "test_id"
            vx_ingest.path = "/tmp"
            vx_ingest.load_spec["load_job_doc"] = {"test": "a line of text"}
            vx_ingest.spec_file = "/tmp/test_file"
            ljd = vx_ingest.build_load_job_doc()
            self.assertTrue(
                ljd["id"].startswith("LJ:netcdf_to_cb.run_ingest_threads:VXIngest")
            )
        except Exception as _e:  # pylint:disable=broad-except
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
            _nc = nc.Dataset(
                "inmemory.nc",
                format="NETCDF3_CLASSIC",
                mode="w",
                memory=1028,
                fill_value=3.402823e38,
            )  # pylint:disable=no-member
            _d = _nc.createDimension("recNum", None)
            """	float temperature(recNum) ;
        		temperature:long_name = "temperature" ;
                temperature:units = "kelvin" ;
                temperature:_FillValue = 3.402823e+38f ;
                temperature:standard_name = "air_temperature" ;
`            """
            _v = _nc.createVariable("temperature", np.float, ("recNum"))
            _v.units = "kelvin"
            _v.standard_name = "air_temperature"
            _v[0] = 250.15

            vx_ingest = self.setup_connection()
            cluster = vx_ingest.cluster
            collection = vx_ingest.collection
            load_spec = vx_ingest.load_spec
            ingest_document_id = vx_ingest.load_spec["ingest_document_id"]
            ingest_document = collection.get(ingest_document_id).content
            builder = NetcdfMetarObsBuilderV01(
                load_spec, ingest_document, cluster, collection
            )
            builder.file_name = "20210920_1700"
            # assign our temporary in-memory dataset to the builder
            builder.ncdf_data_set = _nc
            # assign our handler parameters
            params_dict = {}
            params_dict["recNum"] = 0
            params_dict["temperature"] = "temperature"
            # call the handler
            temp = builder.umask_value_transform(params_dict)
            self.assertTrue(temp == 250.15)
        except Exception as _e:  # pylint:disable=broad-except
            self.fail("test_build_load_job_doc Exception failure: " + str(_e))
        finally:
            vx_ingest.close_cb()
            _nc.close()  # close returns memoryview

    def test_vxingest_get_file_list(self):
        """test the vxingest get_file_list"""
        try:
            vx_ingest = self.setup_connection()
            vx_ingest.load_job_id = "test_id"
            if os.path.exists("/tmp/test"):
                shutil.rmtree("/tmp/test")
            os.mkdir("/tmp/test")
            # order is important to see if the files are getting returned sorted by mtime
            Path("/tmp/test/f_fred_01").touch()
            Path("/tmp/test/f_fred_02").touch()
            Path("/tmp/test/f_fred_04").touch()
            Path("/tmp/test/f_fred_05").touch()
            Path("/tmp/test/f_fred_03").touch()
            Path("/tmp/test/f_1_fred_01").touch()
            Path("/tmp/test/f_2_fred_01").touch()
            Path("/tmp/test/f_3_fred_01").touch()
            query = """ SELECT url, mtime
                FROM mdata
                WHERE
                subset='metar'
                AND type='DF'
                AND fileType='grib2'
                AND originType='model'
                AND model='HRRR_OPS' order by url;"""
            files = vx_ingest.get_file_list(query, "/tmp/test", "f_fred_*")
            self.assertListEqual(
                files,
                [
                    "/tmp/test/f_fred_01",
                    "/tmp/test/f_fred_02",
                    "/tmp/test/f_fred_04",
                    "/tmp/test/f_fred_05",
                    "/tmp/test/f_fred_03",
                ],
                "get_file_list wrong list",
            )
        except Exception as _e:  # pylint:disable=broad-except
            self.fail("test_build_load_job_doc Exception failure: " + str(_e))
        finally:
            shutil.rmtree("/tmp/test")
            vx_ingest.close_cb()

    def test_interpolate_time(self):
        """test the interpolate time routine in netcdf_builder"""
        try:
            vx_ingest = self.setup_connection()
            _cluster = vx_ingest.cluster
            _collection = vx_ingest.collection
            _load_spec = vx_ingest.load_spec
            _ingest_document_id = vx_ingest.load_spec["ingest_document_id"]
            _ingest_document = _collection.get(_ingest_document_id).content
            _builder = NetcdfMetarObsBuilderV01(
                _load_spec, _ingest_document, _cluster, _collection
            )
            for delta in [
                0,
                -1,
                1,
                -1799,
                1799,
                -1800,
                1800,
                -1801,
                1801,
                -3599,
                3599,
                -3600,
                3600,
                -3601,
                3601,
            ]:
                _t = np.array([1636390800 + delta])
                _t.view(np.ma.MaskedArray)
                t_interpolated = _builder.interpolate_time({"timeObs": _t})
                print(
                    "for an offset: "
                    + str(delta)
                    + " results in interpolation: "
                    + str(t_interpolated)
                )
                if delta >= -1800 and delta <= 1799:
                    self.assertEqual(
                        1636390800,
                        t_interpolated,
                        "{t} interpolated to {it} is not equal".format(
                            t=1636390800 - delta, it=t_interpolated
                        ),
                    )
                if delta <= -1801:
                    self.assertEqual(
                        1636390800 - 3600,
                        t_interpolated,
                        "{t} interpolated to {it} is not equal".format(
                            t=1636390800 - delta, it=t_interpolated
                        ),
                    )
                if delta >= 1800:
                    self.assertEqual(
                        1636390800 + 3600,
                        t_interpolated,
                        "{t} interpolated to {it} is not equal".format(
                            t=1636390800 - delta, it=t_interpolated
                        ),
                    )
        except Exception as _e:  # pylint:disable=broad-except
            self.fail("test_interpolate_time Exception failure: " + str(_e))

    def test_interpolate_time_iso(self):
        """test the interpolate time routine in netcdf_builder"""
        try:
            vx_ingest = self.setup_connection()
            _cluster = vx_ingest.cluster
            _collection = vx_ingest.collection
            load_spec = vx_ingest.load_spec
            ingest_document_id = vx_ingest.load_spec["ingest_document_id"]
            ingest_document = _collection.get(ingest_document_id).content
            _builder = NetcdfMetarObsBuilderV01(
                load_spec, ingest_document, _cluster, _collection
            )
            for delta in [
                0,
                -1,
                1,
                -1799,
                1799,
                -1800,
                1800,
                -1801,
                1801,
                -3599,
                3599,
                -3600,
                3600,
                -3601,
                3601,
            ]:
                _t = np.array([1636390800 + delta])
                _t.view(np.ma.MaskedArray)
                t_interpolated = _builder.interpolate_time_iso({"timeObs": _t})
                if delta >= -1800 and delta <= 1799:
                    self.assertEqual(
                        (datetime.utcfromtimestamp(1636390800).isoformat()),
                        t_interpolated,
                        "{t} interpolated to {it} is not equal".format(
                            t=1636390800 - delta, it=t_interpolated
                        ),
                    )
                if delta <= -1801:
                    self.assertEqual(
                        (datetime.utcfromtimestamp(1636390800 - 3600).isoformat()),
                        t_interpolated,
                        "{t} interpolated to {it} is not equal".format(
                            t=1636390800 - delta, it=t_interpolated
                        ),
                    )
                if delta >= 1800:
                    self.assertEqual(
                        (datetime.utcfromtimestamp(1636390800 + 3600).isoformat()),
                        t_interpolated,
                        "{t} interpolated to {it} is not equal".format(
                            t=1636390800 - delta, it=t_interpolated
                        ),
                    )
        except Exception as _e:  # pylint:disable=broad-except
            self.fail("test_interpolate_time_iso Exception failure: " + str(_e))

    def test_handle_station(self):
        """Tests the ability to add or update a station with these possibilities...
        1) The station is new and there is no station document that yet exists so
        a new station document must be created.
        2) The corresponding station document exists but the location has changed so a
        new geo must be created.
        3) The corresponding station document exists and a matching geo exists for this
        station but the geo time range does not include the document validTime so the
        geo entry must be updated for the new time range.
        """
        try:
            _station_name = "ZBAA"
            vx_ingest = self.setup_connection()
            _cluster = vx_ingest.cluster
            _collection = vx_ingest.collection
            load_spec = vx_ingest.load_spec
            ingest_document_id = vx_ingest.load_spec["ingest_document_id"]
            ingest_document = _collection.get(ingest_document_id).content
            _builder = NetcdfMetarObsBuilderV01(
                load_spec, ingest_document, _cluster, _collection
            )
            _builder.file_name = "20211108_0000"
            _pattern = "%Y%m%d_%H%M"
            # fmask is usually set in the run_ingest_threads
            _builder.load_spec['fmask'] = _pattern
            _builder.ncdf_data_set = nc.Dataset(
                "/opt/data/netcdf_to_cb/input_files/20211108_0000"
            )
            rec_num_length = _builder.ncdf_data_set["stationName"].shape[0]
            # find the rec_num of the stationName ZBAA
            for i in range(rec_num_length):
                if "ZBAA" == str(nc.chartostring(_builder.ncdf_data_set["stationName"][i])):
                    break
            _rec_num = i
            # use a station that is in the netcdf file but is not used in any of our domains.
            # like Beijing China ZBAA.
            result = _cluster.query(
                " ".join(("""
                SELECT mdata.*
                FROM mdata
                WHERE mdata.type = 'MD'
                AND mdata.docType = 'station'
                AND mdata.version = 'V01'
                AND name = '""" + _station_name + "'").split()),
                QueryOptions(scan_consistency=QueryScanConsistency.REQUEST_PLUS)
            )
            if len(list(result)) > 0:
                station_zbaa = list(result)[0]
            else:
                station_zbaa = None
            # keep a copy of station_zbaa around for future use
            station_zbaa_copy = deepcopy(station_zbaa)
            if station_zbaa_copy is not None:
                self.cleanup_builder_doc(_cluster, _collection, _builder, station_zbaa_copy)

            # ****************
            # 1) new station test
            # remove station station_zbaa from the database
            self.remove_station_zbaa(_station_name, _cluster, _collection, station_zbaa)
            # initialize builder with missing station_zbaa
            self.setup_builder_doc(_cluster, _collection, _builder)
            # handle_station should give us a new station_zbaa
            _builder.handle_station({"recNum": _rec_num, "stationName": _station_name})
            doc_map = _builder.get_document_map()
            _collection.upsert_multi(doc_map)
            #assert for new station_zbaa
            self.assert_station(_cluster, station_zbaa_copy)
            self.cleanup_builder_doc(_cluster, _collection, _builder, station_zbaa_copy)

            # ****************
            # 2) changed location test
            new_station_zbaa = deepcopy(station_zbaa_copy)
            # add 1 to the existing lat for geo[0] and upsert the modified station_zbaa
            new_station_zbaa["geo"][0]["lat"] = station_zbaa['geo'][0]['lat'] + 1
            new_doc = {station_zbaa['id']:new_station_zbaa}
            _collection.upsert_multi(new_doc)
            # handle_station should see that the existing station_zbaa has a different
            # geo[0]['lat'] and make a new geo[1]['lat'] with the netcdf original lat
            # populate the builder list with the modified station by seting up
            self.setup_builder_doc(_cluster, _collection, _builder)
            _builder.handle_station({"recNum": _rec_num, "stationName": _station_name})
            doc_map = _builder.get_document_map()
            _collection.upsert_multi(doc_map)
            # station ZBAA should now have 2 geo entries
            self.assertTrue(len(doc_map["MD:V01:METAR:station:ZBAA"]["geo"]) == 2,msg="new station ZBAA['geo'] does not have 2 elements")
            #modify the station_zbaa to reflect what handle_station should have done
            station_zbaa['geo'][0]['lat'] = 41.06999
            station_zbaa['geo'].append({
                    "elev": 30,
                    "firstTime": doc_map["MD:V01:METAR:station:ZBAA"]["geo"][0]['firstTime'],
                    "lastTime": doc_map["MD:V01:METAR:station:ZBAA"]["geo"][0]['lastTime'],
                    "lat": 40.06999,
                    "lon": 116.58
            })
            self.assert_station(_cluster, station_zbaa)
            self.cleanup_builder_doc(_cluster, _collection, _builder, station_zbaa_copy)

            # ****************
            # 3) update time range test
            new_station_zbaa = deepcopy(station_zbaa_copy)
            # save the original firstTime
            orig_first_time = new_station_zbaa["geo"][0]["firstTime"]
            # add some time to the firstTime and lastTime of new_station_zbaa
            new_station_zbaa["geo"][0]["firstTime"] = station_zbaa['geo'][0]['firstTime'] + 2 *_builder.cadence
            new_station_zbaa["geo"][0]["lastTime"] = station_zbaa['geo'][0]['lastTime'] + 2 *_builder.cadence
            new_doc = {new_station_zbaa['id']:new_station_zbaa}
            _collection.upsert_multi(new_doc)
            # populate the builder list with the modified station by seting up
            self.setup_builder_doc(_cluster, _collection, _builder)
            # handle station should see that the real station_zbaa doesn't fit within
            # the existing timeframe of geo[0] and modify the geo element with the
            # original firstTime (matches the fcstValidEpoch of the file)
            _builder.handle_station({"recNum": _rec_num, "stationName": _station_name})
            doc_map = _builder.get_document_map()
            _collection.upsert_multi(doc_map)
            # modify the new_station_zbaa['geo'] to reflect what handle_station should have done
            new_station_zbaa['geo'][0]["firstTime"] = orig_first_time
            self.assert_station(_cluster, new_station_zbaa)
            self.cleanup_builder_doc(_cluster, _collection, _builder, station_zbaa_copy)
        except Exception as _e:  # pylint:disable=broad-except
            self.fail("test_handle_station Exception failure: " + str(_e))
        finally:
            # upsert the original ZBAA station document
            station_zbaa["geo"].pop(0)
            _collection.upsert_multi({station_zbaa_copy["id"]: station_zbaa_copy})

    def remove_station_zbaa(self, _station_name, _cluster, _collection, station_zbaa):
        if station_zbaa is not None:
            _collection.remove(station_zbaa["id"])
        result = _cluster.query(
                    " ".join(("""
                    SELECT mdata.*
                    FROM mdata
                    WHERE mdata.type = 'MD'
                    AND mdata.docType = 'station'
                    AND mdata.version = 'V01'
                    AND name = '""" + _station_name + "'").split()),
                    QueryOptions(scan_consistency=QueryScanConsistency.REQUEST_PLUS)
                )
        if len(list(result)) > 0:
            station_zbaa = list(result)[0]
        else:
            station_zbaa = None
        check_station_zbaa = len(list(result)) == 0
        self.assertTrue(check_station_zbaa, msg="station " + _station_name + " did not get deleted")
        return station_zbaa

    def setup_builder_doc(self, _cluster, _collection, _builder):
        result = _cluster.query(
                " ".join("""SELECT mdata.*
                FROM mdata
                WHERE type = 'MD'
                AND docType = 'station'
                AND subset = 'METAR'
                AND version = 'V01';""".split())
            )
        _builder.stations = list(result)
        _builder.initialize_document_map()

    def cleanup_builder_doc(self, _cluster, _collection, _builder, station_zbaa_copy):
        _collection.upsert_multi({station_zbaa_copy["id"]:station_zbaa_copy})
        result = _cluster.query(
                " ".join("""SELECT mdata.*
                FROM mdata
                WHERE type = 'MD'
                AND docType = 'station'
                AND subset = 'METAR'
                AND version = 'V01';""".split())
            )
        _builder.stations = list(result)
        _builder.initialize_document_map()

    def assert_station(self, _cluster, station_zbaa):
        """Asserts that a given station object matches the one that is in the database,
        """
        new_result = _cluster.query(
            " ".join("""
                SELECT mdata.*
                FROM mdata
                WHERE mdata.type = 'MD'
                AND mdata.docType = 'station'
                AND mdata.version = 'V01'
                AND name = 'ZBAA'
                """.split()), QueryOptions(scan_consistency=QueryScanConsistency.REQUEST_PLUS)
        )
        new_station_zbaa = list(new_result)[0]
        if station_zbaa is None:
            self.assertIsNotNone(new_station_zbaa["description"], msg="new station description is missing")
            self.assertIsNotNone(new_station_zbaa["id"], msg="new station id is missing")
            self.assertIsNotNone(new_station_zbaa["name"], msg="new station name is missing")
            self.assertIsNotNone(new_station_zbaa["updateTime"], msg="new station updateTime is missing")
            self.assertIsNotNone(new_station_zbaa["geo"], msg="new station geo is missing")
            self.assertEqual(1, len(new_station_zbaa["geo"]), msg="new station geo is not length 1")
            return
        self.assertEqual(
            new_station_zbaa["description"],
            station_zbaa["description"],
            msg="new 'description'"
            + new_station_zbaa["description"]
            + " does not equal old 'description'"
            + station_zbaa["description"]
        )
        self.assertEqual(
            new_station_zbaa["id"],
            station_zbaa["id"],
            msg="new 'id'"
            + new_station_zbaa["id"]
            + " does not equal old 'id'"
            + station_zbaa["id"]
        )
        self.assertEqual(
            new_station_zbaa["name"],
            station_zbaa["name"],
            msg="new 'name'"
            + new_station_zbaa["name"]
            + " does not equal old 'name'"
            + station_zbaa["name"]
        )
        for geo_index in range(len(new_station_zbaa["geo"])):
            self.assertEqual(
                new_station_zbaa["geo"][geo_index]["lat"],
                station_zbaa["geo"][geo_index]["lat"],
                msg="new '['geo'][geo_index]['lat']'"
                + str(new_station_zbaa["geo"][geo_index]["lat"])
                + " does not equal old '['geo'][geo_index]['lat']'"
                + str(station_zbaa["geo"][geo_index]["lat"])
            )
            self.assertEqual(
                new_station_zbaa["geo"][geo_index]["lon"],
                station_zbaa["geo"][geo_index]["lon"],
                msg="new '['geo'][geo_index]['lon']'"
                + str(new_station_zbaa["geo"][geo_index]["lon"])
                + " does not equal old '['geo'][geo_index]['lon']'"
                + str(station_zbaa["geo"][geo_index]["lon"])
            )
            self.assertEqual(
                new_station_zbaa["geo"][geo_index]["elev"],
                station_zbaa["geo"][geo_index]["elev"],
                msg="new '['geo'][geo_index]['elev']'"
                + str(new_station_zbaa["geo"][geo_index]["elev"])
                + " does not equal old '['geo'][geo_index]['elev']'"
                + str(station_zbaa["geo"][geo_index]["elev"])
            )
            self.assertEqual(
                new_station_zbaa["geo"][geo_index]["firstTime"],
                station_zbaa["geo"][geo_index]["firstTime"],
                msg="new '['geo'][geo_index]['firstTime']'"
                + str(new_station_zbaa["geo"][geo_index]["firstTime"])
                + " does not equal old '['geo'][geo_index]['firstTime']'"
                + str(station_zbaa["geo"][geo_index]["firstTime"])
            )
            self.assertEqual(
                new_station_zbaa["geo"][geo_index]["lastTime"],
                station_zbaa["geo"][geo_index]["lastTime"],
                msg="new '['geo'][geo_index]['lastTime']'"
                + str(new_station_zbaa["geo"][geo_index]["lastTime"])
                + " does not equal old '['geo'][geo_index]['lastTime']'"
                + str(station_zbaa["geo"][geo_index]["lastTime"])
            )

    def test_derive_valid_time_epoch(self):
        """test the derive_valid_time_epoch routine in netcdf_builder"""
        try:
            vx_ingest = self.setup_connection()
            _cluster = vx_ingest.cluster
            _collection = vx_ingest.collection
            load_spec = vx_ingest.load_spec
            ingest_document_id = vx_ingest.load_spec["ingest_document_id"]
            ingest_document = _collection.get(ingest_document_id).content
            _builder = NetcdfMetarObsBuilderV01(
                load_spec, ingest_document, _cluster, _collection
            )
            _builder.file_name = "20211108_0000"
            _pattern = "%Y%m%d_%H%M"
            _file_utc_time = datetime.strptime(_builder.file_name, _pattern)
            expected_epoch = (_file_utc_time - datetime(1970, 1, 1)).total_seconds()
            derived_epoch = _builder.derive_valid_time_epoch(
                {"file_name_pattern": _pattern}
            )
            self.assertEqual(
                expected_epoch,
                derived_epoch,
                "derived epoch {de} is not equal to 1636329600".format(
                    de=derived_epoch
                ),
            )
        except Exception as _e:  # pylint:disable=broad-except
            self.fail("test_derive_valid_time_epoch Exception failure: " + str(_e))

    def test_derive_valid_time_iso(self):
        """test the derive_valid_time_iso routine in netcdf_builder"""
        try:
            vx_ingest = self.setup_connection()
            _cluster = vx_ingest.cluster
            _collection = vx_ingest.collection
            load_spec = vx_ingest.load_spec
            ingest_document_id = vx_ingest.load_spec["ingest_document_id"]
            ingest_document = _collection.get(ingest_document_id).content
            _builder = NetcdfMetarObsBuilderV01(
                load_spec, ingest_document, _cluster, _collection
            )
            _builder.file_name = "20211108_0000"
            derived_epoch = _builder.derive_valid_time_iso(
                {"file_name_pattern": "%Y%m%d_%H%M"}
            )
            self.assertEqual(
                "2021-11-08T00:00:00Z",
                derived_epoch,
                "derived epoch {de} is not equal to 1636390800".format(
                    de=derived_epoch
                ),
            )
        except Exception as _e:  # pylint:disable=broad-except
            self.fail("test_derive_valid_time_epoch Exception failure: " + str(_e))
