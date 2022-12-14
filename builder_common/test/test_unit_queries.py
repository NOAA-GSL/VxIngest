# pylint: disable=missing-module-docstring
import os
from pathlib import Path
from datetime import timedelta
import yaml
from couchbase.cluster import Cluster, ClusterOptions, ClusterTimeoutOptions, QueryOptions
from couchbase_core.cluster import PasswordAuthenticator


def connect_cb():
    """
    create a couchbase connection and maintain the collection and cluster objects.
    """
    # noinspection PyBroadException
    try:
        try:
            cb_connection  # is it defined
        except NameError:
            credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
            assert (
                Path(credentials_file).is_file() is True
            ), f"*** credentials_file file {credentials_file} can not be found!"
            _f = open(credentials_file)
            _yaml_data = yaml.load(_f, yaml.SafeLoader)
            cb_connection = {}
            cb_connection["host"] = _yaml_data["cb_host"]
            cb_connection["user"] = _yaml_data["cb_user"]
            cb_connection["password"] = _yaml_data["cb_password"]
            cb_connection["bucket"] = _yaml_data["cb_bucket"]
            cb_connection["collection"] = _yaml_data["cb_collection"]
            cb_connection["scope"] = _yaml_data["cb_scope"]
            _f.close()

            timeout_options=ClusterTimeoutOptions(kv_timeout=timedelta(seconds=25), query_timeout=timedelta(seconds=120))
            options=ClusterOptions(PasswordAuthenticator(cb_connection["user"], cb_connection["password"]), timeout_options=timeout_options)
            cb_connection["cluster"] = Cluster("couchbase://" + cb_connection["host"], options)
            cb_connection["collection"] = (
                cb_connection["cluster"]
                .bucket(cb_connection["bucket"])
                .collection(cb_connection["collection"])
            )
        return cb_connection
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"test_unit_queries Exception failure connecting: {_e}"


def test_map_station_query_no_let(request):
    """test"""
    try:
        _expected_time = 12
        _name = request.node.name
        _statement = open("./builder_common/test/map_station_query_no_let.n1ql").read()
        result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
        elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
        print(f"{_name}: elapsed_time is {elapsed_time}")
        assert result is not None,f"{_name}: result is None"
        assert len(result.errors) == 0, f"{_name}: result has errors{result.errors}"
        assert elapsed_time < _expected_time, f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"{_name} Exception failure: {_e}"


def test_map_station_query(request):
    """test"""
    try:
        _expected_time = 12
        _name = request.node.name
        _statement = open("./builder_common/test/map_station_query.n1ql").read()
        result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
        elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
        print(f"{_name}: elapsed_time is {elapsed_time}")
        assert result is not None,f"{_name}: result is None"
        assert len(result.errors) == 0, f"{_name}: result has errors{result.errors}"
        assert elapsed_time < _expected_time, f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"{_name} Exception failure: {_e}"


def test_stations_fcst_valid_epoch(request):
    """test"""
    try:
        _expected_time = 10
        _name = request.node.name
        _statement = open("./builder_common/test/stations_fcst_valid_epoch.n1ql").read()
        result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
        elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
        print(f"{_name}: elapsed_time is {elapsed_time}")
        assert result is not None,f"{_name}: result is None"
        assert len(result.errors) == 0, f"{_name}: result has errors{result.errors}"
        assert elapsed_time < _expected_time, f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"{_name} Exception failure: {_e}"


def test_stations_get_file_list_grib2(request):
    """test"""
    try:
        _expected_time = 10
        _name = request.node.name
        _statement = open("./builder_common/test/get_file_list_grib2.n1ql").read()
        result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
        elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
        print(f"{_name}: elapsed_time is {elapsed_time}")
        assert result is not None,f"{_name}: result is None"
        assert len(result.errors) == 0, f"{_name}: result has errors{result.errors}"
        assert elapsed_time < _expected_time, f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"{_name} Exception failure: {_e}"

def test_stations_get_file_list_netcdf(request):
    """test"""
    try:
        _expected_time = 5
        _name = request.node.name
        _statement = open("./builder_common/test/get_file_list_netcdf.n1ql").read()
        result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
        elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
        print(f"{_name}: elapsed_time is {elapsed_time}")
        assert result is not None,f"{_name}: result is None"
        assert len(result.errors) == 0, f"{_name}: result has errors{result.errors}"
        assert elapsed_time < _expected_time, f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"{_name} Exception failure: {_e}"

def test_METAR_count(request):
    """test"""
    try:
        _expected_time = 0.05
        _name = request.node.name
        _statement = open("./builder_common/test/METAR_count.n1ql").read()
        result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
        elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
        print(f"{_name}: elapsed_time is {elapsed_time}")
        assert result is not None,f"{_name}: result is None"
        assert len(result.errors) == 0, f"{_name}: result has errors{result.errors}"
        assert elapsed_time < _expected_time, f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"{_name} Exception failure: {_e}"

def test_final_DieOff(request):
    """test"""
    try:
        _expected_time = 120
        _name = request.node.name
        _statement = open("./builder_common/test/final_DieOff.n1ql").read()
        result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
        elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
        print(f"{_name}: elapsed_time is {elapsed_time}")
        assert result is not None, f"{_name}: result is None"
        assert len(result.errors) == 0, f"{_name}: result has errors{result.errors}"
        assert elapsed_time < _expected_time, f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"{_name} Exception failure: {_e}"

def test_final_Map(request):
    """test"""
    try:
        _expected_time = 10
        _name = request.node.name
        _statement = open("./builder_common/test/final_Map.n1ql").read()
        result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
        elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
        print(f"{_name}: elapsed_time is {elapsed_time}")
        assert result is not None, f"{_name}: result is None"
        assert len(result.errors) == 0, f"{_name}: result has errors{result.errors}"
        assert elapsed_time < _expected_time, f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"{_name} Exception failure: {_e}"


def test_final_TimeSeries(request):
    """test"""
    try:
        _expected_time = 10
        _name = request.node.name
        _statement = open("./builder_common/test/final_TimeSeries.n1ql").read()
        result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
        elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
        print(f"{_name}: elapsed_time is {elapsed_time}")
        assert result is not None,f"{_name}: result is None"
        assert len(result.errors) == 0, f"{_name}: result has errors{result.errors}"
        assert elapsed_time < _expected_time, f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"{_name} Exception failure: {_e}"

def test_final_ValidTime(request):
    """test"""
    try:
        _expected_time = 10
        _name = request.node.name
        _statement = open("./builder_common/test/final_ValidTime.n1ql").read()
        result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
        elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
        print(f"{_name}: elapsed_time is {elapsed_time}")
        assert result is not None,f"{_name}: result is None"
        assert len(result.errors) == 0, f"{_name}: result has errors{result.errors}"
        assert elapsed_time < _expected_time, f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"{_name} Exception failure: {_e}"

