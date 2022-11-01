# pylint: disable=missing-module-docstring
import os
from pathlib import Path
import yaml
from couchbase.cluster import Cluster, ClusterOptions, QueryOptions
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

            options = ClusterOptions(
                PasswordAuthenticator(cb_connection["user"], cb_connection["password"])
            )
            cb_connection["cluster"] = Cluster(
                "couchbase://" + cb_connection["host"], options
            )
            cb_connection["collection"] = (
                cb_connection["cluster"]
                .bucket(cb_connection["bucket"])
                .collection(cb_connection["collection"])
            )
        return cb_connection
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"test_unit_queries Exception failure connecting: {_e}"

def test_epoch_fcstLen_model(request):
    """test"""
    try:
        _name = request.node.name
        _expected_time = 1.0
        _statement = open("./ctc_to_cb/test/test_epoch_fcstLen_model.n1ql").read()
        result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
        elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
        print(f"{_name}: elapsed_time is {elapsed_time}")
        assert result is not None, "{_name}: result is None"
        assert len(result.errors) == 0, f"{_name}: result has errors{result.errors}"
        assert elapsed_time < _expected_time, f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"{_name} Exception failure: {_e}"

def test_epoch_fcstLen_obs(request):
    """test"""
    try:
        _name = request.node.name
        _expected_time = 0.01
        _statement = open("./ctc_to_cb/test/test_epoch_fcstLen_obs.n1ql").read()
        result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
        elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
        print(f"{_name}: elapsed_time is {elapsed_time}")
        assert result is not None, "{_name}: result is None"
        assert len(result.errors) == 0, f"{_name}: result has errors{result.errors}"
        assert elapsed_time < _expected_time, f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"{_name} Exception failure: {_e}"

def test_forecast_valid_epoch(request):
    """test"""
    try:
        _name = request.node.name
        _expected_time = 0.2
        _statement = open("./ctc_to_cb/test/test_forecast_valid_epoch.n1ql").read()
        result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
        elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
        print(f"{_name}: elapsed_time is {elapsed_time}")
        assert result is not None, "{_name}: result is None"
        assert len(result.errors) == 0, f"{_name}: result has errors{result.errors}"
        assert elapsed_time < _expected_time, f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"{_name} Exception failure: {_e}"

def test_get_region_lat_lon(request):
    """test"""
    try:
        _name = request.node.name
        _expected_time = 0.01
        _statement = open("./ctc_to_cb/test/test_get_region_lat_lon.n1ql").read()
        result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
        elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
        print(f"{_name}: elapsed_time is {elapsed_time}")
        assert result is not None, "{_name}: result is None"
        assert len(result.errors) == 0, f"{_name}: result has errors{result.errors}"
        assert elapsed_time < _expected_time, f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"{_name} Exception failure: {_e}"

def test_get_stations(request):
    """test"""
    try:
        _name = request.node.name
        _expected_time = 1.5
        _statement = open("./ctc_to_cb/test/test_get_stations.n1ql").read()
        result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
        elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
        print(f"{_name}: elapsed_time is {elapsed_time}")
        assert result is not None, "{_name}: result is None"
        assert len(result.errors) == 0, f"{_name}: result has errors{result.errors}"
        assert elapsed_time < _expected_time, f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"{_name} Exception failure: {_e}"

def test_get_threshold_descriptions(request):
    """test"""
    try:
        _name = request.node.name
        _expected_time = 0.01
        _statement = open("./ctc_to_cb/test/test_get_threshold_descriptions.n1ql").read()
        result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
        elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
        print(f"{_name}: elapsed_time is {elapsed_time}")
        assert result is not None, "{_name}: result is None"
        assert len(result.errors) == 0, f"{_name}: result has errors{result.errors}"
        assert elapsed_time < _expected_time, f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"{_name} Exception failure: {_e}"


# geo search for stations - currently not implemented
# _result1 = self.load_spec["cluster"].search_query(
#     "station_geo",
#     GeoBoundingBoxQuery(
#         top_left=(_boundingbox["tl_lon"], _boundingbox["tl_lat"]),
#         bottom_right=(_boundingbox["br_lon"], _boundingbox["br_lat"]),
#         field="geo",
#     ),
#     SearchOptions(fields=["name"], limit=10000),
# )