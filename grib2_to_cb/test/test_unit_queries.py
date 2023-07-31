# pylint: disable=missing-module-docstring
import os
from pathlib import Path
from datetime import timedelta
import yaml
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, ClusterTimeoutOptions, QueryOptions
from couchbase.auth import PasswordAuthenticator

def connect_cb():
    """
    create a couchbase connection and maintain the collection and cluster objects.
    """
    # noinspection PyBroadException
    try:
        try:
            cb_connection  # is it defined pylint: disable=used-before-assignment
        except NameError:
            credentials_file = os.environ["HOME"] + "/adb-cb1-credentials"
            assert (
                Path(credentials_file).is_file() is True
            ), f"*** credentials_file file {credentials_file} can not be found!"
            _f = open(credentials_file, encoding="utf-8")
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

def test_ingest_document_id(request):
    """test"""
    try:
        _name = request.node.name
        _expected_time = 0.005
        _statement = open("./grib2_to_cb/test/test_ingest_document_id.n1ql", encoding="utf-8").read()
        result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
        # have to read the rows before we can get to the metadata as of couchbase 4.1
        _rows = list(result.rows())
        elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
        print(f"{_name}: elapsed_time is {elapsed_time}")
        assert result is not None, "{_name}: result is None"
        assert elapsed_time < _expected_time, f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"{_name} Exception failure: {_e}"

def test_ingest_document_fields(request):
    """test"""
    try:
        _name = request.node.name
        _expected_time = 0.01
        _statement = open("./grib2_to_cb/test/test_ingest_document_fields.n1ql", encoding="utf-8").read()
        result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
        # have to read the rows before we can get to the metadata as of couchbase 4.1
        _rows = list(result.rows())
        elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
        print(f"{_name}: elapsed_time is {elapsed_time}")
        assert result is not None, "{_name}: result is None"
        assert elapsed_time < _expected_time, f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"{_name} Exception failure: {_e}"

def test_get_df(request):
    """test"""
    try:
        _name = request.node.name
        _expected_time = 18
        _statement = open("./grib2_to_cb/test/test_get_DF.n1ql", encoding='utf-8').read()
        result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
        # have to read the rows before we can get to the metadata as of couchbase 4.1
        _rows = list(result.rows())
        elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
        print(f"{_name}: elapsed_time is {elapsed_time}")
        assert result is not None, "{_name}: result is None"
        assert elapsed_time < _expected_time, f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"{_name} Exception failure: {_e}"

def test_get_stations(request):
    """test"""
    try:
        _name = request.node.name
        _expected_time = 1
        _statement = open("./grib2_to_cb/test/test_get_stations.n1ql", encoding="utf-8").read()
        result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
        # have to read the rows before we can get to the metadata as of couchbase 4.1
        _rows = list(result.rows())
        elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
        print(f"{_name}: elapsed_time is {elapsed_time}")
        assert result is not None, "{_name}: result is None"
        assert elapsed_time < _expected_time, f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"{_name} Exception failure: {_e}"

def test_get_model_by_fcst_valid_epoch(request):
    """test"""
    try:
        _name = request.node.name
        _expected_time = 1.2
        _statement = open("./grib2_to_cb/test/test_get_model_by_fcstValidEpoch.n1ql", encoding="utf-8").read()
        result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
        # have to read the rows before we can get to the metadata as of couchbase 4.1
        _rows = list(result.rows())
        elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
        print(f"{_name}: elapsed_time is {elapsed_time}")
        assert result is not None, "{_name}: result is None"
        assert elapsed_time < _expected_time, f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"{_name} Exception failure: {_e}"
