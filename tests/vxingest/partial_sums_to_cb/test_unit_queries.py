import os
from datetime import timedelta
from pathlib import Path

import pytest
import yaml
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, ClusterTimeoutOptions, QueryOptions


def connect_cb():
    """
    create a couchbase connection and maintain the collection and cluster objects.
    """

    credentials_file = os.environ["CREDENTIALS"]
    assert Path(credentials_file).is_file() is True, (
        f"*** credentials_file file {credentials_file} can not be found!"
    )
    with Path(credentials_file).open(encoding="utf-8") as _f:
        _yaml_data = yaml.load(_f, yaml.SafeLoader)
    cb_connection = {}
    cb_connection["host"] = _yaml_data["cb_host"]
    cb_connection["user"] = _yaml_data["cb_user"]
    cb_connection["password"] = _yaml_data["cb_password"]
    cb_connection["bucket"] = _yaml_data["cb_bucket"]
    cb_connection["collection"] = _yaml_data["cb_collection"]
    cb_connection["scope"] = _yaml_data["cb_scope"]

    timeout_options = ClusterTimeoutOptions(
        kv_timeout=timedelta(seconds=25), query_timeout=timedelta(seconds=120)
    )
    options = ClusterOptions(
        PasswordAuthenticator(cb_connection["user"], cb_connection["password"]),
        timeout_options=timeout_options,
    )
    cb_connection["cluster"] = Cluster(cb_connection["host"], options)
    cb_connection["collection"] = (
        cb_connection["cluster"]
        .bucket(cb_connection["bucket"])
        .collection(cb_connection["collection"])
    )
    return cb_connection


@pytest.mark.integration
def test_epoch_fcstlen_model(request):
    _name = request.node.name
    _expected_time = 3.0
    testdata = Path(
        "tests/vxingest/partial_sums_to_cb/testdata/test_epoch_fcstLen_model.n1ql"
    )
    with testdata.open(mode="r", encoding="utf-8") as file:
        _statement = file.read()
    result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
    # have to read the rows before we can get to the metadata as of couchbase 4.1
    _rows = list(result.rows())
    elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
    print(f"{_name}: elapsed_time is {elapsed_time}")
    assert result is not None, "{_name}: result is None"
    assert elapsed_time < _expected_time, (
        f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    )


@pytest.mark.integration
def test_epoch_fcstlen_obs(request):
    _name = request.node.name
    _expected_time = 0.2
    testdata = Path(
        "tests/vxingest/partial_sums_to_cb/testdata/test_epoch_fcstLen_obs.n1ql"
    )
    with testdata.open(mode="r", encoding="utf-8") as file:
        _statement = file.read()
    result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
    # have to read the rows before we can get to the metadata as of couchbase 4.1
    _rows = list(result.rows())
    elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
    print(f"{_name}: elapsed_time is {elapsed_time}")
    assert result is not None, "{_name}: result is None"
    assert elapsed_time < _expected_time, (
        f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    )


@pytest.mark.integration
def test_forecast_valid_epoch(request):
    _name = request.node.name
    _expected_time = 4.0
    testdata = Path(
        "tests/vxingest/partial_sums_to_cb/testdata/test_forecast_valid_epoch.n1ql"
    )
    with testdata.open(mode="r", encoding="utf-8") as file:
        _statement = file.read()
    result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
    # have to read the rows before we can get to the metadata as of couchbase 4.1
    _rows = list(result.rows())
    elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
    print(f"{_name}: elapsed_time is {elapsed_time}")
    assert result is not None, "{_name}: result is None"
    assert elapsed_time < _expected_time, (
        f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    )


@pytest.mark.integration
def test_get_region_lat_lon(request):
    _name = request.node.name
    _expected_time = 0.015
    testdata = Path(
        "tests/vxingest/partial_sums_to_cb/testdata/test_get_region_lat_lon.n1ql"
    )
    with testdata.open(mode="r", encoding="utf-8") as file:
        _statement = file.read()
    result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
    # have to read the rows before we can get to the metadata as of couchbase 4.1
    _rows = list(result.rows())
    elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
    print(f"{_name}: elapsed_time is {elapsed_time}")
    assert result is not None, "{_name}: result is None"
    assert elapsed_time < _expected_time, (
        f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    )


@pytest.mark.integration
def test_get_stations(request):
    _name = request.node.name
    _expected_time = 3
    testdata = Path("tests/vxingest/partial_sums_to_cb/testdata/test_get_stations.n1ql")
    with testdata.open(mode="r", encoding="utf-8") as file:
        _statement = file.read()
    result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
    # have to read the rows before we can get to the metadata as of couchbase 4.1
    _rows = list(result.rows())
    elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
    print(f"{_name}: elapsed_time is {elapsed_time}")
    assert result is not None, "{_name}: result is None"
    assert elapsed_time < _expected_time, (
        f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    )


@pytest.mark.integration
def test_get_threshold_descriptions(request):
    _name = request.node.name
    _expected_time = 0.6
    testdata = Path(
        "tests/vxingest/partial_sums_to_cb/testdata/test_get_threshold_descriptions.n1ql"
    )
    with testdata.open(mode="r", encoding="utf-8") as file:
        _statement = file.read()
    result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
    # have to read the rows before we can get to the metadata as of couchbase 4.1
    _rows = list(result.rows())
    elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
    print(f"{_name}: elapsed_time is {elapsed_time}")
    assert result is not None, "{_name}: result is None"
    assert elapsed_time < _expected_time, (
        f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    )
