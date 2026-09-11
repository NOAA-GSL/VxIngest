import os
from datetime import timedelta
from pathlib import Path

import pytest
import yaml
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions, ClusterTimeoutOptions, QueryOptions

from vxingest.builder_common.vx_ingest import CommonVxIngest


class EmptyQueryCluster:
    """Minimal query source for get_file_list unit tests."""

    @staticmethod
    def query(_statement):
        return []


def test_unit_get_file_list_respects_minimum_file_age(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    vx_ingest = CommonVxIngest()
    vx_ingest.cluster = EmptyQueryCluster()
    input_file = tmp_path / "input.grib2"
    input_file.touch()
    two_hours_ago = (input_file.stat().st_mtime - 2 * 3600, ) * 2
    os.utime(input_file, two_hours_ago)

    monkeypatch.delenv("VXINGEST_MIN_FILE_AGE_HOURS", raising=False)
    assert vx_ingest.get_file_list("SELECT", tmp_path, "*.grib2", "") == []

    monkeypatch.setenv("VXINGEST_MIN_FILE_AGE_HOURS", "1")
    assert vx_ingest.get_file_list("SELECT", tmp_path, "*.grib2", "") == [
        str(input_file)
    ]


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
def test_get_file_list(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    vx_ingest = CommonVxIngest()
    vx_ingest.credentials_file = os.environ["CREDENTIALS"]
    vx_ingest.cb_credentials = vx_ingest.get_credentials(vx_ingest.load_spec)
    vx_ingest.connect_cb()
    testdata = Path("tests/vxingest/builder_common/testdata/get_file_list_grib2.n1ql")
    monkeypatch.setenv("VXINGEST_MIN_FILE_AGE_HOURS", "0")
    with testdata.open(mode="r", encoding="utf-8") as file:
        _statement = file.read()
    with Path(tmp_path / "2128723000010").open("w") as f:
        f.write("test")
    with Path.open(Path(tmp_path / "2128723000020"), "w") as f:
        f.write("test")
    with Path.open(Path(tmp_path / "2128723000030"), "w") as f:
        f.write("test")
    with Path.open(Path(tmp_path, "2128723000040"), "w") as f:
        f.write("test")

    file_list = vx_ingest.get_file_list(
        _statement, tmp_path, "21287230000[0123456789]?", "%y%j%H%f"
    )
    assert file_list is not None
    assert len(file_list) > 0
    assert file_list[3] > file_list[2], "file_list is not reverse sorted"


@pytest.mark.integration
def test_stations_fcst_valid_epoch(request: pytest.FixtureRequest):
    _expected_time = 10
    _name = request.node.name
    testdata = Path(
        "tests/vxingest/builder_common/testdata/stations_fcst_valid_epoch.n1ql"
    )
    with testdata.open(mode="r", encoding="utf-8") as file:
        _statement = file.read()
    result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
    # have to read the rows before we can get to the metadata as of couchbase 4.1
    _rows = list(result.rows())
    elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
    print(f"{_name}: elapsed_time is {elapsed_time}")
    assert result is not None, f"{_name}: result is None"
    assert elapsed_time < _expected_time, (
        f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    )


@pytest.mark.integration
def test_stations_get_file_list_grib2(request: pytest.FixtureRequest):
    _expected_time = 16
    _name = request.node.name
    testdata = Path("tests/vxingest/builder_common/testdata/get_file_list_grib2.n1ql")
    with testdata.open(mode="r", encoding="utf-8") as file:
        _statement = file.read()
    result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
    # have to read the rows before we can get to the metadata as of couchbase 4.1
    _rows = list(result.rows())
    elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
    print(f"{_name}: elapsed_time is {elapsed_time}")
    assert result is not None, f"{_name}: result is None"
    assert elapsed_time < _expected_time, (
        f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    )


@pytest.mark.integration
def test_stations_get_file_list_netcdf(request: pytest.FixtureRequest):
    _expected_time = 5
    _name = request.node.name
    testdata = Path("tests/vxingest/builder_common/testdata/get_file_list_netcdf.n1ql")
    with testdata.open(mode="r", encoding="utf-8") as file:
        _statement = file.read()
    result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
    # have to read the rows before we can get to the metadata as of couchbase 4.1
    _rows = list(result.rows())
    elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
    print(f"{_name}: elapsed_time is {elapsed_time}")
    assert result is not None, f"{_name}: result is None"
    assert elapsed_time < _expected_time, (
        f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    )


@pytest.mark.integration
def test_metar_count(request: pytest.FixtureRequest):
    _expected_time = 0.05
    _name = request.node.name
    testdata = Path("tests/vxingest/builder_common/testdata/METAR_count.n1ql")
    with testdata.open(mode="r", encoding="utf-8") as file:
        _statement = file.read()
    result = connect_cb()["cluster"].query(_statement, QueryOptions(metrics=True))
    # have to read the rows before we can get to the metadata as of couchbase 4.1
    _rows = list(result.rows())
    elapsed_time = result.metadata().metrics().elapsed_time().total_seconds()
    print(f"{_name}: elapsed_time is {elapsed_time}")
    assert result is not None, f"{_name}: result is None"
    assert elapsed_time < _expected_time, (
        f"{_name}: elasped_time greater than {_expected_time} {elapsed_time}"
    )
