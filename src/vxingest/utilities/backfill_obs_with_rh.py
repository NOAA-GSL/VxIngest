"""
Module to backfill observations with relative humidity, WindU, and WindV.
"""

import os
import sys
import time
from datetime import timedelta
from pathlib import Path

import yaml
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import TimeoutException
from couchbase.options import ClusterOptions, ClusterTimeoutOptions
from metpy.calc import relative_humidity_from_dewpoint, wind_components
from metpy.units import units


def setup_connection():
    """test setup"""
    try:
        credentials_file = os.environ["CREDENTIALS"]
        assert Path(credentials_file).is_file(), "credentials_file Does not exist"
        _f = Path.open(credentials_file, encoding="utf-8")
        yaml_data = yaml.load(_f, yaml.SafeLoader)
        _host = yaml_data["cb_host"]
        _user = yaml_data["cb_user"]
        _password = yaml_data["cb_password"]
        _bucket = yaml_data["cb_bucket"]
        _collection = yaml_data["cb_collection"]
        _scope = yaml_data["cb_scope"]
        _f.close()

        timeout_options = ClusterTimeoutOptions(
            kv_timeout=timedelta(seconds=125), query_timeout=timedelta(seconds=120)
        )
        options = ClusterOptions(
            PasswordAuthenticator(_user, _password), timeout_options=timeout_options
        )
        options = ClusterOptions(PasswordAuthenticator(_user, _password))
        connection = {}
        connection["cluster"] = Cluster(_host, options)
        connection["bucket"] = connection["cluster"].bucket(_bucket)
        connection["scope"] = connection["bucket"].scope(_scope)
        connection["collection"] = connection["scope"].collection(_collection)
        return connection
    except Exception as _e:  # pylint:disable=broad-except
        print(f"test_credentials_and_load_spec Exception failure: {_e}")


def calc_components(doc):
    """Calculate RH, WindU, and WindV from Temperature, DewPoint, WS, and WD."""
    # doc = {"data":{'station_name': {'Temperature'} {'DewPoint'} {'WS'} {'WD'} ... }
    for _name, station in doc["data"].items():
        # always calculate RH to correct incorrect values that were previously calculated with DegC
        if station["Temperature"] is not None and station["DewPoint"] is not None:
            station["RH"] = (
                relative_humidity_from_dewpoint(
                    station["Temperature"] * units.degF,
                    station["DewPoint"] * units.degF,
                ).magnitude
                * 100
            )
        else:
            station["RH"] = None
        if "WindU" not in station or "WindV" not in station:
            if station["WS"] is not None and station["WD"] is not None:
                _u, _v = wind_components(
                    station["WS"] * units("m/s"), station["WD"] * units.deg
                )
                station["WindU"] = _u.magnitude
                station["WindV"] = _v.magnitude
            else:
                station["WindU"] = None
                station["WindV"] = None


def run_backfill(start_id) -> None:
    """entrypoint"""
    start_clause = ""
    if start_id is not None:
        print(f"starting id is {start_id}")
        start_clause = f"AND meta().id >= '{start_id}'"
    connection = setup_connection()
    _query_result = connection["cluster"].query(
        f"select raw meta().id FROM `vxdata`._default.METAR WHERE type='DD' AND docType ='obs' AND version='V01' AND subset='METAR' {start_clause};"
    )
    _result = list(_query_result)
    print(
        f"number of ids is {len(_result)}: starting id is {_result[0]} and ending id is {_result[-1]}"
    )
    for i, _id in enumerate(_result):
        print(f"processing #{i} with id {_id}")
        ri = 0
        for ri in range(5):  # retry up to 5 times
            try:
                doc = connection["collection"].get(_id).content_as[dict]
                calc_components(doc)
                connection["collection"].upsert(_id, doc)
                break
            except TimeoutException as _e:
                print(f"TimeoutException failure: {_e} retrying id {_id} #{ri}")
                if ri == 5:
                    raise RuntimeWarning(
                        f"Failed to process id {_id} - too many retries"
                    ) from _e
                time.sleep(1)  # give it a second to breathe
            except Exception as _e1:  # pylint:disable=broad-except
                print(f"Exception failure: {_e1}")
                break


if __name__ == "__main__":
    STARTID = None
    if sys.argv[1]:
        STARTID = sys.argv[1]
    run_backfill(STARTID)
