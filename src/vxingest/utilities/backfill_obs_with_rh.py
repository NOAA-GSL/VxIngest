"""
Module to backfill observations with relative humidity, WindU, and WindV.
"""

import os
from datetime import timedelta
from pathlib import Path

import yaml
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
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
            kv_timeout=timedelta(seconds=25), query_timeout=timedelta(seconds=120)
        )
        options = ClusterOptions(
            PasswordAuthenticator(_user, _password), timeout_options=timeout_options
        )
        options = ClusterOptions(PasswordAuthenticator(_user, _password))
        connection = {}
        connection['cluster'] = Cluster("couchbase://" + _host, options)
        connection['bucket'] = connection['cluster'].bucket(_bucket)
        connection['scope'] = connection['bucket'].scope(_scope)
        connection['collection'] = connection['scope'].collection(_collection)
        return connection
    except Exception as _e:  # pylint:disable=broad-except
        print (f"test_credentials_and_load_spec Exception failure: {_e}")

def calc_components(doc):
    """Calculate RH, WindU, and WindV from Temperature, DewPoint, WS, and WD.
    """
    # doc = {"data":{'station_name': {'Temperature'} {'DewPoint'} {'WS'} {'WD'} ... }
    for _name, station in doc['data'].items():
        if "RH" not in station:
            if station["Temperature"] is not None and station["DewPoint"] is not None:
                station["RH"] = relative_humidity_from_dewpoint(station["Temperature"] * units.degC, station["DewPoint"] * units.degC).magnitude * 100
            else:
                station["RH"] = None
        if "WindU" not in station or "WindV" not in station.keys():
            if station["WS"] is not None and station["WD"] is not None:
                _u, _v = wind_components(station["WS"] * units("m/s"), station["WD"] * units.deg)
                station["WindU"] = _u.magnitude
                station["WindV"] = _v.magnitude
            else:
                station["WindU"] = None
                station["WindV"] = None

def run_backfill() -> None:
    """entrypoint"""
    connection = setup_connection()
    _query_result = connection['cluster'].query("select raw meta().id FROM `vxdata`._default.METAR WHERE type='DD' AND docType ='obs' AND version='V01' AND subset='METAR';")
    _result = list(_query_result)
    print(f"number of ids is {len(_result)}: starting id is {_result[0]} and ending id is {_result[-1]}")
    for i,_id in enumerate(_result):
        print (f"processing #{i} with id {_id}")
        doc = connection['collection'].get(_id).content_as[dict]
        calc_components(doc)
        connection['collection'].upsert(_id, doc)

if __name__ == "__main__":
    run_backfill()
