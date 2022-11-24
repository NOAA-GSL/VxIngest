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


def test_simple_ctc(request):
    """test"""
    try:
        _name = request.node.name
        _statement_mdata = open("./builder_common/test/simple_ctc_mdata.n1ql").read()
        _statement_vxdata = open("./builder_common/test/simple_ctc_mdatatest.n1ql").read()
        result_mdata = connect_cb()["cluster"].query(_statement_mdata)
        vxdata = connect_cb()["cluster"].query(_statement_vxdata)

        assert result_mdata is not None,f"{_name}: mdata result is None"
        assert len(result_mdata.errors) == 0, f"{_name}: mdata result has errors{result_mdata.errors}"
        assert vxdata is not None,f"{_name}: vxdata result is None"
        assert len(vxdata.errors) == 0, f"{_name}: vxdata result has errors{vxdata.errors}"
        mdata = list(result_mdata)[0]
        vxdata = list(vxdata)[0]
        for k in mdata['mdata'].keys():
            if k != "data":
                assert mdata['mdata'][k] == vxdata['METAR'][k], f"mdata {mdata['mdata'][k]} and vxdata {vxdata['METAR'][k]} field {k} do not match"
            else:
                mdata_data = mdata['mdata']['data']
                vxdata_data = vxdata['METAR']['data']
                for dk in mdata_data.keys():
                    assert mdata_data[dk] == vxdata_data[dk], f"mdata data field for threshold {dk} {mdata_data[dk]} and vxdata {vxdata_data[dk]} do not match"

    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"{_name} Exception failure: {_e}"

def test_simple_model(request):
    """test"""
    try:
        _name = request.node.name
        _statement_mdata = open("./builder_common/test/simple_model_mdata.n1ql").read()
        _statement_vxdata = open("./builder_common/test/simple_model_mdatatest.n1ql").read()
        result_mdata = connect_cb()["cluster"].query(_statement_mdata)
        result_vxdata = connect_cb()["cluster"].query(_statement_vxdata)

        assert result_mdata is not None,f"{_name}: mdata result is None"
        assert len(result_mdata.errors) == 0, f"{_name}: mdata result has errors{result_mdata.errors}"
        assert result_vxdata is not None,f"{_name}: vxdata result is None"
        assert len(result_vxdata.errors) == 0, f"{_name}: vxdata result has errors{result_vxdata.errors}"
        mdata = list(result_mdata)[0]
        vxdata = list(result_vxdata)[0]
        for k in vxdata['METAR'].keys():
            if k != "data":
                assert vxdata['METAR'][k] == mdata['mdata'][k], f"mdata {mdata['mdata'][k]} and vxdata {vxdata['METAR'][k]} field {k} do not match"
            else:
                mdata_data = mdata['mdata']['data'][0]
                station_id = mdata_data['name']
                vxdata_data = vxdata['METAR']['data'][station_id]
                for dk in mdata_data.keys():
                    assert mdata_data[dk] == vxdata_data[dk], f"mdata data field for station {station_id} field {dk} {mdata_data[dk]} and vxdata {vxdata_data[dk]} do not match"
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"{_name} Exception failure: {_e}"

def test_simple_obs(request):
    """test"""
    try:
        _name = request.node.name
        _statement_mdata = open("./builder_common/test/simple_obs_mdata.n1ql").read()
        _statement_vxdata = open("./builder_common/test/simple_obs_mdatatest.n1ql").read()
        result_mdata = connect_cb()["cluster"].query(_statement_mdata)
        result_vxdata = connect_cb()["cluster"].query(_statement_vxdata)

        assert result_mdata is not None,f"{_name}: mdata result is None"
        assert len(result_mdata.errors) == 0, f"{_name}: mdata result has errors{result_mdata.errors}"
        assert result_vxdata is not None,f"{_name}: vxdata result is None"
        assert len(result_vxdata.errors) == 0, f"{_name}: vxdata result has errors{result_vxdata.errors}"
        mdata = list(result_mdata)[0]
        vxdata = list(result_vxdata)[0]
        for k in mdata['mdata'].keys():
            if k != "data":
                assert mdata['mdata'][k] == vxdata['METAR'][k], f"mdata {mdata['mdata'][k]} and vxdata {vxdata['METAR'][k]} field {k} do not match"
            else:
                mdata_data = mdata['mdata']['data'][0]
                station_id = mdata_data['name']
                vxdata_data = vxdata['METAR']['data'][station_id]
                for dk in mdata_data.keys():
                    assert mdata_data[dk] == vxdata_data[dk], f"mdata data field for station {station_id} field {dk} {mdata_data[dk]} and vxdata {vxdata_data[dk]} do not match"
    except Exception as _e:  # pylint:disable=broad-except
        assert False, f"{_name} Exception failure: {_e}"


