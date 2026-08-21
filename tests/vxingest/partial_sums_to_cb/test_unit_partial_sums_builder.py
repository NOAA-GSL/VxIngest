import pytest

from vxingest.partial_sums_to_cb.partial_sums_builder import (
    PartialSumsSurfaceModelObsBuilderV01,
)


@pytest.fixture
def dummy_builder():
    load_spec = "load_spec"
    ingest_document = {"template": ""}
    return PartialSumsSurfaceModelObsBuilderV01(load_spec, ingest_document)


@pytest.fixture
def model_data():
    data = {
        "data": {
            "KAAA": {
                "temperature": 25,
                "temperature_adj": 22,
            },
            "KBBB": {
                "temperature": 10,
                "temperature_adj": 14,
            },
        }
    }
    return data


@pytest.fixture
def obs_data():
    data = {
        "KAAA": {
            "temperature": 23,
        },
        "KBBB": {
            "temperature": 15,
        },
    }
    return data


def test_handle_sum_simple_param(dummy_builder, model_data, obs_data):
    """Test that handle_sum() returns correct values when passed a params_dict
    with a single variable"""

    builder = dummy_builder
    builder.domain_stations = ["KAAA", "KBBB"]
    builder.obs_data = obs_data
    builder.model_data = model_data
    params_dict = {"temperature": "temperature"}
    sums = builder.handle_sum(params_dict)

    assert sums == {
        "num_recs": 2,
        "sum_obs": 38,
        "sum_model": 35,
        "sum_diff": -3,
        "sum2_diff": 29,
        "sum_abs": 7,
    }


def test_handle_sum_obj_param(dummy_builder, model_data, obs_data):
    """Test that handle_sum() returns correct values when passed a params_dict
    with a dict of model and obs variables"""

    builder = dummy_builder
    builder.domain_stations = ["KAAA", "KBBB"]
    builder.obs_data = obs_data
    builder.model_data = model_data
    params_dict = {
        "model": "temperature_adj",
        "obs": "temperature",
    }
    sums = builder.handle_sum(params_dict)

    assert sums == {
        "num_recs": 2,
        "sum_obs": 38,
        "sum_model": 36,
        "sum_diff": -2,
        "sum2_diff": 2,
        "sum_abs": 2,
    }


class FakeCluster:
    def __init__(self):
        self.statements = []

    def query(self, statement, read_only=True):
        self.statements.append(statement)
        if "maxObsEpoch" in statement:
            return [{"minObsEpoch": 1000, "maxObsEpoch": 5000}]
        if "maxModelEpoch" in statement:
            return [{"minModelEpoch": 2000, "maxModelEpoch": 6000}]
        if "docType='SUMS'" in statement:
            return [3000]
        return []


def test_build_document_bounds_partial_sums_like_ctc():
    cluster = FakeCluster()
    ingest_document_id = "IS:METAR:SUMS:SURFACE:HRRR_OPS:ALL_HRRR:ingest:V01"
    ingest_document = {
        "model": "HRRR_OPS",
        "region": "ALL_HRRR",
        "subDocType": "SURFACE",
        "subset": "METAR",
        "template": {"id": "DD:V01:METAR:SUMS"},
    }
    load_spec = {
        "ingest_documents": {ingest_document_id: ingest_document},
        "cb_connection": {
            "bucket": "vxdata",
            "scope": "_default",
            "collection": "METAR",
        },
        "first_last_params": {"first_epoch": 2500, "last_epoch": 4500},
        "cluster": cluster,
    }
    builder = PartialSumsSurfaceModelObsBuilderV01(load_spec, ingest_document)

    assert builder.build_document(ingest_document_id) == {}

    sums_query = cluster.statements[2]
    model_query = cluster.statements[3]
    obs_query = cluster.statements[4]
    assert "AND fcstValidEpoch >= 2500" in sums_query
    assert "AND fcstValidEpoch <= 4500" in sums_query
    assert "AND fve.fcstValidEpoch > 3000" in model_query
    assert "AND fve.fcstValidEpoch <= 4500" in model_query
    assert "AND obs.fcstValidEpoch > 3000" in obs_query
    assert "AND obs.fcstValidEpoch <= 4500" in obs_query
