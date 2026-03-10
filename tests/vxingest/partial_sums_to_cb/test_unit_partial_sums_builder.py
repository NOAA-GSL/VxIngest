import pytest

from vxingest.partial_sums_to_cb.partial_sums_builder import (
    PartialSumsSurfaceModelObsBuilderV01,
)


@pytest.fixture
def dummy_builder():
    load_spec = 'load_spec'
    ingest_document = {'template': ''}
    return PartialSumsSurfaceModelObsBuilderV01(load_spec, ingest_document)

@pytest.fixture
def model_data():
    data = {
        'data': {
            'KAAA': {
                'temperature': 25,
                'temperature_adj': 22,
            },
            'KBBB': {
                'temperature': 10,
                'temperature_adj': 14,
            }
        }
    }
    return data

@pytest.fixture
def obs_data():
    data = {
        'KAAA': {
            'temperature': 23,
        },
        'KBBB': {
            'temperature': 15,
        }
    }
    return data

def test_handle_sum_simple_param(
        dummy_builder, model_data, obs_data
):
    """Test that handle_sum() returns correct values when passed a params_dict
    with a single variable"""

    builder = dummy_builder
    builder.domain_stations = ['KAAA', 'KBBB']
    builder.obs_data = obs_data
    builder.model_data = model_data
    params_dict = {'temperature': 'temperature'}
    sums = builder.handle_sum(params_dict)

    assert sums == {
        'num_recs': 2,
        'sum_obs': 38,
        'sum_model': 35,
        'sum_diff': -3,
        'sum2_diff': 29,
        'sum_abs': 7
    }

def test_handle_sum_obj_param(
        dummy_builder, model_data, obs_data
):
    """Test that handle_sum() returns correct values when passed a params_dict
    with a dict of model and obs variables"""

    builder = dummy_builder
    builder.domain_stations = ['KAAA', 'KBBB']
    builder.obs_data = obs_data
    builder.model_data = model_data
    params_dict = {
        'model': 'temperature_adj',
        'obs': 'temperature',
        }
    sums = builder.handle_sum(params_dict)

    assert sums == {
        'num_recs': 2,
        'sum_obs': 38,
        'sum_model': 36,
        'sum_diff': -2,
        'sum2_diff': 2,
        'sum_abs': 2
    }


