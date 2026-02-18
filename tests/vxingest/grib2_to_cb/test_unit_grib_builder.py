import numpy as np
import pytest

from vxingest.grib2_to_cb.grib_builder import GribModelBuilderV01


@pytest.fixture
def empty_builder():
    ingest_doc = {
        'template': {
            'subset' : '',
        },
        'validTimeDelta': '',
        'validTimeInterval': '',
    }
    load_spec = ''

    return GribModelBuilderV01(load_spec=load_spec, ingest_document=ingest_doc)

@pytest.fixture
def single_station_list():
    stations = [{
        'name': 'BOB',
        'geo': [{
            'x_gridpoint': 0.5,
            'y_gridpoint': 0.5,
            'elev': 250,
            'lastTime': 999999999,
            'firstTime': -1,
        }]
    }]
    return stations

@pytest.fixture
def make_var_obj():
    """Function to return object with values attribute
        of 2x2 numpy array filled with single value"""
    class VarObj:
        def __init__(self):
            pass

        def set_values(self, arr):
            self.values = arr

    def _make_array(val):
        return np.full(shape=(2,2), fill_value=val)

    def _make_var_obj(val):
        var_obj = VarObj()
        var_obj.set_values(_make_array(val))
        return var_obj

    return _make_var_obj

def test_handle_normalized_surface_pressure(empty_builder, single_station_list, make_var_obj):
    """Test that get_normalized_surface_pressure() returns correct value
        for example case with 1 station with realistic values"""

    builder = empty_builder
    builder.domain_stations = single_station_list
    builder.ds_translate_item_variables_map = {
            'Surface pressure': make_var_obj(97500),
            '2 metre temperature': make_var_obj(285),
            '2 metre dewpoint temperature': make_var_obj(270),
            'Orography': make_var_obj(300),
            'fcst_valid_epoch': 1234,
    }

    norm_pressure_list = builder.handle_normalized_surface_pressure(params_dict=None)

    assert norm_pressure_list == pytest.approx([980.6153])
