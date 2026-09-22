import numpy as np
import pytest
import xarray as xr

from vxingest.grib2_to_cb.grib_metar_builder import GribModelMetarBuilderV01
from vxingest.grib2_to_cb.grib_raob_builder import GribModelRaobBuilderV01


@pytest.fixture
def empty_builder():
    ingest_doc = {
        "template": {
            "subset": "",
        },
        "validTimeDelta": "",
        "validTimeInterval": "",
    }
    load_spec = ""

    return GribModelMetarBuilderV01(load_spec=load_spec, ingest_document=ingest_doc)


@pytest.fixture
def single_station_list():
    stations = [
        {
            "name": "BOB",
            "geo": [
                {
                    "x_gridpoint": 0.5,
                    "y_gridpoint": 0.5,
                    "elev": 250,
                    "lastTime": 999999999,
                    "firstTime": -1,
                }
            ],
        }
    ]
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
        return np.full(shape=(2, 2), fill_value=val)

    def _make_var_obj(val):
        var_obj = VarObj()
        var_obj.set_values(_make_array(val))
        return var_obj

    return _make_var_obj


def test_handle_normalized_surface_pressure(
    empty_builder, single_station_list, make_var_obj
):
    """Test that get_normalized_surface_pressure() returns correct value
    for example case with 1 station with realistic values"""

    builder = empty_builder
    builder.domain_stations = single_station_list
    builder.ds_translate_item_variables_map = {
        "Surface pressure": make_var_obj(97500),
        "2 metre temperature": make_var_obj(285),
        "2 metre dewpoint temperature": make_var_obj(270),
        "Orography": make_var_obj(300),
        "fcst_valid_epoch": 1234,
    }

    norm_pressure_list = builder.handle_normalized_surface_pressure(params_dict=None)

    assert norm_pressure_list == pytest.approx([980.6153])


def test_handle_normalized_surface_pressure_bad_station_elev(
    empty_builder, single_station_list, make_var_obj
):
    """Test that get_normalized_surface_pressure() returns null value
    when station elevation is unrealistic value"""

    builder = empty_builder
    builder.domain_stations = single_station_list
    builder.domain_stations[0]["geo"][0]["elev"] = 9999
    builder.ds_translate_item_variables_map = {
        "Surface pressure": make_var_obj(97500),
        "2 metre temperature": make_var_obj(285),
        "2 metre dewpoint temperature": make_var_obj(270),
        "Orography": make_var_obj(300),
        "fcst_valid_epoch": 1234,
    }

    norm_pressure_list = builder.handle_normalized_surface_pressure(params_dict=None)

    assert norm_pressure_list == [None]


def test_raob_translate_template_item_uses_requested_level(
    single_station_list,
):
    ingest_doc = {
        "template": {"subset": "RAOB"},
        "validTimeDelta": "",
        "validTimeInterval": "",
    }
    builder = GribModelRaobBuilderV01(load_spec="", ingest_document=ingest_doc)
    builder.domain_stations = single_station_list
    temperature = xr.DataArray(
        np.array([[1.0, 2.0], [3.0, 4.0]]),
        dims=("y", "x"),
        attrs={"units": "K", "long_name": "Temperature"},
    )
    builder.ds_translate_item_variables_map = {
        "fcst_valid_epoch": 1234,
        500: {"t": temperature},
    }

    translated = builder.translate_template_item("*t", level=500)

    assert translated == [("1.0", "2.5")]
    assert builder.ds_translate_item_variables_map[500]["t"].dims == ("y", "x")
    assert builder.ds_translate_item_variables_map[500]["t"].attrs["units"] == "K"


def test_raob_wind_speed_uses_u_and_v_interpolated_values():
    ingest_doc = {
        "template": {"subset": "RAOB"},
        "validTimeDelta": "",
        "validTimeInterval": "",
    }
    builder = GribModelRaobBuilderV01(load_spec="", ingest_document=ingest_doc)
    params = {
        "level": 500,
        "u": [(3.0, 3.0)],
        "v": [(4.0, 4.0)],
    }

    assert builder.handle_raob_variable(params, "u") == [3.0]
    assert builder.handle_raob_variable(params, "v") == [4.0]
    assert builder.handle_wind_speed(params) == pytest.approx([(5.0 / 0.447) + 0.5])


def test_raob_named_variable_handler_uses_template_short_name():
    ingest_doc = {
        "template": {"subset": "RAOB"},
        "validTimeDelta": "",
        "validTimeInterval": "",
    }
    builder = GribModelRaobBuilderV01(load_spec="", ingest_document=ingest_doc)
    builder.domain_stations = [
        {
            "name": "BOB",
            "geo": [
                {
                    "x_gridpoint": 0.5,
                    "y_gridpoint": 0.5,
                    "elev": 250,
                    "lastTime": 999999999,
                    "firstTime": -1,
                }
            ],
        }
    ]
    builder.ds_translate_item_variables_map = {
        "fcst_valid_epoch": 1234,
        500: {"u": xr.DataArray(np.array([[3.0, 3.0], [3.0, 3.0]]), dims=("y", "x"))},
    }

    translated = builder.handle_named_function("&handle_raob_variable|*u", level=500)

    assert translated == ["3.0"]
