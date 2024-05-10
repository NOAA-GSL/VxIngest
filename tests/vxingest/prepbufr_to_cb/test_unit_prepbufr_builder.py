from unittest.mock import MagicMock

import numpy as np
import numpy.ma as ma
import pytest
from vxingest.prepbufr_to_cb.prepbufr_builder import PrepbufrRaobsObsBuilderV01


@pytest.fixture()
def mock_header_bufr():
    # Create a mock bufr object
    mock_bufr = MagicMock()
    mock_bufr.read_subset.return_value.squeeze.return_value = [
        b"SID123",
        45.6789,
        -123.4567,
        0.5,
        1,
        100.0,
        2,
    ]
    return mock_bufr

@pytest.fixture()
def mock_err_bufr():
    # Create a mock bufr object
    mock_err_bufr = MagicMock()
    mock_err_bufr.read_subset.return_value.squeeze.return_value = [
        1.10000,
        2.00000,
        1.20000,
        1.40000,
    ]
    return mock_err_bufr

@pytest.fixture()
def mock_obs_bufr():
    # Create a mock bufr object
    mock_obs_bufr = MagicMock()
    mock_obs_bufr.read_subset.return_value.squeeze.return_value = [
        -7.20000,
        -10.2000,
        1.20000,
        1861.0,
        1000.0,
        1000.0,
        1.20000,
        1.40000,
        -7.20000,
        -10.2000,
    ]
    return mock_obs_bufr


def test_read_header(mock_header_bufr):
    # Create an instance of PrepbufrBuilder
    builder = PrepbufrRaobsObsBuilderV01(
        None,
        {
            "template": {"subset": "RAOB"},
            "ingest_document_ids": {},
            "file_type": "PREPBUFR",
            "origin_type": "GDAS",
        },
    )

    hdr_template={
            "station_id": {"mnemonic":"SID","intent":"str"},
            "lon": {"mnemonic":"XOB", "intent":"float"},
            "lat": {"mnemonic":"YOB", "intent":"float"},
            "obs-cycle_time": {"mnemonic":"DHR", "intent":"float"},
            "station_type": {"mnemonic":"TYP", "intent":"int"},
            "elevation": {"mnemonic":"ELV", "intent":"float"},
            "report_type": {"mnemonic":"T29", "intent":"int"}
        }
    # Call the read_header method with the mock bufr object
    header_data = builder.read_data_from_bufr(mock_header_bufr, hdr_template)

    # Assert the expected values
    assert header_data["station_id"] == "SID123"
    assert header_data["lon"] == 45.679
    assert header_data["lat"] == -123.457
    assert header_data["obs-cycle_time"] == 0.5
    assert header_data["station_type"] == 1
    assert header_data["elevation"] == 100.0
    assert header_data["report_type"] == 2

def test_read_obs_err(mock_err_bufr):
    # Create an instance of PrepbufrBuilder
    builder = PrepbufrRaobsObsBuilderV01(
        None,
        {
            "template": {"subset": "RAOB"},
            "ingest_document_ids": {},
            "file_type": "PREPBUFR",
            "origin_type": "GDAS",
        },
    )
    obs_err_template = {
        "pressure_obs_err": {"mnemonic":"POB", "intent":"float"},
        "relative_humidity_obs_err": {"mnemonic":"RHO", "intent":"float"},
        "temperature_obs_err": {"mnemonic":"TOB", "intent":"float"},
        "winds_obs_err": {"mnemonic":"FFO", "intent":"float"},
    }
    # Call the read_obs_err method with the mock bufr object
    obs_err_data = builder.read_data_from_bufr(mock_err_bufr, obs_err_template)

    # Assert the expected values
    assert obs_err_data["pressure_obs_err"] == 1.10000
    assert obs_err_data["relative_humidity_obs_err"] == 2.00000
    assert obs_err_data["temperature_obs_err"] == 1.20000
    assert obs_err_data["winds_obs_err"] == 1.40000

def test_interpolate_heights():
    # Create an instance of PrepbufrBuilder
    builder = PrepbufrRaobsObsBuilderV01(
        None,
        {
            "template": {"subset": "RAOB"},
            "ingest_document_ids": {},
            "file_type": "PREPBUFR",
            "origin_type": "GDAS",
        },
    )

    # Define the test data
    height = ma.asarray([1000, 2000, np.nan, 4000, np.nan, 6000])
    pressure = ma.asarray([1000, 900, 800, 700, 600, 500])
    temperature = ma.asarray([20, 15, 10, 5, 0, -5])
    specific_humidity = ma.asarray([50, 60, 70, 80, 90, 100])

    # Call the interpolate_heights method
    interpolated_height = builder.interpolate_heights(height, pressure, temperature, specific_humidity)

    # Assert the expected interpolated heights
    assert interpolated_height[0][0] == 1000.0
    assert interpolated_height[0][1] == 2000.0
    assert not ma.is_masked(interpolated_height[0][2])
    assert interpolated_height[0][3] == 4000.0
    assert not ma.is_masked(interpolated_height[0][4])
    assert interpolated_height[0][5] == 6000.0



def test_read_obs_data(mock_obs_bufr):
    # Create an instance of PrepbufrBuilder
    builder = PrepbufrRaobsObsBuilderV01(
        None,
        {
            "template": {"subset": "RAOB"},
            "ingest_document_ids": {},
            "file_type": "PREPBUFR",
            "origin_type": "GDAS",
        },
    )
    obs_data_template = {
        "temperature": {"mnemonic": "TOB", "intent": "float"},
        "dewpoint": {"mnemonic": "TDO", "intent": "float"},
        "rh": {"mnemonic": "RHO", "intent": "float"},
        "specific_humidity": {"mnemonic": "QOB", "intent": "float"},
        "pressure": {"mnemonic": "POB", "intent": "float"},
        "height": {"mnemonic": "ZOB", "intent": "float"},
        "wind_speed": {"mnemonic": "FFO", "intent": "float"},
        "U-Wind": {"mnemonic": "UOB", "intent": "float"},
        "V-Wind": {"mnemonic": "VOB", "intent": "float"},
        "wind_direction": {"mnemonic": "DDO", "intent": "float"},
    }


    # Call the read_obs_data method with the mock bufr object
    obs_data = builder.read_data_from_bufr(mock_obs_bufr, obs_data_template)

    # Assert the expected observation data
    assert obs_data["temperature"] == -7.20000
    assert obs_data["dewpoint"] == -10.2000
    assert obs_data["rh"].size == 0
    assert obs_data["specific_humidity"] == 1861.0
    assert obs_data["pressure"] == 1000.0
    assert obs_data["height"].shape == ()
    assert obs_data["wind_speed"] == 1.2
    assert obs_data["U-Wind"] == 1.4
    assert obs_data["V-Wind"] == -7.2
    assert obs_data["wind_direction"] == -10.2
