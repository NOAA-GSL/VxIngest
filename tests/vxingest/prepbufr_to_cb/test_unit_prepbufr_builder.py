from unittest.mock import MagicMock

import pytest
from vxingest.prepbufr_to_cb.prepbufr_builder import (
    PrepbufrBuilder,
    PrepbufrRaobsObsBuilderV01,
)


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
        [-7.20000],
        [-10.2000],
        [1.20000],
        [1861.0],
        [1000.0],
        [1000.0],
        [1.20000],
        [1.40000],
        [-7.20000],
        [-10.2000],
    ]
    return mock_obs_bufr


@pytest.fixture()
def mock_file_bufr():
    # Create a mock bufr object
    mock_file_bufr = MagicMock()
    mock_file_bufr.msg_type = "PREPBUFR"
    mock_file_bufr.msg_date = "2024041012"
    mock_file_bufr.advance.return_value = 0
    mock_file_bufr.load_subset.return_value = 0
    return mock_file_bufr


hdr_template = {
    "events": True,
    "station_id": {"mnemonic": "SID", "intent": "str"},
    "lon": {"mnemonic": "XOB", "intent": "float"},
    "lat": {"mnemonic": "YOB", "intent": "float"},
    "obs-cycle_time": {"mnemonic": "DHR", "intent": "float"},
    "station_type": {"mnemonic": "TYP", "intent": "int"},
    "elevation": {"mnemonic": "ELV", "intent": "float"},
    "report_type": {"mnemonic": "T29", "intent": "int"},
}

obs_err_template = {
    "events": True,
    "pressure_obs_err": {"mnemonic": "POB", "intent": "float"},
    "relative_humidity_obs_err": {"mnemonic": "RHO", "intent": "float"},
    "temperature_obs_err": {"mnemonic": "TOB", "intent": "float"},
    "winds_obs_err": {"mnemonic": "SOB", "intent": "float"},
}

obs_q_marker_template = {
    "events": False,
    "pressure_q_marker": {"mnemonic": "PQM", "intent": "int"},
    "specific_humidity_q_marker": {"mnemonic": "QQM", "intent": "int"},
    "temperature_q_marker": {"mnemonic": "TQM", "intent": "int"},
    "height_q_marker": {"mnemonic": "ZQM", "intent": "int"},
    "u_v_wind_q_marker": {"mnemonic": "WQM", "intent": "int"},
    "wind_direction_q_marker": {"mnemonic": "DFP", "intent": "int"},
    "u_v_component_wind_q_marker": {"mnemonic": "WPC", "intent": "int"},
}

obs_data_template = {
    "events": True,
    "temperature": {
        "mnemonic": "TOB",
        "intent": "float",
        "event_program_code_mnemonic": "TPC",
        "event_value": 1,
    },
    "dewpoint": {"mnemonic": "TDO", "intent": "float"},
    "rh": {"mnemonic": "RHO", "intent": "float"},
    "specific_humidity": {
        "mnemonic": "QOB",
        "intent": "float",
        "event_program_code_mnemonic": "QPC",
        "event_value": 1,
    },
    "pressure": {
        "mnemonic": "POB",
        "intent": "float",
        "event_program_code_mnemonic": "PPC",
        "event_value": 1,
    },
    "height": {
        "mnemonic": "ZOB",
        "intent": "float",
        "event_program_code_mnemonic": "ZPC",
        "event_value": 1,
    },
    "wind_speed": {"mnemonic": "SOB", "intent": "float"},
    "U-Wind": {
        "mnemonic": "UOB",
        "intent": "float",
        "event_program_code_mnemonic": "WPC",
        "event_value": 1,
    },
    "V-Wind": {
        "mnemonic": "VOB",
        "intent": "float",
        "event_program_code_mnemonic": "WPC",
        "event_value": 1,
    },
    "wind_direction": {"mnemonic": "DDO", "intent": "float"},
}


def test_read_header(mock_header_bufr):
    # Create an instance of PrepbufrBuilder
    builder = PrepbufrRaobsObsBuilderV01(
        None,
        {
            "template": {"subset": "RAOB"},
            "ingest_document_ids": {},
            "file_type": "PREPBUFR",
            "origin_type": "GDAS",
            "mnemonic_mapping": hdr_template,
        },
    )

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
            "template": {"subset": "RAOB", "events": True},
            "ingest_document_ids": {},
            "file_type": "PREPBUFR",
            "origin_type": "GDAS",
            "mnemonic_mapping": obs_err_template,
        },
    )
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
            "mnemonic_mapping": obs_data_template,
        },
    )

    # Define the test data
    height = [1000, 2000, 3000, 4000, 5000, 6000]
    pressure = [1000, 900, 800, 700, 600, 500]
    temperature = [20, 15, 10, 5, 0, -5]
    specific_humidity = [50, 60, 70, 80, 90, 100]

    # Call the interpolate_heights method
    interpolated_height = builder.interpolate_heights(
        height, pressure, temperature, specific_humidity
    )

    # Assert the expected interpolated heights
    assert interpolated_height[0] is None


def test_read_obs_data(mock_obs_bufr):
    # Create an instance of PrepbufrBuilder
    builder = PrepbufrRaobsObsBuilderV01(
        None,
        {
            "template": {"subset": "RAOB"},
            "ingest_document_ids": {},
            "file_type": "PREPBUFR",
            "origin_type": "GDAS",
            "mnemonic_mapping": obs_data_template,
        },
    )

    # Call the read_obs_data method with the mock bufr object
    obs_data = builder.read_data_from_bufr(mock_obs_bufr, obs_data_template)

    # Assert the expected observation data
    assert obs_data["temperature"] == [-7.20000]
    assert obs_data["dewpoint"] == [-10.2000]
    assert obs_data["rh"] == [1.2]
    assert obs_data["specific_humidity"] == [1861.0]
    assert obs_data["pressure"] == [1000.0]
    assert obs_data["height"] == [1000.0]
    assert obs_data["wind_speed"] == [1.2]
    assert obs_data["U-Wind"] == [1.4]
    assert obs_data["V-Wind"] == [-7.2]
    assert obs_data["wind_direction"] == [-10.2]
