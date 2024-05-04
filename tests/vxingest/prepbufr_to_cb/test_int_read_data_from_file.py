from multiprocessing import Queue

import ncepbufr
import numpy.ma as ma
from vxingest.prepbufr_to_cb.prepbufr_builder import PrepbufrRaobsObsBuilderV01


def stub_worker_log_configurer(queue: Queue):
    """A stub to replace log_config.worker_log_configurer"""
    pass


def test_read_header():
    queue_element = (
        "/opt/data/prepbufr_to_cb/input_files/241011200.gdas.t12z.prepbufr.nr"
    )
    builder = PrepbufrRaobsObsBuilderV01(
        None,
        {
            "template": {"subset": "RAOB"},
            "ingest_document_ids": {},
            "file_type": "PREPBUFR",
            "origin_type": "GDAS",
        },
    )
    bufr = ncepbufr.open(queue_element)
    bufr.advance()
    assert bufr.msg_type == "ADPUPA", "Expected ADPUPA message type"
    bufr.load_subset()
    hdr_template = {
        "station_id": {"mnemonic": "SID", "intent": "str"},
        "lon": {"mnemonic": "XOB", "intent": "float"},
        "lat": {"mnemonic": "YOB", "intent": "float"},
        "obs-cycle_time": {"mnemonic": "DHR", "intent": "float"},
        "station_type": {"mnemonic": "TYP", "intent": "int"},
        "elevation": {"mnemonic": "ELV", "intent": "float"},
        "report_type": {"mnemonic": "T29", "intent": "int"},
    }

    header = builder.read_data_from_bufr(bufr, hdr_template)
    bufr.close()
    assert header is not None
    assert header["station_id"] == "89571"
    assert header["lon"] == 77.97000000000001
    assert header["lat"] == -68.58000000000001
    assert header["obs-cycle_time"] == -0.5
    assert header["station_type"] == 120
    assert header["elevation"] == 18.0
    assert header["report_type"] == 11


def test_read_qm_data():
    queue_element = (
        "/opt/data/prepbufr_to_cb/input_files/241011200.gdas.t12z.prepbufr.nr"
    )
    builder = PrepbufrRaobsObsBuilderV01(
        None,
        {
            "template": {"subset": "RAOB"},
            "ingest_document_ids": {},
            "file_type": "PREPBUFR",
            "origin_type": "GDAS",
        },
    )
    bufr = ncepbufr.open(queue_element)
    bufr.advance()
    assert bufr.msg_type == "ADPUPA", "Expected ADPUPA message type"
    bufr.load_subset()
    q_marker_template = {
        "pressure_q_marker": {"mnemonic": "PQM", "intent": "int"},
        "specific_humidity_q_marker": {"mnemonic": "QQM", "intent": "int"},
        "temperature_q_marker": {"mnemonic": "TQM", "intent": "int"},
        "height_q_marker": {"mnemonic": "ZQM", "intent": "int"},
        "u_v_wind_q_marker": {"mnemonic": "WQM", "intent": "int"},
    }

    qm_data = builder.read_data_from_bufr(bufr, q_marker_template)
    bufr.close()
    assert qm_data is not None


def test_read_obs_err():
    queue_element = (
        "/opt/data/prepbufr_to_cb/input_files/241011200.gdas.t12z.prepbufr.nr"
    )
    builder = PrepbufrRaobsObsBuilderV01(
        None,
        {
            "template": {"subset": "RAOB"},
            "ingest_document_ids": {},
            "file_type": "PREPBUFR",
            "origin_type": "GDAS",
        },
    )
    bufr = ncepbufr.open(queue_element)
    bufr.advance()
    assert bufr.msg_type == "ADPUPA", "Expected ADPUPA message type"
    bufr.load_subset()
    obs_err_template = {
        "pressure_obs_err": {"mnemonic": "POE", "intent": "float"},
        "relative_humidity_obs_err": {"mnemonic": "QOE", "intent": "float"},
        "temperature_obs_err": {"mnemonic": "TOE", "intent": "float"},
        "winds_obs_err": {"mnemonic": "WOE", "intent": "float"},
    }
    obs_err = builder.read_data_from_bufr(bufr, obs_err_template)
    bufr.close()
    assert obs_err is not None
    assert ma.is_masked(obs_err["pressure_obs_err"]) is True
    assert ma.is_masked(obs_err["relative_humidity_obs_err"]) is True
    assert ma.is_masked(obs_err["temperature_obs_err"]) is True
    assert ma.is_masked(obs_err["winds_obs_err"]) is True


def test_read_obs_data():
    queue_element = (
        "/opt/data/prepbufr_to_cb/input_files/241011200.gdas.t12z.prepbufr.nr"
    )
    builder = PrepbufrRaobsObsBuilderV01(
        None,
        {
            "template": {"subset": "RAOB"},
            "ingest_document_ids": {},
            "file_type": "PREPBUFR",
            "origin_type": "GDAS",
        },
    )
    bufr = ncepbufr.open(queue_element)
    bufr.advance()
    assert bufr.msg_type == "ADPUPA", "Expected ADPUPA message type"
    bufr.load_subset()
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
    obs_data = builder.read_data_from_bufr(bufr, obs_data_template)
    bufr.close()
    assert obs_data is not None


def test_read_data_from_file():
    queue_element = (
        "/opt/data/prepbufr_to_cb/input_files/241011200.gdas.t12z.prepbufr.nr"
    )
    builder = PrepbufrRaobsObsBuilderV01(
        None,
        {
            "template": {"subset": "RAOB"},
            "ingest_document_ids": {},
            "file_type": "PREPBUFR",
            "origin_type": "GDAS",
        },
    )
    templates = {}
    templates["header"] = {
        "station_id": {"mnemonic": "SID", "intent": "str"},
        "lon": {"mnemonic": "XOB", "intent": "float"},
        "lat": {"mnemonic": "YOB", "intent": "float"},
        "obs-cycle_time": {"mnemonic": "DHR", "intent": "float"},
        "station_type": {"mnemonic": "TYP", "intent": "int"},
        "elevation": {"mnemonic": "ELV", "intent": "float"},
        "report_type": {"mnemonic": "T29", "intent": "int"},
    }
    templates["q_marker"] = {
        "pressure_q_marker": {"mnemonic": "PQM", "intent": "int"},
        "specific_humidity_q_marker": {"mnemonic": "QQM", "intent": "int"},
        "temperature_q_marker": {"mnemonic": "TQM", "intent": "int"},
        "height_q_marker": {"mnemonic": "ZQM", "intent": "int"},
        "u_v_wind_q_marker": {"mnemonic": "WQM", "intent": "int"},
    }
    templates["obs_err"] = {
        "pressure_obs_err": {"mnemonic": "POE", "intent": "float"},
        "relative_humidity_obs_err": {"mnemonic": "QOE", "intent": "float"},
        "temperature_obs_err": {"mnemonic": "TOE", "intent": "float"},
        "winds_obs_err": {"mnemonic": "WOE", "intent": "float"},
    }
    templates["obs_data"] = {
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
    raw_data = builder.read_data_from_file(queue_element, templates)
    assert raw_data is not None
    assert len(raw_data.keys()) == 626
    assert len(raw_data["89571"]["header"].keys()) == 7
    assert len(raw_data["89571"]["q_marker"].keys()) == 5
    assert len(raw_data["89571"]["obs_err"].keys()) == 4
    assert len(raw_data["89571"]["obs_data"].keys()) == 10
    assert raw_data["89571"]["header"]["station_id"] == "89571"
    assert raw_data["89571"]["header"]["lon"] == 77.97000000000001
    assert raw_data["89571"]["header"]["lat"] == -68.58000000000001
    assert raw_data["89571"]["header"]["obs-cycle_time"] == -0.5
    assert raw_data["89571"]["header"]["station_type"] == 220.0
    assert raw_data["89571"]["header"]["elevation"] == 18.0
    assert raw_data["89571"]["header"]["report_type"] == 11
    assert (
        raw_data["89571"]["q_marker"]["pressure_q_marker"].data
        == [
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
        ]
    ).all()
    assert ma.is_masked(raw_data["89571"]["q_marker"]["specific_humidity_q_marker"])
    assert ma.is_masked(raw_data["89571"]["q_marker"]["temperature_q_marker"])
    assert ma.is_masked(raw_data["89571"]["q_marker"]["height_q_marker"])
    assert (
        raw_data["89571"]["q_marker"]["u_v_wind_q_marker"].data
        == [
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
        ]
    ).all()
    assert ma.is_masked(raw_data["89571"]["obs_err"]["pressure_obs_err"])
    assert ma.is_masked(raw_data["89571"]["obs_err"]["relative_humidity_obs_err"])
    assert ma.is_masked(raw_data["89571"]["obs_err"]["relative_humidity_obs_err"])
    assert (
        raw_data["89571"]["obs_err"]["winds_obs_err"].data
        == [
            1.4000000000000001,
            1.5,
            1.5,
            1.5,
            1.5,
            1.5,
            1.5,
            1.6,
            1.6,
            1.8,
            2.0,
            2.1,
            2.1,
            2.3000000000000003,
            2.4000000000000004,
            2.5,
            2.6,
            2.6,
            2.7,
            2.8000000000000003,
            3.0,
            3.0,
            3.2,
            3.2,
            2.9000000000000004,
            2.7,
            2.4000000000000004,
            2.3000000000000003,
            2.1,
            2.1,
            2.1,
            2.1,
            2.1,
            2.1,
            2.1,
            2.1,
            2.1,
            2.1,
            2.1,
            2.1,
            2.1,
            2.1,
            2.1,
            2.1,
            2.1,
            2.1,
            2.1,
        ]
    ).all()
    assert ma.is_masked(raw_data["89571"]["obs_data"]["temperature"])
    assert ma.is_masked(raw_data["89571"]["obs_data"]["dewpoint"])
    assert (
        raw_data["89571"]["obs_data"]["rh"].data == []
    ).all()
    assert ma.is_masked(raw_data["89571"]["obs_data"]["specific_humidity"])
    assert (
        raw_data["89571"]["obs_data"]["pressure"].data
        == [
            979.0,
            963.0,
            925.0,
            920.0,
            882.0,
            850.0,
            846.0,
            793.0,
            700.0,
            625.0,
            559.0,
            514.0,
            500.0,
            462.0,
            438.0,
            411.0,
            400.0,
            389.0,
            381.0,
            351.0,
            310.0,
            300.0,
            250.0,
            247.0,
            217.0,
            200.0,
            150.0,
            141.0,
            100.0,
            84.7,
            73.5,
            70.0,
            65.10000000000001,
            50.0,
            38.1,
            30.0,
            29.0,
            26.6,
            21.1,
            20.0,
            19.700000000000003,
            15.200000000000001,
            10.0,
            9.600000000000001,
            8.9,
            7.1000000000000005,
            7.0,
        ]
    ).all()
    assert ma.is_masked(raw_data["89571"]["obs_data"]["height"])
    assert (
        raw_data["89571"]["obs_data"]["wind_speed"].data
        == [
            10.0,
            15.0,
            37.0,
            37.0,
            31.0,
            28.0,
            28.0,
            29.0,
            30.0,
            24.0,
            31.0,
            39.0,
            33.0,
            18.0,
            18.0,
            19.0,
            21.0,
            23.0,
            24.0,
            21.0,
            40.0,
            40.0,
            42.0,
            44.0,
            49.0,
            53.0,
            58.0,
            59.0,
            69.0,
            67.0,
            60.0,
            63.0,
            67.0,
            77.0,
            79.0,
            80.0,
            88.0,
            73.0,
            91.0,
            90.0,
            90.0,
            97.0,
            87.0,
            85.0,
            99.0,
            102.0,
            98.0,
        ]
    ).all()
    assert (
        raw_data["89571"]["obs_data"]["U-Wind"].data
        == [
            -1.7000000000000002,
            -5.4,
            -9.5,
            -8.0,
            -2.8000000000000003,
            0.0,
            0.0,
            -7.4,
            -6.5,
            -7.1000000000000005,
            1.4000000000000001,
            5.2,
            4.4,
            -1.6,
            1.6,
            7.5,
            7.6000000000000005,
            9.700000000000001,
            8.8,
            10.8,
            20.5,
            20.3,
            20.3,
            21.3,
            25.1,
            26.900000000000002,
            29.400000000000002,
            28.6,
            35.0,
            32.4,
            29.8,
            31.900000000000002,
            34.4,
            38.300000000000004,
            38.300000000000004,
            39.800000000000004,
            43.800000000000004,
            37.0,
            46.6,
            46.1,
            46.1,
            48.2,
            44.1,
            43.1,
            50.800000000000004,
            51.7,
            49.7,
        ]
    ).all()
    assert (
        raw_data["89571"]["obs_data"]["V-Wind"].data
        == [
            -4.800000000000001,
            -5.4,
            -16.400000000000002,
            -17.2,
            -15.8,
            -14.4,
            -14.4,
            -12.9,
            -14.0,
            -10.200000000000001,
            -15.9,
            -19.400000000000002,
            -16.400000000000002,
            -9.200000000000001,
            -9.200000000000001,
            -6.300000000000001,
            -7.6000000000000005,
            -6.800000000000001,
            -8.8,
            0.9,
            1.8,
            3.6,
            7.4,
            7.800000000000001,
            2.2,
            4.7,
            5.2,
            10.4,
            6.2,
            11.8,
            8.0,
            5.6000000000000005,
            3.0,
            10.3,
            13.9,
            10.700000000000001,
            11.700000000000001,
            6.5,
            4.1000000000000005,
            4.0,
            4.0,
            12.9,
            7.800000000000001,
            7.6000000000000005,
            4.4,
            9.1,
            8.8,
        ]
    ).all()
    assert (
        raw_data["89571"]["obs_data"]["wind_direction"].data
        == [
            20.0,
            45.0,
            30.0,
            25.0,
            10.0,
            0.0,
            0.0,
            30.0,
            25.0,
            35.0,
            355.0,
            345.0,
            345.0,
            10.0,
            350.0,
            310.0,
            315.0,
            305.0,
            315.0,
            265.0,
            265.0,
            260.0,
            250.0,
            250.0,
            265.0,
            260.0,
            260.0,
            250.0,
            260.0,
            250.0,
            255.0,
            260.0,
            265.0,
            255.0,
            250.0,
            255.0,
            255.0,
            260.0,
            265.0,
            265.0,
            265.0,
            255.0,
            260.0,
            260.0,
            265.0,
            260.0,
            260.0,
        ]
    ).all()
