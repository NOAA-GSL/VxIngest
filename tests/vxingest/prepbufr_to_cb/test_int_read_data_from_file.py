import json
from pathlib import Path

import ncepbufr
import numpy.ma as ma
from vxingest.prepbufr_to_cb.prepbufr_builder import PrepbufrRaobsObsBuilderV01

with Path(
    "tests/vxingest/prepbufr_to_cb/testdata/prepbufr_raob_template.json"
).open() as f:
    template = json.load(f)


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
    header = builder.read_data_from_bufr(bufr, template["header"])
    bufr.close()
    assert header is not None
    assert header["station_id"] == "89571"
    assert header["lon"] == 77.97
    assert header["lat"] == -68.58
    assert header["obs-cycle_time"] == -0.5
    assert header["elevation"] == 18.0
    assert header["data_dump_report_type"] == 11.0
    assert header["report_type"] == 120


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
    qm_data = builder.read_data_from_bufr(bufr, template["q_marker"])
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
    obs_err = builder.read_data_from_bufr(bufr, template["obs_err"])
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
    obs_data = builder.read_data_from_bufr(bufr, template["obs_data"])
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

    raw_data = builder.read_data_from_file(queue_element, template)
    # assert keys
    assert raw_data is not None
    assert len(raw_data.keys()) == 626

    # assert values for MASS - Rawinsonde report _type TYP 120
    assert len(raw_data["89571"][120]["header"].keys()) == 7
    assert len(raw_data["89571"][120]["q_marker"].keys()) == 8
    assert len(raw_data["89571"][120]["obs_err"].keys()) == 5
    assert len(raw_data["89571"][120]["obs_data"].keys()) == 10
    # assert header
    assert raw_data["89571"][120]["header"]["station_id"] == "89571"
    assert raw_data["89571"][120]["header"]["lon"] == 77.97
    assert raw_data["89571"][120]["header"]["lat"] == -68.58
    assert raw_data["89571"][120]["header"]["obs-cycle_time"] == -0.5
    assert raw_data["89571"][120]["header"]["report_type"] == 120.0
    assert raw_data["89571"][120]["header"]["elevation"] == 18.0
    assert raw_data["89571"][120]["header"]["data_dump_report_type"] == 11.0
    # assert q_marker
    assert (
        raw_data["89571"][120]["q_marker"]["pressure_q_marker"].compressed()
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
        ]
    ).all()
    assert (
        raw_data["89571"][120]["q_marker"]["specific_humidity_q_marker"].compressed()
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
            15.0,
            15.0,
            15.0,
            15.0,
            9.0,
            9.0,
            9.0,
            9.0,
            9.0,
            9.0,
            9.0,
            9.0,
            9.0,
            9.0,
            9.0,
            9.0,
            9.0,
            9.0,
            9.0,
        ]
    ).all()
    assert (
        raw_data["89571"][120]["q_marker"]["temperature_q_marker"].compressed()
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
        ]
    ).all()
    assert (
        raw_data["89571"][120]["q_marker"]["height_q_marker"].compressed()
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
        ]
    ).all()
    assert (
        raw_data["89571"][120]["q_marker"]["u_v_wind_q_marker"].compressed() == []
    ).all()
    assert (
        raw_data["89571"][120]["q_marker"]["wind_speed_q_marker"].compressed() == []
    ).all()
    assert (
        raw_data["89571"][120]["q_marker"]["wind_direction_q_marker"].compressed() == []
    ).all()
    assert (
        raw_data["89571"][120]["q_marker"]["u_v_component_wind_q_marker"].compressed()
        == []
    ).all()

    # assert obs_err
    assert (
        raw_data["89571"][120]["obs_err"]["pressure_obs_err"].compressed() == [1.1]
    ).all()
    assert (
        raw_data["89571"][120]["obs_err"]["height_obs_err"].compressed() == []
    ).all()
    assert (
        raw_data["89571"][120]["obs_err"]["relative_humidity_obs_err"].compressed()
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
        ]
    ).all()
    assert (
        raw_data["89571"][120]["obs_err"]["temperature_obs_err"].compressed()
        == [
            1.20,
            1.0,
            1.0,
            0.8,
            0.8,
            0.8,
            0.8,
            0.8,
            0.8,
            0.8,
            0.8,
            0.8,
            0.8,
            0.8,
            0.8,
            0.9,
            1.0,
            1.20,
            1.20,
            1.0,
            0.8,
            0.8,
            0.9,
            0.9,
            1.0,
            1.0,
            1.20,
            1.3,
            1.5,
            1.5,
            1.5,
        ]
    ).all()
    assert (raw_data["89571"][120]["obs_err"]["winds_obs_err"].compressed() == []).all()
    # obs_data
    assert (
        raw_data["89571"][120]["obs_data"]["temperature"].compressed()
        == [
            -7.2,
            -9.8,
            -9.8,
            -14.1,
            -16.1,
            -24.6,
            -28.4,
            -35.3,
            -41.3,
            -43.9,
            -44.1,
            -45.3,
            -45.7,
            -50.1,
            -50.9,
            -48.9,
            -46.7,
            -48.3,
            -48.3,
            -50.1,
            -52.9,
            -55.3,
            -56.3,
            -59.1,
            -57.3,
            -57.1,
            -60.7,
            -59.1,
            -56.9,
            -55.3,
            -55.5,
        ]
    ).all()
    assert (
        raw_data["89571"][120]["obs_data"]["dewpoint"].compressed()
        == [
            -9.7,
            -10.2,
            -10.2,
            -15.4,
            -17.5,
            -25.9,
            -29.9,
            -41.3,
            -45.0,
            -46.7,
            -47.4,
            -56.3,
            -62.7,
            -65.1,
            -64.9,
            -74.9,
            -78.7,
            -80.3,
            -82.3,
            -83.1,
            -85.9,
            -87.3,
            -87.3,
            -88.1,
            -89.3,
            -89.1,
            -89.7,
            -90.1,
            -88.9,
            -88.3,
            -89.5,
        ]
    ).all()
    assert (raw_data["89571"][120]["obs_data"]["rh"].compressed() == []).all()
    assert (
        raw_data["89571"][120]["obs_data"]["specific_humidity"].compressed()
        == [
            1861.0,
            1893.0,
            1903.0,
            1349.0,
            1196.0,
            654.0,
            480.0,
            170.0,
            126.0,
            110.0,
            102.0,
            36.0,
            17.0,
            14.0,
            15.0,
            4.0,
            3.0,
            2.0,
            2.0,
            2.0,
            2.0,
            2.0,
            3.0,
            3.0,
            4.0,
            4.0,
            4.0,
            5.0,
            12.0,
            16.0,
            16.0,
        ]
    ).all()
    assert (
        raw_data["89571"][120]["obs_data"]["pressure"].compressed()
        == [
            1000.0,
            979.0,
            925.0,
            920.0,
            850.0,
            804.0,
            700.0,
            657.0,
            589.0,
            532.0,
            502.0,
            500.0,
            486.0,
            453.0,
            400.0,
            389.0,
            300.0,
            279.0,
            250.0,
            200.0,
            150.0,
            100.0,
            70.0,
            50.0,
            42.80,
            30.0,
            28.6,
            23.6,
            20.0,
            10.0,
            8.3,
            7.0,
        ]
    ).all()
    assert (
        raw_data["89571"][120]["obs_data"]["height"].compressed()
        == [
            -1.4700e02,
            1.8000e01,
            4.5900e02,
            1.0404e03,
            1.1060e03,
            2.3246e03,
            2.5500e03,
            3.7759e03,
            3.7759e03,
            3.7759e03,
            3.7759e03,
            4.9000e03,
            2.3786e03,
            2.3786e03,
            6.3800e03,
            3.0228e03,
            8.2500e03,
            1.9356e03,
            9.4600e03,
            1.0930e04,
            1.2820e04,
            1.5440e04,
            1.7730e04,
            1.9870e04,
            5.1725e03,
            2.3080e04,
            4.0855e03,
            4.0855e03,
            2.5620e04,
            2.9990e04,
            3.6503e03,
            3.6503e03,
        ]
    ).all()
    assert (
        raw_data["89571"][220]["obs_data"]["wind_speed"].compressed()
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

    # assert values for MASS - Rawinsonde report _type TYP 220
    assert len(raw_data["89571"][220]["header"].keys()) == 7
    assert len(raw_data["89571"][220]["q_marker"].keys()) == 8
    assert len(raw_data["89571"][220]["obs_err"].keys()) == 5
    assert len(raw_data["89571"][220]["obs_data"].keys()) == 10
    # assert header
    assert raw_data["89571"][220]["header"]["station_id"] == "89571"
    assert raw_data["89571"][220]["header"]["lon"] == 77.970
    assert raw_data["89571"][220]["header"]["lat"] == -68.580
    assert raw_data["89571"][220]["header"]["obs-cycle_time"] == -0.5
    assert raw_data["89571"][220]["header"]["report_type"] == 220.0
    assert raw_data["89571"][220]["header"]["elevation"] == 18.0
    assert raw_data["89571"][220]["header"]["data_dump_report_type"] == 11.0
    # assert q_marker
    assert (
        raw_data["89571"][220]["q_marker"]["pressure_q_marker"].compressed()
        == [
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
        ]
    ).all()
    assert (
        raw_data["89571"][220]["q_marker"]["specific_humidity_q_marker"].compressed()
        == []
    ).all()
    assert (
        raw_data["89571"][220]["q_marker"]["temperature_q_marker"].compressed() == []
    ).all()
    assert (
        raw_data["89571"][220]["q_marker"]["height_q_marker"].compressed() == []
    ).all()
    assert (
        raw_data["89571"][220]["q_marker"]["u_v_wind_q_marker"].compressed()
        == [
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
        ]
    ).all()
    assert (
        raw_data["89571"][220]["q_marker"]["wind_speed_q_marker"].compressed() == []
    ).all()
    assert (
        raw_data["89571"][220]["q_marker"]["wind_direction_q_marker"].compressed()
        == [
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
        ]
    ).all()
    assert (
        raw_data["89571"][220]["q_marker"]["u_v_component_wind_q_marker"].compressed()
        == [
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
        ]
    ).all()

    # assert obs_err
    assert (
        raw_data["89571"][220]["obs_err"]["pressure_obs_err"].compressed() == [1.1]
    ).all()
    assert (
        raw_data["89571"][220]["obs_err"]["height_obs_err"].compressed() == []
    ).all()
    assert (
        raw_data["89571"][220]["obs_err"]["relative_humidity_obs_err"].compressed()
        == []
    ).all()
    assert (
        raw_data["89571"][220]["obs_err"]["temperature_obs_err"].compressed() == []
    ).all()
    assert (
        raw_data["89571"][220]["obs_err"]["winds_obs_err"].compressed()
        == [
            1.40,
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
            2.30,
            2.40,
            2.5,
            2.6,
            2.6,
            2.7,
            2.80,
            3.0,
            3.0,
            3.2,
            3.2,
            2.90,
            2.7,
            2.40,
            2.30,
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
    # obs_data
    assert (raw_data["89571"][220]["obs_data"]["temperature"].compressed() == []).all()
    assert (raw_data["89571"][220]["obs_data"]["dewpoint"].compressed() == []).all()
    assert (raw_data["89571"][220]["obs_data"]["rh"].compressed() == []).all()
    assert (
        raw_data["89571"][220]["obs_data"]["specific_humidity"].compressed() == []
    ).all()
    assert (
        raw_data["89571"][220]["obs_data"]["pressure"].compressed()
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
            65.10,
            50.0,
            38.1,
            30.0,
            29.0,
            26.6,
            21.1,
            20.0,
            19.70,
            15.20,
            10.0,
            9.600,
            8.9,
            7.10,
            7.0,
        ]
    ).all()
    assert (raw_data["89571"][220]["obs_data"]["height"].compressed() == []).all()
    assert (
        raw_data["89571"][220]["obs_data"]["wind_speed"].compressed()
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
