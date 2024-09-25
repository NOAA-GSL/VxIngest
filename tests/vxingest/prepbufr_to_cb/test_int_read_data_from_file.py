import os
import pathlib

import mysql.connector
import ncepbufr
import pytest
import yaml

from vxingest.prepbufr_to_cb.prepbufr_builder import PrepbufrRaobsObsBuilderV01
from vxingest.prepbufr_to_cb.run_ingest_threads import VXIngest


def setup_cb_connection():
    """test setup"""
    _vx_ingest = VXIngest()
    _vx_ingest.credentials_file = os.environ["CREDENTIALS"]
    _vx_ingest.cb_credentials = _vx_ingest.get_credentials(_vx_ingest.load_spec)
    _vx_ingest.cb_credentials["collection"] = "RAOB"
    _vx_ingest.connect_cb()
    _vx_ingest.load_spec["ingest_document_ids"] = _vx_ingest.collection.get(
        "JOB-TEST:V01:RAOB:PREPBUFR:OBS"
    ).content_as[dict]["ingest_document_ids"]
    return _vx_ingest


def setup_mysql_connection():
    """test setup"""
    credentials_file = os.environ["CREDENTIALS"]
    with pathlib.Path(credentials_file).open(encoding="utf-8") as _f:
        _yaml_data = yaml.load(_f, yaml.SafeLoader)
        _mysql_host = _yaml_data["mysql_host"]
        _mysql_user = _yaml_data["mysql_user"]
        _mysql_password = _yaml_data["mysql_password"]
    _mysql_db = mysql.connector.connect(
        host=_mysql_host, user=_mysql_user, password=_mysql_password
    )
    return _mysql_db


@pytest.mark.integration
def test_read_header():
    queue_element = (
        "/opt/data/prepbufr_to_cb/input_files/241011200.gdas.t12z.prepbufr.nr"
    )
    vx_ingest = setup_cb_connection()
    ingest_doc = vx_ingest.collection.get("MD:V01:RAOB:obs:ingest:prepbufr").content_as[
        dict
    ]
    template = ingest_doc["mnemonic_mapping"]
    builder = PrepbufrRaobsObsBuilderV01(
        None,
        ingest_doc,
    )

    bufr = ncepbufr.open(queue_element)
    bufr.advance()
    assert bufr.msg_type == template["bufr_msg_type"], "Expected ADPUPA message type"
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


@pytest.mark.integration
def test_read_obs_err():
    vx_ingest = setup_cb_connection()
    ingest_doc = vx_ingest.collection.get("MD:V01:RAOB:obs:ingest:prepbufr").content_as[
        dict
    ]
    template = ingest_doc["mnemonic_mapping"]
    queue_element = (
        "/opt/data/prepbufr_to_cb/input_files/241011200.gdas.t12z.prepbufr.nr"
    )
    builder = PrepbufrRaobsObsBuilderV01(
        None,
        ingest_doc,
    )
    bufr = ncepbufr.open(queue_element)
    bufr.advance()
    assert bufr.msg_type == template["bufr_msg_type"], "Expected ADPUPA message type"
    bufr.load_subset()
    obs_err = builder.read_data_from_bufr(bufr, template["obs_err"])
    bufr.close()
    assert obs_err is not None
    assert obs_err["pressure_obs_err"] == [
        None,
        1.1,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    ]
    assert obs_err["relative_humidity_obs_err"] == [
        None,
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
    assert obs_err["temperature_obs_err"] == [
        None,
        1.2,
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
        1.2,
        1.2,
        1.0,
        0.8,
        0.8,
        0.9,
        0.9,
        1.0,
        1.0,
        1.2,
        1.3,
        1.5,
        1.5,
        1.5,
    ]
    assert obs_err["winds_obs_err"] is None


@pytest.mark.integration
def test_read_obs_data():
    vx_ingest = setup_cb_connection()
    ingest_doc = vx_ingest.collection.get("MD:V01:RAOB:obs:ingest:prepbufr").content_as[
        dict
    ]
    template = ingest_doc["mnemonic_mapping"]
    queue_element = (
        "/opt/data/prepbufr_to_cb/input_files/241011200.gdas.t12z.prepbufr.nr"
    )
    builder = PrepbufrRaobsObsBuilderV01(
        None,
        ingest_doc,
    )
    bufr = ncepbufr.open(queue_element)
    bufr.advance()
    assert bufr.msg_type == template["bufr_msg_type"], "Expected ADPUPA message type"
    bufr.load_subset()
    obs_data = builder.read_data_from_bufr(bufr, template["obs_data_120"])
    bufr.close()
    assert obs_data is not None


def test_july_31_2024_0Z_data_diffs_with_legacy():
    """
    This test compares the data that has been imported to CB for the test prepbufr file
    /opt/data/prepbufr_to_cb/input_files/242131200.gdas.t12z.prepbufr.nr
    with the data that has been imported to MYSQL from the legacy system
    for the same file.
    """
    mysql_db = setup_mysql_connection()
    mysql_db_cursor = mysql_db.cursor()
    stmnt_mysql = "select wmoid, press pressure, z height, t temperature, dp dewpoint, rh relative_humidity, wd wind_direction, ws wind_speed from ruc_ua_pb.RAOB where date = '2024-07-31'  and hour = 0 order by wmoid, press;"
    mysql_db_cursor.execute(stmnt_mysql)
    mysql_result = mysql_db_cursor.fetchall()

    vx_ingest = VXIngest()
    vx_ingest.credentials_file = os.environ["CREDENTIALS"]
    vx_ingest.cb_credentials = vx_ingest.get_credentials(vx_ingest.load_spec)
    vx_ingest.cb_credentials["collection"] = "RAOB"
    vx_ingest.connect_cb()
    cb_statement = "SELECT d.data  FROM vxdata._default.RAOB AS d WHERE type='DD' AND subset='RAOB' AND docType='obs' AND subDocType = 'prepbufr' AND fcstValidISO = '2024-07-31T00:00:00Z' ORDER BY d.data.['stationName'], d.data['pressure'];"
    cb_result = list(vx_ingest.cluster.query(cb_statement))
    mysql_wmoid = 0
    mysql_pressure_pos = 1
    mysql_height_pos = 2
    mysql_temperature_pos = 3
    mysql_dewpoint_pos = 4
    mysql_relative_humidity_pos = 5
    mysql_wind_direction_pos = 6
    mysql_wind_speed_pos = 7
    mysql_row = None

    press_tolerance = 1e-2
    height_tolerance = 100
    temperature_tolerance = 15
    dewpoint_tolerance = 20
    relative_humidity_tolerance = 20
    wind_speed_tolerance = 50
    wind_direction_tolerance = 50

    height_max = 0
    temperature_max = 0
    dewpoint_max = 0
    relative_humidity_max = 0
    wind_speed_max = 0
    wind_direction_max = 0

    try:
        for row in range(len(mysql_result)):
            try:
                mysql_row = mysql_result[row]
                m_wmoid = mysql_row[mysql_wmoid]
                m_pressure = mysql_row[mysql_pressure_pos]
                m_height = mysql_row[mysql_height_pos]
                m_temperature = mysql_row[mysql_temperature_pos]
                m_dewpoint = mysql_row[mysql_dewpoint_pos]
                m_relative_humidity = mysql_row[mysql_relative_humidity_pos]
                m_wind_direction = mysql_row[mysql_wind_direction_pos]
                m_wind_speed = mysql_row[mysql_wind_speed_pos]
                cb_data = None
                try:
                    for d in cb_result:
                        if (
                            f"{m_wmoid:05}" in d["data"]
                            and d["data"][f"{m_wmoid:05}"]["pressure"] == m_pressure
                        ):
                            cb_data = d["data"][f"{m_wmoid:05}"]
                            break
                    if cb_data is None:
                        continue
                except KeyError:
                    continue

                cb_wmoid = cb_data["stationName"]
                cb_pressure = cb_data["pressure"]
                cb_height = cb_data["height"]
                cb_temperature = cb_data["temperature"]
                cb_dewpoint = cb_data["dewpoint"]
                cb_relative_humidity = cb_data["relative_humidity"]
                cb_wind_direction = cb_data["wind_direction"]
                cb_wind_speed = cb_data["wind_speed"]

                assert (
                    f"{m_wmoid:05}" == cb_wmoid
                ), f"wmoid mismatch: {m_wmoid} != {cb_wmoid}"

                assert (
                    m_pressure == pytest.approx(cb_pressure, abs=press_tolerance)
                ), f"Pressure mismatch: {m_pressure} != {cb_pressure} +- {press_tolerance} for wmoid {m_wmoid} and pressure {m_pressure}"

                if m_height is not None and cb_height is not None:
                    diff = abs(m_height - cb_height)
                    if diff > height_max:
                        height_max = diff
                    assert (
                        m_height == pytest.approx(cb_height, abs=height_tolerance)
                    ), f"Height mismatch: {m_height} != {cb_height} +- {height_tolerance} for wmoid {m_wmoid} and pressure {m_pressure}"

                if m_temperature is not None and cb_temperature is not None:
                    diff = abs(m_temperature / 100 - cb_temperature)
                    if diff > temperature_max:
                        temperature_max = diff
                    assert (
                        m_temperature / 100
                        == pytest.approx(cb_temperature, abs=temperature_tolerance)
                    ), f"Temperature mismatch: {m_temperature / 100} != {cb_temperature} +- {temperature_tolerance} for wmoid {m_wmoid} and pressure {m_pressure}"

                if m_dewpoint is not None and cb_dewpoint is not None:
                    diff = abs(m_dewpoint / 100 - cb_dewpoint)
                    if diff > dewpoint_max:
                        dewpoint_max = diff
                    assert (
                        m_dewpoint / 100
                        == pytest.approx(cb_dewpoint, abs=dewpoint_tolerance)
                    ), f"Dewpoint mismatch: {m_dewpoint / 100} != {cb_dewpoint} +- {dewpoint_tolerance} for wmoid {m_wmoid} and pressure {m_pressure}"

                if m_relative_humidity is not None and cb_relative_humidity is not None:
                    diff = abs(m_relative_humidity - cb_relative_humidity)
                    if diff > relative_humidity_max:
                        relative_humidity_max = diff
                    assert (
                        m_relative_humidity
                        == pytest.approx(
                            cb_relative_humidity, abs=relative_humidity_tolerance
                        )
                    ), f"Relative Humidity mismatch: {m_relative_humidity} != {cb_relative_humidity} +- {relative_humidity_tolerance} for wmoid {m_wmoid} and pressure {m_pressure}"

                if m_wind_speed is not None and cb_wind_speed is not None:
                    diff = abs(m_wind_speed / 100 - cb_wind_speed)
                    if diff > wind_speed_max:
                        wind_speed_max = diff
                    assert (
                        m_wind_speed / 100
                        == pytest.approx(cb_wind_speed, abs=wind_speed_tolerance)
                    ), f"Wind Speed mismatch: {m_wind_speed / 100} != {cb_wind_speed} +- {wind_speed_tolerance} for wmoid {m_wmoid} and pressure {m_pressure}"

                if m_wind_direction is not None and cb_wind_direction is not None:
                    diff = abs(m_wind_direction - cb_wind_direction)
                    if diff > wind_direction_max:
                        wind_direction_max = diff
                    assert (
                        m_wind_direction
                        == pytest.approx(
                            cb_wind_direction, abs=wind_direction_tolerance
                        )
                    ), f"Wind Direction mismatch: {m_wind_direction} != {cb_wind_direction} +- {wind_direction_tolerance} for wmoid {m_wmoid} and pressure {m_pressure}"
            except Exception as _e:
                print(_e)
                # raise _e
    finally:
        print(f"max height diff: {height_max}")
        print(f"max temperature diff: {temperature_max}")
        print(f"max dewpoint diff: {dewpoint_max}")
        print(f"max relative humidity diff: {relative_humidity_max}")
        print(f"max wind speed diff: {wind_speed_max}")
        print(f"max wind direction diff: {wind_direction_max}")

        mysql_db_cursor.close()
        mysql_db.close()
        vx_ingest.cluster.close()
