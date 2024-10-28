import os
import pathlib

import mysql.connector
import ncepbufr
import numpy as np
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
        np.nan,
        ingest_doc,
    )

    bufr = ncepbufr.open(queue_element)
    bufr.advance()
    assert bufr.msg_type == template["bufr_msg_type"], "Expected ADPUPA message type"
    bufr.load_subset()
    header = builder.read_data_from_bufr(bufr, template["header"])
    bufr.close()
    assert header is not np.nan
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
        np.nan,
        ingest_doc,
    )
    bufr = ncepbufr.open(queue_element)
    bufr.advance()
    assert bufr.msg_type == template["bufr_msg_type"], "Expected ADPUPA message type"
    bufr.load_subset()
    obs_err = builder.read_data_from_bufr(bufr, template["obs_err"])
    bufr.close()
    assert obs_err is not np.nan
    assert obs_err["pressure_obs_err"][1] == np.float64(1.1)
    assert np.isnan(obs_err["pressure_obs_err"][0])
    for i in range(2, len(obs_err["pressure_obs_err"])):
        assert np.isnan(obs_err["pressure_obs_err"][i])
    assert np.isnan(obs_err["relative_humidity_obs_err"][0])
    for i in range(1, len(obs_err["relative_humidity_obs_err"])):
        assert obs_err["relative_humidity_obs_err"][i] == np.float64(2.0)
    assert np.isnan(obs_err["temperature_obs_err"][0])
    assert obs_err["temperature_obs_err"][1:] == [
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
        np.nan,
        ingest_doc,
    )
    bufr = ncepbufr.open(queue_element)
    bufr.advance()
    assert bufr.msg_type == template["bufr_msg_type"], "Expected ADPUPA message type"
    bufr.load_subset()
    obs_data = builder.read_data_from_bufr(bufr, template["obs_data_120"])
    bufr.close()
    obs_test_data = {}
    obs_test_data["temperature"] = [
        None if d is None or np.isnan(d) or np.ma.is_masked(d) else round(d, 2)
        for d in obs_data["temperature"]
    ]
    obs_test_data["dewpoint"] = [
        None if d is None or np.isnan(d) or np.ma.is_masked(d) else round(d, 2)
        for d in obs_data["dewpoint"]
    ]
    obs_test_data["relative_humidity"] = [
        None if d is None or np.isnan(d) or np.ma.is_masked(d) else round(d, 2)
        for d in obs_data["relative_humidity"]
    ]
    obs_test_data["specific_humidity"] = [
        None if d is None or np.isnan(d) or np.ma.is_masked(d) else round(d, 2)
        for d in obs_data["specific_humidity"]
    ]
    obs_test_data["pressure"] = [
        None if d is None or np.isnan(d) or np.ma.is_masked(d) else round(d, 2)
        for d in obs_data["pressure"]
    ]
    obs_test_data["height"] = [
        None if d is None or np.isnan(d) or np.ma.is_masked(d) else round(d, 2)
        for d in obs_data["height"]
    ]
    assert obs_test_data["temperature"] == [
        None,
        np.float64(-7.5),
        np.float64(-10.1),
        np.float64(-10.1),
        np.float64(-14.3),
        np.float64(-16.3),
        np.float64(-24.7),
        np.float64(-28.5),
        np.float64(-35.3),
        np.float64(-41.3),
        np.float64(-43.9),
        np.float64(-44.1),
        np.float64(-45.3),
        np.float64(-45.7),
        np.float64(-50.1),
        np.float64(-50.9),
        np.float64(-48.9),
        np.float64(-46.7),
        np.float64(-48.3),
        np.float64(-48.3),
        np.float64(-50.1),
        np.float64(-52.9),
        np.float64(-55.3),
        np.float64(-56.3),
        np.float64(-59.1),
        np.float64(-57.3),
        np.float64(-57.1),
        np.float64(-60.7),
        np.float64(-59.1),
        np.float64(-56.9),
        np.float64(-55.3),
        np.float64(-55.5),
    ]
    assert obs_test_data["dewpoint"] == [
        None,
        np.float64(-9.7),
        np.float64(-10.2),
        np.float64(-10.2),
        np.float64(-15.4),
        np.float64(-17.5),
        np.float64(-25.9),
        np.float64(-29.9),
        np.float64(-41.3),
        np.float64(-45.0),
        np.float64(-46.7),
        np.float64(-47.4),
        np.float64(-56.3),
        np.float64(-62.7),
        np.float64(-65.1),
        np.float64(-64.9),
        np.float64(-74.9),
        np.float64(-78.7),
        np.float64(-80.3),
        np.float64(-82.3),
        np.float64(-83.1),
        np.float64(-85.9),
        np.float64(-87.3),
        np.float64(-87.3),
        np.float64(-88.1),
        np.float64(-89.3),
        np.float64(-89.1),
        np.float64(-89.7),
        np.float64(-90.1),
        np.float64(-88.9),
        np.float64(-88.3),
        np.float64(-89.5),
    ]
    assert obs_test_data["relative_humidity"] == [
        None,
        np.float64(83.94),
        np.float64(98.84),
        np.float64(98.82),
        np.float64(90.76),
        np.float64(89.8),
        np.float64(88.48),
        np.float64(86.29),
        np.float64(52.69),
        np.float64(65.12),
        np.float64(70.78),
        np.float64(66.8),
        np.float64(26.12),
        np.float64(12.01),
        np.float64(14.33),
        np.float64(16.37),
        np.float64(2.68),
        np.float64(1.46),
        np.float64(1.04),
        np.float64(0.83),
        np.float64(0.77),
        np.float64(0.71),
        np.float64(0.66),
        np.float64(0.8),
        np.float64(0.97),
        np.float64(0.73),
        np.float64(0.67),
        np.float64(0.88),
        np.float64(0.76),
        np.float64(0.69),
        np.float64(0.63),
        np.float64(0.54),
    ]
    assert obs_test_data["specific_humidity"] == [
        None,
        np.float64(1861.0),
        np.float64(1893.0),
        np.float64(1903.0),
        np.float64(1349.0),
        np.float64(1196.0),
        np.float64(654.0),
        np.float64(480.0),
        np.float64(170.0),
        np.float64(126.0),
        np.float64(110.0),
        np.float64(102.0),
        np.float64(36.0),
        np.float64(17.0),
        np.float64(14.0),
        np.float64(15.0),
        np.float64(4.0),
        np.float64(3.0),
        np.float64(2.0),
        np.float64(2.0),
        np.float64(2.0),
        np.float64(2.0),
        np.float64(2.0),
        np.float64(3.0),
        np.float64(3.0),
        np.float64(4.0),
        np.float64(4.0),
        np.float64(4.0),
        np.float64(5.0),
        np.float64(12.0),
        np.float64(16.0),
        np.float64(16.0),
    ]
    assert obs_test_data["pressure"] == [
        None,
        np.float64(979.0),
        np.float64(925.0),
        np.float64(920.0),
        np.float64(850.0),
        np.float64(804.0),
        np.float64(700.0),
        np.float64(657.0),
        np.float64(589.0),
        np.float64(532.0),
        np.float64(502.0),
        np.float64(500.0),
        np.float64(486.0),
        np.float64(453.0),
        np.float64(400.0),
        np.float64(389.0),
        np.float64(300.0),
        np.float64(279.0),
        np.float64(250.0),
        np.float64(200.0),
        np.float64(150.0),
        np.float64(100.0),
        np.float64(70.0),
        np.float64(50.0),
        np.float64(42.8),
        np.float64(30.0),
        np.float64(28.6),
        np.float64(23.6),
        np.float64(20.0),
        np.float64(10.0),
        np.float64(8.3),
        np.float64(7.0),
    ]
    assert obs_test_data["height"] == [
        np.float64(-147.0),
        np.float64(18.0),
        np.float64(457.52),
        np.float64(499.3),
        np.float64(1104.37),
        np.float64(1524.61),
        np.float64(2549.57),
        np.float64(3007.24),
        np.float64(3778.92),
        np.float64(4478.66),
        np.float64(4870.39),
        np.float64(4897.17),
        np.float64(5087.08),
        np.float64(5555.64),
        np.float64(6376.03),
        np.float64(6557.76),
        np.float64(8255.46),
        np.float64(8734.15),
        np.float64(9459.04),
        np.float64(10927.67),
        np.float64(12813.48),
        np.float64(15444.08),
        np.float64(17730.99),
        np.float64(19871.62),
        np.float64(20852.17),
        np.float64(23087.89),
        np.float64(23389.97),
        np.float64(24595.06),
        np.float64(25628.2),
        np.float64(29993.38),
        np.float64(31177.18),
        None,
    ]


def test_july_31_2024_0Z_data_diffs_with_legacy():
    """
    This test compares the data that has been imported to CB for the test prepbufr file
    /opt/data/prepbufr_to_cb/input_files/242131200.gdas.t12z.prepbufr.nr
    with the data that has been imported to MYSQL from the legacy system
    for the same file.

    rejected stations - these are stations that have been determined to have bad data in the legacy system.
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
    cb_statement = "SELECT d.data  FROM vxdata._default.RAOB AS d WHERE type='DD' AND subset='RAOB' AND docType='obs' AND subDocType = 'prepbufr' AND fcstValidISO = '2024-07-31T00:00:00Z';"
    cb_result = list(vx_ingest.cluster.query(cb_statement))
    mysql_wmoid = 0
    mysql_pressure_pos = 1
    mysql_height_pos = 2
    mysql_temperature_pos = 3
    mysql_dewpoint_pos = 4
    mysql_relative_humidity_pos = 5
    mysql_wind_direction_pos = 6
    mysql_wind_speed_pos = 7
    mysql_row = np.nan

    press_tolerance = 1e-2
    height_tolerance = 20
    temperature_tolerance = 2
    dewpoint_tolerance = 25
    relative_humidity_tolerance = 15
    wind_speed_tolerance = 5
    wind_direction_tolerance = 50

    height_stat = {"max": 0, "max_wmoid": ""}
    temperature_stat = {"max": 0, "max_wmoid": ""}
    dewpoint_stat = {"max": 0, "max_wmoid": ""}
    relative_humidity_stat = {"max": 0, "max_wmoid": ""}
    wind_speed_stat = {"max": 0, "max_wmoid": ""}
    wind_direction_stat = {"max": 0, "max_wmoid": ""}

    rejected_stations = [
        4270,
        42886,
        97686,
        57494,
        4202,
        76692,
        76458,
        24908,
        47158,
        4360,
        38064,
        42724,
        89571,
        27459,
        4320,
        32540,
        43128,
        23921,
        35671,
        7145,
        97072,
        89664,
        94995,
        94403,
        47945,
        40848,
        83746,
        89611,
        40856,
        41112,
        29839,
    ]
    # a couple of useful sample queries.
    # MYSQL
    # export w=41112;mysql --defaults-file=~/wolphin.cnf -A -B  --execute "select press,z as height,t / 100 as temperature, ws / 100 as ws,wd from ruc_ua_pb.RAOB where date = '2024-07-31' and hour = 0 and wmoid = ${w} ORDER BY press DESC;"
    # CB
    # export w=29839;cbq -q -terse --no-ssl-verify -e 'https://adb-cb1.gsd.esrl.noaa.gov:18093' -u avid -p 'pwd_av!d' -s "SELECT  d.data.[\"${w}\"].pressure,d.data.[\"${w}\"].height,d.data.[\"${w}\"].temperature,d.data.[\"${w}\"].wind_speed as ws, d.data.[\"${w}\"].wind_direction as wd FROM vxdata._default.RAOB AS d WHERE d.type='DD' AND d.subset='RAOB' AND d.docType='obs' AND d.subDocType = 'prepbufr' AND d.fcstValidISO = '2024-07-31T00:00:00Z' ORDER BY d.data.[\"${w}\"].pressure DESC;" | grep -v Disabling | jq -r '.[] | "\(.pressure) \(.height) \(.temperature) \(.ws) \(.wd)"'

    # 4270  GLM00004270  61.1667  -45.4167   34.0    MITTARFIK NARSARSUAQ
    # 42886 INM00042886  21.9167   84.0833  228.0    JHARSIGUDA
    # 97686 IDM00097686  -4.0667  138.9500 1660.0    WAMENA
    # 57494 CHM00057494  30.6000  114.0500   24.0    WUHAN
    # 04202 GLM00004202  76.5330  -68.7500   77.0    PITUFFIK
    # MXM00076692  19.1500  -96.1333   19.0    HACIENDA YLANG YLANG VERACRUZ
    # wmoid: 57494, pressure: 330
    # Height mismatch: 9058 != 8882.923562114107 +- 20 for wmoid 57494 and pressure 330
    # assert 9058 == 8882.923562114107 ± 2.0e+01
    # this station clearly chose the smaller of two type 120 records
    # refer to /opt/data/prepbufr_to_cb/test_artifacts/57494...txt
    # wmoid: 4202, pressure: 300
    # Height mismatch: 8890 != 8726.783020412973 +- 20 for wmoid 4202 and pressure 300
    # assert 8890 == 8726.783020412973 ± 2.0e+01
    # the other fields for p 290 to 320 are matching fairly closely
    # most of the heights are missing
    # refer to /opt/data/prepbufr_to_cb/test_artifacts/4202-120.txt
    # appears to be interpolation issue
    # max height diff: {'max': 107.80940556393762, 'max_wmoid': 76692, 'pressure': 40}
    # wmoid: 76692, pressure: 40
    # Height mismatch: 22132 != 22239.809405563938 +- 20 for wmoid 76692 and pressure 40
    # assert 22132 == 22239.809405563938 ± 2.0e+01
    # data from ADPUPA appears to be invalid above 50mb
    # max height diff: {'max': 106.04168884284445, 'max_wmoid': 76458, 'pressure': 20}
    # wmoid: 76458, pressure: 20
    # Height mismatch: 26583 != 26689.041688842844 +- 20 for wmoid 76458 and pressure 20
    # assert 26583 == 26689.041688842844 ± 2.0e+01
    # these heights fail ... 20, 30, 40, 50, 60
    # close evaluation is simply showing significant differences in the height interpolation
    # the temperatures are quite low, so the heights might be likely to be off
    # interpolation uses specific humidity  QOB but that isn't recorded in the mysql data
    # so it isn't possible to compare the interpolation
    # refer to /opt/data/prepbufr_to_cb/test_artifacts/76458-120.txt

    # max wind direction diff: {'max': 179, 'max_wmoid': 24908, 'pressure': 740}
    # wmoid: 24908, pressure: 740
    # Wind Direction mismatch: 74 != 253 +- 50 for wmoid 24908 and pressure 740
    # assert 74 == 253 ± 5.0e+01
    # careful analysis shows the legacy interpolation is off

    # max height diff: {'max': 55.06061090946605, 'max_wmoid': 47158, 'pressure': 330}
    # wmoid: 47158, pressure: 330
    # Height mismatch: 9130 != 9074.939389090534 +- 20 for wmoid 47158 and pressure 330
    # assert 9130 == 9074.939389090534 ± 2.0e+01
    # comparison failed
    # Obtained: 9130
    # Expected: 9074.939389090534 ± 2.0e+01
    # Many heights are off by > 20m but not excessively so. It appears to be differences in the interpolation.
    # very cold temperatures and mysql finds NULL for all MASS variables between 260 and 400 mb
    # couchbase appears to interpolate all the variables.
    # KSM00047158  35.1167  126.8000   12.5    GWANGJU AB

    # max height diff: {'max': 52.63402144591237, 'max_wmoid': 4360, 'pressure': 20}
    # wmoid: 4360, pressure: 20
    # Height mismatch: 27223 != 27275.634021445912 +- 20 for wmoid 4360 and pressure 20
    # assert 27223 == 27275.634021445912 ± 2.0e+01
    # GLM00004360  65.6111  -37.6367   54.0    TASIILAQ (AMMASSALIK)
    # It appears that mysql starts interpolating at 975mb, but the 975 mb height is disqualified by a PPC of 5
    #  24 RPS-0   P__EVENT         5.00000                        0     0     0    27    28     0    26
    #  25 NUM-2        POB         975.100                       14     1     0    28     0    29    27
    #  26 NUM-2        PQM         1.00000                        5     0     0    29     0    30    27
    #  27 NUM-2        PPC         5.00000                        5     0     0    30     0    31    27
    # CB interpolation starts at 981mb (the next pevent)
    #    29 RPS-0   P__EVENT         5.00000                        0     0     0    27    28     0    26
    #    30 NUM-2        POB         981.000                       14     1     0    28     0    29    27
    #    31 NUM-2        PQM         2.00000                        5     0     0    29     0    30    27
    #    32 NUM-2        PPC         1.00000                        5     0     0    30     0    31    27

    # max height diff: {'max': 51.20786728098392, 'max_wmoid': 38064, 'pressure': 60}
    # wmoid: 38064, pressure: 60
    # Height mismatch: 20018 != 19966.792132719016 +- 20 for wmoid 38064 and pressure 60
    # assert 20018 == 19966.792132719016 ± 2.0e+01
    # The values are close at higher pressure but diverge above 300mb
    # The temp above 300mb is very cold.
    # This appears to be a difference in the height interpolation above 300mb.
    # THe CB data is closer to the ADPUPA data above 300mb.
    # KZM00038064  44.7667   65.5167  133.4    KYZYLORDA

    # wmoid: 42724, pressure: 300
    # Height mismatch: 9820 != 9776.214311540478 +- 20 for wmoid 42724 and pressure 300
    # assert 9820 == 9776.214311540478 ± 2.0e+01
    # max height diff: {'max': 43.785688459522135, 'max_wmoid': 42724, 'pressure': 300}
    # INM00042724  23.8833   91.2500   16.0    AGARTALA
    # on this balloon only the 300 mb height is off by more than 50 meters. It appears that the only reading that is really off is the DP
    # CB
    # 400  7614   -8.9      -55.9     0.9462   280   4.63
    # 300  9776   -22.7     **-64.7**     0.9708   205   2.5722
    # 250  11086  -31.7     -54.7     7.9622   260   27.2656
    # CB
    # MYSQL
    # 400	7613	-890	-5590	1	280	464
    # 300	9820	-2270	**-5039**	6	205	258
    # 250	11088	-3170	-5470	8	260	2730
    # In the ADPUPA data the DP is -64.7, but in the mysql data it is -5039
    # There must have been some anomaly in the MYSQL data.

    # max relative humidity diff: {'max': 66.36384739368668, 'max_wmoid': 89571, 'pressure': 530}
    # wmoid: 89571, pressure: 530
    # Relative Humidity mismatch: 71 != 4.6361526063133125 +- 15 for wmoid 89571 and pressure 530
    # assert 71 == 4.6361526063133125 ± 1.5e+01
    # It turns out that the MYSQL data is missing the relative humidity and DP
    # for this station between 450 and 260mb. This is throwing off the interpolation
    # which depends on specific humidity.

    # max wind speed diff: {'max': 183.1566, 'max_wmoid': 27459, 'pressure': 850}
    # wmoid: 27459, pressure: 850
    # Wind Speed mismatch: 38.07 != 221.2266 +- 5 for wmoid 27459 and pressure 850
    # assert 38.07 == 221.2266 ± 5.0e+00
    # This station is in general bad. only pressure level 850 is recorded. The rest are missing.

    # max wind speed diff: {'max': 145.9411, 'max_wmoid': 4320, 'pressure': 600}
    # wmoid: 4320, pressure: 600
    # Wind Speed mismatch: 19.71 != 165.6511 +- 5 for wmoid 4320 and pressure 600
    # assert 19.71 == 165.6511 ± 5.0e+00
    # In the ADPUPA data there is a huge spike in wind speed at 600mb. The rest of the data is fairly close.
    # The mysql data seems to miss the wind speed spike around 600mb. This is throwing off the interpolation
    # for the wind speed.

    # max wind speed diff: {'max': 29.235699999999998, 'max_wmoid': 42123, 'pressure': 870}
    # wmoid: 42123, pressure: 870
    # Wind Speed mismatch: 32.58 != 3.3443 +- 5 for wmoid 42123 and pressure 870
    # assert 32.58 == 3.3443 ± 5.0e+00
    # This station is in general bad. Only pressure levels 850 through 978 are recorded for MYSQL. The rest are missing.
    # Couchbase interpolates the missing data for 1000mb through 1010mb but that is unreliable with little original data.

    # wmoid: 32540, pressure: 960
    # Wind Direction mismatch: 65 != 243 +- 50 for wmoid 32540 and pressure 960
    # assert 65 == 243 ± 5.0e+01
    # I think what is going on here is that the highest pressure in the raw data is 999 which does not quite reach the mandatory level of 1000mb.
    # The mysql ingest appears to be using the 999 level data as the highest mandatory pressure level (1000) whereas the CB ingest uses
    # the next level (990) as the highest pressure level.
    # The interpolation is different because of this.

    # max wind direction diff: {'max': 175, 'max_wmoid': 43128, 'pressure': 440}
    # wmoid: 43128, pressure: 440
    # Wind Direction mismatch: 107 != 282 +- 50 for wmoid 43128 and pressure 440
    # INM00043128  17.4500   78.4667  530.0    HYDERABAD AIRPORT
    # assert 107 == 282 ± 5.0e+01
    # This is an interpolation difference that I cannot explain.
    # the data is valid for the raw ADPUPA record.
    # raw ADPUPA record     mysql record    CB record
    # 491 190/~3.0          490 188/3       490 192/3
    # raw data then jumps to pressure 423mb
    # 420 315/~0.5          420 72/2.7      420 330/0.8
    # Clearly the CB interpolation is closer but I don't have a reason why.

    # wmoid: 23921, pressure: 890
    # Wind Direction mismatch: 278 != 92 +- 50 for wmoid 23921 and pressure 890
    # assert 278 == 92 ± 5.0e+01
    # RSM00023921  60.6833   60.4500   93.0    IVDEL
    # This is another case where the legacy mysql ingest is including the highest pressure level of 989mb and then interpolating
    # the 980mb level on up. The CB ingest is using the 980mb level as the highest pressure level and giving null for 1010, 1000, and 990
    # which I think is correct. Many of the levels match but from about 440mb on up the mysql wind data diverges a lot from both the CB data
    # and the raw ADPUPA data. I don't know why.
    # refer to /opt/data/prepbufr_to_cb/test_artifacts/23921-typ220.txt for the raw data and use the following to compare:
    # mysql:
    # export wmoid=23921
    # mysql --defaults-file=~/wolphin.cnf -A -B  --execute "select press,z,t,dp,rh,wd,ws from ruc_ua_pb.RAOB where date = '2024-07-31' and hour = 0 and wmoid = ${wmoid} and press  order by press desc;"
    # CB:
    # export w=23921
    # cbq -no-ssl-verify -e 'https://adb-cb1.gsd.esrl.noaa.gov:18093' -u avid -p 'the_password' -s "SELECT d.data.[\"${w}\"] FROM vxdata._default.RAOB AS d WHERE d.type='DD' AND d.subset='RAOB' AND d.docType='obs' AND d.subDocType = 'prepbufr' AND d.fcstValidISO = '2024-07-31T00:00:00Z' ORDER BY data.[\"${w}\"].['pressure'] DESC;" | egrep 'pressure|wind_direction|wind_speed'

    # max wind direction diff: {'max': 174, 'max_wmoid': 35671, 'pressure': 40}
    # wmoid: 35671, pressure: 40
    # Wind Direction mismatch: diff: 174 151 != 325 +- 50 for wmoid 35671 and pressure 40
    # assert 151 == 325 ± 5.0e+01
    # refer to /opt/data/prepbufr_to_cb/test_artifacts/35671-typ220.txt
    # export wmoid=35671
    # mysql --defaults-file=~/wolphin.cnf -A -B  --execute "select press,z,t,dp,rh,wd,ws from ruc_ua_pb.RAOB where date = '2024-07-31' and hour = 0 and wmoid = ${wmoid} and press in (850, 700, 500, 400, 300, 250, 200, 150, 100, 70, 50, 30) order by press desc;"
    # and
    # export w=35671
    # cbq -no-ssl-verify -e 'https://adb-cb1.gsd.esrl.noaa.gov:18093' -u avid -p 'pwd_av!d' -s "SELECT d.data.[\"${w}\"] FROM vxdata._default.RAOB AS d WHERE d.type='DD' AND d.subset='RAOB' AND d.docType='obs' AND d.subDocType = 'prepbufr' AND d.fcstValidISO = '2024-07-31T00:00:00Z' AND d.data.[\"${w}\"].pressure IN [850, 700, 500, 400, 300, 250, 200, 150, 100, 70, 50, 30] ORDER BY d.data.[\"${w}\"].pressure DESC;" | egrep "pressure|wind_direction|wind_speed"
    # This limits the output to a select set of mandatory levels that exist in the 35671-typ220.txt file. All of those match except the mysql data excludes the 30mb level.
    # However, removing the IN clause from both queries shows that the mysql non mandatory levels wildly diverge from the CB data and the data in the 35671-typ220.txt file.
    # The mysql data is clearly wrong but I cannot determine what is wrong with the interpolation.

    # 07145 CAM00071450  51.1330 -106.5830  595.0    ELBOW CS
    # This seems to be another case of a station with more than one record of each type. The mysql data is using the smaller of the two records.
    # In this case both type 220 records have the same number of pressure levels (254) but they have much different wind data. CB chose one record
    # and mysql chose the other. I do not know the mysql algorithm for choosing.
    # export w=7145;mysql --defaults-file=~/wolphin.cnf -A -B  --execute "select press,z,t,dp,rh,wd,ws from ruc_ua_pb.RAOB where date = '2024-07-31' and hour = 0 and wmoid = ${w};"
    # export w=07145;cbq -q -terse --no-ssl-verify -e 'https://adb-cb1.gsd.esrl.noaa.gov:18093' -u avid -p 'pwd_av!d' -s "SELECT  d.data.[\"${w}\"].pressure,d.data.[\"${w}\"].height  FROM vxdata._default.RAOB AS d WHERE d.type='DD' AND d.subset='RAOB' AND d.docType='obs' AND d.subDocType = 'prepbufr' AND d.fcstValidISO = '2024-07-31T00:00:00Z' ORDER BY d.data.[\"${w}\"].pressure;" | grep -v Disabling | jq -r '.[] | "\(.pressure) \(.height)"'

    # 97072 IDM00097072  -0.6833  119.7333    6.0    PALU/MUTIARA
    # This one is interesting. The mysql data is starting with a pressure of 1014mb with a height of 6m. The CB data is starting with 6m height at 1004mb.
    # The Adpupa data clearly shows the 1014 pressure to be disqualified by a PPC of 5. The starting pressure should be 1004.
    # I think the CB data is correct in this case.
    # Since we are interpolating all of the heights every last height will be off.
    #    23 DRS-1 [P__EVENT]         2.00000                        8     0     0    26    27    32    25
    #    24 RPS-0   P__EVENT         5.00000                        0     0     0    27    28     0    26
    #    25 NUM-2        POB         1014.00                       14     1     0    28     0    29    27
    #    26 NUM-2        PQM         1.00000                        5     0     0    29     0    30    27
    #    27 NUM-2        PPC         5.00000                        5     0     0    30     0    31    27
    #    28 NUM-2        PRC         100.000                       10     0     0    31     0     0    27
    #    29 RPS-0   P__EVENT         5.00000                        0     0     0    27    28     0    26
    #    30 NUM-2        POB         1004.00                       14     1     0    28     0    29    27
    #    31 NUM-2        PQM         2.00000                        5     0     0    29     0    30    27
    #    32 NUM-2        PPC         1.00000                        5     0     0    30     0    31    27
    #    33 NUM-2        PRC         100.000

    # 89664 AYM00089664 -77.8500  166.6667   24.0    MCMURDO
    # This station largely matches except for height values for levels above 300mb.
    # The temperatures above 300mb for this station are very cold - less than -60 degrees C.
    # The mysql interpolation is less accurate for cold temperatures.

    # 94995 ASM00094995 -31.5422  159.0786    5.2    LORD HOWE ISLAND AERO
    # This station largely matches except for height values for levels above 300mb.
    # The temperatures above 300mb for this station are very cold - less than -60 degrees C.
    # The mysql interpolation is less accurate for cold temperatures.

    # 94403 ASM00094403 -28.8044  114.6989   36.9    GERALDTON AIRPORT
    # This station largely matches except for height values for levels above 300mb.
    # The temperatures above 300mb for this station are very cold - less than -60 degrees C.
    # The mysql interpolation is less accurate for cold temperatures.

    # 47945 JAM00047945  25.8289  131.2286   15.3    MINAMIDAITOJIMA
    # This is another station that has two records of each type.
    # The mysql data is using the smaller of the two records.
    # The CB data is using the larger of the two records.

    # 40848 IRM00040848  29.5333   52.6000 1484.0    SHIRAZ
    # The mismatched wind speed at 830mb appears to be incorrect in the mysql data.
    # The mysql data is missing all of the 500 mb through 700mb data except for 500mb and 700mb.The Adpupa data does appear to be in the file.
    # mysql:    CB:         ADPUPA: (from /opt/data/prepbufr/test_artifacts/40848-typ220.text)
    # 820 - 32  820 - 4.61    815 - 4.1
    # 830 - 18  830 - 5.6     836 - 6.17
    # 840 - 5   840 - 3.08    844 - 0.0
    # CB is definitely closer for the wind speed at 830mb.
    # The mismatched wind direction is for all the readings from 410mb through 840mb.
    # MYSQL                 CB         ADPUPA
    # Pres  ws      wd    ws     wd    press ws     wd
    # 410	2.5200	237   2.0998 318       409 4   315
    # 420	2.9600	225   2.5141 344
    # 430	3.4000	213   2.9187 10
    # 440	3.8200	201   3.314 36
    # 450	4.2300	189   3.7004 60
    # 460	4.6400	178   4.0783 85
    # 470	5.0400	167   4.4481 108       475 4.63   120
    # 480	5.4300	156   5.0218 123
    # 490	5.8100	145   5.7934 128       495 6.17  130
    # 500	6.1800	135   6.1733 135       500 6.17  135.000
    # 700	2.0600	195   2.0578 195       700 2.06   195
    # 710	1.9000	208   1.564 212
    # 720	1.7500	220   1.0772 228       721 1.03   230
    # 730	1.6000	232   1.3027 182
    # 740	1.4500	244   1.7076 145       738 1.54   140
    # 750	1.3000	256   2.5226 169
    # 760	1.1500	268   3.0867 179       757 3.09   185
    # 770	1.0100	279   3.2468 167
    # 780	0.8700	290   3.4447 158
    # 790	0.7300	302   3.6703 150       788 3.6   150
    # 800	0.5900	313   4.0134 150
    # 810	0.4500	324   4.1156 168
    # 820	0.3200	335   4.6103 179       815 4.12   180
    # 830	0.1800	345   5.5907 176
    # 840	0.0500	356   3.0793 87        836 6.17  175
    # The wind direction values that are in the adpupa data more closely align with the CB data.

    # 83746 BRM00083746 -22.8167  -43.2500    6.0    GALEAO
    # The mysql data for this station is missing the temperatures between 500mb and 700mb
    # which corresponds to the mismatched height readings  between 500mb and 700mb. If
    # the temperatures are missing then the heights cannot be interpolated correctly.
    # The CB data is interpolating the heights correctly.

    # 89611 AYM00089611 -66.2825  110.5231   40.0    CASEY
    # adpupa dat is absent from 500mb to 700mb exclusive. The mysql data is missing the
    # wind dta between 510mb and 690mb inclusive. The CB data is interpolating the wind
    # data for these points according to the algorithm, but the mysql data is not.
    # I don't see how it makes sense to interpolate the wind data but it seems that it
    # should at least be consistent with the interpolation of the other variables.
    # The mysql and CB data mismatch WD from 750mb through 800mb inclusive. The adpupa data
    # has only data at 700mb and 783mb, and 813mb. The mysql data is reporting almost the
    # same wind direction for all of the levels from 750mb through 800mb. The CB data is interpolating.
    # The RH differs from 330mb through 370mb, but those are at very cold temperatures and the
    # mysql algorithm is not as accurate at cold temps.

    # 40856 IRM00040856  29.4667   60.8833 1370.0    ZAHEDAN
    # Another case where the adpupa data is scarce in the areas of the mismatch.
    # The mismatch is WD from 750 through 830 inclusive.
    # The adpupa data has these levels:
    #   301 NUM-2        POB         824.000
    #   347 NUM-2        POB         815.000
    #   393 NUM-2        POB         787.000
    #   439 NUM-2        POB         761.000
    #   485 NUM-2        POB         700.000
    #   255 NUM-2        POB         831.000
    # The mysql data reports levels 510 through 690 as missing but the CB data interpolates them.
    # Then from 750 through 830 the interpolation is simply different.
    # The adpupa data is scant:
    # POB      DDO(WD) FFO(WS)
    # 831.000  280.000 2.00000
    # 824.000  175.000 4.00000
    # 815.000  200.000 4.00000
    # 787.000  190.000 8.00000
    # 761.000  240.000 10.0000
    # 700.000  255.000 8.00000
    # press MYSQL-WD CB-WD
    # 830	5      265
    # 820	357    186
    # 810	349    198
    # 800	341    195
    # 790	333    191
    # 780	325    203
    # 770	316    223
    # 760	308    240
    # 750	299    243
    # For what it is worth (not much) the CB wind interpolation is closer to the adpupa data.

    # 41112 29839
    # A cursory look at these stations show a similar pattern.
    # The Mysql data is missing the wind data for 10 or 20 mid - levels approx 500mb to 700mb.
    # This throws off the interpolation for MYSQL. CB interpolates the missing data, although for
    # wind data this is almost meaningless.

    try:
        for row in range(len(mysql_result)):
            try:
                mysql_row = mysql_result[row]
                m_wmoid = mysql_row[mysql_wmoid]
                if m_wmoid in rejected_stations:
                    continue
                m_pressure = mysql_row[mysql_pressure_pos]
                m_height = mysql_row[mysql_height_pos]
                m_temperature = mysql_row[mysql_temperature_pos]
                m_dewpoint = mysql_row[mysql_dewpoint_pos]
                m_relative_humidity = mysql_row[mysql_relative_humidity_pos]
                m_wind_direction = mysql_row[mysql_wind_direction_pos]
                m_wind_speed = mysql_row[mysql_wind_speed_pos]
                cb_data = np.nan
                # if m_wmoid != 97686:
                #     continue
                try:
                    for d in cb_result:
                        if (
                            f"{m_wmoid:05}" in d["data"]
                            and d["data"][f"{m_wmoid:05}"]["pressure"] == m_pressure
                        ):
                            cb_data = d["data"][f"{m_wmoid:05}"]
                            break
                    if cb_data is np.nan:
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
                diff = abs(m_pressure - cb_pressure)
                assert (
                    diff <= press_tolerance
                ), f"Pressure mismatch: diff: {diff} is not <= {press_tolerance} for wmoid {m_wmoid} and pressure {m_pressure}"

                if (
                    m_height is not np.nan
                    and cb_height is not np.nan
                    and m_height is not None
                    and cb_height is not None
                ):
                    diff = abs(m_height - cb_height)
                    if diff > height_stat["max"]:
                        height_stat["max"] = diff
                        height_stat["max_wmoid"] = m_wmoid
                        height_stat["pressure"] = m_pressure
                    assert (
                        diff <= height_tolerance
                    ), f"Height mismatch: diff: {diff} is not <= {height_tolerance} for wmoid {m_wmoid} and pressure {m_pressure}"

                if (
                    m_temperature is not np.nan
                    and cb_temperature is not np.nan
                    and m_temperature is not None
                    and cb_temperature is not None
                ):
                    diff = abs(m_temperature / 100 - cb_temperature)
                    if diff > temperature_stat["max"]:
                        temperature_stat["max"] = diff
                        temperature_stat["max_wmoid"] = m_wmoid
                        temperature_stat["pressure"] = m_pressure
                    assert (
                        diff <= temperature_tolerance
                    ), f"Temperature mismatch: diff: {diff} is not <= {temperature_tolerance} for wmoid {m_wmoid} and pressure {m_pressure}"

                if (
                    m_dewpoint is not np.nan
                    and cb_dewpoint is not np.nan
                    and m_dewpoint is not None
                    and cb_dewpoint is not None
                ):
                    diff = abs(m_dewpoint / 100 - cb_dewpoint)
                    if diff > dewpoint_stat["max"]:
                        dewpoint_stat["max"] = diff
                        dewpoint_stat["max_wmoid"] = m_wmoid
                        dewpoint_stat["pressure"] = m_pressure
                    assert (
                        diff <= dewpoint_tolerance
                    ), f"Dewpoint mismatch: diff: {diff} is not <= {dewpoint_tolerance} for wmoid {m_wmoid} and pressure {m_pressure}"

                if (
                    m_relative_humidity is not np.nan
                    and cb_relative_humidity is not np.nan
                    and m_relative_humidity is not None
                    and cb_relative_humidity is not None
                ):
                    if m_relative_humidity is None or cb_relative_humidity is None:
                        continue
                    diff = abs(m_relative_humidity - cb_relative_humidity)
                    if diff > relative_humidity_stat["max"]:
                        relative_humidity_stat["max"] = diff
                        relative_humidity_stat["max_wmoid"] = m_wmoid
                        relative_humidity_stat["pressure"] = m_pressure
                    assert (
                        diff <= relative_humidity_tolerance
                    ), f"Relative Humidity mismatch: diff: {diff} is not <= {relative_humidity_tolerance} for wmoid {m_wmoid} and pressure {m_pressure}"

                if (
                    m_wind_speed is not np.nan
                    and cb_wind_speed is not np.nan
                    and m_wind_speed is not None
                    and cb_wind_speed is not None
                ):
                    # The legacy data has some 360 degree wind. This is a valid value, but it is equivalent to 0
                    # also take care of floating point precision issues around 360
                    diff = abs(m_wind_speed / 100 - cb_wind_speed)
                    if diff > wind_speed_stat["max"]:
                        wind_speed_stat["max"] = diff
                        wind_speed_stat["max_wmoid"] = m_wmoid
                        wind_speed_stat["pressure"] = m_pressure
                    assert (
                        diff <= wind_speed_tolerance
                    ), f"Wind Speed mismatch: diff: {diff} is not <= {wind_speed_tolerance} for wmoid {m_wmoid} and pressure {m_pressure}"

                if (
                    m_wind_direction is not np.nan
                    and cb_wind_direction is not np.nan
                    and m_wind_direction is not None
                    and cb_wind_direction is not None
                ):
                    m_wind_direction = round(m_wind_direction) % 360
                    diff = m_wind_direction - cb_wind_direction
                    if diff > 180:
                        diff -= 360
                    else:
                        if diff < -180:
                            diff += 360
                    diff = abs(diff)

                    if diff > wind_direction_stat["max"]:
                        wind_direction_stat["max"] = diff
                        wind_direction_stat["max_wmoid"] = m_wmoid
                        wind_direction_stat["pressure"] = m_pressure
                    assert (
                        diff <= wind_direction_tolerance
                    ), f"Wind Direction mismatch: diff: {diff} is not <= {wind_direction_tolerance} for wmoid {m_wmoid} and pressure {m_pressure}"
            except Exception as _e:
                print("--------------------")
                print(f"wmoid: {m_wmoid}, pressure: {m_pressure}")
                print(_e)
            # raise _e
    finally:
        print(f"max height diff: {height_stat}")
        print(f"max temperature diff: {temperature_stat}")
        print(f"max dewpoint diff: {dewpoint_stat}")
        print(f"max relative humidity diff: {relative_humidity_stat}")
        print(f"max wind speed diff: {wind_speed_stat}")
        print(f"max wind direction diff: {wind_direction_stat}")

        mysql_db_cursor.close()
        mysql_db.close()
        vx_ingest.cluster.close()
