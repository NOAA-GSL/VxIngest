import os

import ncepbufr
import pytest
from vxingest.prepbufr_to_cb.prepbufr_builder import PrepbufrRaobsObsBuilderV01
from vxingest.prepbufr_to_cb.run_ingest_threads import VXIngest


def setup_connection():
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


@pytest.mark.integration()
def test_read_header():
    queue_element = (
        "/opt/data/prepbufr_to_cb/input_files/241011200.gdas.t12z.prepbufr.nr"
    )
    vx_ingest = setup_connection()
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


@pytest.mark.integration()
def test_read_qm_data():
    vx_ingest = setup_connection()
    queue_element = (
        "/opt/data/prepbufr_to_cb/input_files/241011200.gdas.t12z.prepbufr.nr"
    )
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
    qm_data = builder.read_data_from_bufr(bufr, template["q_marker"])
    bufr.close()
    assert qm_data is not None


@pytest.mark.integration()
def test_read_obs_err():
    vx_ingest = setup_connection()
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


@pytest.mark.integration()
def test_read_obs_data():
    vx_ingest = setup_connection()
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
