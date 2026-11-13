import logging
from unittest.mock import Mock

import pytest

from vxingest.prepbufr_to_cb import prepbufr_builder


@pytest.fixture
def reader(monkeypatch):
    builder = object.__new__(prepbufr_builder.PrepbufrRaobsObsBuilderV01)
    bufr = Mock(msg_type="ADPUPA", msg_date=2026110200)
    bufr.advance.side_effect = [0, 1]
    bufr.load_subset.side_effect = [0, 0, 1]
    monkeypatch.setattr(prepbufr_builder.ncepbufr, "open", Mock(return_value=bufr))
    builder.read_data_from_bufr = Mock()
    templates = {
        "bufr_msg_type": "ADPUPA",
        "bufr_report_types": [120],
        "header": {},
        "obs_err": {},
        "obs_data_120": {},
    }
    return builder, bufr, templates


def test_missing_header_skips_subset_and_continues(reader, caplog):
    builder, bufr, templates = reader
    header = {"station_id": "40375", "report_type": 120}
    observations = {"pressure": [1000, 900]}
    builder.read_data_from_bufr.side_effect = [None, header, {}, observations]

    result = builder.read_data_from_file("example.prepbufr", templates)

    assert result["40375"][120]["obs_data"] == observations
    assert not any(record.levelno >= logging.ERROR for record in caplog.records)
    bufr.close.assert_called_once()


@pytest.mark.parametrize(
    "bad_observations", [None, {"pressure": None}, {"pressure": []}]
)
@pytest.mark.parametrize("bad_first", [True, False])
def test_invalid_observations_do_not_poison_duplicate_reports(
    reader, caplog, bad_observations, bad_first
):
    builder, bufr, templates = reader
    header = {"station_id": "40375", "report_type": 120}
    observations = {"pressure": [1000, 900]}
    reports = (
        [bad_observations, observations]
        if bad_first
        else [observations, bad_observations]
    )
    builder.read_data_from_bufr.side_effect = [
        value for report in reports for value in (header, {}, report)
    ]

    result = builder.read_data_from_file("example.prepbufr", templates)

    assert result["40375"][120]["obs_data"] == observations
    assert not any(record.levelno >= logging.ERROR for record in caplog.records)
    bufr.close.assert_called_once()


@pytest.mark.parametrize("larger_first", [True, False])
def test_largest_valid_duplicate_report_is_retained(reader, larger_first):
    builder, bufr, templates = reader
    header = {"station_id": "40375", "report_type": 120}
    smaller = {"pressure": [1000, 900]}
    larger = {"pressure": [1000, 900, 800]}
    reports = [larger, smaller] if larger_first else [smaller, larger]
    builder.read_data_from_bufr.side_effect = [
        value for report in reports for value in (header, {}, report)
    ]

    result = builder.read_data_from_file("example.prepbufr", templates)

    assert result["40375"][120]["obs_data"] == larger
    bufr.close.assert_called_once()


def test_unexpected_decode_error_logs_traceback_and_continues(reader, caplog):
    builder, bufr, templates = reader
    header = {"station_id": "40375", "report_type": 120}
    observations = {"pressure": [1000, 900]}
    builder.read_data_from_bufr.side_effect = [
        RuntimeError("decode failed"),
        header,
        {},
        observations,
    ]

    result = builder.read_data_from_file("example.prepbufr", templates)

    assert result["40375"][120]["obs_data"] == observations
    assert caplog.records[-1].exc_info is not None
    assert "example.prepbufr" in caplog.text
    assert "decode failed" in caplog.text
    bufr.close.assert_called_once()
