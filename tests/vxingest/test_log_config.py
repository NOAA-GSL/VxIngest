import logging

import pytest

from vxingest.log_config import LOG_LEVEL_ENV_VAR, get_loglevel, parse_loglevel


def test_get_loglevel_defaults_to_info(monkeypatch):
    monkeypatch.delenv(LOG_LEVEL_ENV_VAR, raising=False)

    assert get_loglevel() == logging.INFO


def test_get_loglevel_ignores_debug_env_var(monkeypatch):
    monkeypatch.delenv(LOG_LEVEL_ENV_VAR, raising=False)
    monkeypatch.setenv("DEBUG", "true")

    assert get_loglevel() == logging.INFO


@pytest.mark.parametrize(
    ("loglevel", "expected"),
    [
        ("DEBUG", logging.DEBUG),
        ("info", logging.INFO),
        (" warning ", logging.WARNING),
        ("ERROR", logging.ERROR),
        ("CRITICAL", logging.CRITICAL),
    ],
)
def test_get_loglevel_uses_log_level_env_var(monkeypatch, loglevel, expected):
    monkeypatch.setenv(LOG_LEVEL_ENV_VAR, loglevel)
    monkeypatch.setenv("DEBUG", "true")

    assert get_loglevel() == expected


def test_parse_loglevel_rejects_unknown_loglevel():
    with pytest.raises(ValueError, match=LOG_LEVEL_ENV_VAR):
        parse_loglevel("VERBOSE")
