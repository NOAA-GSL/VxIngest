import math
from collections.abc import Mapping

DEFAULT_CB_TIMEOUT_SECONDS = 120


def get_cb_timeout_seconds(credentials: Mapping[str, object]) -> float:
    """Return the configured Couchbase query timeout, defaulting to 120 seconds."""
    value = credentials.get("cb_timeout_seconds", DEFAULT_CB_TIMEOUT_SECONDS)
    if isinstance(value, bool):
        raise ValueError("cb_timeout_seconds must be a positive number of seconds")

    try:
        timeout_seconds = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(
            "cb_timeout_seconds must be a positive number of seconds"
        ) from error

    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError("cb_timeout_seconds must be a positive number of seconds")
    return timeout_seconds
