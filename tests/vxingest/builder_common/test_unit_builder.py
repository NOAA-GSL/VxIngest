import numpy as np

from vxingest.builder_common.builder import Builder


def test_is_a_number_accepts_finite_python_and_numpy_numbers():
    assert Builder.is_a_number(1)
    assert Builder.is_a_number(1.5)
    assert Builder.is_a_number(np.float64(1.5))


def test_is_a_number_rejects_missing_nonfinite_and_non_numeric_values():
    assert not Builder.is_a_number(None)
    assert not Builder.is_a_number(np.nan)
    assert not Builder.is_a_number(np.inf)
    assert not Builder.is_a_number("1.5")
