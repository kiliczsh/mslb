import sys
from fractions import Fraction


def test_python_version_and_exact_arithmetic() -> None:
    assert sys.version_info[:2] == (3, 13)
    assert sum([Fraction(10, 3)] * 3) == 10
