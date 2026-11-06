from fractions import Fraction

import pytest
from mslb_contracts.fractions import FractionValue
from pydantic import ValidationError


def test_large_metric_survives_json_round_trip() -> None:
    consumed = (
        Fraction(numerator=5)
        - sum(Fraction(numerator=1, denominator=n) for n in range(1, 83))
        + Fraction(numerator=82, denominator=83)
    )
    assert consumed.numerator.bit_length() == 120
    encoded = FractionValue.from_fraction(value=consumed)
    assert (
        FractionValue.model_validate_json(json_data=encoded.model_dump_json()).to_fraction()
        == consumed
    )
    assert '"numerator":"732666138205131859729707206340762241"' in encoded.model_dump_json()


@pytest.mark.parametrize(
    argnames=("numerator", "denominator"),
    argvalues=[("0", "1"), ("-2", "3"), ("1", "1"), (str(object=2**4095), "1")],
)
def test_canonical_values(numerator: str, denominator: str) -> None:
    assert FractionValue(numerator=numerator, denominator=denominator).to_fraction() == Fraction(
        numerator=int(numerator), denominator=int(denominator)
    )


@pytest.mark.parametrize(
    argnames=("numerator", "denominator"),
    argvalues=[
        ("-0", "1"),
        ("01", "1"),
        ("+1", "1"),
        ("1\n", "1"),
        ("1", "0"),
        ("1", "-1"),
        ("2", "4"),
        ("0", "2"),
        ("1", "01"),
        ("１", "1"),
        ("9" * 10000, "1"),
        (str(object=2**4096), "1"),
    ],
)
def test_invalid_encodings(numerator: str, denominator: str) -> None:
    with pytest.raises(expected_exception=ValidationError):
        FractionValue(numerator=numerator, denominator=denominator)


def test_numbers_and_unknown_fields_are_rejected() -> None:
    for raw in [
        '{"numerator":1,"denominator":"2"}',
        '{"numerator":"1","denominator":"2","extra":true}',
    ]:
        with pytest.raises(expected_exception=ValidationError):
            FractionValue.model_validate_json(json_data=raw)


def test_domain_complexity_limit() -> None:
    with pytest.raises(expected_exception=ValueError, match="4096-bit"):
        FractionValue.from_fraction(value=Fraction(numerator=2**4096))
