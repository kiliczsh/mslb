from fractions import Fraction

import pytest
from mslb_contracts.fractions import FractionValue
from pydantic import ValidationError


def test_large_metric_survives_json_round_trip() -> None:
    consumed = Fraction(5) - sum(Fraction(1, n) for n in range(1, 83)) + Fraction(82, 83)
    assert consumed.numerator.bit_length() == 120
    encoded = FractionValue.from_fraction(consumed)
    assert FractionValue.model_validate_json(encoded.model_dump_json()).to_fraction() == consumed
    assert '"numerator":"732666138205131859729707206340762241"' in encoded.model_dump_json()


@pytest.mark.parametrize(
    ("numerator", "denominator"),
    [("0", "1"), ("-2", "3"), ("1", "1"), (str(2**4095), "1")],
)
def test_canonical_values(numerator: str, denominator: str) -> None:
    assert FractionValue(numerator=numerator, denominator=denominator).to_fraction() == Fraction(
        int(numerator), int(denominator)
    )


@pytest.mark.parametrize(
    ("numerator", "denominator"),
    [
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
        (str(2**4096), "1"),
    ],
)
def test_invalid_encodings(numerator: str, denominator: str) -> None:
    with pytest.raises(ValidationError):
        FractionValue(numerator=numerator, denominator=denominator)


def test_numbers_and_unknown_fields_are_rejected() -> None:
    for raw in [
        '{"numerator":1,"denominator":"2"}',
        '{"numerator":"1","denominator":"2","extra":true}',
    ]:
        with pytest.raises(ValidationError):
            FractionValue.model_validate_json(raw)


def test_domain_complexity_limit() -> None:
    with pytest.raises(ValueError, match="4096-bit"):
        FractionValue.from_fraction(Fraction(2**4096))
