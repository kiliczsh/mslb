"""Canonical, lossless fractions for API, artifact, and checkpoint JSON."""

from fractions import Fraction
from math import gcd
from typing import Annotated, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

MAX_FRACTION_BITS = 4096
# ceil(4096 * log10(2)); cap text before int() so conversion remains bounded.
MAX_COMPONENT_DIGITS = 1234


class FractionValue(BaseModel):
    """A reduced fraction whose JSON components are canonical decimal strings."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    numerator: Annotated[
        str, Field(max_length=MAX_COMPONENT_DIGITS + 1, pattern=r"^(0|-?[1-9][0-9]*)$")
    ]
    denominator: Annotated[str, Field(max_length=MAX_COMPONENT_DIGITS, pattern=r"^[1-9][0-9]*$")]

    @model_validator(mode="after")
    def validate_reduced_value(self) -> Self:
        """Reject oversized arithmetic and noncanonical equivalent encodings."""
        numerator, denominator = int(self.numerator), int(self.denominator)
        if max(numerator.bit_length(), denominator.bit_length()) > MAX_FRACTION_BITS:
            raise ValueError("Fraction exceeds the 4096-bit complexity limit")
        if gcd(numerator, denominator) != 1:
            raise ValueError("Fraction must be reduced; zero must be 0/1")
        return self

    @classmethod
    def from_fraction(cls, value: Fraction) -> Self:
        """Encode domain arithmetic without passing through floating point."""
        if max(value.numerator.bit_length(), value.denominator.bit_length()) > MAX_FRACTION_BITS:
            raise ValueError("Fraction exceeds the 4096-bit complexity limit")
        return cls(numerator=str(value.numerator), denominator=str(value.denominator))

    def to_fraction(self) -> Fraction:
        """Restore exact arithmetic from a validated wire value."""
        return Fraction(int(self.numerator), int(self.denominator))
