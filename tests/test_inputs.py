import io
import json
from pathlib import Path

import pytest
from mslb_contracts.inputs import InputError, ServerInput, input_digest, read_requests, read_servers
from pydantic import ValidationError


def test_bom_unicode_quotes_and_input_order_normalize() -> None:
    first = read_requests(
        io.BytesIO(
            (
                '\ufefft,request_id,work_units,mem_mb\r\n2," e\u0301,quoted ",3,4\r\n0,a,1,2\r\n'
            ).encode()
        )
    )
    second = read_requests(
        io.BytesIO('t,request_id,work_units,mem_mb\n0,a,1,2\n2,"é,quoted",3,4\n'.encode())
    )
    assert first == second
    assert input_digest(first) == input_digest(second)


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"t,request_id,work_units,mem_mb\n",
        b"t,t,work_units,mem_mb\n",
        b"t,request_id,work_units,mem_mb\n0,a,1\n",
        b"t,request_id,work_units,mem_mb\n0,a,NaN,1\n",
        b"t,request_id,work_units,mem_mb\n0,a,0,1\n",
        b"t,request_id,work_units,mem_mb\n0,a,1.5,1\n",
        b"t,request_id,work_units,mem_mb\n0,a,1,1\n1, a ,1,1\n",
        b"t,request_id,work_units,mem_mb\n0,\xff,1,1\n",
        b"t,request_id,work_units,mem_mb\n0,a," + b"9" * 10000 + b",1\n",
        b't,request_id,work_units,mem_mb\n0,"unterminated,1,1\n',
    ],
)
def test_invalid_csv_is_rejected_without_echoing_payload(payload: bytes) -> None:
    with pytest.raises(InputError) as failure:
        read_requests(io.BytesIO(payload))
    assert len(str(failure.value)) < 200


def test_extra_named_columns_and_quoted_newlines() -> None:
    result = read_requests(
        io.BytesIO(b't,request_id,work_units,mem_mb,note\n0,"a\nb",1,1,ignored\n')
    )
    assert result[0].request_id == "a\nb"


@pytest.mark.parametrize("value", [True, 1.5, "10", 0, -1, 1_000_000_001])
def test_server_capacities_are_strict_positive_bounded_integers(value: object) -> None:
    with pytest.raises(ValidationError):
        ServerInput.model_validate(
            {
                "id": "s1",
                "cpu_units_per_tick": value,
                "mem_mb": 10,
                "rate_limit_per_sec": 1,
            }
        )


def test_server_order_and_normalized_duplicate_rejection() -> None:
    servers = [
        {"id": name, "cpu_units_per_tick": 1, "mem_mb": 1, "rate_limit_per_sec": 1}
        for name in [" b ", "a"]
    ]
    parsed = read_servers(io.BytesIO(json.dumps({"tick_seconds": 1, "servers": servers}).encode()))
    assert [server.id for server in parsed] == ["a", "b"]
    servers[1]["id"] = "b"
    with pytest.raises(InputError):
        read_servers(io.BytesIO(json.dumps({"tick_seconds": 1, "servers": servers}).encode()))


def test_supplied_and_expanded_inputs_obey_declared_envelope() -> None:
    root = Path("/fixtures")
    for folder in [root / "core", *sorted((root / "expanded/scenarios").iterdir())]:
        with (folder / "servers.json").open("rb") as stream:
            assert read_servers(stream)
        with (folder / "requests.csv").open("rb") as stream:
            if folder.name in {"20_sustained_12k", "21_stress_50k"}:
                with pytest.raises(InputError, match="request_count_exceeded"):
                    read_requests(stream)
            else:
                assert read_requests(stream)


def test_all_supplied_negative_fixtures_rejected() -> None:
    root = Path("/fixtures/expanded/invalid")
    for path in (root / "csv").glob("*.csv"):
        with path.open("rb") as stream, pytest.raises(InputError):
            read_requests(stream)
    for path in (root / "servers").glob("*.json"):
        with path.open("rb") as stream, pytest.raises(InputError):
            read_servers(stream)
