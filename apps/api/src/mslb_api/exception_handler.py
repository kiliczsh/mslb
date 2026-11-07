"""Bounded, redacted exception translation for the REST boundary."""

from http import HTTPStatus

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from mslb_application.diagnostics import report_failure
from mslb_application.errors import OperationError
from mslb_contracts.http import FieldError
from mslb_contracts.inputs import InputError
from mslb_engine.errors import SimulationError
from mslb_persistence.errors import StoreError
from pydantic import ValidationError
from python_multipart.exceptions import MultipartParseError
from sqlalchemy.exc import DBAPIError
from starlette.exceptions import HTTPException
from starlette.formparsers import MultiPartException
from starlette.responses import Response

from mslb_api.errors import error_response


async def failure(request: Request, error: Exception) -> Response:
    """Translate safe bounded errors while retaining the existing request ID."""
    identifier = str(object=getattr(request.state, "request_id", "unavailable"))
    if isinstance(error, (OperationError, StoreError, SimulationError)):
        return error_response(code=error.code, message=str(object=error), request_id=identifier)
    if isinstance(error, InputError):
        return error_response(
            code="VALIDATION_ERROR",
            message="Input validation failed",
            request_id=identifier,
            details=(FieldError(field=error.field[:256], reason=error.reason[:128]),),
        )
    if isinstance(error, (RequestValidationError, ValidationError)):
        details = tuple(
            FieldError(
                field=".".join(str(object=part) for part in item["loc"])[:256],
                reason=item["type"][:128],
            )
            for item in error.errors()[:32]
        )
        return error_response(
            code="VALIDATION_ERROR",
            message="Request validation failed",
            request_id=identifier,
            details=details,
        )
    if isinstance(error, HTTPException):
        code = (
            "NOT_FOUND" if error.status_code == HTTPStatus.NOT_FOUND.value else "VALIDATION_ERROR"
        )
        return error_response(
            code=code,
            message="Route not found"
            if code == "NOT_FOUND"
            else "Malformed or unsupported request",
            request_id=identifier,
        )
    if isinstance(error, (MultiPartException, MultipartParseError)):
        return error_response(
            code="VALIDATION_ERROR", message="Malformed multipart upload", request_id=identifier
        )
    if isinstance(error, (DBAPIError, OSError, TimeoutError)):
        report_failure(error=error, code="RESOURCE_PRESSURE", source="rest", request_id=identifier)
        return error_response(
            code="RESOURCE_PRESSURE",
            message="A required dependency or managed file is unavailable",
            request_id=identifier,
        )
    report_failure(error=error, code="INTERNAL_ERROR", source="rest", request_id=identifier)
    return error_response(
        code="INTERNAL_ERROR",
        message="The operation could not be completed",
        request_id=identifier,
    )
