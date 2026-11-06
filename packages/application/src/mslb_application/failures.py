"""Safe transport-neutral application error descriptions, without raw input or SQL."""

from mslb_contracts.http import ErrorEnvelope, ErrorInfo, FieldError
from mslb_contracts.inputs import InputError
from mslb_engine.models import SimulationError
from mslb_persistence.database import StoreError
from pydantic import ValidationError
from sqlalchemy.exc import DBAPIError

from mslb_application.diagnostics import report_failure
from mslb_application.errors import OperationError


def failure(error: Exception, request_id: str) -> ErrorEnvelope:
    """Translate ordinary failures safely; cancellation/control signals stay outside this API."""
    code, message, details = "INTERNAL_ERROR", "The operation could not be completed", ()
    if isinstance(error, (OperationError, StoreError, SimulationError)):
        code, message = error.code, str(error)
    elif isinstance(error, InputError):
        code, message = "VALIDATION_ERROR", "Input validation failed"
        details = (FieldError(field=error.field[:256], reason=error.reason[:128]),)
    elif isinstance(error, ValidationError):
        code, message = "VALIDATION_ERROR", "Request validation failed"
        details = tuple(
            FieldError(
                field=".".join(str(part) for part in item["loc"])[:256], reason=item["type"][:128]
            )
            for item in error.errors()[:32]
        )
    elif isinstance(error, (DBAPIError, OSError, TimeoutError)):
        code, message = "RESOURCE_PRESSURE", "A required dependency or managed file is unavailable"
        report_failure(
            error=error, code="RESOURCE_PRESSURE", source="application", request_id=request_id
        )
    else:
        report_failure(
            error=error, code="INTERNAL_ERROR", source="application", request_id=request_id
        )
    return ErrorEnvelope(
        error=ErrorInfo(code=code, message=message, details=details), request_id=request_id
    )
