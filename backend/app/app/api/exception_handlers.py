from http import HTTPStatus

from fastapi import HTTPException, Request
from fastapi.exception_handlers import http_exception_handler
from fastapi.responses import JSONResponse

from app.schemas.commonData import InvalidParam, ProblemDetails

# APIs that return ProblemDetails on errors. To apply ProblemDetails to all
# NEF northbound APIs, remove the path filtering in the handler below.
PROBLEM_DETAILS_API_PATHS = [
    "/3gpp-analyticsexposure/",
]

# Normally Type and Cause would be application-specific URIs and strings
# pointing to an actual reference of the type and cause of the error (API dependent and not the same for similar status codes)
# For now, to avoid changing the rest of the code (e.g. custom problem details exceptions), we will use generic references

# type URIs with RFC 9110 section references
_TYPE_URI = {
    400: "https://tools.ietf.org/html/rfc9110#section-15.5.1",
    401: "https://tools.ietf.org/html/rfc9110#section-15.5.2",
    403: "https://tools.ietf.org/html/rfc9110#section-15.5.4",
    404: "https://tools.ietf.org/html/rfc9110#section-15.5.5",
    422: "https://tools.ietf.org/html/rfc9110#section-15.5.21",
    500: "https://tools.ietf.org/html/rfc9110#section-15.6.1",
    501: "https://tools.ietf.org/html/rfc9110#section-15.6.2",
}

# machine-readable cause strings (custom)
_CAUSE = {
    400: "INVALID_PARAMETER",
    401: "UNAUTHORIZED",
    403: "INSUFFICIENT_PERMISSIONS",
    404: "RESOURCE_NOT_FOUND",
    422: "INVALID_PARAMETER",
    500: "SYSTEM_FAILURE",
    501: "NOT_IMPLEMENTED",
}


def _applies(request: Request) -> bool:
    return any(path in request.url.path for path in PROBLEM_DETAILS_API_PATHS)


def _title(status_code: int) -> str:
    try:
        return HTTPStatus(status_code).phrase
    except ValueError:
        return "Unknown Error"


async def problem_details_http_handler(request: Request, exc: HTTPException):
    if not _applies(request):
        return await http_exception_handler(request, exc)

    invalid_params = None
    if exc.status_code == 422 and isinstance(exc.detail, list):
        invalid_params = [
            InvalidParam(
                param="/".join(str(loc) for loc in err.get("loc", [])),
                reason=err.get("msg"),
            )
            for err in exc.detail
            if isinstance(err, dict)
        ] or None

    problem_details = ProblemDetails(
        type=_TYPE_URI.get(exc.status_code, "about:blank"),
        title=_title(exc.status_code),
        status=exc.status_code,
        detail="Request validation failed." if invalid_params else str(exc.detail) if exc.detail is not None else None,
        instance=str(request.url.path),
        cause=_CAUSE.get(exc.status_code),
        invalidParams=invalid_params,
    )
    return JSONResponse(
        status_code=exc.status_code,
        content=problem_details.dict(exclude_none=True),
        media_type="application/problem+json",
    )