"""Errors safe to expose through the CLI, HTTP, and MCP interfaces."""

from google.api_core.exceptions import BadRequest, Forbidden, GoogleAPIError, NotFound
from google.auth.exceptions import DefaultCredentialsError, RefreshError, TransportError
from requests.exceptions import ConnectionError as RequestsConnectionError, Timeout as RequestsTimeout

LOGIN = "Run `gcloud auth application-default login`"


class SoundingError(ValueError):
    status = 400


class MissingTable(SoundingError):
    status = 404


class CatalogBusy(SoundingError):
    status = 503


class CatalogVersionError(SoundingError):
    status = 409


def public_error(error: Exception) -> tuple[str, int]:
    if isinstance(error, SoundingError):
        return str(error), error.status
    if isinstance(error, DefaultCredentialsError):
        return f"No Google credentials found. {LOGIN}, or set GOOGLE_APPLICATION_CREDENTIALS.", 401
    if isinstance(error, RefreshError):
        return f"Your Google credentials have expired or were revoked. {LOGIN} again.", 401
    if isinstance(error, (TransportError, RequestsConnectionError, RequestsTimeout)):
        return "Could not reach Google. Check your network connection and retry.", 502
    if isinstance(error, Forbidden):
        return "BigQuery denied access. Check your Google credentials and permissions.", 403
    if isinstance(error, NotFound):
        return "BigQuery could not find the requested resource.", 404
    if isinstance(error, BadRequest):
        return "BigQuery rejected the request. Check the SQL or table identifier.", 400
    if isinstance(error, GoogleAPIError):
        return "BigQuery is unavailable. Try again later.", 502
    return "The operation failed. See the server's stderr for details.", 500


def text(value: str, name: str, maximum: int = 2048, *, empty: bool = False) -> str:
    if not isinstance(value, str) or len(value) > maximum or (not empty and not value.strip()):
        raise SoundingError(f"{name} must be {'an optional' if empty else 'a nonempty'} string of at most {maximum} characters.")
    return value


def integer(value: int, name: str, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise SoundingError(f"{name} must be an integer between {minimum} and {maximum}.")
    return value
