"""eltdx exception hierarchy."""


class EltdxError(Exception):
    """Base class for package errors."""


class ProtocolError(EltdxError):
    """Raised when protocol encoding or decoding fails."""


class TransportError(EltdxError):
    """Raised when the transport cannot complete a request."""


class ConnectionClosedError(TransportError):
    """Raised when the remote server closes the connection."""


class ResponseTimeoutError(TransportError):
    """Raised when a request does not receive a response in time."""


class UnsupportedCommandError(EltdxError):
    """Raised when an API method has no migrated 7709 command yet."""
