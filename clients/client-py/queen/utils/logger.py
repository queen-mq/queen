"""
Logger utility for Queen Client v2
Controlled by QUEEN_CLIENT_LOG environment variable
"""

import os
import sys
import json
from datetime import datetime, timezone
from typing import Any, Union

LOG_ENABLED = os.environ.get("QUEEN_CLIENT_LOG", "").lower() == "true"


def _get_timestamp() -> str:
    """Get formatted timestamp"""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _format_log(operation: str, details: Union[str, dict[str, Any]], level: str = "INFO") -> str:
    """Format log message with timestamp and operation"""
    timestamp = _get_timestamp()
    if isinstance(details, dict):
        # default=str: a config dict can hold a token function or a transport,
        # which JSON has no form for; a log line must not raise.
        details_str = json.dumps(details, default=str)
    else:
        details_str = str(details)
    return f"[{timestamp}] [{level}] [{operation}] {details_str}"


def log(operation: str, details: Union[str, dict[str, Any]]) -> None:
    """Log an operation"""
    if not LOG_ENABLED:
        return
    print(_format_log(operation, details), file=sys.stdout)


def warn(operation: str, details: Union[str, dict[str, Any]]) -> None:
    """Log a warning"""
    if not LOG_ENABLED:
        return
    print(_format_log(operation, details, "WARN"), file=sys.stderr)


def error(operation: str, details: Union[str, dict[str, Any]]) -> None:
    """Log an error"""
    if not LOG_ENABLED:
        return
    print(_format_log(operation, details, "ERROR"), file=sys.stderr)


def is_enabled() -> bool:
    """Check if logging is enabled"""
    return LOG_ENABLED

