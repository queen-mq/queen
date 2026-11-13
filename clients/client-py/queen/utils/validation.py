"""
Validation utilities
"""

import re
from typing import Any, Dict, List, Mapping, Optional

UUID_V4_REGEX = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", re.IGNORECASE
)

# A header name: 1 to 64 bytes of a-z, 0-9, '.', '_' or '-'. The broker sets the
# names that start with "queen-".
HEADER_NAME_REGEX = re.compile(r"^[a-z0-9._-]{1,64}\Z")
RESERVED_HEADER_PREFIX = "queen-"


def is_valid_uuid(s: str) -> bool:
    """Check if string is a valid UUID v4"""
    return isinstance(s, str) and UUID_V4_REGEX.match(s) is not None


def validate_headers(headers: Any) -> Optional[Dict[str, str]]:
    """A message's headers, checked: a dict of strings, or None for none.

    Only the rules that never change are checked here (the names, the reserved
    ``queen-`` prefix, string values). How many headers and how many bytes a
    message may carry are the broker's defaults (64 and 8 KiB,
    QUEEN_MAX_HEADERS / QUEEN_MAX_HEADER_BYTES): it refuses more with a 400.
    """
    if headers is None:
        return None
    if not isinstance(headers, Mapping):
        raise ValueError(f"headers must be a dict of strings, got {type(headers).__name__}")
    for name, value in headers.items():
        if not isinstance(name, str) or not HEADER_NAME_REGEX.match(name):
            raise ValueError(
                f"header name {name!r}: 1 to 64 characters of a-z, 0-9, '.', '_' or '-'"
            )
        if name.startswith(RESERVED_HEADER_PREFIX):
            raise ValueError(f"header name {name!r}: names starting with 'queen-' are the broker's")
        if not isinstance(value, str):
            raise ValueError(f"header {name!r}: the value must be a string, got {type(value).__name__}")
    return dict(headers) if headers else None


def push_item_headers(item: Mapping[str, Any]) -> Optional[Dict[str, str]]:
    """The headers of one pushed item, checked; None when it has none.

    Read only beside ``data`` or ``payload``: an item with neither is itself
    the payload, as it always was, so a ``headers`` key in it stays data.
    """
    if "data" not in item and "payload" not in item:
        return None
    return validate_headers(item.get("headers"))


def validate_queue_name(name: str) -> str:
    """Validate and normalize queue name"""
    if not isinstance(name, str) or not name.strip():
        raise ValueError("Queue name must be a non-empty string")
    return name.strip()


def validate_url(url: str) -> str:
    """Validate URL"""
    if not isinstance(url, str) or not url.startswith("http"):
        raise ValueError(f"Invalid URL: {url}")
    return url


def validate_urls(urls: List[str]) -> List[str]:
    """Validate list of URLs"""
    if not isinstance(urls, list) or not urls:
        raise ValueError("URLs must be a non-empty list")
    return [validate_url(url) for url in urls]

