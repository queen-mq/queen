"""
NameOperator — ``.name('perTenant')``: the name of the stateful step before
it, the one a live read asks for (``runner.state('perTenant')``,
``Stream.view(client, query_id).get('perTenant', key)``). Unnamed steps are
``step1``, ``step2``, ... in chain order. Port of NameOperator.js.
"""

from __future__ import annotations

import re
from typing import Any

from ._feed_opts import jstr

_NAME = re.compile(r"[A-Za-z0-9_.-]{1,64}")


class NameOperator:
    kind = "name"

    def __init__(self, name: Any):
        if not isinstance(name, str) or not _NAME.fullmatch(name):
            raise ValueError(f".name(): a step name is 1 to 64 letters, digits, '_', '.' or '-', got {jstr(name)}")
        self.name = name
        self.config = {"kind": "name", "name": name}
