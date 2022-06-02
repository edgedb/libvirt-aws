from __future__ import annotations

from . import _routing


class InvalidInstanceID_NotFound(_routing.ClientError):

    code = "InvalidInstanceID.NotFound"
