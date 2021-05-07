from __future__ import annotations

import asyncio
import textwrap
from typing import Any, Dict, Tuple

import libvirt

from . import _routing

from .. import objects


@_routing.handler("DescribeAvailabilityZones")
async def describe_availability_zones(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    return {
        "availabilityZoneInfo": [{
            "optInStatus": "opt-in-not-required",
            "zoneName": "us-east-2a",
            "zoneId": "us-east-2a",
            "zoneState": "available",
            "regionName": "us-east-2",
        }],
    }