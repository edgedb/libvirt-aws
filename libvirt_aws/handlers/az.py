from __future__ import annotations
from typing import (
    Any,
    Dict,
)

from . import _routing


@_routing.handler("DescribeAvailabilityZones")
async def describe_availability_zones(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    return {
        "availabilityZoneInfo": [
            {
                "optInStatus": "opt-in-not-required",
                "zoneName": "us-east-2a",
                "zoneId": "us-east-2a",
                "zoneState": "available",
                "regionName": "us-east-2",
            }
        ],
    }
