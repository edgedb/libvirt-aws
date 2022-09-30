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
    region = app["region"]
    return {
        "availabilityZoneInfo": [
            {
                "optInStatus": "opt-in-not-required",
                "zoneName": f"{region}{az}",
                "zoneId": f"{region}{az}",
                "zoneState": "available",
                "regionName": region,
            }
            for az in ["a", "b", "c"]
        ],
    }
