from __future__ import annotations

from typing import Any

import libvirt

from .. import objects

from . import _routing
from . import ips
from . import volumes


@_routing.handler("DescribeInstances")
async def describe_instances(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> dict[str, Any]:
    pool: libvirt.virStoragePool = app["libvirt_pool"]
    net: libvirt.virNetwork = app["libvirt_net"]
    lvirt_conn: libvirt.virConnect = app["libvirt"]

    instance_ids = set(args.get("InstanceId", ()))
    result = []

    for domain in objects.get_all_domains(lvirt_conn):
        domname = domain.name
        if not instance_ids or domname in instance_ids:
            block_devices = await _describe_block_devices(pool, domain)
            network_ifaces = await ips.describe_network_ifaces(
                lvirt_conn, net, domain
            )

            result.append(
                {
                    "instanceId": domname,
                    "instanceType": "t2.micro",
                    "blockDeviceMapping": block_devices,
                    "networkInterfaceSet": network_ifaces,
                }
            )

    return {
        "reservationSet": [
            {
                "reservationId": "dummy",
                "ownerId": "dummy",
                "instancesSet": result,
            }
        ]
    }


async def _describe_block_devices(
    lvirt_pool: libvirt.virStoragePool,
    domain: libvirt.virDomain,
) -> list[dict[str, Any]]:
    block_devices = []
    existing = set()
    for disk in domain.disks:
        if disk.pool != lvirt_pool.name():
            continue

        att = disk.attachment
        block_devices.append(
            {
                "deviceName": f"/dev/{att.device}",
                "ebs": {
                    "volumeId": att.volume,
                    "status": volumes.get_attachment_status(att),
                },
            }
        )
        existing.add((att.volume, att.domain))

    recent_atts = volumes.get_known_attachments()
    for (vol, dom), (device, status) in recent_atts.items():
        if (vol, dom) not in existing and status != "detached":
            block_devices.append(
                {
                    "deviceName": f"/dev/{device}",
                    "ebs": {
                        "volumeId": vol,
                        "status": status,
                    },
                }
            )

    return block_devices
