from __future__ import annotations
from typing import (
    Any,
    Dict,
    FrozenSet,
    List,
)

import collections
import itertools
import ipaddress
import json
import sqlite3
import uuid

import libvirt

from . import _routing
from . import errors
from .. import objects
from .. import qemu


PUBLIC_IP_BLOCK_SIZE = 16


class AddressLimitExceededError(_routing.ClientError):

    code = "AddressLimitExceeded"


class InvalidAddressID_NotFound(_routing.ClientError):

    code = "InvalidAddressID.NotFound"


class InvalidAddress_NotFound(_routing.ClientError):

    code = "InvalidAddress.NotFound"


class InvalidAddress_InUse(_routing.ClientError):

    code = "InvalidIPAddress.InUse"


class InvalidAssociationID_NotFound(_routing.ClientError):

    code = "InvalidAssociationID.NotFound"


@_routing.handler("DescribeAddresses")
async def describe_addresses(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    filters = args.get("Filter")
    requested_ips = args.get("PublicIp")
    tags = []
    ips = []
    instances = []
    alloc_ids = args.get("AllocationId", [])
    assoc_ids = []
    if filters:
        assert isinstance(filters, list)
        for flt in filters:
            if flt["Name"].startswith("tag:"):
                tagname = flt["Name"][len("tag:") :]
                tagvalue = flt["Value"]
                tags.append((tagname, tagvalue))
            elif flt["Name"] == "public-ip":
                ips.append(flt["Value"])
            elif flt["Name"] == "instance-id":
                instances.append(flt["Value"])
            elif flt["Name"] == "allocation-id":
                alloc_ids.append(flt["Value"])
            elif flt["Name"] == "association-id":
                assoc_ids.append(flt["Value"])
            else:
                raise _routing.InvalidParameterError(
                    f"unsupported filter type: {flt['Name']}"
                )

    requested_ip_set: FrozenSet[str]

    if requested_ips:
        requested_ip_set = frozenset(requested_ips) - frozenset(ips)
    elif ips:
        requested_ip_set = frozenset(ips)

    quals = []
    qargs: List[Any] = []
    if requested_ips:
        quals.append(
            f"ip_address IN ({','.join(('?',) * len(requested_ip_set))})"
        )
        qargs.extend(requested_ip_set)
    if instances:
        quals.append(f"instance_id IN ({','.join(('?',) * len(instances))})")
        qargs.extend(instances)
    if alloc_ids:
        quals.append(f"allocation_id IN ({','.join(('?',) * len(alloc_ids))})")
        qargs.extend(alloc_ids)
    if assoc_ids:
        quals.append(
            f"association_id IN ({','.join(('?',) * len(assoc_ids))})"
        )
        qargs.extend(assoc_ids)

    if tags:
        tag_filters = " OR ".join(
            f"(tagname = ? AND tagvalue IN ({','.join(('?',) * len(tvals))}))"
            for (_tn, tvals) in tags
        )

        quals.append(
            f"""ip_address IN (
                    SELECT resource_name FROM tags
                    WHERE
                      resource_type = 'ip_address'
                      AND ({tag_filters})
                )
            """
        )
        qargs.extend(
            itertools.chain.from_iterable(
                [tagname] + tagvalues for (tagname, tagvalues) in tags
            )
        )

    query = """
        SELECT
            ip_address,
            instance_id,
            allocation_id,
            association_id
        FROM
            ip_addresses
    """
    if quals:
        query += f" WHERE {' AND '.join(quals)}"

    with app["db"]:
        cur = app["db"].execute(query, qargs)
        addresses = cur.fetchall()

        cur = app["db"].execute(
            f"""
                SELECT
                    resource_name, tagname, tagvalue
                FROM
                    tags
                WHERE
                    resource_type = 'ip_address'
                    AND resource_name IN (
                        {','.join(('?',) * len(addresses))}
                    )
            """,
            [addr[0] for addr in addresses],
        )
        addr_tags: Dict[str, Dict[str, str]] = collections.defaultdict(dict)
        for tag in cur.fetchall():
            addr_tags[tag[0]][tag[1]] = tag[2]

    return {
        "addressesSet": [
            {
                "publicIp": addr[0],
                "instanceId": addr[1],
                "allocationId": addr[2],
                "associationId": addr[3],
                "domain": "vpc",
                "tagSet": [
                    {"key": k, "value": v}
                    for k, v in addr_tags[addr[0]].items()
                ],
            }
            for addr in addresses
        ],
    }


@_routing.handler("AllocateAddress")
async def allocate_address(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    address = args.get("Address")
    if address:
        raise _routing.InvalidParameterError(
            "claiming existing addresses is not supported"
        )

    domain = args.get("Domain")
    if domain and domain != "vpc":
        raise _routing.InvalidParameterError(
            "standard domain is not supported"
        )

    cur = app["db"].cursor()
    cur.execute(
        f"""
        SELECT ip_address FROM ip_addresses
    """
    )
    existing = {ipaddress.IPv4Address(row[0]) for row in cur.fetchall()}
    cur.close()

    net = objects.network_from_xml(app["libvirt_net"].XMLDesc())
    ip_range_start = int(net.static_ip_range[0])
    ip_range_end = max(
        ip_range_start + PUBLIC_IP_BLOCK_SIZE,
        int(net.static_ip_range[1]),
    )
    for int_addr in range(ip_range_start, ip_range_end):
        address = ipaddress.IPv4Address(int_addr)
        if address not in existing:
            break
    else:
        raise AddressLimitExceededError(
            "libvirt network is out of static addresses"
        )

    tags = {}
    tag_spec = args.get("TagSpecification")
    if tag_spec:
        for spec_entry in tag_spec:
            tag_entries = spec_entry["Tag"]
            for tag in tag_entries:
                tags[tag["Key"]] = tag["Value"]

    cur = app["db"].cursor()

    if tags:
        cur.executemany(
            """
                INSERT INTO tags
                    (resource_name, resource_type, tagname, tagvalue)
                VALUES (?, ?, ?, ?)
            """,
            [[str(address), "ip_address", n, v] for n, v in tags.items()],
        )

    allocation_id = f"eipalloc-{uuid.uuid4()}"
    cur.execute(
        """
            INSERT INTO ip_addresses
                (allocation_id, ip_address)
            VALUES (?, ?)
        """,
        [allocation_id, str(address)],
    )

    app["db"].commit()

    return {
        "publicIp": str(address),
        "domain": "vpc",
        "allocationId": allocation_id,
    }


@_routing.handler("AssociateAddress")
async def associate_address(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    pool: libvirt.virStoragePool = app["libvirt_pool"]

    alloc_id = args.get("AllocationId")
    if not alloc_id:
        raise _routing.InvalidParameterError("missing required AllocationId")

    instance_id = args.get("InstanceId")
    if not alloc_id:
        raise _routing.InvalidParameterError("missing required InstanceId")

    vir_conn = pool.connect()
    try:
        new_virdom = vir_conn.lookupByName(instance_id)
    except libvirt.libvirtError as e:
        raise errors.InvalidInstanceID_NotFound(
            f"invalid InstanceId: {e}"
        ) from e

    net = objects.network_from_xml(app["libvirt_net"].XMLDesc())

    assoc_id = f"eipassoc-{uuid.uuid4()}"

    db_conn = app["db"]

    with db_conn:
        cur = db_conn.execute(
            """
                SELECT instance_id, ip_address
                FROM ip_addresses
                WHERE allocation_id = ?
            """,
            [alloc_id],
        )

        row = cur.fetchone()
        if row is None:
            raise InvalidAddressID_NotFound(
                "could not find address for specified AllocationId"
            )

        cur_instance_id, ip_address = row

    if cur_instance_id is not None:
        try:
            cur_virdom = vir_conn.lookupByName(cur_instance_id)
        except libvirt.libvirtError:
            app["logger"].warning(
                "cannot find currently associated instance",
                exc_info=True,
            )
        else:
            iface = await _find_interface(cur_virdom, net.ip_network)
            await qemu.agent_exec(
                cur_virdom,
                ["ip", "addr", "del", ip_address, "dev", iface],
            )

    iface = await _find_interface(new_virdom, net.ip_network)
    result = await qemu.agent_exec(
        new_virdom,
        ["ip", "addr", "add", ip_address, "dev", iface],
    )

    if result.returncode != 0:
        raise _routing.InternalServerError(
            f"could not assign address in VM: {result.returncode}\n"
            f"{result.stderr.read().decode('utf-8', errors='replace')}"
        )

    with db_conn:
        db_conn.execute(
            """
                UPDATE
                    ip_addresses
                SET
                    association_id = ?,
                    instance_id = ?
                WHERE
                    allocation_id = ?
            """,
            [assoc_id, instance_id, alloc_id],
        )

    return {
        "return": "true",
        "associationId": assoc_id,
    }


@_routing.handler("DisassociateAddress")
async def disassociate_address(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    pool: libvirt.virStoragePool = app["libvirt_pool"]

    assoc_id = args.get("AssociationId")
    if not assoc_id:
        raise _routing.InvalidParameterError("missing required AssociationId")

    db_conn = app["db"]

    with db_conn:
        cur = db_conn.execute(
            """
                SELECT instance_id, ip_address
                FROM ip_addresses
                WHERE association_id = ?
            """,
            [assoc_id],
        )

        row = cur.fetchone()
        if row is None:
            raise InvalidAssociationID_NotFound(
                "could not find address for specified AssociationId"
            )

        cur_instance_id, ip_address = row

    if cur_instance_id is not None:
        vir_conn = pool.connect()

        try:
            cur_virdom = vir_conn.lookupByName(cur_instance_id)
        except libvirt.libvirtError:
            app["logger"].warning(
                "cannot find currently associated instance",
                exc_info=True,
            )
        else:
            await qemu.agent_exec(
                cur_virdom,
                ["ip", "addr", "del", ip_address, "dev", "vif0"],
            )

    with db_conn:
        db_conn.execute(
            """
                UPDATE
                    ip_addresses
                SET
                    association_id = NULL,
                    instance_id = NULL
                WHERE
                    association_id = ?
            """,
            [assoc_id],
        )

    return {
        "return": "true",
    }


@_routing.handler("ReleaseAddress")
async def release_address(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    alloc_id = args.get("AllocationId")
    if not alloc_id:
        raise _routing.InvalidParameterError("missing required AllocationId")

    db_conn = app["db"]

    with db_conn:
        cur = db_conn.execute(
            """
                SELECT instance_id
                FROM ip_addresses
                WHERE allocation_id = ?
            """,
            [alloc_id],
        )

        row = cur.fetchone()
        if row is None:
            raise InvalidAddress_NotFound(
                "could not find address for specified AllocationId"
            )

        cur_instance_id = row[0]
        if cur_instance_id is not None:
            raise InvalidAddress_InUse(
                f"specified address is in use by instance {cur_instance_id}, "
                f"call DisassociateAddress first"
            )

        db_conn.execute(
            """
                DELETE FROM
                    tags
                WHERE
                    resource_name = (
                        SELECT
                            ip_address
                        FROM
                            ip_addresses
                        WHERE
                            allocation_id = ?
                    )
                    AND resource_type = 'ip_address'
            """,
            [alloc_id],
        )

        db_conn.execute(
            """
                DELETE FROM
                    ip_addresses
                WHERE
                    allocation_id = ?
            """,
            [alloc_id],
        )

    return {
        "return": "true",
    }


@_routing.handler("AssignPrivateIpAddresses")
async def assign_private_ip_addresses(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    lvirt_conn: libvirt.virConnect = app["libvirt"]

    interface_id = args.get("NetworkInterfaceId")
    if not interface_id:
        raise _routing.InvalidParameterError(
            "missing required NetworkInterfaceId"
        )

    instance_id, _, ifname = interface_id[4:].rpartition("::")
    if not instance_id:
        raise _routing.InvalidParameterError(
            f"NetworkInterfaceId is expected to be in the "
            f"eni-<instance_id>::<ifname> format', got {interface_id!r}"
        )

    count = args.get("SecondaryPrivateIpAddressCount")
    if not count:
        raise _routing.InvalidParameterError(
            "missing required SecondaryPrivateIpAddressCount"
        )
    try:
        addr_count = int(count)
    except ValueError:
        raise _routing.InvalidParameterError(
            "SecondaryPrivateIpAddressCount must be a positive integer"
        )

    if addr_count <= 0:
        raise _routing.InvalidParameterError(
            "SecondaryPrivateIpAddressCount must be a positive integer"
        )

    if not interface_id.startswith("eni-"):
        raise _routing.InvalidParameterError(
            f"NetworkInterfaceId must start with 'eni-', got {interface_id!r}"
        )

    try:
        vir_domain = lvirt_conn.lookupByName(instance_id)
    except libvirt.libvirtError as e:
        raise errors.InvalidInstanceID_NotFound(
            f"invalid InstanceId: {e}"
        ) from e

    net = objects.network_from_xml(app["libvirt_net"].XMLDesc())

    db_conn: sqlite3.Connection = app["db"]

    new_addrs = []

    with db_conn:
        cur = db_conn.execute(
            """
                SELECT ip_address
                FROM private_ip_addresses
                WHERE instance_id IS NOT NULL
            """,
        )
        taken_addrs = {ipaddress.ip_address(row[0]) for row in cur.fetchall()}

        ip_range_start = int(net.static_ip_range[0]) + PUBLIC_IP_BLOCK_SIZE
        ip_range_end = int(net.static_ip_range[1])

        for int_addr in range(ip_range_start, ip_range_end):
            address = ipaddress.IPv4Address(int_addr)
            if address in taken_addrs:
                continue

            new_addrs.append(address)

            db_conn.execute(
                """
                    INSERT INTO private_ip_addresses(
                        ip_address,
                        instance_id,
                        interface
                    )
                    VALUES
                        (?, ?, ?)
                """,
                [str(address), instance_id, ifname],
            )
            if len(new_addrs) == addr_count:
                break

        if len(new_addrs) < addr_count:
            raise AddressLimitExceededError(
                "libvirt network is out of static addresses"
            )

        assigned_addrs = []

        for new_addr in new_addrs:
            result = await qemu.agent_exec(
                vir_domain,
                ["ip", "addr", "add", str(new_addr), "dev", ifname],
            )

            if result.returncode != 0:
                placeholders = ", ".join(["?"] * len(assigned_addrs))
                db_conn.execute(
                    f"""
                        DELETE FROM private_ip_addresses
                        WHERE ip_address IN ({placeholders})
                    """,
                    assigned_addrs,
                )

                raise _routing.InternalServerError(
                    f"could not assign address in VM: {result.returncode}\n"
                    f"{result.stderr.read().decode('utf-8', errors='replace')}"
                )
            else:
                assigned_addrs.append(str(new_addr))

    return {
        "networkInterfaceId": interface_id,
        "return": True,
        "assignedPrivateIpAddressesSet": [
            {
                "privateIpAddress": new_addr,
            }
            for new_addr in assigned_addrs
        ],
        "assignedIpv4PrefixSet": [],
    }


@_routing.handler("UnassignPrivateIpAddresses")
async def unassign_private_ip_addresses(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    lvirt_conn: libvirt.virConnect = app["libvirt"]

    interface_id = args.get("NetworkInterfaceId")
    if not interface_id:
        raise _routing.InvalidParameterError(
            "missing required NetworkInterfaceId"
        )

    instance_id, _, ifname = interface_id[4:].rpartition("::")
    if not instance_id:
        raise _routing.InvalidParameterError(
            f"NetworkInterfaceId is expected to be in the "
            f"eni-<instance_id>::<ifname> format', got {interface_id!r}"
        )

    try:
        vir_domain = lvirt_conn.lookupByName(instance_id)
    except libvirt.libvirtError as e:
        raise errors.InvalidInstanceID_NotFound(
            f"invalid InstanceId: {e}"
        ) from e

    addrs = args.get("PrivateIpAddress")
    if not addrs:
        raise _routing.InvalidParameterError(
            "missing required PrivateIpAddress"
        )

    db_conn: sqlite3.Connection = app["db"]

    with db_conn:
        placeholders = ", ".join(["?"] * len(addrs))
        cur = db_conn.execute(
            f"""
                SELECT ip_address
                FROM private_ip_addresses
                WHERE
                    instance_id = ?
                    AND interface = ?
                    AND ip_address IN ({placeholders})
            """,
            [instance_id, ifname] + addrs,
        )
        recorded_addrs = cur.fetchall()
        if len(recorded_addrs) != len(addrs):
            raise _routing.InvalidParameterError(
                f"Some of the specified addresses are not assigned to "
                f"interface {interface_id}"
            )

        for addr in addrs:
            result = await qemu.agent_exec(
                vir_domain,
                ["ip", "addr", "del", addr, "dev", ifname],
            )

            if result.returncode != 0:
                raise _routing.InternalServerError(
                    f"could not unassign address in VM: {result.returncode}\n"
                    f"{result.stderr.read().decode('utf-8', errors='replace')}"
                )
            else:
                db_conn.execute(
                    f"""
                        DELETE FROM private_ip_addresses
                        WHERE ip_address = ?
                    """,
                    [addr],
                )

    return {
        "return": True,
    }


async def describe_network_ifaces(
    lvirt_conn: libvirt.virConnect,
    domain: objects.Domain,
) -> list[dict[str, Any]]:
    vir_domain = lvirt_conn.lookupByName(domain.name)
    if vir_domain.state()[0] != libvirt.VIR_DOMAIN_RUNNING:
        return []

    ifaces = []

    result = await qemu.agent_exec(
        vir_domain,
        ["ip", "-json", "addr", "list"],
    )

    if result.returncode != 0:
        raise _routing.InternalServerError(
            f"could not read interfaces in VM: {result.returncode}\n"
            f"{result.stderr.read().decode('utf-8', errors='replace')}"
        )
    else:
        output = result.stdout.read()
        try:
            ip_output = json.loads(output)
        except Exception as e:
            output_str = output.decode("utf-8", errors="replace")
            raise _routing.InternalServerError(
                f"could not decode output of `ip addr list` in VM: \n"
                f"{e}\nOUTPUT:\n{output_str}"
            )

        for iface in ip_output:
            if iface.get("link_type") != "ether":
                continue

            ifname = iface["ifname"]
            addrs = [
                addr["local"]
                for addr in iface["addr_info"]
                if addr["family"] == "inet"
            ]
            if not addrs:
                # No assigned addresses?  Just skip it.
                continue

            iface_id = f"{domain.name}::{ifname}"

            iface_desc = {
                "networkInterfaceId": f"eni-{iface_id}",
                "attachment": {
                    "attachmentId": f"eni-attach-{iface_id}",
                    "deviceIndex": int(iface["ifindex"]) - 1,
                    "status": "attached",
                    "attachTime": "2023-01-08T16:46:19.000Z",
                    "deleteOnTermination": True,
                },
                "subnetId": "subnet-decafbad",
                "groupSet": [
                    {
                        "groupId": "sg-decafbad",
                        "groupName": "SecurityGroup",
                    }
                ],
                "vpcId": "vpc-12345678",
                "ownerId": "000000000000",
                "status": "in-use",
                "macAddress": iface["address"],
                "privateDnsName": addrs[0],
                "privateIpAddress": addrs[0],
                "privateIpAddressesSet": [
                    {
                        "privateIpAddress": addr,
                        "privateDnsName": addr,
                        "primary": i == 0,
                    }
                    for i, addr in enumerate(addrs)
                ],
            }

            ifaces.append(iface_desc)

    return ifaces


async def _find_interface(
    domain: libvirt.virDomain,
    network: ipaddress.IPv4Network,
) -> str:
    result = await qemu.agent_exec(
        domain,
        ["ip", "-json", "addr", "show"],
    )

    if result.returncode != 0:
        raise _routing.InternalServerError(
            "could not list VM network interfaces"
        )

    interfaces = json.loads(result.stdout.read())
    for iface_desc in interfaces:
        for addr in iface_desc["addr_info"]:
            if addr["family"] != "inet":
                continue
            addr_net = ipaddress.IPv4Network(
                f"{addr['local']}/{addr['prefixlen']}",
                strict=False,
            )
            if addr_net == network:
                return iface_desc["ifname"]  # type: ignore [no-any-return]

    raise _routing.InternalServerError(
        f"could not find interface for network {network}"
    )
