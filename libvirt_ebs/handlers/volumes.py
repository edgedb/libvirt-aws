from __future__ import annotations

import asyncio
import datetime
import textwrap
from typing import Any, Dict, Tuple
import uuid

import libvirt

from . import _routing

from .. import objects


class InvalidAttachmentNotFound(_routing.ClientError):

    code = "InvalidAttachment.NotFound"


class InvalidVolumeNotFound(_routing.ClientError):

    code = "InvalidVolume.NotFound"


_known_attachments: Dict[Tuple[str, str], Tuple[str, str]] = {}


@_routing.handler("CreateVolume")
async def create_volume(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    pool: libvirt.virStoragePool = app['libvirt_pool']
    size = args.get("Size")
    if not size:
        raise _routing.InvalidParameterError(
            "missing required Size")

    az = args.get("AvailabilityZone")
    if not az:
        raise _routing.InvalidParameterError(
            "missing required AvailabilityZone")

    voltype = args.get("VolumeType")
    if not voltype:
        voltype = "gp2"

    volname = f'{uuid.uuid4()}.qcow2'

    xml = textwrap.dedent(f"""\
    <volume type='file'>
        <name>{volname}</name>
        <capacity unit="G">{size}</capacity>
        <target>
            <path>{volname}</path>
            <format type='qcow2'/>
            <compat>1.1</compat>
        </target>
    </volume>""")

    pool.createXML(
        xml,
        flags=libvirt.VIR_STORAGE_VOL_CREATE_PREALLOC_METADATA
    )

    create_time = datetime.datetime.now(datetime.timezone.utc)

    tags = {}
    tag_spec = args.get("TagSpecification")
    if tag_spec:
        for spec_entry in tag_spec:
            tag_entries = spec_entry["Tag"]
            for tag in tag_entries:
                tags[tag["Key"]] = tag["Value"]

    if tags:
        cur = app["db"].cursor()
        cur.executemany(
            """
                INSERT INTO volume_tags (volname, tagname, tagvalue)
                VALUES (?, ?, ?)
            """,
            [[volname, n, v] for n, v in tags.items()],
        )
        app["db"].commit()

    return {
        "volumeId": volname,
        "size": size,
        "iops": 10000,
        "availabilityZone": az,
        "snapshotId": None,
        "status": "creating",
        "createTime": create_time.strftime("%Y-%m-%dT%H:%M:%S.%f000Z"),
        "volumeType": voltype,
        "tagSet": [{"key": k, "value": v} for k, v in tags.items()],
        "multiAttachEnabled": "false",
    }


@_routing.handler("DeleteVolume")
async def delete_volume(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    pool: libvirt.virStoragePool = app['libvirt_pool']
    volname = args.get("VolumeId")
    if not volname:
        raise _routing.InvalidParameterError(
            "missing required VolumeId")

    try:
        vol = pool.storageVolLookupByName(volname)
    except libvirt.libvirtError as e:
        raise InvalidVolumeNotFound(e.args[0]) from None

    vol.delete()

    return {
        "return": "true",
    }


@_routing.handler("DescribeVolumes")
async def describe_volumes(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    pool: libvirt.virStoragePool = app['libvirt_pool']
    volume_ids = set(args.get('VolumeId', ()))
    result = []
    filters = args.get("Filter")
    if filters:
        filtered_volume_ids = set()
        for flt in filters:
            if flt["Name"].startswith("tag:"):
                tagname = flt["Name"][len("tag:")]
                tagvalue = flt["Value"]

                cur = app["db"].cursor()
                cur.execute(f"""
                    SELECT volname FROM volume_tags
                    WHERE tagname = ?
                    AND tagvalue IN ({",".join(["?"] * len(tagvalue))})
                """, [tagname] + list(tagvalue))
                filtered_volume_ids.update(cur.fetchall())
                cur.close()
            else:
                raise _routing.InvalidParameterError(
                    f"unsupported filter type: {flt['Name']}")

        if volume_ids:
            volume_ids -= filtered_volume_ids
        else:
            volume_ids = filtered_volume_ids

    for volume in objects.get_all_volumes(pool):
        volname = volume.name
        if (not volume_ids and not filters) or volname in volume_ids:
            result.append(_describe_volume(pool, volume))

    return {
        "volumeSet": result,
    }


@_routing.handler("AttachVolume")
async def attach_volume(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    pool: libvirt.virStoragePool = app['libvirt_pool']
    instance_id = args.get("InstanceId")
    if not instance_id:
        raise _routing.InvalidParameterError("missing required InstanceId")
    if not isinstance(instance_id, str):
        raise _routing.InvalidParameterError("invalid InstanceId value")
    volume_id = args.get("VolumeId")
    if not volume_id:
        raise _routing.InvalidParameterError("missing required VolumeId")
    device = args.get("Device")
    if not device:
        raise _routing.InvalidParameterError("missing required Device")
    if not isinstance(device, str):
        raise _routing.InvalidParameterError("invalid Device value")
    if not isinstance(volume_id, str):
        raise _routing.InvalidParameterError("invalid VolumeId value")

    if device.startswith('/'):
        if not device.startswith('/dev/'):
            raise _routing.InvalidParameterError(
                "invalid Device, must start with /dev")
        device = device[len('/dev/'):]

    conn = pool.connect()
    try:
        virdom = conn.lookupByName(instance_id)
    except libvirt.libvirtError as e:
        raise _routing.InvalidParameterError(f"invalid InstanceId: {e}") from e

    try:
        virvol = pool.storageVolLookupByName(volume_id)
    except libvirt.libvirtError as e:
        raise _routing.InvalidParameterError(f"invalid VolumeId: {e}") from e

    volume = objects.volume_from_xml(virvol.XMLDesc(0))

    if _get_volume_status(pool, volume) != "available":
        raise _routing.IncorrectStateError(
            f"Volume {volume.name} is in use and cannot be attached."
        )

    xml = textwrap.dedent(f"""\
    <disk type='volume' device='disk'>
        <driver name='qemu' type='qcow2'/>
        <source pool='{pool.name()}' volume='{volume_id}' />
        <target dev='{device}' bus='virtio'/>
        <serial>lvirtebs-{device}</serial>
    </disk>""")

    try:
        virdom.attachDevice(xml)
    except libvirt.libvirtError as e:
        raise _routing.InternalServerError(str(e)) from e

    # Give the new attachment time to settle.  Alas, there seems to be
    # no obvious way to actually verify the status of the device in the
    # target VM.
    key = (volume_id, instance_id)
    dev = device
    for (vol, dom), (_, att_status) in tuple(_known_attachments.items()):
        if vol == volume_id and att_status == "detached":
            del _known_attachments[vol, dom]
    _known_attachments[key] = (dev, "attaching")
    def _mark_attached() -> None:
        _known_attachments[key] = (dev, "attached")
    asyncio.get_running_loop().call_later(3, _mark_attached)

    return {
        "volumeId": volume_id,
        "instanceId": instance_id,
        "device": f'/dev/{device}',
        "status": "attaching",
    }


@_routing.handler("DetachVolume")
async def detach_volume(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    pool: libvirt.virStoragePool = app['libvirt_pool']
    instance_id = args.get("InstanceId")
    if not instance_id:
        raise _routing.InvalidParameterError("missing required InstanceId")
    if not isinstance(instance_id, str):
        raise _routing.InvalidParameterError("invalid InstanceId value")
    volume_id = args.get("VolumeId")
    if not volume_id:
        raise _routing.InvalidParameterError("missing required VolumeId")
    if not isinstance(volume_id, str):
        raise _routing.InvalidParameterError("invalid VolumeId value")

    conn = pool.connect()
    try:
        virdom = conn.lookupByName(instance_id)
    except libvirt.libvirtError as e:
        raise _routing.InvalidParameterError(f"invalid InstanceId: {e}") from e

    try:
        virvol = pool.storageVolLookupByName(volume_id)
    except libvirt.libvirtError as e:
        raise _routing.InvalidParameterError(f"invalid VolumeId: {e}") from e

    volume = objects.volume_from_xml(virvol.XMLDesc(0))

    attachments = objects.get_vol_attachments(pool, volume)
    device = None
    for attachment in attachments:
        if attachment.domain == instance_id:
            device = attachment.device
            break

    if device is None:
        raise InvalidAttachmentNotFound(
            f"Volume {volume_id} is not attached to Instance {instance_id}")

    xml = textwrap.dedent(f"""\
    <disk type='volume' device='disk'>
        <driver name='qemu' type='qcow2'/>
        <source pool='{pool.name()}' volume='{volume_id}' />
        <target dev='{device}' bus='virtio'/>
    </disk>""")

    try:
        virdom.detachDevice(xml)
    except libvirt.libvirtError as e:
        raise _routing.InternalServerError(str(e)) from e

    # Give the detachment time to settle.  Alas, there seems to be
    # no obvious way to actually verify the status of the device in the
    # target VM.
    key = (volume_id, instance_id)
    dev = device
    _known_attachments[key] = (dev, "detaching")
    def _mark_detached() -> None:
        _known_attachments[key] = (dev, "detached")

    asyncio.get_running_loop().call_later(3, _mark_detached)

    return {
        "volumeId": volume_id,
        "instanceId": instance_id,
        "status": "detaching",
        "device": f'/dev/{device}',
    }


def get_attachment_status(att: objects.VolumeAttachment) -> str:
    key = (att.volume, att.domain)
    state = _known_attachments.get(key)
    if state is None:
        return "attached"
    else:
        return state[1]


def get_known_attachments() -> Dict[Tuple[str, str], Tuple[str, str]]:
    return _known_attachments


def _get_volume_status(
    pool: libvirt.virStoragePool,
    volume: objects.Volume,
) -> str:
    attachments = objects.get_vol_attachments(pool, volume)
    existing = {(att.volume, att.domain) for att in attachments}

    att_set = []
    for att in attachments:
        att_set.append({
            "status": get_attachment_status(att),
        })

    for (vol, dom), (_, status) in _known_attachments.items():
        if (vol, dom) not in existing:
            att_set.append({
                "status": status,
            })

    if all(att["status"] == "detached" for att in att_set):
        status = "available"
    else:
        status = "in-use"

    return status


def _describe_volume(
    pool: libvirt.virStoragePool,
    volume: objects.Volume,
) -> Dict[str, Any]:

    attachments = objects.get_vol_attachments(pool, volume)
    existing = {(att.volume, att.domain) for att in attachments}

    att_set = []
    for att in attachments:
        att_set.append({
            "instanceId": att.domain,
            "volumeId": att.volume,
            "device": f"/dev/{att.device}",
            "status": get_attachment_status(att),
        })

    for (vol, dom), (device, status) in _known_attachments.items():
        if (vol, dom) not in existing and vol == volume.name:
            att_set.append({
                "instanceId": dom,
                "volumeId": vol,
                "device": f"/dev/{device}",
                "status": status,
            })

    if all(att["status"] == "detached" for att in att_set):
        status = "available"
    else:
        status = "in-use"

    return {
        "volumeId": volume.name,
        "volumeType": "standard",
        "size": volume.capacity // 1073741824,
        "status": status,
        "attachmentSet": att_set,
    }
