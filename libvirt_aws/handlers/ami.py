from __future__ import annotations
from typing import (
    Any,
    Dict,
    Optional,
)

import base64
import dataclasses
import glob
import json
import os
import random
import subprocess
import tempfile
import uuid
from xml.etree import ElementTree

import libvirt

from . import _routing


@_routing.handler("DescribeImages")
async def describe_images(
    args: _routing.HandlerArgs,
    app: _routing.App
) -> Dict[str, Any]:
    pool = app["libvirt_pool"]
    xml = ElementTree.fromstring(pool.XMLDesc())
    pool_dir = xml.findall("./target/path")[0].text
    print(pool_dir)

    images = []
    if pool_dir:
        for entry in glob.glob(os.path.join(pool_dir, "*.json")):
            with open(entry) as f:
                images.append(json.load(f))

    return {"imagesSet": images}


@dataclasses.dataclass
class Capacity:
    value: int
    unit: str


@dataclasses.dataclass
class Volume:
    name: str
    capacity: Capacity
    path: Optional[str]


def describe_volume(vol: libvirt.virStorageVol) -> Volume:
    xml = ElementTree.fromstring(vol.XMLDesc())
    capacity = xml.find("./capacity")
    if capacity is None:
        raise ValueError("vol xml is missing required tag ./capacity")

    path = xml.find("./target/path")
    if path is None:
        raise ValueError("vol xml is missing required tag ./backingStore/path")

    return Volume(
        name=vol.name(),
        capacity=Capacity(
            value=int(capacity.text or "", base=10),
            unit=capacity.attrib['unit'],
        ),
        path=path.text,
    )


def rnd_mac_addr() -> str:
    a = random.randint(0, 255)
    b = random.randint(0, 255)
    c = random.randint(0, 255)
    return f"02:00:00:{a:02x}:{b:02x}:{c:02x}"


def create_iso_vol(pool: libvirt.virStoragePool, name: str, data: bytes) -> Volume:

    with tempfile.TemporaryDirectory(prefix='libvirt-aws') as tmp_dir:
        user_data = os.path.join(tmp_dir, 'user-data')
        with open(user_data, 'wb') as f:
            f.write(data)

        meta_data = os.path.join(tmp_dir, 'meta-data')
        with open(meta_data, 'wb') as f:
            pass

        iso = os.path.join(tmp_dir, name)
        subprocess.check_call([
            'mkisofs',
            '-output', iso,
            '-volid', 'cidata',
            '-joliet',
            '-rock',
            user_data,
            meta_data,
        ])

        size = os.stat(iso).st_size
        vol = pool.createXML(f'''
            <volume>
                <name>{name}</name>
                <capacity unit="bytes">{size}</capacity>
                <target>
                    <format type="raw"></format>
                    <permissions>
                        <mode>644</mode>
                    </permissions>
                </target>
            </volume>
        ''')

        subprocess.check_call([
            'virsh',
            'vol-upload',
            '--pool', pool.name(),
            '--vol', name,
            '--file', iso,
        ])

    return describe_volume(vol)


@_routing.handler("RunInstances")
async def run_instance(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    lvirt_conn: libvirt.virConnect = app['libvirt']
    pool: libvirt.virStoragePool = app["libvirt_pool"]
    net: libvirt.virNetwork = app["libvirt_net"]
    print("RunInstances args:", args)

    image_id = args.get("ImageId")
    if not image_id:
        raise _routing.InvalidParameterError("missing required ImageId")

    user_data = args.get("UserData")
    if not user_data:
        raise _routing.InvalidParameterError("missing required UserData")
    user_data = base64.b64decode(user_data)

    inst_id = f'{random.randint(0, 9999):04}'
    os_backing_vol = describe_volume(pool.storageVolLookupByName(image_id))
    sys_vol = pool.createXML(f'''
        <volume>
            <name>{inst_id}-{os_backing_vol.name}</name>
            <capacity unit="{os_backing_vol.capacity.unit}">{os_backing_vol.capacity.value}</capacity>
            <target>
                <format type="qcow2"></format>
                <permissions>
                    <mode>644</mode>
                </permissions>
            </target>
            <backingStore>
                <path>{os_backing_vol.path}</path>
                <format type="qcow2"></format>
            </backingStore>
        </volume>
    ''')

    cloud_init_vol = create_iso_vol(pool, f'{inst_id}-cloud-init.iso', user_data)

    filesystem_dir = os.path.join(app["fs_dir"], inst_id)
    os.mkdir(filesystem_dir)
    domain = lvirt_conn.defineXML(f'''
        <domain type="kvm">
            <name>{inst_id}</name>
            <memory unit="MiB">512</memory>
            <vcpu>1</vcpu>
            <os>
                <type>hvm</type>
            </os>
            <features>
                <pae/>
                <acpi/>
                <apic/>
            </features>
            <cpu><topology sockets="1" cores="1" threads="1"/></cpu>
            <devices>
                <disk type="volume" device="disk">
                    <driver name="qemu" type="qcow2"/>
                    <source pool="{pool.name()}" volume="{sys_vol.name()}"/>
                    <target dev="vda" bus="virtio"/>
                </disk>
                <disk type="file" device="cdrom">
                    <driver name="qemu" type="raw"/>
                    <source file="{cloud_init_vol.path}"/>
                    <target dev="hdd" bus="ide"/>
                    <serial>cloudinit</serial>
                </disk>
                <interface type="network">
                    <mac address="{rnd_mac_addr()}"/>
                    <source network="{net.name()}"/>
                    <model type="virtio"/>
                </interface>
                <console>
                    <target type="serial" port="0"/>
                </console>
                <channel type="unix">
                    <target type="virtio" name="org.qemu.guest_agent.0"/>
                </channel>
                <graphics type="spice" autoport="yes"/>
                <rng model="virtio">
                    <backend model="random">/dev/urandom</backend>
                </rng>
                <filesystem type="mount" accessmode="passthrough">
                    <driver type="virtiofs"/>
                    <source dir="{filesystem_dir}"/>
                    <target dir="/virtio/cache"/>
                </filesystem>
            </devices>
            <memoryBacking>
                <source type="memfd"/>
                <access mode="shared"/>
            </memoryBacking>
        </domain>
    ''')
    domain.setAutostart(1)
    domain.create()


    # array<TagSpecification> https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_TagSpecification.html
    tags = {}
    for spec_entry in args.get("TagSpecification", []):
        tag_entries = spec_entry["Tag"]
        for tag in tag_entries:
            tags[tag["Key"]] = tag["value"]

    if tags:
        cur = app["db"].cursor()
        cur.executemany(
            """
                INSERT INTO tags
                (resource_name, resource_type, tagname, tagvalue)
                VALUES (?, ?, ?, ?)
            """,
            [[inst_id, "instance", n, v] for n, v in tags.items()],
        )
        app["db"].commit()

    return {'instancesSet': [{
        'instanceId': inst_id,
    }]}
