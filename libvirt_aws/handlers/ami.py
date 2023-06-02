from __future__ import annotations
from typing import (
    Any,
    Dict,
    Optional,
)

import dataclasses
import glob
import json
import os
import random
import subprocess
import tempfile
from xml.etree import ElementTree

import libvirt

from . import _routing
from . import utils


@_routing.handler("DescribeImages")
async def describe_images(
    args: _routing.HandlerArgs,
    app: _routing.App
) -> Dict[str, Any]:
    with app['db'] as db:
        rows = db.execute("SELECT name FROM machine_images").fetchall()

    images = []
    for row in rows:
        images.append({
            'imageId': row[0],
            'name': row[0],
        })

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


@_routing.handler("CreateImage")
async def create_image(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    instance_id = args['InstanceId']
    name = args['Name']
    assert isinstance(name, str), "name is not a string"
    tags = args.get('TagSpecification')

    lvirt_conn: libvirt.virConnect = app['libvirt']
    domain = lvirt_conn.lookupByName(instance_id)
    if domain is None:
        raise ValueError(f"no domain found with name {instance_id!r}")

    if domain.isActive():
        raise ValueError(
            f"the domain {instance_id!r} can't be cloned while it is running"
        )


    with app["db"] as db:
        cur = db.execute(
            "INSERT INTO machine_images (name) VALUES (?)",
            [name],
        )
        utils.add_tags(cur, name, "image", tags)
        subprocess.check_call([
            'virt-clone',
            f'--original={instance_id}',
            f'--name={name}',
            '--auto-clone',
            '--connect=qemu+tcp://10.56.1.1/system',
        ])

    return { "imageId": name }


@_routing.handler("DeregisterImage")
async def create_image(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    image_id = args['ImageId']

    lvirt_conn: libvirt.virConnect = app['libvirt']
    domain = lvirt_conn.lookupByName(image_id)
    if domain is None:
        raise ValueError(f"no domain found with name {image_id!r}")

    if domain.isActive():
        raise ValueError(
            f"the domain {image_id!r} can't be undefined while it is running"
        )

    name = domain.name()
    with app["db"] as db:
        cur = db.execute(
            """
            DELETE FROM machine_images
            WHERE name = ?
            """,
            [name],
        )
        utils.remove_tags(cur, name, "image")
        domain.undefine()

    return {"return": True}
