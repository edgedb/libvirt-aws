from __future__ import annotations

import asyncio
import base64
import datetime
import subprocess
import sys
import uuid
from typing import Any
from xml.etree import ElementTree

import libvirt

from .. import objects

from . import _routing
from . import errors
from . import ips
from . import utils
from . import volumes


def garbage_collect_terminated_instances(db) -> None:
    with db as cur:
        cur.execute('''
            DELETE FROM ec2_instance
            WHERE
                terminated_at IS NOT NULL AND
                terminated_at < ?
        ''', [datetime.datetime.now() - datetime.timedelta(minutes=2)])


@_routing.handler("DescribeInstanceTypes")
async def describe_instance_types(
    args: _routing.HandlerArgs,
    app: _routing.App
) -> dict[str, Any]:
    return {
        "nextToken": None,
        "instanceTypeSet": [{
            "instanceType": "t2.micro",
        }],
    }

def domstate(virdom: libvirt.virDomain) -> str:
    virstate, _ = virdom.state()
    return domain_state_lookup[virstate]


# libvirt domain state mapped to aws instance state
# https://libvirt.org/html/libvirt-libvirt-domain.html#virDomainState
# https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_InstanceState.html
domain_state_lookup = {
    0: "??? no-state", # libvirt: no state
    1: "running",
    2: "??? blocked", # libvirt: blocked
    3: "stopped", # libvirt: paused
    4: "stopping",
    5: "stopped",
    6: "??? crashed", # libvirt: crashed
    7: "stopped",
}

def lg(txt: str):
    with open("/tmp/libvirt-aws.log", 'at') as f:
        print(txt, file=f)


# https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeInstances.html
@_routing.handler("DescribeInstances")
async def describe_instances(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> dict[str, Any]:
    pool: libvirt.virStoragePool = app["libvirt_pool"]
    net: libvirt.virNetwork = app["libvirt_net"]
    lvirt_conn: libvirt.virConnect = app["libvirt"]

    result = []
    for instance_id in set(args.get("InstanceId", ())):
        with app['db'] as db:
            row = db.execute('''
                SELECT state, availability_zone, subnet_id
                FROM ec2_instance
                WHERE id = ?
            ''', [instance_id]).fetchone()

            if row is None:
                raise errors.InvalidInstanceID_NotFound(
                    f'invalid InstanceId: {instance_id}')

            state, availability_zone, subnet_id = row
            tags = utils.get_tags(db, instance_id, 'instance')

        if state == 'terminated':
            result.append({
                "instanceId": instance_id,
                "instanceType": "t2.micro",
                "instanceState": {
                    "name": state,
                },

                # needed so that the terraform provider doesn't force
                # replacement of the instance on every apply.
                'placement': {
                    'availabilityZone': availability_zone,
                },
                'networkInterfaceSet': [{
                    'attachment': {
                        'deviceIndex': 0,
                    },
                    'subnetId': subnet_id,
                }],
                'tagSet': tags,
            })
            continue

        try:
            virdom = lvirt_conn.lookupByName(instance_id)
        except libvirt.libvirtError as e:
            raise errors.InvalidInstanceID_NotFound(
                f'invalid InstanceId: {instance_id}') from e

        domain = objects.domain_from_xml(virdom.XMLDesc())
        block_devices = await _describe_block_devices(pool, domain)
        state = domstate(virdom)
        if state != row[0]:
            with app['db'] as db:
                db.execute('''
                    UPDATE ec2_instance SET state = ? WHERE id = ?
                ''', [state, instance_id]).fetchone()
        result.append({
            "instanceId": instance_id,
            "instanceType": "t2.micro",
            "instanceState": {
                "name": row[0],
            },
            "blockDeviceMapping": block_devices,
            "networkInterfaceSet": [],

            # needed so that the terraform provider doesn't force
            # replacement of the instance on every apply.
            'placement': {
                'availabilityZone': availability_zone,
            },
            'networkInterfaceSet': [{
                'attachment': {
                    'deviceIndex': 0,
                },
                'subnetId': subnet_id,
            }],
            'tagSet': tags,
        })


    return {
        "nextToken": None,
        "reservationSet": [
            {
                "instancesSet": result,
            }
        ]
    }


async def _describe_block_devices(
    lvirt_pool: libvirt.virStoragePool,
    domain: objects.Domain,
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


@_routing.handler("RunInstances")
async def run_instance(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> dict[str, Any]:
    garbage_collect_terminated_instances(app['db'])

    lvirt_conn: libvirt.virConnect = app['libvirt']
    pool: libvirt.virStoragePool = app["libvirt_pool"]
    net: libvirt.virNetwork = app["libvirt_net"]

    availability_zone = 'default-1a'
    if 'Placement' in args and 'AvailabilityZone' in args['Placement']:
        availability_zone = args['Placement']['AvailabilityZone']

    subnet_id = 'subnet-default'
    if 'SubnetId' in args:
        subnet_id = args['SubnetId']
    elif 'NetworkInterface' in args:
        for ni in args['NetworkInterface']:
            if ni.get('DeviceIndex') == '0':
                subnet_id = ni.get('SubnetId', subnet_id)

    image_id = None
    lt = args.get("LaunchTemplate")
    if lt is not None:
        with app['db'] as db:
            row = db.execute(
                'SELECT image_id FROM ec2_launch_template WHERE id = ?',
                [lt['LaunchTemplateId']],
            ).fetchone()

            if row is None:
                raise _routing.InvalidParameterError(
                    f"Launch template with id {lt['LaunchTemplateId']!r} "+
                    f"doesn't exist.")

            image_id, = row
    else:
        image_id = args.get("ImageId")

    if not image_id:
        raise _routing.InvalidParameterError("missing required ImageId")

    id = f"i-{uuid.uuid4().hex}"
    name = id
    tag_specs = args.get("TagSpecification")
    for tag_spec in tag_specs or []:
        for tag in tag_spec["Tag"]:
            if tag["Key"] == "Name":
                name = tag["Value"]

    user_data = args.get("UserData")
    if not user_data:
        user_data = "#cloud-config\n---\n"
    else:
        user_data = base64.b64decode(user_data)

    with app['db'] as db:
        utils.add_tags(db, id, "instance", tag_specs)
        db.execute('''
            INSERT INTO ec2_instance (id, state, availability_zone, subnet_id)
            VALUES (?, 'running', ?, ?)
        ''', [id, availability_zone, subnet_id])
        subprocess.check_call([
            'virt-clone',
            f'--original={image_id}',
            f'--name={id}',
            '--auto-clone',
            '--connect=qemu+tcp://10.56.1.1/system',
        ], stdout=sys.stdout, stderr=sys.stderr)
        subprocess.check_call([
            'virt-sysprep',
            '-d', id,
            '--hostname', name,
            '--operation', 'defaults,-ssh-userdir,-ssh-hostkeys',
            '--connect', 'qemu+tcp://10.56.1.1/system',
        ], stdout=sys.stdout, stderr=sys.stderr, env={
            'LIBGUESTFS_DEBUG':'1',
            'LIBGUESTFS_TRACE':'1'
        })
        domain = lvirt_conn.lookupByName(id)
        domain.setAutostart(1)
        domain.create()

    return {'instancesSet': [{'instanceId': id}]}


@_routing.handler("DescribeTags")
async def describe_tags(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> dict[str, Any]:
    return {
        "nextToken": None,
        "tagSet": [],
    }


@_routing.handler("DescribeInstanceAttribute")
async def describe_instance_attribute(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> dict[str, Any]:
    return {
        "instanceInitiatedShutdownBehavior": {"value": "STOP"},
        "disableApiStop": {"value": False},
        "disableApiTermination": {"value": False},
    }

@_routing.handler("TerminateInstances")
async def terminate_instances(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> dict[str, Any]:
    lvirt_conn: libvirt.virConnect = app['libvirt']
    instance_ids = set(args.get("InstanceId", ()))
    results = []
    for instance_id in instance_ids:
        with app['db'] as db:
            utils.remove_tags(db, instance_id, "instance")
            db.execute('''
                UPDATE ec2_instance
                SET
                    state = 'terminated',
                    terminated_at = ?
                WHERE id = ? AND terminated_at IS NULL
            ''', [datetime.datetime.now(), instance_id])
            virdom = lvirt_conn.lookupByName(instance_id)
            xml = ElementTree.fromstring(virdom.XMLDesc())
            try:
                virdom.destroy()
                virdom.undefine()
            finally:
                disks = xml.findall(
                    './devices/disk[@device="disk"]/source[@file]')
                for disk in disks:
                    vol = lvirt_conn.storageVolLookupByPath(disk.get('file'))
                    i = vol.info()
                    vol.delete()
            results.append({"instanceId": instance_id})

    return {
        "instancesSet": results,
    }


@_routing.handler("StopInstances")
async def stop_instances(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> dict[str, Any]:
    lvirt_conn: libvirt.virConnect = app['libvirt']
    results = []
    for instance_id in set(args.get("InstanceId", ())):
        with app['db'] as db:
            db.execute('''
                UPDATE ec2_instance
                SET state = 'stopping'
                WHERE
                    id = ? AND
                    terminated_at IS NULL
            ''', [instance_id])
            virdom = lvirt_conn.lookupByName(instance_id)

            # janky: should do this in the background
            while domstate(virdom) != 'stopped':
                virdom.shutdown()
                await asyncio.sleep(5)
            
            results.append({"instanceId": instance_id})

    return {
        "instancesSet": results,
    }


@_routing.handler("DescribeInstanceCreditSpecifications")
async def describe_instance_credit_specifications(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> dict[str, Any]:
    return {
        "nextToken": None,
        "instanceCreditSpecificationSet": [],
    }
