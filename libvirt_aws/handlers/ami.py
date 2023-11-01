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
import sys
import tempfile
import uuid
from xml.etree import ElementTree

import libvirt

from . import _routing
from . import utils
from . import instances


def quote_name(name: str) -> str:
    return name.replace('/', '%2F')


def unquote_name(name: str) -> str:
    return name.replace('%2F', '/')


@_routing.handler("DescribeImages")
async def describe_images(
    args: _routing.HandlerArgs,
    app: _routing.App
) -> Dict[str, Any]:
    with app['db'] as db:
        rows = db.execute("SELECT name FROM ec2_machine_image").fetchall()

    images = []
    for row in rows:
        images.append({
            'imageId': row[0],
            'imageState': 'available',
            'name': unquote_name(row[0]),
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
        raise _routing.InvalidParameterError(
            "vol xml is missing required tag ./capacity")

    path = xml.find("./target/path")
    if path is None:
        raise _routing.InvalidParameterError(
            "vol xml is missing required tag ./backingStore/path")

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
        ], stdout=sys.stdout, stderr=sys.stderr)

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
        ], stdout=sys.stdout, stderr=sys.stderr)

    return describe_volume(vol)


@_routing.handler("CreateImage")
async def create_image(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    instance_id = args['InstanceId']
    name = args['Name']
    assert isinstance(name, str), "name is not a string"
    name = quote_name(name)
    tags = args.get('TagSpecification')

    lvirt_conn: libvirt.virConnect = app['libvirt']
    domain = lvirt_conn.lookupByName(instance_id)
    if domain is None:
        raise _routing.InvalidParameterError(
            f"no domain found with name {instance_id!r}")

    virstate, _ = domain.state()
    state = instances.domain_state_lookup[virstate]
    if state != 'stopped':
        raise _routing.InvalidParameterError(
            f"the domain {instance_id!r} can't be cloned while it is {state}"
        )


    with app["db"] as db:
        cur = db.execute(
            "INSERT INTO ec2_machine_image (name) VALUES (?)",
            [name],
        )
        utils.add_tags(cur, name, "image", tags)
        subprocess.check_call([
            'virt-clone',
            f'--original={instance_id}',
            f'--name={name}',
            '--auto-clone',
            '--connect=qemu+tcp://10.56.1.1/system',
        ], stdout=sys.stdout, stderr=sys.stderr)

    return { "imageId": unquote_name(name) }


@_routing.handler("DeregisterImage")
async def create_image(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    image_id = quote_name(args['ImageId'])

    lvirt_conn: libvirt.virConnect = app['libvirt']
    domain = lvirt_conn.lookupByName(image_id)
    if domain is None:
        raise _routing.InvalidParameterError(
            f"no domain found with name {image_id!r}")

    if domain.isActive():
        raise _routing.InvalidParameterError(
            f"the domain {image_id!r} can't be undefined while it is running"
        )

    with app["db"] as db:
        cur = db.execute(
            """
            DELETE FROM ec2_machine_image
            WHERE name = ?
            """,
            [image_id],
        )
        utils.remove_tags(cur, image_id, "image")
        domain.undefine()

    return {"return": True}


def get_template_data(data):
    meta = data.get('MetadataOptions', {})
    result = {
        'instanceType': data.get('InstanceType', 't2.micro'),
        'iamInstanceProfile': {
            'arn': data['IamInstanceProfile']['Arn'],
        },
        'metadataOptions': {
            'httpProtocolIpv6': meta.get('HttpProtocolIpv6') or 'disabled',
            'instanceMetadataTags': meta.get('InstanceMetadataTags') or 'enabled',
        },
        'networkInterfaceSet': [{
            # 'deviceIndex': ni['DeviceIndex'],
            'ipv6AddressCount': ni['Ipv6AddressCount'],
        } for ni in data.get('NetworkInterface', [])],
        'tagSpecificationSet': [{
            'resourceType': spec['ResourceType'],
            'Tags': [{
                'key': t['Key'],
                'value': t['Value'],
            } for t in spec.get('Tag') or []]
        } for spec in data.get('TagSpecification', [])],
    }
    return result


@_routing.handler("CreateLaunchTemplate")
async def create_launch_template(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    data = args['LaunchTemplateData']
    image_id = quote_name(data['ImageId'])
    name = args['LaunchTemplateName']
    id = f'lt-{uuid.uuid4().hex}'
    with app["db"] as db:
        cur = db.execute(
            '''
            INSERT INTO ec2_launch_template (id, name, image_id, data)
            VALUES (?, ?, ?, ?)
            ''',
            [id, name, image_id, json.dumps(get_template_data(data))],
        )
        utils.add_tags(
            cur, id, "launch-template", data.get('TagSpecification', []))

    return {
        'launchTemplate':  {
            'launchTemplateId': id,
            'launchTemplateName': name,
            'defaultVersionNumber': 1,
            'latestVersionNumber': 1,
        }
    }


@_routing.handler("DescribeLaunchTemplates")
async def describe_launch_templates(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    result = []
    for template_id in set(args.get("LaunchTemplateId", ())):
        with app["db"] as db:
            row = db.execute(
                'SELECT name FROM ec2_launch_template WHERE id = ?',
                [template_id],
            ).fetchone()
            if row is None:
                raise _routing.InvalidParameterError(
                    f"Launch template with id {template_id!r} doesn't exist.")
            name, = row
            tags = utils.get_tags(db, template_id, 'image')

        result.append({
            'launchTemplateId': template_id,
            'launchTemplateName': name,
            'defaultVersionNumber': 1,
            'latestVersionNumber': 1,
            'tagSet': tags,
        })

    return {
        'launchTemplates': result,
    }


@_routing.handler("DescribeLaunchTemplateVersions")
async def describe_launch_template_versions(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    template_id = args['LaunchTemplateId']

    with app["db"] as db:
        row = db.execute(
            'SELECT name, image_id, data FROM ec2_launch_template WHERE id = ?',
            [template_id],
        ).fetchone()
        name, image_id, data = row
        data = json.loads(data)

    data |= {
        'imageId': image_id,

        'blockDeviceMapping': [],
        'disableApiStop': False,
        'disableApiTermination': False,
        'elasticGpuSpecificationSet': [],
        'elasticInferenceAcceleratorSet': [],
        'licenseSet': [],
        'securityGroupSet': [],
        'userData': '',
    }

    return {
        'launchTemplateVersionSet': [{
            'defaultVersionNumber': 1,
            'launchTemplateData': data,
            'launchTemplateId': template_id,
            'launchTemplateName': name,
            'versionDescription': '',
            'versionNumber': 1,
        }],
        'nextToken': None,
    }


@_routing.handler("DeleteLaunchTemplate")
async def describe_launch_templates(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    if 'LaunchTemplateName' in args:
        with app["db"] as db:
            row = db.execute(
                'SELECT id FROM ec2_launch_template WHERE name = ?',
                [args['LaunchTemplateName' ]],
            ).fetchone()
            id, = row
    else:
        id = args['LaunchTemplateId']

    with app["db"] as db:
        cur = db.execute(
            'DELETE FROM ec2_launch_template WHERE id = ?',
            [id],
        )
        utils.remove_tags(cur, id, "launch-template")

    return {
        'launchTemplate': {},
    }


@_routing.handler("CreateLaunchTemplateVersion")
async def create_launch_template_version(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    '''
    <MultiDictProxy(
        'Action': 'CreateLaunchTemplateVersion',

        'ClientToken': 'terraform-20230920205125567400000001',

        'LaunchTemplateData.IamInstanceProfile.Arn': 'arn:aws:iam::000000000000:instance-profile/nebula/consul_cluster_member',
        'LaunchTemplateData.ImageId': 'nebula/local/local-1/consul-server-20230920200144',
        'LaunchTemplateData.InstanceType': 't3a.small',
        'LaunchTemplateData.MetadataOptions.HttpEndpoint': '',
        'LaunchTemplateData.MetadataOptions.HttpProtocolIpv6': 'disabled',
        'LaunchTemplateData.NetworkInterface.1.DeviceIndex': '0',
        'LaunchTemplateData.NetworkInterface.1.Ipv6AddressCount': '1',
        'LaunchTemplateData.NetworkInterface.1.NetworkCardIndex': '0',
        'LaunchTemplateData.TagSpecification.1.ResourceType': 'instance',
        'LaunchTemplateData.TagSpecification.1.Tag': '',
        'LaunchTemplateData.UserData': '',

        'LaunchTemplateId': 'lt-3e1053852950492da06ff20051ddd990',
        'Version': '2016-11-15'
    )>
    '''
    template_id = args['LaunchTemplateId']
    data = get_template_data(args['LaunchTemplateData'])

    with app["db"] as db:
        row = db.execute(
            '''
            UPDATE ec2_launch_template
            SET data = ?
            WHERE id = ?
            RETURNING name, image_id
            ''',
            [data, template_id],
        ).fetchone()
        if row is None:
            raise _routing.InvalidParameterError(
                f"Launch template with id {template_id!r} doesn't exist.")
        name, image_id = row

    data |= {
        'imageId': image_id,

        'blockDeviceMapping': [],
        'disableApiStop': False,
        'disableApiTermination': False,
        'elasticGpuSpecificationSet': [],
        'elasticInferenceAcceleratorSet': [],
        'licenseSet': [],
        'securityGroupSet': [],
        'userData': '',
    }
    return {
        'launchTemplateVersion': {
            'defaultVersionNumber': 1,
            'launchTemplateData': data,
            'launchTemplateId': template_id,
            'launchTemplateName': name,
            'versionDescription': '',
            'versionNumber': 1,
        },
    }
