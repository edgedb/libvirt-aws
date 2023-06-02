from __future__ import annotations
from typing import (
    Any,
    Dict,
    List,
    Optional,
)

import json
import re
import uuid
import sqlite3

import yaml
from libvirt_aws.qemu import RemoteProcess

from . import _routing
from .. import qemu

document_name_re = re.compile(r'^[a-zA-Z0-9_\-.]{3,128}$')


@_routing.handler("AmazonSSM.CreateDocument", methods="POST", protocol="json")
async def create_document(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    name = args['Name']
    if document_name_re.match(name) is None:
        raise ValueError(
            f"the name {name!r} doesn't match the regex "
            "^[a-zA-Z0-9_\\-.]{3,128}$"
        )

    doc_type = args.get("DocumentType", "Command")
    if doc_type != "Command":
        raise NotImplementedError(
            f"the {doc_type!r} document type is not implemented"
        )

    match args.get('DocumentFormat', 'json').lower():
        case 'json':
            content = json.loads(args['Content'])
        case 'yaml':
            content = yaml.safe_load(args['Content'])
        case 'text':
            raise NotImplementedError(
                "text document format is not implemented"
            )
        case unknown:
            raise ValueError(f"{unknown!r} is not a valid document format")

    with app["db"] as db:
        db.cursor().execute(
            """
                INSERT INTO ssm_documents
                (name, content)
                VALUES (?, ?)
            """,
            [name, json.dumps(content)],
        )

    return {
        "Name": name,
        "Status": "Active",
    }


@_routing.handler("AmazonSSM.SendCommand", methods="POST", protocol="json")
async def send_command(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    name: str = args["DocumentName"]
    instance_ids: List[str] = args["InstanceIds"]
    db: sqlite3.Connection = app['db']

    with db:
        cur = db.execute(
            "SELECT content FROM ssm_documents WHERE name = ?",
            [name],
        )
        document = cur.fetchone()

    if not document:
        raise ValueError(f"no document found with name {name!r}")

    document = json.loads(document[0])
    steps = document['mainSteps']
    match len(steps):
        case 0:
            raise ValueError("document doesn't have any steps")
        case 1:
            pass
        case _:
            raise NotImplementedError("multiple steps are not currently implemented")

    cmd = "\n".join(steps[0]['inputs']['runCommand'])
    command_id = f'{uuid.uuid4()}'

    with app['db'] as db:
        cur = db.cursor()
        for instance_id in instance_ids:
            domain = app['libvirt'].lookupByName(instance_id)

            result: RemoteProcess = await qemu.agent_exec(
                domain,
                ['bash', '-c', cmd]
            )

            cur.execute(
                """
                    INSERT INTO ssm_command_invocations
                    (command_id, instance_id, response_code, stdout, stderr)
                    VALUES (?, ?, ?, ?, ?)
                """,
                [
                    command_id,
                    instance_id,
                    result.returncode,
                    result.stdout.read().decode('utf-8'),
                    result.stderr.read().decode('utf-8'),
                ],
            )

    return {
        'Command': {
            "CommandId": command_id,
        },
    }


@_routing.handler(
    "AmazonSSM.GetCommandInvocation",
    methods="POST",
    protocol="json"
)
async def get_command_invocation(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    command_id = args['CommandId']
    instance_id = args['InstanceId']
    result = app['db'].execute(
        """
            SELECT
                response_code, stdout, stderr
            FROM
                ssm_command_invocations
            WHERE
                command_id = ? AND instance_id = ?
        """,
        [command_id, instance_id]
    ).fetchone()
    
    if not result:
        raise ValueError("no invocation found")

    return {
        "CommandId": command_id,
        "InstanceId": instance_id,
        "ResponseCode": result[0],
        "StandardOutputContent": result[1],
        "StandardErrorContent": result[2],
        "Status": "Failed" if result[0] else "Success",
    }
