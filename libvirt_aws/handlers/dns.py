from __future__ import annotations
from typing import (
    Any,
    Dict,
    Set,
    Tuple,
)

import bisect
import datetime
import functools

import libvirt
import xmltodict
import uuid

from . import _routing
from .. import objects


XMLNS = "https://route53.amazonaws.com/doc/2013-04-01/"


def format_route53_error_xml(err: _routing.ServiceError) -> str:
    return _routing.format_xml_response(
        {
            "ErrorResponse": {
                "RequestId": str(uuid.uuid4()),
                "Error": {
                    "Code": err.code,
                    "Message": err.msg,
                    "Type": "Sender",
                },
            },
        },
        xmlns=XMLNS,
    )


class NoSuchHostedZoneError(_routing.NotFoundError):

    code = "NoSuchHostedZone"


class InvalidInputError(_routing.ClientError):

    code = "InvalidInput"


class InvalidChangeBatchError(_routing.ClientError):

    code = "InvalidChangeBatch"


class NoSuchChangeError(_routing.NotFoundError):

    code = "NoSuchChange"


route53_handler = functools.partial(
    _routing.direct_handler,
    xmlns=XMLNS,
    list_format="condensed",
    error_formatter=format_route53_error_xml,
)


@route53_handler(
    "ListHostedZones",
    methods="GET",
    path="/2013-04-01/hostedzone",
)
async def list_hosted_zones(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    net = objects.network_from_xml(app["libvirt_net"].XMLDesc())
    domain = net.dns_domain
    if domain is None:
        raise _routing.InternalServerError(
            "libvirt network does not define a domain"
        )

    return {
        "HostedZones": {
            "HostedZone": {
                "Id": f"/hostedzone/{net.name}",
                "Name": domain,
                "Config": {
                    "Comment": "libvirt network zone",
                    "PrivateZone": False,
                },
                "ResourceRecordSetCount": len(net.dns_records),
            }
        },
        "IsTruncated": False,
    }


@route53_handler(
    "ListTagsForResource",
    path="/2013-04-01/tags/{ResourceType}/{ResourceId}",
    methods="GET",
)
async def list_tags_for_resource(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    res_type = args.get("ResourceType")
    if not res_type:
        raise _routing.InvalidParameterError("missing required ResourceType")

    if res_type != "hostedzone":
        raise _routing.InvalidParameterError(
            f"unsupported ResourceType: {res_type}"
        )

    res_id = args.get("ResourceId")
    if not res_id:
        raise _routing.InvalidParameterError("missing required ResourceId")

    with app["db"]:
        cur = app["db"].execute(
            f"""
                SELECT
                    tagname, tagvalue
                FROM
                    tags
                WHERE
                    resource_type = ?  AND resource_name = ?
            """,
            [res_type, res_id],
        )
        tags = cur.fetchall()

    return {
        "ResourceTagSet": {
            "ResourceId": res_id,
            "ResourceType": res_type,
            "Tags": [
                {
                    "Key": tag[0],
                    "Value": tag[1],
                }
                for tag in tags
            ],
        }
    }


@route53_handler(
    "GetHostedZone",
    methods="GET",
    path="/2013-04-01/hostedzone/{Id}",
)
async def get_hosted_zone(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    zone_id = args.get("Id")
    if not zone_id:
        raise _routing.InvalidParameterError("missing required Id")

    net = objects.network_from_xml(app["libvirt_net"].XMLDesc())
    domain = net.dns_domain
    if domain is None:
        raise _routing.InternalServerError(
            "libvirt network does not define a domain"
        )

    if zone_id != net.name:
        raise NoSuchHostedZoneError(f"zone {zone_id} does not exist")

    return {
        "HostedZone": {
            "Id": f"/hostedzone/{zone_id}",
            "Name": domain,
            "Config": {
                "Comment": "libvirt network zone",
                "PrivateZone": False,
            },
            "ResourceRecordSetCount": len(net.dns_records),
        },
        "DelegationSet": {"NameServers": {"NameServer": f"gw.{domain}"}},
    }


@route53_handler(
    "ListResourceRecordSets",
    path="/2013-04-01/hostedzone/{Id}/rrset",
    methods="GET",
)
async def list_resource_record_sets(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    zone_id = args.get("Id")
    if not zone_id:
        raise _routing.InvalidParameterError("missing required Id")

    name = args.get("name")
    type = args.get("type")
    limit = args.get("maxitems")

    net = objects.network_from_xml(app["libvirt_net"].XMLDesc())
    domain = net.dns_domain
    if domain is None:
        raise _routing.InternalServerError(
            "libvirt network does not define a domain"
        )

    if zone_id != net.name:
        raise NoSuchHostedZoneError(f"zone {zone_id} does not exist")

    def _name_key(name: str) -> str:
        return ".".join(reversed(name.split(".")))

    def _sort_key(rec: Tuple[Tuple[str, str], Set[str]]) -> Tuple[str, str]:
        type, name = rec[0]
        return (type, _name_key(name))

    records = list(net.dns_records.items())
    records.sort(key=_sort_key)

    print(records)

    if name and type:
        keys = [_sort_key(r) for r in records]
        offset = bisect.bisect_left(keys, (type, _name_key(name)))
    elif name:
        names = [_sort_key(r)[1] for r in records]
        offset = bisect.bisect_left(names, _name_key(name))
    elif type:
        raise InvalidInputError("cannot specify Type without Name")
    else:
        offset = 0

    if limit is None:
        limit = len(records)
    else:
        try:
            limit = int(limit)
        except (TypeError, ValueError):
            raise InvalidInputError("invalid MaxItems value")

    return {
        "ResourceRecordSets": [
            {
                "Name": name,
                "Type": type,
                "TTL": 300,
                "ResourceRecords": [{"Value": value} for value in values],
            }
            for (type, name), values in records[offset : offset + limit]
        ],
        "IsTruncated": "false",
    }


@route53_handler(
    "ChangeResourceRecordSets",
    path="/2013-04-01/hostedzone/{Id}/rrset/",
    methods="POST",
)
async def change_resource_record_sets(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    zone_id = args.get("Id")
    if not zone_id:
        raise _routing.InvalidParameterError("missing required Id")

    net = objects.network_from_xml(app["libvirt_net"].XMLDesc())
    domain = net.dns_domain
    if domain is None:
        raise _routing.InternalServerError(
            "libvirt network does not define a domain"
        )

    if zone_id != net.name:
        raise NoSuchHostedZoneError(f"zone {zone_id} does not exist")

    parsed = xmltodict.parse(args["BodyText"])

    try:
        request = parsed["ChangeResourceRecordSetsRequest"]
        comment = request["ChangeBatch"].get("Comment", "")
        changes_el = request["ChangeBatch"]["Changes"]["Change"]
        changes = changes_el if isinstance(changes_el, list) else [changes_el]
    except (TypeError, KeyError, AssertionError):
        raise InvalidInputError("input is not valid") from None

    table = {k: set(r) for k, r in net.dns_records.items()}

    try:
        for change in changes:
            rrset = change["ResourceRecordSet"]
            name = rrset["Name"]
            type = rrset["Type"]
            records_el = rrset["ResourceRecords"]["ResourceRecord"]
            records = (
                records_el if isinstance(records_el, list) else [records_el]
            )
            values = {rec["Value"] for rec in records}
            key = (type, name)

            if change["Action"] == "CREATE":
                if key in table:
                    raise InvalidChangeBatchError(
                        f"{name} {type} is already present in the record set"
                    )
                else:
                    table[key] = values
            elif change["Action"] == "DELETE":
                if table.get(key) == values:
                    table.pop(key)
                else:
                    raise InvalidChangeBatchError(
                        f"{name} {type} with specified values is "
                        f"not present in the record set"
                    )
            elif change["Action"] == "UPSERT":
                table[key] = values
            else:
                raise InvalidInputError(
                    f"Action = {change['Action']} is not supported"
                )

    except (KeyError, TypeError):
        raise InvalidInputError("input is not valid") from None

    added, removed = net.get_dns_diff(table)

    for typ, xml in removed:
        section = f"VIR_NETWORK_SECTION_DNS_{typ.upper()}"
        _net_update(
            app["libvirt_net"],
            libvirt.VIR_NETWORK_UPDATE_COMMAND_DELETE,
            getattr(libvirt, section),
            xml,
        )

    for typ, xml in added:
        section = f"VIR_NETWORK_SECTION_DNS_{typ.upper()}"
        _net_update(
            app["libvirt_net"],
            libvirt.VIR_NETWORK_UPDATE_COMMAND_ADD_LAST,
            getattr(libvirt, section),
            xml,
        )

    change_id = str(uuid.uuid4()).replace("-", "")
    submitted_at = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()

    with app["db"]:
        app["db"].execute(
            f"""
                INSERT INTO dns_changes (id, submitted_at, comment)
                VALUES (?, ?, ?)
            """,
            [change_id, submitted_at, comment],
        )

    return {
        "ChangeInfo": {
            "Comment": comment,
            "Id": change_id,
            "Status": "INSYNC",
            "SubmittedAt": submitted_at,
        },
    }


@route53_handler(
    "GetChange",
    path="/2013-04-01/change/{Id}",
    methods="GET",
)
async def get_change(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    change_id = args.get("Id")
    if not change_id:
        raise _routing.InvalidParameterError("missing required Id")

    with app["db"]:
        cur = app["db"].execute(
            f"""
            SELECT id, submitted_at, comment
            FROM dns_changes
            WHERE id = ?
        """,
            [change_id],
        )

        rec = cur.fetchone()
        if not rec:
            raise NoSuchChangeError(f"no such change: {change_id}")

    return {
        "ChangeInfo": {
            "Comment": rec[2],
            "Id": change_id,
            "Status": "INSYNC",
            "SubmittedAt": rec[1],
        },
    }


# Work around the issue in libvirt <7.2.0 where NetworkUpdate arguments
# were incorrectly swapped on the client side.
#
# See https://listman.redhat.com/archives/libvir-list/2021-March/msg00054.html
# and https://listman.redhat.com/archives/libvir-list/2021-March/msg00760.html
def _net_update(
    net: libvirt.virNetwork,
    command: int,
    section: int,
    xml: str,
) -> None:
    lvconn = net.connect()
    if lvconn.getLibVersion() < 7002000:
        net.update(section, command, -1, xml)
    else:
        net.update(command, section, -1, xml)
