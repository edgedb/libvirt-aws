from __future__ import annotations
from typing import (
    Any,
    Dict,
    List,
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


class InvalidDomainNameError(_routing.ClientError):

    code = "InvalidDomainName"


class HostedZoneNotEmptyError(_routing.ClientError):

    code = "HostedZoneNotEmpty"


class InvalidChangeBatchError(_routing.ClientError):

    code = "InvalidChangeBatch"


class NoSuchChangeError(_routing.NotFoundError):

    code = "NoSuchChange"


Zone = Tuple[str, str, str]


route53_handler = functools.partial(
    _routing.direct_handler,
    xmlns=XMLNS,
    list_format="condensed",
    error_formatter=format_route53_error_xml,
)


@route53_handler(
    "CreateHostedZone",
    methods="POST",
    path="/2013-04-01/hostedzone",
)
async def create_hosted_zone(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    net = objects.network_from_xml(app["libvirt_net"].XMLDesc())
    domain = net.dns_domain
    if domain is None:
        raise _routing.InternalServerError(
            "libvirt network does not define a domain"
        )

    parsed = xmltodict.parse(args["BodyText"])

    try:
        request = parsed["CreateHostedZoneRequest"]
    except KeyError:
        raise InvalidInputError("input is not valid") from None

    name = request.get("Name")
    if not name:
        raise _routing.InvalidParameterError("missing required Name")

    caller_ref = request.get("CallerReference")
    if not caller_ref:
        raise _routing.InvalidParameterError(
            "missing required CallerReference"
        )

    if not name.endswith(f".{domain}"):
        raise _routing.InvalidParameterError(
            f"hosted zone name must end with .{domain}"
        )

    zone_id = str(uuid.uuid4()).replace("-", "")
    comment = request.get("Comment")
    change_id = str(uuid.uuid4()).replace("-", "")
    submitted_at = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()

    with app["db"]:
        app["db"].execute(
            f"""
                INSERT INTO dns_zones (id, name, comment)
                VALUES (?, ?, ?)
            """,
            [zone_id, name, comment],
        )

        app["db"].execute(
            f"""
                INSERT INTO dns_changes (id, submitted_at, comment)
                VALUES (?, ?, ?)
            """,
            [change_id, submitted_at, comment],
        )

    config = {
        "PrivateZone": False,
    }

    if comment:
        config["Comment"] = comment

    return {
        "HostedZone": {
            "Id": f"/hostedzone/{zone_id}",
            "Name": name,
            "Config": config,
            "ResourceRecordSetCount": 2,
            "CallerReference": caller_ref,
        },
        "ChangeInfo": {
            "Id": change_id,
            "Status": "INSYNC",
            "SubmittedAt": submitted_at,
        },
        "DelegationSet": {
            "CallerReference": caller_ref,
            "NameServers": {"NameServer": f"gw.{domain}"},
        },
    }


@route53_handler(
    "UpdateHostedZoneComment",
    methods="POST",
    path="/2013-04-01/hostedzone/{Id}",
)
async def update_hosted_zone(
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

    if zone_id == net.name:
        raise InvalidDomainNameError(f"zone {zone_id} cannot be updated")

    zone = _get_subzone(zone_id, app)

    try:
        parsed = xmltodict.parse(args["BodyText"])
        request = parsed["UpdateHostedZoneCommentRequest"]
    except (KeyError, ValueError):
        raise InvalidInputError("input is not valid") from None

    comment = request.get("Comment")
    if comment == "":
        comment = None

    db = app["db"]
    with db:
        db.execute(
            f"""
                UPDATE dns_zones
                SET comment = ?
                WHERE id = ?
            """,
            [comment, zone_id],
        )

    caller_ref = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
    config = {
        "PrivateZone": False,
    }
    if comment:
        config["Comment"] = comment

    return {
        "HostedZone": {
            "Id": f"/hostedzone/{zone_id}",
            "Name": zone[1],
            "Config": config,
            "ResourceRecordSetCount": len(_get_records(zone[1], net, app)),
            "CallerReference": caller_ref,
        },
    }


@route53_handler(
    "DeleteHostedZone",
    methods="DELETE",
    path="/2013-04-01/hostedzone/{Id}",
)
async def delete_hosted_zone(
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

    if zone_id == net.name:
        raise InvalidDomainNameError(f"zone {zone_id} cannot be deleted")
    else:
        zone_tuple = _get_subzone(zone_id, app)
        records = _get_records(zone_tuple[1], net, app, include_soa_ns=False)
        if records:
            raise HostedZoneNotEmptyError(
                f"zone {zone_id} contains resource records"
            )

    change_id = str(uuid.uuid4()).replace("-", "")
    submitted_at = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()

    db = app["db"]

    with db:
        db.execute(
            """
                DELETE FROM
                    tags
                WHERE
                    resource_name = ?
                    AND resource_type = 'hostedzone'
            """,
            [zone_id],
        )

        db.execute(
            """
                DELETE FROM
                    dns_zones
                WHERE
                    id = ?
            """,
            [zone_id],
        )

        db.execute(
            f"""
                INSERT INTO dns_changes (id, submitted_at, comment)
                VALUES (?, ?, ?)
            """,
            [change_id, submitted_at, "deleting zone"],
        )

    return {
        "ChangeInfo": {
            "Id": change_id,
            "Status": "INSYNC",
            "SubmittedAt": submitted_at,
        },
    }


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

    subzones = _get_subzones(app)

    zones = [
        {
            "Id": f"/hostedzone/{net.name}",
            "Name": domain,
            "Config": {
                "Comment": "libvirt network zone",
                "PrivateZone": False,
            },
            "ResourceRecordSetCount": len(_get_records("", net, app)),
        }
    ]

    for zone in subzones:
        config = {
            "PrivateZone": False,
        }
        if zone[2]:
            config["Comment"] = zone[2]
        zones.append(
            {
                "Id": f"/hostedzone/{zone[0]}",
                "Name": zone[1],
                "Config": config,
                "ResourceRecordSetCount": len(_get_records(zone[1], net, app)),
            },
        )

    return {
        "HostedZones": zones,
        "IsTruncated": False,
    }


@route53_handler(
    "ListHostedZonesByName",
    methods="GET",
    path="/2013-04-01/hostedzonesbyname",
)
async def list_hosted_zones_by_name(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    net = objects.network_from_xml(app["libvirt_net"].XMLDesc())
    domain = net.dns_domain
    if domain is None:
        raise _routing.InternalServerError(
            "libvirt network does not define a domain"
        )

    dns_name = args.get("dnsname")
    zone_id = args.get("hostedzoneid")
    max_items_str = args.get("maxitems")

    if max_items_str:
        try:
            max_items = int(max_items_str)
        except ValueError:
            raise _routing.InvalidParameterError("maxitems must be an integer")

        if max_items > 100:
            raise _routing.InvalidParameterError(
                "maxitems cannot be greater than 100"
            )
    else:
        max_items = 100

    def _name_key(name: str) -> str:
        return ".".join(reversed(name.split(".")))

    def _sort_key(zone: Dict[str, Any]) -> str:
        return _name_key(zone["Name"])

    subzones = _get_subzones(app)

    zones = [
        {
            "Id": f"/hostedzone/{net.name}",
            "Name": domain,
            "Config": {
                "Comment": "libvirt network zone",
                "PrivateZone": False,
            },
            "ResourceRecordSetCount": len(_get_records("", net, app)),
        }
    ]

    for zone in subzones:
        config = {
            "PrivateZone": False,
        }
        if zone[2]:
            config["Comment"] = zone[2]
        zones.append(
            {
                "Id": f"/hostedzone/{zone[0]}",
                "Name": zone[1],
                "Config": config,
                "ResourceRecordSetCount": len(_get_records(zone[1], net, app)),
            },
        )

    zones.sort(key=_sort_key)
    if dns_name:
        names = [_sort_key(z) for z in zones]
        offset = bisect.bisect_left(names, _name_key(dns_name))
    else:
        offset = 0

    sliced = zones[offset : offset + max_items]
    is_truncated = len(zones[offset :]) > max_items

    response = {
        "HostedZones": sliced,
        "IsTruncated": is_truncated,
        "MaxItems": max_items,
    }

    if is_truncated:
        response["NextDNSName"] = zones[offset + max_items]["Name"]
        response["NextHostedZoneId"] = zones[offset + max_items]["Id"]

    if dns_name:
        response["DNSName"] = dns_name
    if zone_id:
        response["HostedZoneId"] = zone_id

    return response


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
    "ChangeTagsForResource",
    path="/2013-04-01/tags/{ResourceType}/{ResourceId}",
    methods="POST",
)
async def change_tags_for_resource(
    args: _routing.HandlerArgs,
    app: _routing.App,
) -> Dict[str, Any]:
    net = objects.network_from_xml(app["libvirt_net"].XMLDesc())
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

    if res_id != net.name:
        _get_subzone(res_id, app)

    parsed = xmltodict.parse(args["BodyText"])

    try:
        request = parsed["ChangeTagsForResourceRequest"]
    except KeyError:
        raise InvalidInputError("input is not valid") from None

    tags_to_update = []
    tags_to_remove = []

    add_tags = request.get("AddTags")
    if add_tags:
        tags = add_tags.get("Tag")
        if not tags:
            raise InvalidInputError("input is not valid") from None

        if not isinstance(tags, list):
            tags = [tags]

        tags_to_update = tags
        for tag in tags_to_update:
            if not tag.get("Key"):
                raise InvalidInputError("input is not valid") from None

    remove_tags = request.get("RemoveTagKeys")
    if remove_tags:
        tags = remove_tags.get("Key")
        if not tags:
            raise InvalidInputError("input is not valid") from None

        if not isinstance(tags, list):
            tags = [tags]

        tags_to_remove = tags

    db = app["db"]
    with db:
        for tag in tags_to_update:
            db.execute(
                f"""
                    INSERT INTO tags
                        (resource_name, resource_type, tagname, tagvalue)
                    VALUES
                        (?, ?, ?, ?)
                    ON CONFLICT
                        (resource_name, resource_type, tagname)
                    DO UPDATE
                        SET tagvalue = excluded.tagvalue
                """,
                [res_id, res_type, tag["Key"], tag["Value"]],
            )

        for tag in tags_to_remove:
            db.execute(
                f"""
                    DELETE FROM tags
                    WHERE
                        resource_name = ?
                        AND resource_type = ?
                        AND tagname = ?
                """,
                [res_id, res_type, tag],
            )

    return {}


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

    if zone_id == net.name:
        zone = {
            "Id": f"/hostedzone/{zone_id}",
            "Name": domain,
            "Config": {
                "Comment": "libvirt network zone",
                "PrivateZone": False,
            },
            "ResourceRecordSetCount": len(_get_records("", net, app)),
        }
    else:
        zone_tuple = _get_subzone(zone_id, app)
        zone = {
            "Id": f"/hostedzone/{zone_tuple[0]}",
            "Name": zone_tuple[1],
            "Config": {
                "Comment": zone_tuple[2],
                "PrivateZone": False,
            },
            "ResourceRecordSetCount": len(
                _get_records(zone_tuple[1], net, app)
            ),
        }

    return {
        "HostedZone": zone,
        "DelegationSet": {
            "NameServers": {"NameServer": f"gw.{domain}"},
        },
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

    if zone_id == net.name:
        zone_name = domain
    else:
        zone_name = _get_subzone(zone_id, app)[1]

    def _name_key(name: str) -> str:
        return ".".join(reversed(name.split(".")))

    def _sort_key(rec: Tuple[Tuple[str, str], Set[str]]) -> Tuple[str, str]:
        type, name = rec[0]
        return (type, _name_key(name))

    records = list(_get_records(zone_name, net, app).items())
    records.sort(key=_sort_key)

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
        _get_subzone(zone_id, app)[1]

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
            if not isinstance(records_el, list):
                records_el = [records_el]
            values = {rec["Value"] for rec in records_el}
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
    if libvirt.getVersion("QEMU")[1] < 7002000:
        net.update(section, command, -1, xml)
    else:
        net.update(command, section, -1, xml)


def _get_subzones(
    app: _routing.App,
) -> List[Zone]:
    with app["db"]:
        cur = app["db"].execute(
            f"""
                SELECT
                    id, name, comment
                FROM
                    dns_zones
            """,
        )
        return cur.fetchall()  # type: ignore [no-any-return]


def _get_subzone(
    zone_id: str,
    app: _routing.App,
) -> Zone:
    with app["db"]:
        cur = app["db"].execute(
            f"""
                SELECT
                    id, name, comment
                FROM
                    dns_zones
                WHERE
                    id = ?
            """,
            [zone_id],
        )
        zone_tuple = cur.fetchone()
        if zone_tuple is None:
            raise NoSuchHostedZoneError(f"zone {zone_id} does not exist")

    return zone_tuple  # type: ignore [no-any-return]


def _get_records(
    zone_name: str,
    net: objects.Network,
    app: _routing.App,
    *,
    include_soa_ns: bool = True,
) -> objects.DNSRecords:
    subzones = _get_subzones(app)
    subzone_names = {z[1] for z in subzones}

    return net.get_dns_records(
        zone=zone_name,
        exclude_zones=subzone_names - {zone_name},
        include_soa_ns=include_soa_ns,
    )
