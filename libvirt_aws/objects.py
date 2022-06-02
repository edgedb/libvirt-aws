from __future__ import annotations
from typing import (
    Any,
    List,
    Mapping,
    Optional,
)

import functools
import ipaddress

import libvirt
import xmltodict


def get_volume(pool: libvirt.virStoragePool, name: str) -> Volume:

    for virvol in pool.listAllVolumes():
        vol = volume_from_xml(virvol.XMLDesc(0))
        if vol.name == name:
            return vol

    raise LookupError(f"volume {name} does not exist")


def get_all_volumes(
    pool: libvirt.virStoragePool,
) -> List[Volume]:
    vols = []

    for virvol in pool.listAllVolumes():
        vol = volume_from_xml(virvol.XMLDesc(0))
        vols.append(vol)

    return vols


def get_all_domains(
    conn: libvirt.virConnect,
) -> List[Domain]:

    domains = []

    for virdom in conn.listAllDomains():
        dom = domain_from_xml(virdom.XMLDesc(0))
        domains.append(dom)

    return domains


def get_vol_attachments(
    pool: libvirt.virStoragePool,
    volume: Volume,
) -> List[VolumeAttachment]:

    conn = pool.connect()
    attachments = []

    for dom in get_all_domains(conn):
        for disk in dom.disks:
            if volume.name == disk.volume and disk.pool == pool.name():
                attachments.append(disk.attachment)

    return attachments


@functools.lru_cache
def domain_from_xml(xml: str) -> Domain:
    return Domain(xmltodict.parse(xml)["domain"])


class Domain:
    def __init__(self, dom: Mapping[str, Any]) -> None:
        self._dom = dom
        self._disks: Optional[List[DiskDevice]] = None

    @property
    def name(self) -> str:
        return self._dom["name"]  # type: ignore[no-any-return]

    @property
    def disks(self) -> List[DiskDevice]:
        if self._disks is None:
            disks = self._dom["devices"]["disk"]
            if not isinstance(disks, list):
                disks = [disks]
            self._disks = [
                DiskDevice(self, d) for d in disks if d["@type"] == "volume"
            ]

        return self._disks


class DiskDevice:
    def __init__(self, dom: Domain, desc: Mapping[str, Any]) -> None:
        self._dom = dom
        self._desc = desc

    @property
    def volume(self) -> str:
        return self._desc["source"]["@volume"]  # type: ignore

    @property
    def pool(self) -> str:
        return self._desc["source"]["@pool"]  # type: ignore

    @property
    def attachment(self) -> VolumeAttachment:
        return VolumeAttachment(
            self._dom.name,
            self.volume,
            self.pool,
            self._desc["target"],
        )


@functools.lru_cache
def volume_from_xml(xml: str) -> Volume:
    return Volume(xml)


class Volume:
    def __init__(self, volxml: str) -> None:
        parsed = xmltodict.parse(volxml)
        self._vol = parsed["volume"]

    @property
    def name(self) -> str:
        return self._vol["name"]  # type: ignore[no-any-return]

    @property
    def key(self) -> str:
        return self._vol["key"]  # type: ignore[no-any-return]

    @property
    def target_path(self) -> str:
        return self._vol["target"]["path"]  # type: ignore[no-any-return]

    @property
    def backing_store(self) -> Optional[str]:
        bs = self._vol.get("backingStore")
        if bs is not None:
            return bs["path"]  # type: ignore[no-any-return]
        else:
            return None

    @property
    def capacity(self) -> int:
        return int(self._vol["capacity"]["#text"])


class VolumeAttachment:
    def __init__(
        self,
        domain: str,
        volume: str,
        pool: str,
        desc: Mapping[str, Any],
    ) -> None:
        self._domain = domain
        self._volume = volume
        self._pool = pool
        self._desc = desc

    @property
    def domain(self) -> str:
        return self._domain

    @property
    def volume(self) -> str:
        return self._volume

    @property
    def pool(self) -> str:
        return self._pool

    @property
    def device(self) -> str:
        return self._desc["@dev"]  # type: ignore[no-any-return]


@functools.lru_cache
def network_from_xml(xml: str) -> Network:
    return Network(xml)


class Network:
    def __init__(self, netxml: str) -> None:
        parsed = xmltodict.parse(netxml)
        self._net = parsed["network"]

    @property
    def ip_network(self) -> ipaddress.IPv4Network:
        ip = self._net.get("ip")
        if ip is None:
            return ValueError("network does not define an IP block")
        if ip["@family"] != "ipv4":
            raise ValueError("network is not an IPv4 network")
        return ipaddress.IPv4Network(
            f"{ip['@address']}/{ip['@prefix']}",
            strict=False,
        )

    @property
    def static_ip_range(
        self,
    ) -> tuple[ipaddress.IPv4Address, ipaddress.IPv4Address]:
        ip = self._net.get("ip")
        if ip is None:
            return ValueError("network does not define an IP block")
        if ip["@family"] != "ipv4":
            raise ValueError("network is not an IPv4 network")
        dhcp = ip.get("dhcp")
        if dhcp is None:
            return ValueError("network does not define a DHCP block")
        dhcpRange = dhcp.get("range")
        if dhcpRange is None:
            return ValueError("network does not define a DHCP range block")

        ip_iter = self.ip_network.hosts()
        next(ip_iter)
        start = next(ip_iter)

        return (
            start,
            ipaddress.IPv4Address(dhcpRange["@start"]),
        )
