from aiohttp import web
import aiohttp.abc
import aiohttp.web_log
import aiohttp.log
import click
import libvirt
import logging
import sqlite3
from typing import Any, Mapping, Optional

from . import handlers


class AccessLogger(aiohttp.web_log.AccessLogger):
    def log(
        self,
        request: web.BaseRequest,
        response: web.StreamResponse,
        time: float,
    ) -> None:
        try:
            fmt_info = self._format_line(request, response, time)

            values = list()
            extra = dict()
            for key, value in fmt_info:
                values.append(value)

                if key.__class__ is str:
                    extra[key] = value
                else:
                    k1, k2 = key  # type: ignore
                    dct = extra.get(k1, {})  # type: ignore
                    dct[k2] = value  # type: ignore
                    extra[k1] = dct  # type: ignore

            self.logger.info(self._log_format % tuple(values), extra=extra)
            args: Mapping[Any, Any]
            if request.method == "POST":
                args = request._post or {}
            else:
                args = request.query
            self.logger.debug(f"Request:\n---------\n{args}")
        except Exception:
            self.logger.exception("Error in logging")


def init_db(db: sqlite3.Connection) -> None:
    with db:
        db.executescript("""
            CREATE TABLE IF NOT EXISTS tags (
                resource_name text,
                resource_type text,
                tagname       text,
                tagvalue      text,
                UNIQUE (resource_name, resource_type, tagname)
            );

            CREATE TABLE IF NOT EXISTS ip_addresses (
                allocation_id      text,
                ip_address         text,
                association_id     text,
                instance_id        text,
                private_ip_address text,
                UNIQUE (ip_address),
                UNIQUE (allocation_id),
                UNIQUE (association_id)
            );

            CREATE TABLE IF NOT EXISTS private_ip_addresses (
                ip_address     text,
                instance_id    text,
                interface      text,
                UNIQUE (ip_address)
            );

            CREATE TABLE IF NOT EXISTS dns_zones (
                id           text,
                name         text,
                comment      text,
                UNIQUE (id)
            );

            CREATE TABLE IF NOT EXISTS dns_changes (
                id           text,
                submitted_at text,
                comment      text,
                UNIQUE (id)
            );

            CREATE TABLE IF NOT EXISTS ssm_documents (
                name    text not null,
                content text not null,
                UNIQUE (name)
            );

            CREATE TABLE IF NOT EXISTS ssm_command_invocations (
                command_id    text not null,
                instance_id   text not null,
                response_code integer,
                stdout        text,
                stderr        text,
                UNIQUE (command_id, instance_id)
            );

            CREATE TABLE IF NOT EXISTS machine_images (
                name    text not null,
                UNIQUE (name)
            );

            CREATE TABLE IF NOT EXISTS ec2_instance (
                name  text not null,
                state text check(state IN ('running', 'terminated')) not null,
                terminated_at timestamptz,
                UNIQUE (name)
            )
        """)


def init_app(
    pool: str,
    network: str,
    libvirt_uri: str,
    database: str,
    region: str,
    fs_dir: str,
) -> web.Application:
    app = web.Application()
    # logging.basicConfig(level=logging.DEBUG)
    aiohttp.log.access_logger.setLevel(logging.DEBUG)
    app["libvirt"] = libvirt.open(libvirt_uri)

    try:
        app["libvirt_pool"] = app["libvirt"].storagePoolLookupByName(pool)
    except libvirt.libvirtError:
        app["libvirt_pool"] = app["libvirt"].storagePoolLookupByUUIDString(
            pool
        )

    try:
        app["libvirt_net"] = app["libvirt"].networkLookupByName(network)
    except libvirt.libvirtError:
        app["libvirt_net"] = app["libvirt"].networkLookupByUUIDString(network)

    app["db"] = sqlite3.connect(database)
    app["logger"] = logging.getLogger("libvirt-aws")
    app["region"] = region
    app["fs_dir"] = fs_dir
    init_db(app["db"])
    app.add_routes(handlers.routes)
    app.on_cleanup.append(close_libvirt)
    return app


async def close_libvirt(app: web.Application) -> None:
    app["libvirt"].close()


@click.command()
@click.option("--bind-to", default=None, type=str, help="Address to listen on")
@click.option("--port", default=5100, type=int, help="TCP port to listen on")
@click.option("--database", default="pool.db", help="Path to sqlite db")
@click.option(
    "--fs-dir",
    required=True,
    type=str,
    help="directory to mount domain file systems in"
)
@click.option("--libvirt-uri", default="qemu:///system", help="Libvirtd URI")
@click.option(
    "--libvirt-image-pool",
    default="default",
    help="Name of libvirt image pool to use for EBS emulation.",
)
@click.option(
    "--libvirt-network",
    default="default",
    help="Name of libvirt network to use for EIP emulation.",
)
@click.option(
    "--region",
    default="us-east-2",
    type=str,
    help="AWS region to pretend to be in",
)
def main(
    *,
    bind_to: Optional[str],
    port: int,
    database: str,
    fs_dir: str,
    libvirt_image_pool: str,
    libvirt_network: str,
    libvirt_uri: str,
    region: str,
) -> None:
    web.run_app(
        init_app(
            pool=libvirt_image_pool,
            network=libvirt_network,
            libvirt_uri=libvirt_uri,
            database=database,
            region=region,
            fs_dir=fs_dir,
        ),
        access_log_class=AccessLogger,
        host=bind_to,
        port=port,
    )
