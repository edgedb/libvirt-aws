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
    cur = db.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS volume_tags (
            volname text,
            tagname text,
            tagvalue text,
            UNIQUE (volname, tagname)
        )
    ''')
    db.commit()


def init_app(pool: str, libvirt_uri: str, database: str) -> web.Application:
    app = web.Application()
    # logging.basicConfig(level=logging.DEBUG)
    aiohttp.log.access_logger.setLevel(logging.DEBUG)
    app['libvirt'] = libvirt.open(libvirt_uri)
    app['libvirt_pool'] = app['libvirt'].storagePoolLookupByName(pool)
    app['db'] = sqlite3.connect(database)
    init_db(app['db'])
    app.add_routes([
        web.post("/", handlers.handle_request),
        web.get("/", handlers.handle_request),
        web.put("/", handlers.handle_request),
        web.patch("/", handlers.handle_request),
        web.options("/", handlers.handle_request),
        web.delete("/", handlers.handle_request),
    ])
    app.on_cleanup.append(close_libvirt)
    return app


async def close_libvirt(app: web.Application) -> None:
    app['libvirt'].close()


@click.command()
@click.option('--bind-to', default=None, type=str, help='Address to listen on')
@click.option('--port', default=9100, type=int, help='TCP port to listen on')
@click.option('--pool', default='default', help='Image pool to use')
@click.option('--libvirt-uri', default='qemu:///system', help='Libvirtd URI')
@click.option('--database', default='pool.db', help='Path to sqlite db')
def main(
    *,
    bind_to: Optional[str],
    port: int,
    pool: str,
    libvirt_uri: str,
    database: str,
) -> None:
    web.run_app(
        init_app(pool=pool, libvirt_uri=libvirt_uri, database=database),
        access_log_class=AccessLogger,
        host=bind_to,
        port=port,
    )
