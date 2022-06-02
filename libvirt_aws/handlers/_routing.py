from __future__ import annotations
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    Mapping,
    Optional,
    Tuple,
    Union,
)

import traceback
import uuid
from xml.dom import minidom

from aiohttp import web
import aiohttp.log
import dicttoxml
import multidict
import libvirt

HandlerArgs = Dict[str, Union[str, Tuple[str, ...]]]

_HandlerType = Callable[
    [HandlerArgs, libvirt.virStoragePool],
    Awaitable[Dict[str, Any]],
]
_handlers: Dict[Tuple[str, str], _HandlerType] = {}

App = web.Application


def format_xml_response(
    data: Mapping[str, Any],
    root: Optional[str] = None,
    xmlns: Optional[str] = None,
) -> str:
    bbody = dicttoxml.dicttoxml(data, root=False, attr_type=False)
    body = bbody.decode("utf-8")
    if root is not None:
        if xmlns is not None:
            rootxml = f'<{root} xmlns="{xmlns}">'
        else:
            rootxml = f'<{root}>'
        body = f'{rootxml}\n{body}\n</{root}>'
    return f'<?xml version="1.0" encoding="UTF-8"?>\n{body}'


class ServiceError(web.HTTPError):

    code: str

    def __init__(
        self,
        msg: str,
        *,
        status_code: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        text = format_xml_response({
            "Response": {
                "RequestID": str(uuid.uuid4()),
                "Errors": {
                    "Error": {
                        "Code": self.code,
                        "Message": msg,
                    },
                },
            },
        })
        if status_code is not None:
            self.status_code = status_code

        dom = minidom.parseString(text)
        aiohttp.log.access_logger.debug(
            f"Error Response:\n\n{dom.toprettyxml()}")
        super().__init__(text=text, content_type='text/xml', **kwargs)


class ClientError(ServiceError, web.HTTPBadRequest):
    pass


class NotFoundError(ServiceError, web.HTTPNotFound):
    pass


class InvalidActionError(ClientError):

    code = "InvalidAction"


class InvalidMethodError(ServiceError, web.HTTPMethodNotAllowed):

    code = "InvalidAction"


class InvalidParameterError(ClientError):

    code = "InvalidParameterValue"


class IncorrectStateError(ClientError):

    code = "IncorrectState"


class ServerError(ServiceError, web.HTTPInternalServerError):
    pass


class InternalServerError(ServerError):

    code = "InternalError"


class SparseList(list):

    def __setitem__(self, index, value):
        gap = index - len(self) + 1
        if gap > 0:
            self.extend([None] * gap)
        super().__setitem__(index, value)


def handler(
    action: str,
    methods: Iterable[str] = frozenset({'GET', 'POST'}),
) -> Callable[[_HandlerType], _HandlerType]:
    def inner(handler: _HandlerType) -> _HandlerType:
        global _handlers
        for method in methods:
            _handlers[action, method] = handler
        return handler

    return inner


async def handle_request(request: web.Request) -> web.StreamResponse:
    data: Union[
        multidict.MultiDictProxy[str],
        multidict.MultiDictProxy[Union[str, bytes, web.FileField]],
    ]

    if request.method == 'POST':
        data = await request.post()
    elif request.method == 'GET':
        data = request.query
    else:
        raise InvalidMethodError(
            f"Method Not Allowed: {request.method}",
            method=request.method,
            allowed_methods=['GET', 'POST'],
        )

    action = data.get("Action")

    if not isinstance(action, str):
        raise InvalidActionError(f"Invalid Action: {action!r}")

    handler = _handlers.get((action, request.method))
    if handler is None:
        raise InvalidActionError(
            f"The action {action} is not valid for this web service."
        )
    else:
        args: Dict[str, Any] = {}

        for k, v in data.items():
            if not isinstance(v, str):
                raise InvalidParameterError(
                    f"Value {v!r} for parameter {k} is invalid: "
                    f"must be a string."
                )

            path = k.split('.')
            path_len = len(path)
            if path_len == 1:
                args[k] = v
            else:
                i = 0
                ptr = args
                while i < path_len:
                    subkey = path[i]
                    if subkey.isnumeric():
                        subkey = int(subkey) - 1
                    i += 1
                    try:
                        ptr = ptr[subkey]
                    except (KeyError, IndexError):
                        if i == path_len:
                            ptr[subkey] = v
                            break
                        elif path[i].isnumeric():
                            ptr[subkey] = SparseList()
                        else:
                            ptr[subkey] = {}

                        ptr = ptr[subkey]

        try:
            result = await handler(args, request.app)
            result["RequestID"] = str(uuid.uuid4())
            version = args.get("Version")
            text = format_xml_response(
                result,
                root=f"{action}Response",
                xmlns=(
                    f"http://ec2.amazonaws.com/doc/{version}/"
                    if version else None
                ),
            )
            dom = minidom.parseString(text)
            aiohttp.log.access_logger.debug(
                f"Response:\n---------\n{dom.toprettyxml()}")
            return web.Response(text=text, content_type="text/xml")
        except ServiceError:
            raise
        except Exception:
            raise InternalServerError("\n" + traceback.format_exc()) from None
