from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
)
import collections
import re
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
        cleaned_data: Dict[str, Union[str, Tuple[str, ...]]] = {}
        lists: Dict[str, List[Tuple[int, str]]] = collections.defaultdict(list)

        for k, v in data.items():
            if not isinstance(v, str):
                raise InvalidParameterError(
                    f"Value {v!r} for parameter {k} is invalid: "
                    f"must be a string."
                )

            if m := re.match(r"^(.*)\.(\d+)$", k):
                listname = m.group(1)
                index = int(m.group(2))
                lists[listname].append((index, v))
            else:
                cleaned_data[k] = v

        for k, l in lists.items():
            l.sort(key=lambda v: v[0])
            cleaned_data[k] = tuple(v[1] for v in l)

        try:
            result = await handler(
                cleaned_data,
                request.app['libvirt_pool'],
            )
            result["RequestID"] = str(uuid.uuid4())
            version = cleaned_data.get("Version")
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
