from __future__ import annotations
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Generic,
    List,
    Literal,
    Mapping,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
)

import functools
import traceback
import uuid
from xml.dom import minidom

from aiohttp import web
import aiohttp.log
import dicttoxml
import multidict
import libvirt


App = web.Application
routes = web.RouteTableDef()


def _format_condensed_list(parent: str) -> str:
    return parent[:-1]


def _format_expanded_list(parent: str) -> str:
    return "item"


def format_xml_response(
    data: Mapping[str, Any],
    root: Optional[str] = None,
    xmlns: Optional[str] = None,
    list_format: Literal["condensed", "expanded"] = "expanded",
) -> str:
    bbody = dicttoxml.dicttoxml(
        data,
        root=False,
        attr_type=False,
        item_func=(
            _format_condensed_list
            if list_format == "condensed"
            else _format_expanded_list
        ),
    )
    body = bbody.decode("utf-8")
    if root is not None:
        if xmlns is not None:
            rootxml = f'<{root} xmlns="{xmlns}">'
        else:
            rootxml = f"<{root}>"
        body = f"{rootxml}\n{body}\n</{root}>"
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
        if status_code is not None:
            self.status_code = status_code
        self.msg = msg
        super().__init__(content_type="text/xml", **kwargs)


def format_ec2_error_xml(err: ServiceError) -> str:
    return format_xml_response(
        {
            "Response": {
                "RequestID": str(uuid.uuid4()),
                "Errors": {
                    "Error": {
                        "Code": err.code,
                        "Message": err.msg,
                        "Type": "Sender",
                    },
                },
            },
        },
    )


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


T = TypeVar("T")


class SparseList(Generic[T], List[Optional[T]]):
    def __setitem__(self, index: int, value: T) -> None:  # type: ignore
        gap = index - len(self) + 1
        if gap > 0:
            self.extend([None] * gap)
        super().__setitem__(index, value)


HandlerArgs = Dict[str, Any]

_HandlerType = Callable[
    [HandlerArgs, libvirt.virStoragePool],
    Awaitable[Dict[str, Any]],
]


class _HandlerData(NamedTuple):
    handler: _HandlerType
    xmlns: Optional[str]
    list_format: Literal["condensed", "expanded"]
    error_formatter: Callable[[ServiceError], str]
    include_request_id: bool
    protocol: str


_handlers: Dict[Tuple[str, str], _HandlerData] = {}
_path_handlers: Set[Tuple[str, str]] = set()


def handler(
    action: str,
    *,
    methods: str | Tuple[str, ...] = ("GET", "POST"),
    path: str = "/",
    xmlns: Optional[str] = None,
    list_format: Literal["condensed", "expanded"] = "expanded",
    error_formatter: Callable[[ServiceError], str] = format_ec2_error_xml,
    protocol: str = "ec2",
) -> Callable[[_HandlerType], _HandlerType]:
    if isinstance(methods, str):
        methods = (methods,)

    def inner(handler: _HandlerType) -> _HandlerType:
        global _handlers
        for method in methods:
            key = (action, method)
            if key not in _handlers:
                _handlers[key] = _HandlerData(
                    handler=handler,
                    xmlns=xmlns,
                    list_format=list_format,
                    error_formatter=error_formatter,
                    include_request_id=True,
                    protocol=protocol,
                )
                if (method, path) not in _path_handlers:
                    routes.route(method, path)(handle_request)
                    _path_handlers.add((method, path))

            else:
                raise AssertionError(f"{method} {action} is already handled")
        return handler

    return inner


def direct_handler(
    action: str,
    *,
    methods: str | Tuple[str, ...] = ("GET", "POST"),
    path: str = "/",
    xmlns: Optional[str] = None,
    list_format: Literal["condensed", "expanded"] = "expanded",
    error_formatter: Callable[[ServiceError], str] = format_ec2_error_xml,
    protocol: str = "ec2",
) -> Callable[[_HandlerType], _HandlerType]:

    if isinstance(methods, str):
        methods = (methods,)

    def inner(handler: _HandlerType) -> _HandlerType:
        global _handlers
        for method in methods:
            key = (action, method)
            if key not in _handlers:
                _handlers[key] = _HandlerData(
                    handler=handler,
                    xmlns=xmlns,
                    list_format=list_format,
                    error_formatter=error_formatter,
                    include_request_id=False,
                    protocol=protocol,
                )
                if (method, path) not in _path_handlers:
                    routes.route(method, path)(
                        functools.partial(
                            handle_request,
                            action=action,
                        )
                    )
                    _path_handlers.add((method, path))
                else:
                    raise AssertionError(f"{method} {path} is already handled")
            else:
                raise AssertionError(f"{method} {action} is already handled")
        return handler

    return inner


Args = Union[
    multidict.MultiDictProxy[str],
    multidict.MultiDictProxy[Union[str, bytes, web.FileField]],
]


def parse_args(data: Args) -> HandlerArgs:
    args: HandlerArgs = {}

    for k, v in data.items():
        if not isinstance(v, str):
            raise InvalidParameterError(
                f"Value {v!r} for parameter {k} is invalid: "
                f"must be a string."
            )

        path = k.split(".")
        path_len = len(path)
        if path_len == 1:
            args[k] = v
        else:
            i = 0
            ptr: Any = args
            while i < path_len:
                subkey: str | int
                if path[i].isnumeric():
                    subkey = int(path[i]) - 1
                else:
                    subkey = path[i]
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

    return args


async def handle_request(
    request: web.Request,
    *,
    action: Optional[str] = None,
) -> web.StreamResponse:
    if action is None:
        action = request.headers.get("X-Amz-Target", None)

    if action is None:
        action_arg = (await request.post()).get("Action")
        if not isinstance(action_arg, str):
            raise InvalidActionError(f"Invalid Action: {action_arg!r}")
        action = action_arg

    handler_data = _handlers.get((action, request.method))
    if handler_data is None:
        raise InvalidActionError(
            f"The action {action} is not valid for this web service."
        )

    try:
        fn = _protocol_handers[handler_data.protocol]
        return await fn(request, action, handler_data)
    except ServiceError as e:
        e.text = handler_data.error_formatter(e)
        dom = minidom.parseString(e.text)
        aiohttp.log.access_logger.debug(
            f"Error Response:\n\n{dom.toprettyxml()}"
        )
        raise e
    except Exception:
        exc = InternalServerError("\n" + traceback.format_exc())
        exc.text = handler_data.error_formatter(exc)
        dom = minidom.parseString(exc.text)
        aiohttp.log.access_logger.debug(
            f"Error Response:\n\n{dom.toprettyxml()}"
        )
        raise exc from None


async def handle_ec2_request(
    request: web.Request,
    action: str,
    handler_data: _HandlerData,
) -> web.StreamResponse:
    data: Args

    if request.method == "POST":
        data = await request.post()
        body = await request.text()
    elif request.method in {"GET", "DELETE"}:
        data = request.query
        body = ""
    else:
        raise InvalidMethodError(
            f"Method Not Allowed: {request.method}",
            method=request.method,
            allowed_methods=["GET", "POST"],
        )

    args = dict(request.match_info)
    args["BodyText"] = body
    args.update(parse_args(data))

    xmlns = handler_data.xmlns

    if xmlns is None:
        version = args.get("Version")
        if version is not None:
            xmlns = f"http://ec2.amazonaws.com/doc/{version}/"

    result = await handler_data.handler(args, request.app)

    if handler_data.include_request_id:
        result["RequestID"] = str(uuid.uuid4())

    text = format_xml_response(
        result,
        root=f"{action}Response",
        xmlns=xmlns,
        list_format=handler_data.list_format,
    )
    dom = minidom.parseString(text)
    aiohttp.log.access_logger.debug(
        f"Response:\n---------\n{dom.toprettyxml()}"
    )
    return web.Response(text=text, content_type="text/xml")


async def handle_json_request(
    request: web.Request,
    action: str,
    handler_data: _HandlerData,
    ) -> web.StreamResponse:
    assert request.headers.getone("Content-Type") == "application/x-amz-json-1.1"
    args = await request.json()
    result = await handler_data.handler(args, request.app)
    return web.json_response(result, content_type="application/x-amz-json-1.1")


_protocol_handers = {
    "ec2": handle_ec2_request,
    "json": handle_json_request,
}
